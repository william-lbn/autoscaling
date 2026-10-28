package owner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"

	coordination "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 仅本机HTTP/RFC6902和Lease UID/RV语义夹具，不是apiserver/etcd、真实RBAC或
// VM许可。选主循环使用锁定版本的生产client-go，journal走真实生成clientset。
type ownerAPI struct {
	mu                sync.Mutex
	vm                *vmv1.VirtualMachine
	lease             *coordination.Lease
	vmVersion         int
	leaseVersion      int
	denyLeaseWrites   bool
	denyLeaseHolder   string
	loseJournalReply  bool
	loseLeaseReply    bool
	calls             int
	config            *rest.Config
	blockVMGet        bool
	blockVMGetStarted chan<- struct{}
}

func binding() journal.Binding {
	return journal.Binding{Namespace: "testing", Name: "vm", VMUID: "vm-uid", Incarnation: "1234567890abcdef1234567890abcdef"}
}

func status(w http.ResponseWriter, code int) {
	w.WriteHeader(code)
	reason := metav1.StatusReasonUnknown
	switch code {
	case http.StatusNotFound:
		reason = metav1.StatusReasonNotFound
	case http.StatusConflict:
		reason = metav1.StatusReasonConflict
	}
	var metadata metav1.ListMeta
	_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, ListMeta: metadata, Status: metav1.StatusFailure, Message: "owner API test failure", Reason: reason, Details: nil, Code: int32(code)})
}

func (api *ownerAPI) serve(w http.ResponseWriter, r *http.Request) {
	// 不持fixture锁阻塞VM读，让Lease续约仍正常，避免靠失租掩盖检查自身没期限。
	api.mu.Lock()
	blocked := api.blockVMGet && r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/apis/vm.neon.tech/")
	blockedStarted := api.blockVMGetStarted
	api.mu.Unlock()
	if blocked {
		// 管理入口取消组合用此只读通知确定阻塞已进入真实HTTP服务；旧
		// 测试不配置通道，原语义不变，不靠固定sleep猜请求已经执行。
		select {
		case blockedStarted <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		return
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	api.calls++
	w.Header().Set("Content-Type", "application/json")
	leasePath := "/apis/coordination.k8s.io/v1/namespaces/testing/leases"
	if r.URL.Path == leasePath || r.URL.Path == leasePath+"/"+LeaseName(binding()) {
		switch r.Method {
		case http.MethodGet:
			if api.lease == nil {
				status(w, http.StatusNotFound)
				return
			}
		case http.MethodPost, http.MethodPut:
			if api.denyLeaseWrites {
				status(w, http.StatusServiceUnavailable)
				return
			}
			var next coordination.Lease
			if json.NewDecoder(r.Body).Decode(&next) != nil {
				status(w, http.StatusBadRequest)
				return
			}
			// 仅让指定旧进程的真实续约请求失败；竞争者仍使用同一
			// UID/RV API。不能全局停写后手工换holder来模拟接管。
			if api.denyLeaseHolder != "" && next.Spec.HolderIdentity != nil && *next.Spec.HolderIdentity == api.denyLeaseHolder {
				status(w, http.StatusServiceUnavailable)
				return
			}
			if r.Method == http.MethodPost {
				if api.lease != nil {
					status(w, http.StatusConflict)
					return
				}
				next.UID = "lease-uid-1"
			} else if api.lease == nil || next.UID != api.lease.UID || next.ResourceVersion != api.lease.ResourceVersion {
				status(w, http.StatusConflict)
				return
			}
			api.leaseVersion++
			next.ResourceVersion = "lease-rv-" + strconv.Itoa(api.leaseVersion)
			api.lease = &next
			if api.loseLeaseReply {
				api.loseLeaseReply = false
				status(w, http.StatusServiceUnavailable)
				return
			}
		default:
			status(w, http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(w).Encode(api.lease)
		return
	}
	if r.URL.Path != "/apis/vm.neon.tech/v1/namespaces/testing/virtualmachines/vm" {
		status(w, http.StatusNotFound)
		return
	}
	if r.Method == http.MethodPatch {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			status(w, http.StatusBadRequest)
			return
		}
		operations, err := jsonpatch.DecodePatch(body)
		if err != nil {
			status(w, http.StatusBadRequest)
			return
		}
		previous, _ := json.Marshal(api.vm)
		updated, err := operations.Apply(previous)
		if err != nil {
			status(w, http.StatusUnprocessableEntity)
			return
		}
		var next vmv1.VirtualMachine
		if json.Unmarshal(updated, &next) != nil {
			status(w, http.StatusBadRequest)
			return
		}
		api.vmVersion++
		next.ResourceVersion = "vm-rv-" + strconv.Itoa(api.vmVersion)
		api.vm = &next
		if api.loseJournalReply {
			api.loseJournalReply = false
			status(w, http.StatusServiceUnavailable)
			return
		}
	} else if r.Method != http.MethodGet {
		status(w, http.StatusMethodNotAllowed)
		return
	}
	_ = json.NewEncoder(w).Encode(api.vm)
}

func newAPI(t *testing.T) (*ownerAPI, *journal.Store, coordclient.CoordinationV1Interface) {
	t.Helper()
	var vmStatus vmv1.VirtualMachineStatus
	api := &ownerAPI{mu: sync.Mutex{}, vm: &vmv1.VirtualMachine{TypeMeta: metav1.TypeMeta{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine"}, ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "vm", UID: binding().VMUID, ResourceVersion: "vm-rv-1"}, Spec: vmv1.VirtualMachineSpec{}, Status: vmStatus}, lease: nil, vmVersion: 1, leaseVersion: 0, denyLeaseWrites: false, denyLeaseHolder: "", loseJournalReply: false, loseLeaseReply: false, calls: 0, config: nil, blockVMGet: false}
	api.vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: binding().Incarnation, ComputeInstanceID: "compute-1", VMUID: "", ConfigSecretUID: "", Ready: false}
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	t.Cleanup(server.Close)
	config := new(rest.Config)
	config.Host = server.URL
	config.QPS = 10000
	config.Burst = 10000
	config.Timeout = time.Second
	api.config = config
	client, err := vmclient.NewForConfig(config)
	require.NoError(t, err)
	store, err := journal.NewStore(client.NeonvmV1().VirtualMachines("testing"), binding())
	require.NoError(t, err)
	initial, err := store.Initialize(context.Background(), "vm-rv-1", "bootstrap-process")
	require.NoError(t, err)
	_, err = store.ReleaseBootstrap(context.Background(), initial.ResourceVersion(), "config-uid")
	require.NoError(t, err)
	api.mu.Lock()
	api.vm.Status.Phase = vmv1.VmRunning
	api.vm.Status.PodName = "runner"
	api.vm.Status.PodIP = "127.0.0.1"
	api.vmVersion++
	api.vm.ResourceVersion = "vm-rv-" + strconv.Itoa(api.vmVersion)
	api.mu.Unlock()
	coord, err := coordclient.NewForConfig(config)
	require.NoError(t, err)
	return api, store, coord
}

func unknownOperation(t *testing.T, store *journal.Store) control.Request {
	t.Helper()
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: string(binding().VMUID), ComputeInstanceID: "compute-1", PGBootID: 11}, IssuerEpoch: snapshot.State().Owner.Epoch, Revision: 1, OperationID: control.OperationID{High: 1, Low: 2}}, Kind: control.Grow, ExpectedCapacityGeneration: 1, TargetBuffers: 512, TargetVMMemoryBytes: 1 << 30, BudgetCommitmentID: "original-budget", DeadlineUnixMillis: control.Uint64(math.MaxUint64)}
	snapshot, disposition, err := store.Begin(context.Background(), snapshot, snapshot.State().Owner, request, 0)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	_, _, err = store.Start(context.Background(), snapshot, snapshot.State().Owner, request.Key, journal.PrepareBudget, "budget-instance", []byte(`{"test_only_unknown_external_command":true}`))
	require.NoError(t, err)
	return request
}

func testOptions() Options {
	return Options{LeaseDuration: time.Second, RenewDeadline: 400 * time.Millisecond, RetryPeriod: 40 * time.Millisecond, PodUID: ""}
}

func TestConfirmedLeaseAndJournalClaimPreserveUnknownIntentOnLostReply(t *testing.T) {
	api, store, client := newAPI(t)
	original := unknownOperation(t, store)
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	elector, err := New(client, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var acquired *Session
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		acquired = session
		require.NoError(t, session.Check(ctx))
		require.EqualValues(t, 2, session.Actor().Epoch)
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, original, snapshot.State().Active.Request)
		require.Equal(t, journal.Unconfirmed, snapshot.State().Active.Steps[0].Phase)
		_, _, err = store.Start(ctx, snapshot, session.Actor(), original.Key, journal.PrepareBudget, "budget-instance", []byte(`{"test_only":true}`))
		require.Error(t, err, "takeover never grants old operation redispatch")
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, acquired)
	require.ErrorIs(t, acquired.Check(context.Background()), ErrLeadershipLost)
	_, err = store.Load(context.Background())
	require.NoError(t, err)
	require.Error(t, elector.Run(ctx, func(context.Context, *Session) error { return nil }), "single-use elector cannot reuse an old epoch")
	api.mu.Lock()
	defer api.mu.Unlock()
	require.NotNil(t, api.lease)
	require.Equal(t, elector.holder, *api.lease.Spec.HolderIdentity, "cancel must not prematurely release the lease")
}

func TestRenewalLossCancelsAndJoinsRegisteredWorker(t *testing.T) {
	api, store, client := newAPI(t)
	original := unknownOperation(t, store)
	elector, err := New(client, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ready := make(chan *Session, 1)
	cancelled := make(chan struct{})
	release := make(chan struct{})
	finish := make(chan error, 1)
	stop := sync.OnceFunc(func() { close(release) })
	defer stop()
	go func() {
		finish <- elector.Run(ctx, func(ctx context.Context, session *Session) error {
			ready <- session
			<-ctx.Done()
			close(cancelled)
			<-release
			return nil
		})
	}()
	var session *Session
	select {
	case session = <-ready:
	case <-ctx.Done():
		t.Fatal("owner did not start")
	}
	api.mu.Lock()
	api.denyLeaseWrites = true
	api.mu.Unlock()
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("lost renewal did not cancel worker")
	}
	require.ErrorIs(t, session.Check(context.Background()), ErrLeadershipLost)
	select {
	case err := <-finish:
		t.Fatalf("Run returned before actual worker exit: %v", err)
	case <-time.After(120 * time.Millisecond):
	}
	stop()
	select {
	case err := <-finish:
		require.ErrorIs(t, err, ErrLeadershipLost)
	case <-ctx.Done():
		t.Fatal("owner failed to join completed worker")
	}
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, original, snapshot.State().Active.Request)
	require.Equal(t, journal.Unconfirmed, snapshot.State().Active.Steps[0].Phase)
}

func TestContendingElectorsTransferMonotonicEpochWithoutReset(t *testing.T) {
	_, store, client := newAPI(t)
	original := unknownOperation(t, store)
	a, err := New(client, store, testOptions())
	require.NoError(t, err)
	b, err := New(client, store, testOptions())
	require.NoError(t, err)
	require.NotEqual(t, a.holder, b.holder)
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	ctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	ctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	readyA, readyB := make(chan *Session, 1), make(chan *Session, 1)
	doneA, doneB := make(chan error, 1), make(chan error, 1)
	go func() {
		doneA <- a.Run(ctxA, func(ctx context.Context, s *Session) error { readyA <- s; <-ctx.Done(); return nil })
	}()
	var sessionA *Session
	select {
	case sessionA = <-readyA:
	case <-ctx.Done():
		t.Fatal("first owner failed")
	}
	go func() {
		doneB <- b.Run(ctxB, func(ctx context.Context, s *Session) error { readyB <- s; <-ctx.Done(); return nil })
	}()
	select {
	case <-readyB:
		t.Fatal("contender bypassed a renewing leader")
	case <-time.After(200 * time.Millisecond):
	}
	cancelA()
	select {
	case err := <-doneA:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("first owner did not stop")
	}
	var sessionB *Session
	select {
	case sessionB = <-readyB:
	case <-ctx.Done():
		t.Fatal("successor did not acquire after expiry")
	}
	require.EqualValues(t, 2, sessionA.Actor().Epoch)
	require.EqualValues(t, 3, sessionB.Actor().Epoch)
	require.ErrorIs(t, sessionA.Check(ctx), ErrLeadershipLost)
	require.NoError(t, sessionB.Check(ctx))
	snapshot, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, original, snapshot.State().Active.Request)
	cancelB()
	select {
	case err := <-doneB:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("successor did not stop")
	}
}

func newLock(t *testing.T, api *ownerAPI) (*boundLease, resourcelock.LeaderElectionRecord) {
	t.Helper()
	client, err := coordclient.NewForConfig(api.config)
	require.NoError(t, err)
	lock := &boundLease{client: client.Leases("testing"), binding: binding(), holder: "test-holder", name: LeaseName(binding()), lease: nil, mu: sync.Mutex{}, uid: ""}
	now := metav1.Now()
	record := resourcelock.LeaderElectionRecord{HolderIdentity: lock.holder, LeaseDurationSeconds: 1, AcquireTime: now, RenewTime: now, LeaderTransitions: 0, Strategy: "", PreferredHolder: ""}
	require.NoError(t, lock.Create(context.Background(), record))
	return lock, record
}

func TestLeaseRejectsForeignMissingMalformedOrRecreatedIdentity(t *testing.T) {
	for _, mode := range []string{"vm-uid", "namespace", "marker", "owner", "uid-missing", "lease-recreated", "rv-missing", "holder-missing", "duration-zero", "duration-missing", "transition-negative", "renew-missing", "deleting"} {
		t.Run(mode, func(t *testing.T) {
			api, _, _ := newAPI(t)
			lock, _ := newLock(t, api)
			uid := lock.fixedUID()
			api.mu.Lock()
			lease := api.lease
			switch mode {
			case "vm-uid":
				lease.OwnerReferences[0].UID = "different-vm"
			case "namespace":
				lease.Namespace = "other"
			case "marker":
				delete(lease.Annotations, leaseBindingAnnotation)
			case "owner":
				lease.OwnerReferences = nil
			case "uid-missing":
				lease.UID = ""
			case "lease-recreated":
				lease.UID = "replacement-lease"
			case "rv-missing":
				lease.ResourceVersion = ""
			case "holder-missing":
				lease.Spec.HolderIdentity = nil
			case "duration-zero":
				*lease.Spec.LeaseDurationSeconds = 0
			case "duration-missing":
				lease.Spec.LeaseDurationSeconds = nil
			case "transition-negative":
				*lease.Spec.LeaseTransitions = -1
			case "renew-missing":
				lease.Spec.RenewTime = nil
			case "deleting":
				now := metav1.Now()
				lease.DeletionTimestamp = &now
			}
			api.mu.Unlock()
			_, _, err := lock.Get(context.Background())
			require.Error(t, err)
			require.Equal(t, uid, lock.fixedUID())
		})
	}
}

func TestLeaseLostUpdateReplyRequiresReadbackAndRejectsCounterWrap(t *testing.T) {
	api, _, _ := newAPI(t)
	lock, record := newLock(t, api)
	originalVersion := lock.lease.ResourceVersion
	record.RenewTime = metav1.NewTime(record.RenewTime.Add(time.Second))
	api.mu.Lock()
	api.loseLeaseReply = true
	api.mu.Unlock()
	require.Error(t, lock.Update(context.Background(), record))
	require.Equal(t, originalVersion, lock.lease.ResourceVersion)
	read, _, err := lock.Get(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, originalVersion, lock.lease.ResourceVersion)
	require.Equal(t, record.HolderIdentity, read.HolderIdentity)
	record.LeaderTransitions = math.MaxInt32
	require.NoError(t, lock.Update(context.Background(), record))
	record.LeaderTransitions++
	require.Error(t, lock.Update(context.Background(), record))
	api.mu.Lock()
	require.EqualValues(t, math.MaxInt32, *api.lease.Spec.LeaseTransitions, "overflow rejection must happen before the persisted lease is changed")
	api.mu.Unlock()
}

func TestWorkerFailurePanicAndPrecancelNeverResetJournal(t *testing.T) {
	for _, mode := range []string{"error", "panic", "precancel"} {
		t.Run(mode, func(t *testing.T) {
			api, store, client := newAPI(t)
			original := unknownOperation(t, store)
			elector, err := New(client, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			if mode == "precancel" {
				cancel()
			}
			canary := errors.New("private-credential-canary")
			called := false
			err = elector.Run(ctx, func(context.Context, *Session) error {
				called = true
				if mode == "panic" {
					panic(canary)
				}
				return canary
			})
			switch mode {
			case "error":
				require.ErrorIs(t, err, canary)
				require.NotContains(t, err.Error(), canary.Error())
			case "panic":
				require.ErrorIs(t, err, ErrWorkerPanic)
			case "precancel":
				require.ErrorIs(t, err, context.Canceled)
				require.False(t, called)
				api.mu.Lock()
				require.Nil(t, api.lease)
				api.mu.Unlock()
			}
			snapshot, err := store.Load(context.Background())
			require.NoError(t, err)
			require.Equal(t, original, snapshot.State().Active.Request)
		})
	}
}

func TestOptionsAndLeaseNameBoundFullInstance(t *testing.T) {
	_, store, client := newAPI(t)
	defaults := DefaultOptions()
	require.Greater(t, defaults.LeaseDuration, defaults.RenewDeadline)
	for _, opts := range []Options{{LeaseDuration: time.Millisecond, RenewDeadline: time.Millisecond, RetryPeriod: time.Millisecond, PodUID: ""}, {LeaseDuration: time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: time.Millisecond, PodUID: ""}, {LeaseDuration: time.Second, RenewDeadline: 400 * time.Millisecond, RetryPeriod: time.Hour, PodUID: ""}} {
		_, err := New(client, store, opts)
		require.Error(t, err)
	}
	base := binding()
	name := LeaseName(base)
	require.Len(t, name, 61)
	require.True(t, strings.HasPrefix(name, "dsga-"))
	base.VMUID = types.UID("same-name-new-vm")
	require.NotEqual(t, name, LeaseName(base))
	base = binding()
	base.Incarnation = "1234567890abcdef1234567890abcdee"
	require.NotEqual(t, name, LeaseName(base))
}

func TestSessionRejectsAChangedJournalOwnerEvenBeforeLeaseRenewalNotices(t *testing.T) {
	_, store, client := newAPI(t)
	elector, err := New(client, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		_, err = store.Takeover(ctx, snapshot, session.Actor(), "another-writer")
		require.NoError(t, err)
		require.ErrorIs(t, session.Check(ctx), ErrLeadershipLost)
		return nil
	})
	require.NoError(t, err)
}

func TestJournalReadTimeoutIsBoundedWhileLeaseRenewalStaysHealthy(t *testing.T) {
	api, store, client := newAPI(t)
	elector, err := New(client, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		api.mu.Lock()
		api.blockVMGet = true
		api.mu.Unlock()
		defer func() { api.mu.Lock(); api.blockVMGet = false; api.mu.Unlock() }()
		started := time.Now()
		err := session.Check(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, time.Since(started), 2*testOptions().RenewDeadline, "healthy renewal cannot permit a hung journal read to block indefinitely")
		lease, err := client.Leases("testing").Get(ctx, LeaseName(binding()), metav1.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, session.Actor().HolderID, *lease.Spec.HolderIdentity)
		require.NoError(t, session.Context().Err(), "call timeout must not expire the long-lived session")
		return nil
	})
	require.NoError(t, err)
}
