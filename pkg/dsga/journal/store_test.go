package journal

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// HTTP服务只模拟VM对象和真实RFC6902条件应用，不伪装成apiserver/etcd认证。
// Store必须通过仓库生成的clientset走真正网络/序列化，不能让fake client跳过test。
type storeAPI struct {
	mu              sync.Mutex
	vm              *vmv1.VirtualMachine
	version         int
	patches         int
	failAfterCommit bool
	corruptResponse bool
	beforePatch     func()
	server          *httptest.Server
}

func testBinding() Binding {
	return Binding{Namespace: "testing", Name: "vm-name", VMUID: "vm-uid", Incarnation: testIncarnation}
}

func newStoreAPI(t *testing.T) (*storeAPI, *Store) {
	t.Helper()
	var status vmv1.VirtualMachineStatus
	status.Phase = vmv1.VmPending
	spec := new(vmv1.VirtualMachineSpec)
	spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: testIncarnation, ComputeInstanceID: "compute-1", VMUID: "", ConfigSecretUID: "", Ready: false}
	f := &storeAPI{
		mu: sync.Mutex{}, vm: &vmv1.VirtualMachine{
			TypeMeta:   metav1.TypeMeta{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "vm-name", UID: "vm-uid", ResourceVersion: "opaque-1", Annotations: map[string]string{"keep": "unchanged"}}, Spec: *spec, Status: status,
		},
		version: 1, patches: 0, failAfterCommit: false, corruptResponse: false, beforePatch: nil, server: nil,
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	store := f.client(t)
	return f, store
}

func (f *storeAPI) client(t *testing.T) *Store {
	t.Helper()
	config := new(rest.Config)
	config.Host = f.server.URL
	client, err := vmclient.NewForConfig(config)
	require.NoError(t, err)
	store, err := NewStore(client.NeonvmV1().VirtualMachines("testing"), testBinding())
	require.NoError(t, err)
	return store
}

func (f *storeAPI) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path != "/apis/vm.neon.tech/v1/namespaces/testing/virtualmachines/vm-name" {
		http.Error(w, "unexpected resource", 404)
		return
	}
	if r.Method == http.MethodPatch {
		f.mu.Lock()
		callback := f.beforePatch
		f.beforePatch = nil
		f.mu.Unlock()
		if callback != nil {
			callback()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == http.MethodPatch {
		f.patches++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read error", 400)
			return
		}
		if r.Header.Get("Content-Type") != "application/json-patch+json" {
			http.Error(w, "patch type", 400)
			return
		}
		operations, err := jsonpatch.DecodePatch(body)
		if err != nil {
			http.Error(w, "invalid patch", 400)
			return
		}
		original, err := json.Marshal(f.vm)
		if err != nil {
			http.Error(w, "fixture error", 500)
			return
		}
		updated, err := operations.Apply(original)
		if err != nil {
			w.WriteHeader(422)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "Invalid", "message": "JSON test failed", "code": 422})
			return
		}
		var vm vmv1.VirtualMachine
		if err := json.Unmarshal(updated, &vm); err != nil {
			http.Error(w, "fixture error", 500)
			return
		}
		f.version++
		vm.ResourceVersion = "opaque-" + strconv.Itoa(f.version)
		f.vm = &vm
		if f.failAfterCommit {
			http.Error(w, "lost reply after commit", 500)
			return
		}
	} else if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	response := f.vm.DeepCopy()
	if r.Method == http.MethodPatch && f.corruptResponse {
		response.Annotations[control.JournalAnnotation] = "corrupt"
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (f *storeAPI) change(change func(*vmv1.VirtualMachine)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change(f.vm)
	f.version++
	f.vm.ResourceVersion = "opaque-" + strconv.Itoa(f.version)
}

func initializeStore(t *testing.T, f *storeAPI, store *Store) *Snapshot {
	t.Helper()
	initial, err := store.Initialize(context.Background(), "opaque-1", "process-a")
	require.NoError(t, err)
	_, err = store.ReleaseBootstrap(context.Background(), initial.ResourceVersion(), "config-uid")
	require.NoError(t, err)
	f.change(func(vm *vmv1.VirtualMachine) {
		vm.Status.Phase = vmv1.VmRunning
		vm.Status.PodName = "runner"
		vm.Status.PodIP = "10.1.2.3"
	})
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	return snapshot
}

func TestStoreExplicitInitializePreservesMetadataAndIsNotLoadFallback(t *testing.T) {
	f, store := newStoreAPI(t)
	_, err := store.Load(context.Background())
	require.Error(t, err)
	_, err = store.Initialize(context.Background(), "wrong-version", "process-a")
	require.Error(t, err)
	snapshot, err := store.Initialize(context.Background(), "opaque-1", "process-a")
	require.NoError(t, err)
	require.EqualValues(t, 1, snapshot.State().Sequence)
	f.mu.Lock()
	require.Equal(t, "unchanged", f.vm.Annotations["keep"])
	f.mu.Unlock()
	_, err = store.Initialize(context.Background(), snapshot.ResourceVersion(), "process-b")
	require.Error(t, err)
	f.change(func(vm *vmv1.VirtualMachine) { delete(vm.Annotations, control.JournalAnnotation) })
	_, err = store.Load(context.Background())
	require.ErrorContains(t, err, "history is missing")
	_, err = store.Initialize(context.Background(), "opaque-3", "process-b")
	require.Error(t, err)
}

func TestStoreInitializationRejectsRunningAndRestartedVMs(t *testing.T) {
	for _, change := range []func(*vmv1.VirtualMachine){
		func(vm *vmv1.VirtualMachine) { vm.Status.Phase = vmv1.VmRunning }, func(vm *vmv1.VirtualMachine) { vm.Status.PodName = "existing-runner" },
		func(vm *vmv1.VirtualMachine) { vm.Status.RestartCount = 1 }, func(vm *vmv1.VirtualMachine) { vm.Annotations[control.ModeAnnotation] = "corrupt" },
	} {
		f, store := newStoreAPI(t)
		f.change(change)
		f.mu.Lock()
		version := f.vm.ResourceVersion
		f.mu.Unlock()
		_, err := store.Initialize(context.Background(), version, "process-a")
		require.Error(t, err)
		f.mu.Lock()
		require.Zero(t, f.patches)
		f.mu.Unlock()
	}
}

func TestStoreLostCommitResponseNeverReturnsDispatchAndRestartKeepsUnknown(t *testing.T) {
	f, store := newStoreAPI(t)
	snapshot := initializeStore(t, f, store)
	request := testRequest(snapshot.State(), 1, control.Shrink)
	snapshot, disposition, err := store.Begin(context.Background(), snapshot, snapshot.State().Owner, request, 0)
	require.NoError(t, err)
	require.Equal(t, Accepted, disposition)
	f.mu.Lock()
	f.failAfterCommit = true
	f.mu.Unlock()
	returned, ticket, err := store.Start(context.Background(), snapshot, snapshot.State().Owner, request.Key, PrepareBudget, "budget-instance", testCommand)
	require.Nil(t, returned)
	require.Nil(t, ticket)
	var unconfirmed *UnconfirmedWrite
	require.ErrorAs(t, err, &unconfirmed)
	// 无派发票据，调用方不得执行外部动作；重建客户端只读到已提交的未知状态，
	// 不能把Go进程内为空当作可以重发或换ID的依据。
	restarted := f.client(t)
	loaded, err := restarted.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, Unconfirmed, loaded.State().Active.Steps[0].Phase)
	_, ticket, err = restarted.Start(context.Background(), loaded, loaded.State().Owner, request.Key, PrepareBudget, "budget-instance", testCommand)
	require.Error(t, err)
	require.Nil(t, ticket)
	_, err = restarted.Finish(context.Background(), loaded, loaded.State().Owner, request.Key)
	require.Error(t, err)
	f.mu.Lock()
	f.failAfterCommit = false
	f.mu.Unlock()
	oldOwner := loaded.State().Owner
	loaded, err = restarted.Takeover(context.Background(), loaded, oldOwner, "process-b")
	require.NoError(t, err)
	require.Equal(t, request, loaded.State().Active.Request)
	_, err = restarted.RequestAbort(context.Background(), loaded, oldOwner, request.Key)
	require.Error(t, err)
	loaded, err = restarted.RequestAbort(context.Background(), loaded, loaded.State().Owner, request.Key)
	require.NoError(t, err)
	_, _, err = restarted.StartCleanup(context.Background(), loaded, loaded.State().Owner, request.Key, "cleanup", testCommand)
	require.Error(t, err)
}

func TestStoreConfirmedDispatchBindsCompleteCommandAndSnapshotCopies(t *testing.T) {
	f, store := newStoreAPI(t)
	snapshot := initializeStore(t, f, store)
	request := testRequest(snapshot.State(), 1, control.Grow)
	snapshot, _, err := store.Begin(context.Background(), snapshot, snapshot.State().Owner, request, 0)
	require.NoError(t, err)
	snapshot, ticket, err := store.Start(context.Background(), snapshot, snapshot.State().Owner, request.Key, PrepareBudget, "budget-instance", testCommand)
	require.NoError(t, err)
	require.NotNil(t, ticket)
	require.Equal(t, snapshot.State().Sequence, ticket.JournalSequence)
	require.Equal(t, string(testCommand), string(ticket.Step.Command))
	ticket.Step.Command[0] = 'x'
	view := snapshot.State()
	view.Active.Request.TargetBuffers++
	require.NoError(t, snapshot.State().Validate())
	fact := observation(snapshot.State(), snapshot.State().Active.Steps[0], Completed)
	snapshot, err = store.Record(context.Background(), snapshot, snapshot.State().Owner, request.Key, fact)
	require.NoError(t, err)
	f.mu.Lock()
	patches := f.patches
	f.mu.Unlock()
	snapshot, err = store.Record(context.Background(), snapshot, snapshot.State().Owner, request.Key, fact)
	require.NoError(t, err)
	f.mu.Lock()
	require.Equal(t, patches, f.patches)
	f.mu.Unlock()
	require.Equal(t, Settled, snapshot.State().Active.Steps[0].Phase)
}

func TestStoreCASRejectsConcurrentOwnerAndRecreatedVM(t *testing.T) {
	f, store := newStoreAPI(t)
	snapshot := initializeStore(t, f, store)
	owner := snapshot.State().Owner
	first, err := store.Takeover(context.Background(), snapshot, owner, "process-b")
	require.NoError(t, err)
	_, err = store.Takeover(context.Background(), snapshot, owner, "process-c")
	var unconfirmed *UnconfirmedWrite
	require.ErrorAs(t, err, &unconfirmed)
	actual, err := store.Load(context.Background())
	require.NoError(t, err)
	require.Equal(t, first.State(), actual.State())
	f.mu.Lock()
	journalBeforeRecreate := f.vm.Annotations[control.JournalAnnotation]
	f.beforePatch = func() { f.mu.Lock(); f.vm.UID = "vm-recreated"; f.mu.Unlock() }
	f.mu.Unlock()
	_, err = store.Takeover(context.Background(), actual, actual.State().Owner, "process-c")
	require.ErrorAs(t, err, &unconfirmed)
	// 返回对象再验UID只能阻止返回票据，无法撤销已覆盖的对象。必须证明API内的
	// UID条件在提交之前拒绝写入，不能用写后发现换对象掩盖同名VM被修改的事实。
	f.mu.Lock()
	require.Equal(t, journalBeforeRecreate, f.vm.Annotations[control.JournalAnnotation])
	f.mu.Unlock()
	_, err = store.Load(context.Background())
	require.Error(t, err)
}

func TestStoreStaleNoopCannotConfirmOldOwnerOrOldResult(t *testing.T) {
	f, store := newStoreAPI(t)
	snapshot := initializeStore(t, f, store)
	request := testRequest(snapshot.State(), 1, control.Grow)
	snapshot, _, err := store.Begin(context.Background(), snapshot, snapshot.State().Owner, request, 0)
	require.NoError(t, err)
	_, err = store.Takeover(context.Background(), snapshot, snapshot.State().Owner, "process-b")
	require.NoError(t, err)
	_, _, err = store.Begin(context.Background(), snapshot, snapshot.State().Owner, request, 0)
	require.ErrorContains(t, err, "stale")
}

func TestStoreCorruptReplyAndOversizedAnnotationsReturnNoDispatch(t *testing.T) {
	f, store := newStoreAPI(t)
	snapshot := initializeStore(t, f, store)
	request := testRequest(snapshot.State(), 1, control.Grow)
	snapshot, _, err := store.Begin(context.Background(), snapshot, snapshot.State().Owner, request, 0)
	require.NoError(t, err)
	f.mu.Lock()
	f.corruptResponse = true
	f.mu.Unlock()
	_, ticket, err := store.Start(context.Background(), snapshot, snapshot.State().Owner, request.Key, PrepareBudget, "budget", testCommand)
	require.Error(t, err)
	require.Nil(t, ticket)
	f.mu.Lock()
	f.corruptResponse = false
	f.mu.Unlock()
	f.change(func(vm *vmv1.VirtualMachine) { vm.Annotations["unrelated"] = string(make([]byte, 256*1024)) })
	loaded, err := store.Load(context.Background())
	require.NoError(t, err)
	_, err = store.RequestAbort(context.Background(), loaded, loaded.State().Owner, request.Key)
	require.Error(t, err)
}
