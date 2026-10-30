package hostguard

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"

	coord "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 真clientset/HTTP/RFC6902和并发CAS；Node/Lease服务是显式apiserver夹具，
// 不认证实际Scheduler配额、Node物理内存、Pod上界或Host源生产者已经接通。
type nodeAPI struct {
	mu          sync.Mutex
	node        core.Node
	pods        map[string]core.Pod
	lease       coord.Lease
	version     int
	patches     int
	loseReply   bool
	beforePatch func()
	config      *rest.Config
}

func nodeBinding() Binding {
	return Binding{NodeName: "node-one", NodeUID: "node-uid", Namespace: "system", LeaseName: "node-one-allocator", LeaseUID: "host-lease-uid"}
}

func newNodeAPI(t *testing.T) (*nodeAPI, *Store) {
	t.Helper()
	b := nodeBinding()
	n := core.Node{ObjectMeta: metav1.ObjectMeta{Name: b.NodeName, UID: b.NodeUID, ResourceVersion: "node-rv-1", Annotations: map[string]string{"keep": "original"}}, Status: core.NodeStatus{Allocatable: core.ResourceList{core.ResourceMemory: resource.MustParse("8Gi")}}}
	controller, holder, transitions, duration := true, "host-controller-one", int32(0), int32(60)
	stamp := metav1.NewMicroTime(time.Now())
	l := coord.Lease{TypeMeta: metav1.TypeMeta{Kind: "Lease", APIVersion: "coordination.k8s.io/v1"}, ObjectMeta: metav1.ObjectMeta{Name: b.LeaseName, Namespace: b.Namespace, UID: b.LeaseUID, ResourceVersion: "lease-rv-1", Annotations: map[string]string{LeaseAnnotation: LeaseMarker(b)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: b.NodeName, UID: b.NodeUID, Controller: &controller, BlockOwnerDeletion: nil}}}, Spec: coord.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, AcquireTime: &stamp, RenewTime: &stamp, LeaseTransitions: &transitions, Strategy: nil, PreferredHolder: nil}}
	f := &nodeAPI{mu: sync.Mutex{}, node: n, pods: make(map[string]core.Pod), lease: l, version: 1, patches: 0, loseReply: false, beforePatch: nil, config: nil}
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	f.config = new(rest.Config)
	f.config.Host = server.URL
	store := f.store(t, holder)
	return f, store
}

func (f *nodeAPI) store(t *testing.T, holder string) *Store {
	t.Helper()
	c, err := kubernetes.NewForConfig(f.config)
	require.NoError(t, err)
	s, err := NewStore(c.CoreV1(), c.CoordinationV1(), nodeBinding(), holder)
	require.NoError(t, err)
	return s
}

func (f *nodeAPI) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
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
	if strings.Contains(r.URL.Path, "/leases/") {
		if r.Method == http.MethodPut {
			var next coord.Lease
			if json.NewDecoder(r.Body).Decode(&next) != nil || next.UID != f.lease.UID || next.ResourceVersion != f.lease.ResourceVersion {
				http.Error(w, "original Lease UID/RV condition failed", http.StatusConflict)
				return
			}
			f.version++
			next.ResourceVersion = "lease-rv-" + strconv.Itoa(f.version)
			f.lease = next
		} else if r.Method != http.MethodGet {
			http.Error(w, "unsupported original Lease method", http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(w).Encode(f.lease)
		return
	}
	// v9容量写入仍必须真实GET同UID的受保护原Pod；与Node共用同一HTTP
	// peer与互斥状态，不用另一个脱离Node CAS的fake视图代替来源。
	if strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/") {
		parts := strings.Split(r.URL.Path, "/")
		if r.Method == http.MethodGet && len(parts) == 7 && parts[5] == "pods" {
			if pod, ok := f.pods[parts[4]+"/"+parts[6]]; ok {
				_ = json.NewEncoder(w).Encode(pod)
				return
			}
		}
		http.Error(w, "original Pod unavailable", http.StatusNotFound)
		return
	}
	if r.URL.Path != "/api/v1/nodes/node-one" {
		http.Error(w, "unexpected Node resource", http.StatusNotFound)
		return
	}
	if r.Method == http.MethodPatch {
		f.patches++
		raw, err := io.ReadAll(r.Body)
		if err != nil || r.Header.Get("Content-Type") != "application/json-patch+json" {
			http.Error(w, "bad patch body", http.StatusBadRequest)
			return
		}
		ops, err := jsonpatch.DecodePatch(raw)
		if err != nil {
			http.Error(w, "bad patch", http.StatusBadRequest)
			return
		}
		original, err := json.Marshal(f.node)
		if err != nil {
			http.Error(w, "bad fixture Node", http.StatusInternalServerError)
			return
		}
		updated, err := ops.Apply(original)
		if err != nil {
			http.Error(w, "UID/RV condition failed", http.StatusConflict)
			return
		}
		var next core.Node
		if json.Unmarshal(updated, &next) != nil {
			http.Error(w, "bad patched Node", http.StatusBadRequest)
			return
		}
		f.version++
		next.ResourceVersion = "node-rv-" + strconv.Itoa(f.version)
		f.node = next
		if f.loseReply {
			f.loseReply = false
			http.Error(w, "fixture reply lost after commit", http.StatusServiceUnavailable)
			return
		}
	} else if r.Method != http.MethodGet {
		http.Error(w, "unsupported fixture method", http.StatusMethodNotAllowed)
		return
	}
	_ = json.NewEncoder(w).Encode(f.node)
}

func plan(vm string) Plan {
	return Plan{Namespace: "testing", VMName: vm, Incarnation: "1234567890abcdef1234567890abcdef", RunnerPodUID: vm + "-pod", Request: control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: vm + "-uid", ComputeInstanceID: vm + "-compute", PGBootID: 1}, IssuerEpoch: 2, Revision: 1, OperationID: control.OperationID{High: 1, Low: 1}}, Kind: control.Grow, ExpectedCapacityGeneration: 1, TargetBuffers: 1024, TargetVMMemoryBytes: 2 << 30, BudgetCommitmentID: "budget-original", DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Hour).UnixMilli())}, BeforeBytes: 1 << 30, HostOverheadBytes: 64 << 20}
}

func initialized(t *testing.T, s *Store) *Snapshot {
	t.Helper()
	v, err := s.Initialize(context.Background(), "node-rv-1", 1<<30)
	require.NoError(t, err)
	return v
}

func TestNodeLedgerOriginalHoldLostRepliesAndReadOnlyRetry(t *testing.T) {
	f, store := newNodeAPI(t)
	ctx := context.Background()
	f.mu.Lock()
	f.loseReply = true
	f.mu.Unlock()
	_, err := store.Initialize(ctx, "node-rv-1", 1<<30)
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, "original", s.node.Annotations["keep"])
	p := plan("vm-one")
	f.mu.Lock()
	f.loseReply = true
	f.mu.Unlock()
	returned, empty, err := store.Reserve(ctx, s, p)
	require.ErrorAs(t, err, &unknown)
	require.Nil(t, returned)
	require.Empty(t, empty.SourceID, "未知写不返回容量票据")
	restored := f.store(t, "host-controller-one")
	s, err = restored.Load(ctx)
	require.NoError(t, err)
	held, err := s.Hold(p)
	require.NoError(t, err)
	before := s.ResourceVersion()
	s, again, err := restored.Reserve(ctx, s, p)
	require.NoError(t, err)
	require.Equal(t, held, again)
	require.Equal(t, before, s.ResourceVersion())
	state := s.State()
	state.Holds[0].SourceID = "caller-mutated"
	require.Equal(t, held, s.model.Holds[0])
	changed := p
	changed.Request.TargetBuffers++
	_, _, err = restored.Reserve(ctx, s, changed)
	require.Error(t, err)
}

func TestNodeLedgerCountsSameVMByPeakAndRetainsOldPod(t *testing.T) {
	_, store := newNodeAPI(t)
	s := initialized(t, store)
	p := plan("vm-one")
	var err error
	s, _, err = store.Reserve(context.Background(), s, p)
	require.NoError(t, err)
	p.Request.Key.Revision++
	p.Request.Key.OperationID.Low++
	p.Request.Kind, p.BeforeBytes, p.Request.TargetVMMemoryBytes = control.Shrink, 2<<30, 1<<30
	s, _, err = store.Reserve(context.Background(), s, p)
	require.NoError(t, err)
	used, err := s.State().UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, (3<<30)+(64<<20), used, "同一物理Guest不按两个承诺重复计费，Shrink仍保before")
	p.Request.Key.Revision++
	p.Request.Key.OperationID.Low++
	p.RunnerPodUID = "new-pod-must-not-hide-old"
	s, _, err = store.Reserve(context.Background(), s, p)
	require.NoError(t, err)
	used, err = s.State().UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, (5<<30)+(128<<20), used)
	var m Model
	raw, err := json.Marshal(s.State())
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &m))
	require.Equal(t, s.State(), m)
}

func TestNodeLedgerConcurrentCASNeverGrantsSameCapacityTwice(t *testing.T) {
	f, store := newNodeAPI(t)
	f.mu.Lock()
	f.node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("4Gi")
	f.mu.Unlock()
	s := initialized(t, store)
	errs := make(chan error, 2)
	for _, vm := range []string{"vm-one", "vm-two"} {
		go func() { _, _, err := store.Reserve(context.Background(), s, plan(vm)); errs <- err }()
	}
	successes := 0
	for range 2 {
		if <-errs == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
	current, err := store.Load(context.Background())
	require.NoError(t, err)
	require.Len(t, current.State().Holds, 1)
	missing := "vm-one"
	if current.State().Holds[0].Plan.VMName == missing {
		missing = "vm-two"
	}
	_, _, err = store.Reserve(context.Background(), current, plan(missing))
	require.ErrorContains(t, err, "capacity")
}

func (f *nodeAPI) changeNode(fn func(*core.Node)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(&f.node)
	f.version++
	f.node.ResourceVersion = "node-rv-" + strconv.Itoa(f.version)
}

func (f *nodeAPI) changeHolder(holder string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lease.Spec.HolderIdentity = &holder
	*f.lease.Spec.LeaseTransitions++
	stamp := metav1.NewMicroTime(time.Now())
	f.lease.Spec.RenewTime = &stamp
}

func TestNodeLedgerTakeoverPreservesHoldAndFencesLateOldPatch(t *testing.T) {
	f, original := newNodeAPI(t)
	ctx := context.Background()
	s := initialized(t, original)
	p := plan("vm-one")
	s, held, err := original.Reserve(ctx, s, p)
	require.NoError(t, err)
	next := f.store(t, "host-controller-two")
	f.mu.Lock()
	f.beforePatch = func() {
		f.changeHolder("host-controller-two")
		current, err := next.Load(ctx)
		require.NoError(t, err)
		_, _, err = next.Reserve(ctx, current, plan("vm-three"))
		require.ErrorContains(t, err, "fenced floor", "接管不能在持久floor确认前授予")
		fenced, err := next.Fence(ctx, current)
		require.NoError(t, err)
		require.Equal(t, held, fenced.State().Holds[0])
	}
	f.mu.Unlock()
	_, _, err = original.Reserve(ctx, s, plan("vm-two"))
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown, "旧owner迟到PATCH受新floor的Node RV屏障拒绝")
	current, err := next.Load(ctx)
	require.NoError(t, err)
	require.Len(t, current.State().Holds, 1)
	require.EqualValues(t, 2, current.State().LeaseFloor.Epoch)
	_, _, err = original.Reserve(ctx, current, plan("vm-old-owner"))
	require.Error(t, err)
	current, _, err = next.Reserve(ctx, current, plan("vm-two"))
	require.NoError(t, err)
	require.Len(t, current.State().Holds, 2)
}

func TestNodeLedgerNeverResetsLostChangedOrExpiredSources(t *testing.T) {
	for _, mode := range []string{"lost-ledger", "lost-origin", "empty-ledger", "lost-protection", "new-node-uid", "new-lease-uid", "expired-lease", "capacity-lowered", "node-deleting"} {
		t.Run(mode, func(t *testing.T) {
			f, store := newNodeAPI(t)
			ctx := context.Background()
			s := initialized(t, store)
			p := plan("vm-one")
			s, held, err := store.Reserve(ctx, s, p)
			require.NoError(t, err)
			switch mode {
			case "lost-ledger":
				f.changeNode(func(n *core.Node) { delete(n.Annotations, LedgerAnnotation) })
			case "lost-origin":
				f.changeNode(func(n *core.Node) { delete(n.Annotations, OriginAnnotation) })
			case "empty-ledger":
				f.changeNode(func(n *core.Node) { n.Annotations[LedgerAnnotation] = "" })
			case "lost-protection":
				f.changeNode(func(n *core.Node) { n.Finalizers = nil })
			case "new-node-uid":
				f.changeNode(func(n *core.Node) { n.UID = "same-name-new-node" })
			case "new-lease-uid":
				f.mu.Lock()
				f.lease.UID = "same-name-new-lease"
				f.mu.Unlock()
			case "expired-lease":
				f.mu.Lock()
				past := metav1.NewMicroTime(time.Now().Add(-time.Hour))
				f.lease.Spec.RenewTime = &past
				f.mu.Unlock()
			case "capacity-lowered":
				f.changeNode(func(n *core.Node) { n.Status.Allocatable[core.ResourceMemory] = resource.MustParse("1Gi") })
			case "node-deleting":
				f.changeNode(func(n *core.Node) { now := metav1.Now(); n.DeletionTimestamp = &now })
			}
			loaded, err := store.Load(ctx)
			if mode == "new-lease-uid" || mode == "expired-lease" || mode == "capacity-lowered" || mode == "node-deleting" {
				require.NoError(t, err, "写资格失效不清或隐藏原持有")
				got, err := loaded.Hold(p)
				require.NoError(t, err)
				require.Equal(t, held, got)
				_, _, err = store.Reserve(ctx, loaded, plan("vm-two"))
				require.Error(t, err)
			} else {
				require.Error(t, err)
				require.NotErrorIs(t, err, ErrUninitialized)
			}
			_, err = store.Initialize(ctx, s.ResourceVersion(), 0)
			require.Error(t, err, "已有/丢失部分WAL不允许初始化重造空闲")
		})
	}
}
