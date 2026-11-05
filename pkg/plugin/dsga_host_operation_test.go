package plugin

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/neondatabase/autoscaling/pkg/plugin/reconcile"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type hostOperationTransport struct {
	delay time.Duration
	calls atomic.Int32
}

func (t *hostOperationTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	timer := time.NewTimer(t.delay)
	defer timer.Stop()
	select {
	case <-r.Context().Done():
		return nil, r.Context().Err()
	case <-timer.C:
	}
	return http.DefaultTransport.RoundTrip(r)
}

// 复用原真实REST/Node UID-RV CAS夹具。每个HTTP均受1秒约束，完整串行
// 来源读取+CAS超过1秒并不是单CRUD超时；不能为等一次队列事件而绕过Node账本。
func TestHostEventTransactionHasOperationBudgetBeyondSingleCRUD(t *testing.T) {
	f, b := dsgatest.NewSchedulerAPI(t, "operation-holder")
	profile := hostguard.Profile{ID: "qualified-test-only", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}
	store, err := hostguard.NewStore(f.Core.CoreV1(), f.Core.CoordinationV1(), b, "operation-holder")
	require.NoError(t, err)
	_, err = store.InitializeAllocations(context.Background(), "1", 1<<30, profile, []hostguard.Placement{})
	require.NoError(t, err)
	originalURL := f.Core.CoreV1().RESTClient().Get().URL()
	originalURL.Path = ""
	originalURL.RawQuery = ""
	transport := &hostOperationTransport{delay: 220 * time.Millisecond}
	delayed, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: originalURL.String(), QPS: 1000, Burst: 1000}, &http.Client{Timeout: time.Second, Transport: transport})
	require.NoError(t, err)
	host, err := hostscheduler.New(delayed.CoreV1(), delayed.CoordinationV1(), f.VM, hostscheduler.Config{Nodes: []hostscheduler.NodeConfig{{Binding: b, Profile: profile, SystemBytes: 1 << 30}}}, profile.SchedulerName)
	require.NoError(t, err)
	s := &PluginState{config: Config{SchedulerName: profile.SchedulerName, IgnoredNamespaces: []string{"testing"}, K8sCRUDTimeoutSeconds: 1}, host: host, hostContext: context.Background()}
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "slow-transaction", UID: "slow-transaction-uid", ResourceVersion: "1"}, Spec: core.PodSpec{NodeName: b.NodeName, SchedulerName: profile.SchedulerName, Containers: []core.Container{{Name: "app", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("2Gi")}}}}}}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/slow-transaction"] = pod.DeepCopy() })
	beforeWrites := 0
	f.Mutate(func(f *dsgatest.SchedulerAPI) { beforeWrites = f.NodeWrites })
	started := time.Now()
	_, err = s.HandlePodEvent(zap.NewNop(), reconcile.EventKindModified, pod)
	writes := 0
	f.Mutate(func(f *dsgatest.SchedulerAPI) { writes = f.NodeWrites - beforeWrites })
	t.Logf("actual event elapsed=%s REST_calls=%d Node_CAS=%d err_type=%T", time.Since(started), transport.calls.Load(), writes, err)
	require.NoError(t, err)
	require.Greater(t, time.Since(started), time.Second)
	require.Equal(t, 1, writes)
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	held, err := snapshot.State().Placement(string(pod.UID))
	require.NoError(t, err)
	require.EqualValues(t, 2<<30, held.MemoryBytes)
}

func TestHostOperationBudgetKeepsCallerLeaseAndUnknownCASGuards(t *testing.T) {
	for _, mode := range []string{"cancelled", "parent-deadline", "expired-lease", "lost-CAS-reply"} {
		t.Run(mode, func(t *testing.T) {
			f, b := dsgatest.NewSchedulerAPI(t, "operation-holder")
			profile := hostguard.Profile{ID: "qualified-test-only", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}
			store, err := hostguard.NewStore(f.Core.CoreV1(), f.Core.CoordinationV1(), b, "operation-holder")
			require.NoError(t, err)
			_, err = store.InitializeAllocations(context.Background(), "1", 1<<30, profile, []hostguard.Placement{})
			require.NoError(t, err)
			host, err := hostscheduler.New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, hostscheduler.Config{Nodes: []hostscheduler.NodeConfig{{Binding: b, Profile: profile, SystemBytes: 1 << 30}}}, profile.SchedulerName)
			require.NoError(t, err)
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "parent-deadline" {
				base, cancel = context.WithDeadline(base, time.Now().Add(-time.Second))
				defer cancel()
			}
			if mode == "cancelled" {
				cancel()
			}
			s := &PluginState{config: Config{SchedulerName: profile.SchedulerName, IgnoredNamespaces: []string{"testing"}, K8sCRUDTimeoutSeconds: 1}, host: host, hostContext: base}
			call, done := s.hostCallContext()
			defer done()
			deadline, ok := call.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(deadline), 10*time.Second)
			pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "guarded", UID: "guarded-uid", ResourceVersion: "1"}, Spec: core.PodSpec{NodeName: b.NodeName, SchedulerName: profile.SchedulerName, Containers: []core.Container{{Name: "app", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("2Gi")}}}}}}
			beforeWrites := 0
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				f.Pods["testing/guarded"] = pod.DeepCopy()
				beforeWrites = f.NodeWrites
				if mode == "expired-lease" {
					stamp := metav1.NewMicroTime(time.Now().Add(-time.Hour))
					f.Lease.Spec.RenewTime = &stamp
				}
				if mode == "lost-CAS-reply" {
					f.LoseNodeReply = true
				}
			})
			_, err = s.HandlePodEvent(zap.NewNop(), reconcile.EventKindModified, pod)
			require.Error(t, err)
			writes := 0
			f.Mutate(func(f *dsgatest.SchedulerAPI) { writes = f.NodeWrites - beforeWrites })
			if mode == "lost-CAS-reply" {
				require.Equal(t, 1, writes)
				snapshot, e := store.Load(context.Background())
				require.NoError(t, e)
				held, e := snapshot.State().Placement(string(pod.UID))
				require.NoError(t, e)
				require.EqualValues(t, 2<<30, held.MemoryBytes)
			} else {
				require.Zero(t, writes)
			}
		})
	}
}
