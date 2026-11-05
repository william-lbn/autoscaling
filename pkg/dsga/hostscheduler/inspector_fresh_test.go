package hostscheduler

import (
	"context"
	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"sync"
	"testing"
	"time"
)

type inspectorTransport struct {
	mu         sync.Mutex
	calls      map[string]int
	beforeNode func()
}

func (c *inspectorTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.mu.Lock()
	key := r.Method + " " + r.URL.Path
	c.calls[key]++
	hook := c.beforeNode
	if r.Method == "GET" && r.URL.Path == "/api/v1/nodes/node-one" && hook != nil {
		c.beforeNode = nil
	} else {
		hook = nil
	}
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	return http.DefaultTransport.RoundTrip(r)
}
func (c *inspectorTransport) take() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := c.calls
	c.calls = map[string]int{}
	return v
}

// 原typed HTTP/RFC6902夹具：缓存不减少真实GET，也不能越过失租或Node版本改变。
func TestHostModelInspectorKeepsFreshLeaseAndNetworkReads(t *testing.T) {
	_, f, n, pod := schedulerPodFloorFixture(t)
	url := f.Core.CoreV1().RESTClient().Get().URL()
	url.Path = ""
	url.RawQuery = ""
	transport := &inspectorTransport{calls: map[string]int{}}
	httpClient := &http.Client{Timeout: time.Second, Transport: transport}
	cfg := &rest.Config{Host: url.String(), QPS: 1000, Burst: 1000}
	core, err := kubernetes.NewForConfigAndClient(cfg, httpClient)
	require.NoError(t, err)
	vms, err := vmclient.NewForConfigAndClient(cfg, httpClient)
	require.NoError(t, err)
	s, err := New(core.CoreV1(), core.CoordinationV1(), vms, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Lease.Spec.HolderIdentity = &s.holder })
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	transport.take()
	_, _, err = s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	cold := transport.take()
	canonical, _, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	warm := transport.take()
	require.Equal(t, cold, warm, "命中不能减少来源读取或增加额外VM GET")
	require.Equal(t, 2, warm["GET /apis/vm.neon.tech/v1/namespaces/testing/virtualmachines/vm"])
	require.Zero(t, warm["PATCH /api/v1/nodes/node-one"])
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.VMs["testing/vm"].ResourceVersion = "status-only-rv" })
	canonical, _, err = s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	require.Equal(t, "status-only-rv", canonical.Annotations[observedVMVersion])
	transport.take()
	writes := 0
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		writes = f.NodeWrites
		f.Lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now().Add(-time.Hour)}
	})
	_, _, err = s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.Error(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		require.Equal(t, writes, f.NodeWrites)
		f.Lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}
	})
	// 在Load以后、写前重读以前制造真实Node RV变化；原CAS前置仍必须拒绝。
	transport.take()
	seen := 0
	transport.beforeNode = func() {
		seen++
		transport.mu.Lock()
		transport.beforeNode = func() { f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Node.ResourceVersion += "-external-change" }) }
		transport.mu.Unlock()
	}
	_, _, err = s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.Error(t, err)
	require.Equal(t, 1, seen)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { require.Equal(t, writes, f.NodeWrites) })
}
