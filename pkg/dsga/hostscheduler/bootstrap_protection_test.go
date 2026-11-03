package hostscheduler

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

// 原夹具使用真实HTTP Clientset，直接观察出站方法，不能用fake action API代替。
type bootstrapProtectionTransport struct {
	next   http.RoundTripper
	writes atomic.Int64
}

func (t *bootstrapProtectionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		t.writes.Add(1)
	}
	return t.next.RoundTrip(r)
}

// PG17-U-HOST-BOOT-001：未初始化Host时零Pod保护写，原Pod UID/Finalizer不变。
func TestSchedulerUninitializedHostDoesNotProtectPod(t *testing.T) {
	s, f, n := schedulerFixture(t)
	pod := boundedPod("scheduler-startup", n.Binding.NodeName, "2Gi")
	pod.Finalizers = []string{"example.test/original-protection"}
	before := pod.DeepCopy()
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[pod.Namespace+"/"+pod.Name] = pod.DeepCopy() })
	client := f.Core.CoreV1().RESTClient().(*rest.RESTClient).Client
	next := client.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	observed := &bootstrapProtectionTransport{next: next}
	client.Transport = observed
	canonical, _, err := s.Reserve(context.Background(), n.Binding.NodeName, pod, 0)
	require.ErrorIs(t, err, hostguard.ErrUninitialized)
	require.Nil(t, canonical)
	require.Zero(t, observed.writes.Load(), "未建账前不得发出保护/容量HTTP写请求")
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		require.Equal(t, before, f.Pods[pod.Namespace+"/"+pod.Name])
		require.NotContains(t, f.Node.Annotations, hostguard.LedgerAnnotation)
		require.Zero(t, f.PodWrites)
		require.Zero(t, f.NodeWrites)
		require.Zero(t, f.VMWrites)
	})
}
