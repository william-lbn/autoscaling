package hostscheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

type unboundBudgetCore struct {
	coreclient.CoreV1Interface
	mu             sync.Mutex
	slowName       string
	blocked        bool
	observedBudget time.Duration
}

func (c *unboundBudgetCore) Pods(ns string) coreclient.PodInterface {
	return &unboundBudgetPods{PodInterface: c.CoreV1Interface.Pods(ns), state: c}
}

type unboundBudgetPods struct {
	coreclient.PodInterface
	state *unboundBudgetCore
}

func (p *unboundBudgetPods) Get(ctx context.Context, name string, opts metav1.GetOptions) (*core.Pod, error) {
	p.state.mu.Lock()
	stall := name == p.state.slowName && !p.state.blocked
	if stall {
		p.state.blocked = true
		if deadline, ok := ctx.Deadline(); ok {
			p.state.observedBudget = time.Until(deadline)
		}
	}
	p.state.mu.Unlock()
	if stall {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return p.PodInterface.Get(ctx, name, opts)
}

// 慢未绑定对象耗尽自己的两秒后，原有assigned工作仍在同轮实际完成Node CAS；
// 下一轮同UID恢复关闭。阻塞由子ctx取消驱动，不用睡眠或放宽生产超时。
func TestSchedulerUnboundDeadlineLeavesBudgetForAssignedWork(t *testing.T) {
	s, f, n, pod := unboundFixture(t)
	good := boundedPod("assigned-good", n.Binding.NodeName, "1Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/assigned-good"] = good })
	wrapped := &unboundBudgetCore{CoreV1Interface: s.core, slowName: pod.Name}
	s.core = wrapped
	ctx, cancel := context.WithTimeout(context.Background(), HostOperationTimeout)
	defer cancel()
	d := s.domains[n.Binding.NodeName]
	require.ErrorIs(t, s.reconcile(ctx, d), context.DeadlineExceeded)
	require.NoError(t, ctx.Err(), "未绑定子预算不能取消原Host事务")
	wrapped.mu.Lock()
	require.True(t, wrapped.blocked)
	require.Positive(t, wrapped.observedBudget)
	require.LessOrEqual(t, wrapped.observedBudget, 2*time.Second)
	wrapped.mu.Unlock()
	snapshot, err := d.store.Load(ctx)
	require.NoError(t, err)
	held, err := snapshot.State().Placement(string(good.UID))
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		require.Contains(t, f.Pods["testing/runner"].Finalizers, hostguard.PodProtectionFinalizer)
	})
	require.NoError(t, s.reconcile(ctx, d))
	after, err := d.store.Load(ctx)
	require.NoError(t, err)
	again, err := after.State().Placement(string(good.UID))
	require.NoError(t, err)
	require.Equal(t, held, again)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		require.NotContains(t, f.Pods["testing/runner"].Finalizers, hostguard.PodProtectionFinalizer)
	})
}
