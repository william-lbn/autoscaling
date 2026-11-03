package hostscheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestSchedulerElectionUsesExplicitClientWithoutChangingStoreClient(t *testing.T) {
	legacy, f, n := schedulerFixture(t)
	require.Same(t, legacy.coord, legacy.election, "原New保持既有嵌入调用合同")
	election := fake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var gets atomic.Int32
	election.PrependReactor("get", "leases", func(ktesting.Action) (bool, runtime.Object, error) {
		gets.Add(1)
		cancel()
		return true, nil, context.Canceled
	})
	electionClient := election.CoordinationV1()
	s, err := NewWithElectionClient(f.Core.CoreV1(), f.Core.CoordinationV1(), electionClient, f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	require.Same(t, electionClient, s.election)
	require.NotSame(t, s.coord, s.election)
	_, loadErr := s.domains[n.Binding.NodeName].store.Load(context.Background())
	require.ErrorContains(t, loadErr, "not initialized")
	require.NoError(t, s.Run(ctx, zap.NewNop()))
	require.Eventually(t, func() bool { return gets.Load() == 1 }, time.Second, time.Millisecond)
	// 新入口不接受缺失的续租客户端，也不在配置失败时降级回工作桶。
	_, err = NewWithElectionClient(f.Core.CoreV1(), f.Core.CoordinationV1(), nil, f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.Error(t, err)
	require.True(t, errors.Is(ctx.Err(), context.Canceled))
}
