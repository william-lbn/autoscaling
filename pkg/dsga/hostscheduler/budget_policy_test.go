package hostscheduler

import (
	"context"
	"testing"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func schedulerBudgetFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig) {
	t.Helper()
	_, f, n := schedulerFixture(t)
	digest, err := hostguard.ProfileSHA256(n.Profile)
	require.NoError(t, err)
	n.KubernetesBudget = &hostguard.KubernetesBudgetPolicy{ProtocolVersion: 1, Scope: hostguard.KubernetesBudgetScope, NodeName: n.Binding.NodeName, NodeUID: string(n.Binding.NodeUID), CapacityBytes: 12 << 30, AllocatableBytes: 8 << 30, SystemReservationBytes: n.SystemBytes, ProfileSHA256: digest}
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		f.Node.Status.Capacity = core.ResourceList{core.ResourceMemory: resource.MustParse("12Gi")}
	})
	s, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Lease.Spec.HolderIdentity = &s.holder })
	return s, f, n
}

func TestSchedulerKubernetesBudgetAccountingAndRestart(t *testing.T) {
	s, f, n := schedulerBudgetFixture(t)
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	pod := boundedPod("full", "", "6Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/full"] = pod })
	_, _, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err, "Kube 已扣的4Gi不得再减，否则合法6Gi会被误拒")
	snapshot, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	used, err := snapshot.State().UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, (7<<30)+(16<<20), used)
	extra := boundedPod("over", "", "1Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/over"] = extra })
	_, _, err = s.Reserve(ctx, n.Binding.NodeName, extra, 0)
	require.ErrorContains(t, err, "capacity")
	restart, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	_, _, err = restart.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
	// New 已复制可信策略，调用方之后修改配置不能改变运行服务的边界。
	n.KubernetesBudget.AllocatableBytes++
	_, _, err = s.Reserve(ctx, n.Binding.NodeName, pod, 0)
	require.NoError(t, err)
}

func TestSchedulerKubernetesBudgetRejectsDriftBeforeBootstrapAndGrant(t *testing.T) {
	for _, stage := range []string{"bootstrap", "grant"} {
		for _, field := range []string{"capacity", "allocatable"} {
			t.Run(stage+"/"+field, func(t *testing.T) {
				s, f, n := schedulerBudgetFixture(t)
				ctx := context.Background()
				if stage == "grant" {
					require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
				}
				f.Mutate(func(f *dsgatest.SchedulerAPI) {
					if field == "capacity" {
						f.Node.Status.Capacity[core.ResourceMemory] = resource.MustParse("13Gi")
					} else {
						f.Node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("9Gi")
					}
				})
				if stage == "bootstrap" {
					require.Error(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
					f.Mutate(func(f *dsgatest.SchedulerAPI) { require.Empty(t, f.Node.Annotations[hostguard.LedgerAnnotation]) })
				} else {
					pod := boundedPod("drift", "", "1Gi")
					f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/drift"] = pod })
					_, _, err := s.Reserve(ctx, n.Binding.NodeName, pod, 0)
					require.ErrorContains(t, err, "budget capacity/allocatable changed")
				}
			})
		}
	}
}

func TestSchedulerKubernetesBudgetRejectsMismatchedStartupPolicy(t *testing.T) {
	for _, field := range []string{"uid", "name", "profile", "reservation"} {
		t.Run(field, func(t *testing.T) {
			_, _, n := schedulerBudgetFixture(t)
			switch field {
			case "uid":
				n.KubernetesBudget.NodeUID = "replacement"
			case "name":
				n.KubernetesBudget.NodeName = "replacement"
			case "profile":
				n.Profile.VMOverheadBytes++
			case "reservation":
				n.SystemBytes++
			}
			require.Error(t, (Config{Nodes: []NodeConfig{n}}).Validate(n.Profile.SchedulerName))
		})
	}
}
