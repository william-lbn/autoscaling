package plugin

import (
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/neondatabase/autoscaling/pkg/plugin/state"
	"github.com/neondatabase/autoscaling/pkg/util"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

func TestHostCPUPreviewKeepsOriginalBudgetMissingFromSchedulerCache(t *testing.T) {
	node := state.NodeStateFromParams("node", 4000, 8<<30, 0.8, map[string]string{})
	pod := state.Pod{UID: "live", NamespacedName: util.NamespacedName{Namespace: "ns", Name: "live"},
		CPU: state.PodResources[vmv1.MilliCPU]{Reserved: 500, Requested: 500, Overcommit: resource.NewQuantity(1, resource.DecimalSI)},
		Mem: state.PodResources[api.Bytes]{Reserved: 1 << 30, Requested: 1 << 30, Overcommit: resource.NewQuantity(1, resource.DecimalSI)}}
	node.AddPod(pod)
	candidate := pod
	candidate.UID, candidate.Name = "candidate", "candidate"
	candidate.HostCPUAccountingVersion, candidate.HostCommittedCPU = 1, 1000
	preview := &hostscheduler.PlacementPreview{Allocation: hostguard.Placement{CPUAccountingVersion: 1},
		AllocatableCPUMillicores: 4000, SystemCPUMillicores: 250, Held: []hostguard.Placement{
			{PodUID: "live", CPUAccountingVersion: 1, CommittedCPUMillicores: 2000},
			{PodUID: "missing-from-cache", CPUAccountingVersion: 1, CommittedCPUMillicores: 1000}}}
	require.True(t, applyHostCPUPreview(node, candidate, preview))
	require.EqualValues(t, 2750, node.CPU.Total)
	require.EqualValues(t, 2000, node.CPU.Reserved)
	node.AddPod(candidate)
	require.True(t, node.OverBudget(), "cache没有旧Pod仍不得重复授予其固定CPU")
}
