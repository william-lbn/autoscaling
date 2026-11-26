package hostguard

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"strings"
	"testing"
)

func cpuAccountingProfile() Profile {
	p := allocationProfile()
	p.CPUAccountingVersion = CPUAccountingVersion
	p.SystemCPUMillicores = 250
	return p
}

func setCPUNodeCapacity(f *nodeAPI, cpu string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.node.Status.Capacity = core.ResourceList{core.ResourceCPU: resource.MustParse(cpu), core.ResourceMemory: resource.MustParse("8Gi")}
	f.node.Status.Allocatable[core.ResourceCPU] = resource.MustParse(cpu)
}

func cpuPodAndPlacement(name string, cpu control.Uint64) (core.Pod, Placement) {
	row := podAllocation(name, 512<<20)
	row.CPUAccountingVersion, row.CommittedCPUMillicores = CPUAccountingVersion, cpu
	q := *resource.NewMilliQuantity(int64(cpu), resource.DecimalSI)
	pod := core.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: row.Namespace, UID: types.UID(row.PodUID),
		ResourceVersion: "pod-1", Finalizers: []string{PodProtectionFinalizer}},
		Spec: core.PodSpec{NodeName: "node-one", SchedulerName: allocationProfile().SchedulerName,
			Containers: []core.Container{{Name: "app", Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: q},
				Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("512Mi")}}}}}}
	return pod, row
}

func TestCPUOriginalNodeCASRejectsCPUOnlyExhaustionAndRecoversLostWrite(t *testing.T) {
	f, store := newNodeAPI(t)
	setCPUNodeCapacity(f, "4")
	ctx := context.Background()
	snapshot, err := store.InitializeAllocations(ctx, "node-rv-1", 1<<30, cpuAccountingProfile(), []Placement{})
	require.NoError(t, err)
	require.EqualValues(t, 11, snapshot.State().ProtocolVersion)
	one, a := cpuPodAndPlacement("one", 2000)
	two, b := cpuPodAndPlacement("two", 2000)
	f.mu.Lock()
	f.pods["testing/one"], f.pods["testing/two"] = one, two
	f.loseReply = true
	f.mu.Unlock()
	_, _, err = store.HoldPlacement(ctx, snapshot, a)
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	restarted := f.store(t, "other-scheduler")
	snapshot, err = restarted.Load(ctx)
	require.NoError(t, err)
	version := snapshot.ResourceVersion()
	snapshot, held, err := restarted.HoldPlacement(ctx, snapshot, a)
	require.NoError(t, err)
	require.Equal(t, a, held)
	require.Equal(t, version, snapshot.ResourceVersion(), "丢回复只读恢复原CPU承诺")
	used, err := snapshot.State().UsedCPUMillicores()
	require.NoError(t, err)
	require.EqualValues(t, 2250, used)
	_, _, err = restarted.HoldPlacement(ctx, snapshot, b)
	require.ErrorContains(t, err, "CPU", "memory足够时仍必须核同RV CPU轴")
	final, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, snapshot.State(), final.State())
	bad := a
	bad.CommittedCPUMillicores = 1000
	_, _, err = restarted.HoldPlacement(ctx, snapshot, bad)
	require.Error(t, err, "同Pod不能悄悄缩固定CPU")
}

func TestCPUFullPodRequestsIncludeInitAndOverhead(t *testing.T) {
	pod, _ := cpuPodAndPlacement("pod", 500)
	pod.Spec.InitContainers = []core.Container{{Name: "init", Resources: core.ResourceRequirements{
		Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("2")}}}}
	pod.Spec.Overhead = core.ResourceList{core.ResourceCPU: resource.MustParse("100m")}
	request, err := PodCPURequest(&pod)
	require.NoError(t, err)
	require.EqualValues(t, 2100, request)
}

func TestCPUMemorySettlementRetainsOriginalMaximumCPUWithoutDoubleCountingHolds(t *testing.T) {
	_, _, snapshot, _, current, closed := settlementFixture(t)
	model := snapshot.State()
	model.ProtocolVersion = 11
	profile := cpuAccountingProfile()
	model.Profile = &profile
	for i := range *model.Placements {
		(*model.Placements)[i].CPUAccountingVersion = 1
		(*model.Placements)[i].CommittedCPUMillicores = 2000
		(*model.Placements)[i].PodMemoryLimitBytes = 4 << 30
	}
	for i := range model.Holds {
		model.Holds[i].Plan.PodMemoryLimitBytes = 4 << 30
		model.Holds[i].SourceID = SourceID(model.Origin, model.Holds[i].Plan)
	}
	require.NoError(t, model.Validate())
	before, err := model.UsedCPUMillicores()
	require.NoError(t, err)
	next, _, err := model.settle(model.LeaseFloor, current.RunnerPodUID, closed)
	require.NoError(t, err)
	after, err := next.UsedCPUMillicores()
	require.NoError(t, err)
	require.EqualValues(t, 2250, before)
	require.Equal(t, before, after, "内存归还/旧峰值结清不得归还出生CPU包络")
}

func TestCPUUpgradeRequiresEveryOriginalSourceAndPreservesUnknown(t *testing.T) {
	for _, mode := range []string{"valid", "missing-pod", "changed-uid", "under-request", "lost-reply", "insufficient-cpu", "future-version"} {
		t.Run(mode, func(t *testing.T) {
			f, store := newNodeAPI(t)
			setCPUNodeCapacity(f, "4")
			if mode == "insufficient-cpu" {
				setCPUNodeCapacity(f, "2")
			}
			_, row := cpuPodAndPlacement("runner", 0)
			row.CPUAccountingVersion = 0
			row.VMName, row.VMUID, row.Managed, row.Incarnation = "vm", "vm-uid", true, strings.Repeat("b", 32)
			row.HostOverheadBytes = allocationProfile().VMOverheadBytes
			row.PodMemoryLimitBytes = 2 << 30
			controller := true
			pod := core.Pod{ObjectMeta: metav1.ObjectMeta{Name: row.PodName, Namespace: row.Namespace, UID: types.UID(row.PodUID),
				ResourceVersion: "pod-1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine",
					Name: row.VMName, UID: types.UID(row.VMUID), Controller: &controller}}},
				Spec: core.PodSpec{NodeName: "node-one", Containers: []core.Container{{Name: "neonvm-runner",
					Resources: core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("2")}}}}}}
			snapshot, err := store.InitializeAllocations(context.Background(), "node-rv-1", 1<<30, allocationProfile(), []Placement{row})
			require.NoError(t, err)
			require.EqualValues(t, 10, snapshot.State().ProtocolVersion)
			original := snapshot.State()
			nextRow := row
			nextRow.CPUAccountingVersion, nextRow.CommittedCPUMillicores = 1, 2000
			profile := cpuAccountingProfile()
			f.mu.Lock()
			f.pods[row.Namespace+"/"+row.PodName] = pod
			switch mode {
			case "missing-pod":
				delete(f.pods, row.Namespace+"/"+row.PodName)
			case "changed-uid":
				pod.UID = "replacement"
				f.pods[row.Namespace+"/"+row.PodName] = pod
			case "under-request":
				nextRow.CommittedCPUMillicores = 1000
			case "lost-reply":
				f.loseReply = true
			case "insufficient-cpu":
				f.node.Status.Allocatable[core.ResourceCPU] = resource.MustParse("2")
			case "future-version":
				profile.CPUAccountingVersion = 2
			}
			f.mu.Unlock()
			_, err = store.UpgradeCPUAccounting(context.Background(), snapshot, profile, []Placement{nextRow})
			if mode == "valid" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			restored, readErr := store.Load(context.Background())
			require.NoError(t, readErr)
			if mode == "valid" || mode == "lost-reply" {
				require.EqualValues(t, 11, restored.State().ProtocolVersion)
				require.Equal(t, original.Origin, restored.State().Origin)
				require.Equal(t, original.Holds, restored.State().Holds)
				actual, readErr := restored.State().Placement(row.PodUID)
				require.NoError(t, readErr)
				actual.CPUAccountingVersion, actual.CommittedCPUMillicores = 0, 0
				require.Equal(t, row, actual, "CPU升级不改变旧memory/floor/原身份")
			} else {
				require.Equal(t, original, restored.State(), "未知或缺来源不补0、不重置旧账")
			}
		})
	}
}
