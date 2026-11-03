package hostguard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 单元模型/HTTP夹具，不代表真实Pod或PG已经验收。实际VM值与保守计费必须分开。
func TestHostPodFloorV10AccountingAndSettlement(t *testing.T) {
	_, _, snapshot, _, current, closed := settlementFixture(t)
	p := current
	p.HostOverheadBytes = 3 << 30
	p.PodMemoryLimitBytes = 11 << 30
	p.BeforeBytes = 7 << 30
	p.Request.Kind = control.Grow
	p.Request.TargetVMMemoryBytes = 8 << 30
	n, err := p.Bytes()
	require.NoError(t, err)
	require.EqualValues(t, 11<<30, n)
	p.BeforeBytes = 8 << 30
	p.Request.Kind = control.Shrink
	p.Request.TargetVMMemoryBytes = 7 << 30
	n, err = p.Bytes()
	require.NoError(t, err)
	require.EqualValues(t, 11<<30, n)
	row := Placement{Namespace: p.Namespace, PodName: "runner", PodUID: p.RunnerPodUID, VMName: p.VMName, VMUID: p.Request.Key.Instance.VMUID, Incarnation: p.Incarnation, Managed: true, MemoryBytes: 7 << 30, HostOverheadBytes: 3 << 30, PodMemoryLimitBytes: 11 << 30}
	n, err = row.Bytes()
	require.NoError(t, err)
	require.EqualValues(t, 11<<30, n)
	row.MemoryBytes = 8 << 30
	n, err = row.Bytes()
	require.NoError(t, err)
	require.EqualValues(t, 11<<30, n)
	m := snapshot.State()
	m.ProtocolVersion = 10
	for i := range *m.Placements {
		(*m.Placements)[i].PodMemoryLimitBytes = 11 << 30
	}
	for i := range m.Holds {
		m.Holds[i].Plan.PodMemoryLimitBytes = 11 << 30
		m.Holds[i].SourceID = SourceID(m.Origin, m.Holds[i].Plan)
	}
	require.NoError(t, m.Validate())
	missing := current
	missing.Request.Key.Revision++
	missing.Request.Key.OperationID.Low++
	_, _, err = m.reserve(m.LeaseFloor, missing, 32<<30)
	require.ErrorContains(t, err, "floor", "v10新Key省略floor也必须拒绝")
	next, _, err := m.settle(m.LeaseFloor, current.RunnerPodUID, closed)
	require.NoError(t, err)
	actual, err := next.Placement(current.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, closed.MemoryBytes(), actual.MemoryBytes, "不得把保守计费伪造为实际VM值")
	require.EqualValues(t, 11<<30, actual.PodMemoryLimitBytes)
	used, err := next.UsedBytes()
	require.NoError(t, err)
	require.EqualValues(t, 12<<30, used, "已settled Hold被忽略后仍由独立floor全额计费")
	raw, err := json.Marshal(next)
	require.NoError(t, err)
	var restored Model
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, next, restored)
	next.ProtocolVersion = 9
	require.Error(t, next.Validate(), "旧版本不能携带非零floor")
}

func TestHostPodFloorV10OriginalCASAndUnknown(t *testing.T) {
	f, store := newNodeAPI(t)
	ctx := context.Background()
	f.mu.Lock()
	f.node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("24Gi")
	f.mu.Unlock()
	profile := allocationProfile()
	profile.VMOverheadBytes = 3 << 30
	ordinary := podAllocation("ordinary", 2<<30)
	snapshot, err := store.InitializeAllocations(ctx, "node-rv-1", 1<<30, profile, []Placement{ordinary})
	require.NoError(t, err)
	// 现场对应v5/seq17；这只是HTTP服务夹具初始状态，不是生产账本迁移。
	legacyV5 := snapshot.State()
	legacyV5.ProtocolVersion = 5
	legacyV5.Sequence = 17
	rawV5, err := json.Marshal(legacyV5)
	require.NoError(t, err)
	f.mu.Lock()
	f.node.Annotations[LedgerAnnotation] = string(rawV5)
	f.node.ResourceVersion = "legacy-v5-fixture-rv"
	f.mu.Unlock()
	snapshot, err = store.Load(ctx)
	require.NoError(t, err)
	original := snapshot.State()
	controller := true
	pod := core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "runner", UID: "new-vm-pod-uid", ResourceVersion: "pod-1", Finalizers: []string{PodProtectionFinalizer}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: "new-vm", UID: "new-vm-uid", Controller: &controller}}}, Spec: core.PodSpec{NodeName: "node-one", SchedulerName: profile.SchedulerName, Containers: []core.Container{{Name: "runner", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("11Gi")}}}}}}
	p := Placement{Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID), VMName: "new-vm", VMUID: "new-vm-uid", Incarnation: strings.Repeat("a", 32), Managed: true, MemoryBytes: 7 << 30, HostOverheadBytes: 3 << 30, PodMemoryLimitBytes: 11 << 30}
	f.mu.Lock()
	f.pods["testing/runner"] = pod
	f.loseReply = true
	f.mu.Unlock()
	_, _, err = store.HoldPlacement(ctx, snapshot, p)
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	snapshot, err = store.Load(ctx)
	require.NoError(t, err)
	m := snapshot.State()
	require.EqualValues(t, 10, m.ProtocolVersion)
	require.Equal(t, original.Origin, m.Origin)
	require.Equal(t, original.Binding, m.Binding)
	require.Equal(t, original.LeaseFloor, m.LeaseFloor)
	kept, err := m.Placement(ordinary.PodUID)
	require.NoError(t, err)
	require.Equal(t, ordinary, kept, "CAS升级不能改普通行或安全预留")
	require.Equal(t, original.OutsideBytes, m.OutsideBytes)
	version := snapshot.ResourceVersion()
	again, held, err := store.HoldPlacement(ctx, snapshot, p)
	require.NoError(t, err)
	require.Equal(t, version, again.ResourceVersion())
	require.Equal(t, p, held)
	floor, err := PodMemoryFloor(snapshot.Node(), &pod)
	require.NoError(t, err)
	require.EqualValues(t, 11<<30, floor)
	altered := pod.DeepCopy()
	altered.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("12Gi")
	_, err = PodMemoryFloor(snapshot.Node(), altered)
	require.Error(t, err)
	altered = pod.DeepCopy()
	altered.UID = "recreated-pod"
	_, err = PodMemoryFloor(snapshot.Node(), altered)
	require.Error(t, err)
	bad := p
	bad.PodMemoryLimitBytes = 10 << 30
	_, _, err = store.HoldPlacement(ctx, snapshot, bad)
	require.ErrorContains(t, err, "hardlimit")
	f.mu.Lock()
	past := metav1.NewMicroTime(time.Now().Add(-time.Hour))
	f.lease.Spec.RenewTime = &past
	f.mu.Unlock()
	_, _, err = store.HoldPlacement(ctx, snapshot, p)
	require.ErrorContains(t, err, "expired")
	latest, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, version, latest.ResourceVersion(), "所有失败/未知重读不得释放或多写floor")
}

func TestHostPodFloorV10LegacyBytesAndMigrationGates(t *testing.T) {
	_, _, snapshot, old, _, _ := settlementFixture(t)
	raw, err := json.Marshal(old)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "pod_memory_limit_bytes")
	legacy := struct {
		Namespace         string          `json:"namespace"`
		VMName            string          `json:"vm_name"`
		Incarnation       string          `json:"incarnation"`
		RunnerPodUID      string          `json:"runner_pod_uid"`
		Request           control.Request `json:"request"`
		BeforeBytes       control.Uint64  `json:"before_bytes"`
		HostOverheadBytes control.Uint64  `json:"host_overhead_bytes"`
	}{old.Namespace, old.VMName, old.Incarnation, old.RunnerPodUID, old.Request, old.BeforeBytes, old.HostOverheadBytes}
	bytes, err := json.Marshal([]any{snapshot.State().Origin, legacy})
	require.NoError(t, err)
	digest := sha256.Sum256(append([]byte("neon-dsga-node-held-memory-v1\x00"), bytes...))
	require.Equal(t, hex.EncodeToString(digest[:]), SourceID(snapshot.State().Origin, old))
	p := Placement{Namespace: "testing", PodName: "new", PodUID: "new-pod", VMName: "new-vm", VMUID: "new-vm-uid", Incarnation: strings.Repeat("a", 32), Managed: true, MemoryBytes: 7 << 30, HostOverheadBytes: 3 << 30, PodMemoryLimitBytes: 11 << 30}
	m := snapshot.State()
	_, err = m.reservePlacement(p, 32<<30)
	require.ErrorContains(t, err, "migration")
	m.Holds = []Hold{}
	_, err = m.reservePlacement(p, 32<<30)
	require.ErrorContains(t, err, "managed")
	m.Placements = &[]Placement{}
	m.Profile.VMOverheadBytes = 3 << 30
	next, err := m.reservePlacement(p, 32<<30)
	require.NoError(t, err)
	require.EqualValues(t, 10, next.ProtocolVersion)
	ordinary := p
	ordinary.Managed = false
	ordinary.VMName = ""
	ordinary.VMUID = ""
	ordinary.Incarnation = ""
	ordinary.PodMemoryLimitBytes = 0
	ordinary.MemoryBytes = 1 << 30
	ordinary.HostOverheadBytes = 16 << 20
	n, err := ordinary.Bytes()
	require.NoError(t, err)
	require.EqualValues(t, (1<<30)+(16<<20), n)
}
