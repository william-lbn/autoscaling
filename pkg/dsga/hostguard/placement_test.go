package hostguard

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	coord "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

func allocationProfile() Profile {
	return Profile{ID: "qualified-fixture-only", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}
}

func podAllocation(name string, memory control.Uint64) Placement {
	return Placement{Namespace: "testing", PodName: name, PodUID: name + "-uid", VMName: "", VMUID: "", Incarnation: "", Managed: false, MemoryBytes: memory, HostOverheadBytes: allocationProfile().PodOverheadBytes}
}

func TestCompletePlacementsMultiWriterCASAndUnknownRecovery(t *testing.T) {
	f, owner := newNodeAPI(t)
	ctx := context.Background()
	snapshot, err := owner.InitializeAllocations(ctx, "node-rv-1", 1<<30, allocationProfile(), []Placement{})
	require.NoError(t, err)
	// 这个副本不是节点Lease holder，但可以单调保留容量；绝无发布权限。
	writer := f.store(t, "global-scheduler-other-replica")
	a, b := podAllocation("one", 3<<30), podAllocation("two", 3<<30)
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	for _, p := range []Placement{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := writer.HoldPlacement(ctx, snapshot, p)
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	succeeded := 0
	for err := range errors {
		if err == nil {
			succeeded++
		}
	}
	require.Equal(t, 1, succeeded, "同一原Node RV只有一个容量CAS可线性化")
	snapshot, err = writer.Load(ctx)
	require.NoError(t, err)
	f.mu.Lock()
	f.loseReply = true
	f.mu.Unlock()
	var missing Placement
	for _, p := range []Placement{a, b} {
		if _, err := snapshot.State().Placement(p.PodUID); err != nil {
			missing = p
		}
	}
	_, held, err := writer.HoldPlacement(ctx, snapshot, missing)
	var unknown *UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	require.Empty(t, held.PodUID)
	restarted := f.store(t, "third-replica")
	snapshot, err = restarted.Load(ctx)
	require.NoError(t, err)
	version := snapshot.ResourceVersion()
	snapshot, held, err = restarted.HoldPlacement(ctx, snapshot, missing)
	require.NoError(t, err)
	require.Equal(t, missing, held)
	require.Equal(t, version, snapshot.ResourceVersion(), "丢回复后原持有只读恢复，不重复写")
	_, _, err = restarted.HoldPlacement(ctx, snapshot, podAllocation("third", 1<<30))
	require.ErrorContains(t, err, "capacity")
	_, err = restarted.Fence(ctx, snapshot)
	require.Error(t, err, "多写者容量角色不获得节点owner权限")
	copyModel := snapshot.State()
	(*copyModel.Placements)[0].MemoryBytes = 1
	copyModel.Profile.ID = "changed"
	require.NotEqual(t, copyModel, snapshot.State(), "返回配置/行均为深拷贝")
	f.mu.Lock()
	f.node.Status.Allocatable["memory"] = resource.MustParse("1Gi")
	f.node.ResourceVersion = "capacity-drop-rv"
	f.mu.Unlock()
	snapshot, err = owner.Load(ctx)
	require.NoError(t, err)
	_, _, err = writer.HoldPlacement(ctx, snapshot, missing)
	require.ErrorContains(t, err, "capacity", "幂等原持有也不得绕过现已不足的Node容量")
}

func TestInitialVMRequiresOriginalCompletePlacement(t *testing.T) {
	f, store := newNodeAPI(t)
	p := podAllocation("runner", 1<<30)
	p.VMName, p.VMUID, p.Managed, p.Incarnation, p.HostOverheadBytes = "vm", "vm-uid", true, "1234567890abcdef1234567890abcdef", allocationProfile().VMOverheadBytes
	s, err := store.InitializeAllocations(context.Background(), "node-rv-1", 1<<30, allocationProfile(), []Placement{p})
	require.NoError(t, err)
	b := journal.Binding{Namespace: p.Namespace, Name: p.VMName, VMUID: "vm-uid", Incarnation: p.Incarnation}
	f.mu.Lock()
	n := f.node.DeepCopy()
	f.mu.Unlock()
	require.NoError(t, InspectInitialVM(n, b, p.PodName, p.PodUID, 1<<30))
	for _, what := range []string{"pod", "vm", "incarnation", "memory", "protection", "node", "legacy", "profile"} {
		t.Run(what, func(t *testing.T) {
			copyNode, binding, pod, memory := n.DeepCopy(), b, p.PodUID, control.Uint64(1<<30)
			switch what {
			case "pod":
				pod = "replacement-pod"
			case "vm":
				binding.VMUID = "replacement-vm"
			case "incarnation":
				binding.Incarnation = "2234567890abcdef1234567890abcdef"
			case "memory":
				memory++
			case "protection":
				copyNode.Finalizers = nil
			case "node":
				copyNode.UID = "replacement-node"
			case "legacy", "profile":
				m := s.State()
				if what == "legacy" {
					m.ProtocolVersion, m.Profile, m.Placements = 2, nil, nil
				} else {
					m.Profile.VMOverheadBytes++
				}
				raw, err := json.Marshal(m)
				require.NoError(t, err)
				copyNode.Annotations[LedgerAnnotation] = string(raw)
			}
			require.Error(t, InspectInitialVM(copyNode, binding, p.PodName, pod, memory))
		})
	}
	_, _, err = store.HoldPlacement(context.Background(), s, Placement{Namespace: p.Namespace, PodName: p.PodName, PodUID: p.PodUID, VMName: p.VMName, VMUID: "other-vm", Incarnation: p.Incarnation, Managed: true, MemoryBytes: p.MemoryBytes, HostOverheadBytes: p.HostOverheadBytes})
	require.Error(t, err, "同Pod UID不能换VM原身份")
}

func TestBoundHostElectionPreservesOriginalUIDAndEpoch(t *testing.T) {
	f, _ := newNodeAPI(t)
	client, err := kubernetes.NewForConfig(f.config)
	require.NoError(t, err)
	lock, err := NewLeaseLock(client.CoordinationV1(), nodeBinding(), "next-owner")
	require.NoError(t, err)
	ctx := context.Background()
	prior, _, err := lock.Get(ctx)
	require.NoError(t, err)
	now := metav1.Now()
	next := resourcelock.LeaderElectionRecord{HolderIdentity: lock.Identity(), LeaseDurationSeconds: 15, AcquireTime: now, RenewTime: now, LeaderTransitions: prior.LeaderTransitions + 1, Strategy: "", PreferredHolder: ""}
	require.NoError(t, lock.Update(ctx, next))
	got, _, err := lock.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, "next-owner", got.HolderIdentity)
	require.Equal(t, prior.LeaderTransitions+1, got.LeaderTransitions)
	// 旧RV更新必须冲突；重读原UID后才能继续，不在未知回复后自动改epoch。
	f.mu.Lock()
	f.lease.ResourceVersion = "other-renew-rv"
	f.mu.Unlock()
	next.RenewTime = metav1.NewTime(time.Now().Add(time.Millisecond))
	require.Error(t, lock.Update(ctx, next))
	_, _, err = lock.Get(ctx)
	require.NoError(t, err)
	require.NoError(t, lock.Update(ctx, next))
	f.mu.Lock()
	f.lease.UID = "same-name-new-uid"
	f.mu.Unlock()
	_, _, err = lock.Get(ctx)
	require.Error(t, err)
	require.Error(t, lock.Create(ctx, next))
	f.mu.Lock()
	f.lease.UID, f.lease.Spec = nodeBinding().LeaseUID, coord.LeaseSpec{HolderIdentity: nil, LeaseDurationSeconds: nil, AcquireTime: nil, RenewTime: nil, LeaseTransitions: nil, Strategy: nil, PreferredHolder: nil}
	f.mu.Unlock()
	empty, _, err := lock.Get(ctx)
	require.NoError(t, err, "仅原新Lease空Spec允许首次选主")
	require.Empty(t, empty.HolderIdentity)
}

func TestPlacementSchemaBoundsAndRoundTripRemainStrict(t *testing.T) {
	_, store := newNodeAPI(t)
	snapshot, err := store.InitializeAllocations(context.Background(), "node-rv-1", 1<<30, allocationProfile(), []Placement{})
	require.NoError(t, err)
	m := snapshot.State()
	for i := range MaxPlacements {
		*m.Placements = append(*m.Placements, podAllocation(fmt.Sprintf("pod-%03d", i), 1<<20))
	}
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	var restored Model
	require.NoError(t, json.Unmarshal(raw, &restored), "支持的最大行数须可被原有界解码器读回")
	require.Equal(t, m, restored)
	_, err = m.reservePlacement(podAllocation("one-more", 1), 8<<30)
	require.ErrorContains(t, err, "bound exhausted")
	p := podAllocation("overflow", control.Uint64(math.MaxUint64))
	_, err = p.Bytes()
	require.Error(t, err)
	for _, what := range []string{"folded-profile", "folded-placement", "missing-memory", "unknown-profile"} {
		t.Run(what, func(t *testing.T) {
			changed := string(raw)
			switch what {
			case "folded-profile":
				changed = strings.Replace(changed, `"scheduler_name":`, `"Scheduler_Name":`, 1)
			case "folded-placement":
				changed = strings.Replace(changed, `"pod_uid":`, `"Pod_UID":`, 1)
			case "missing-memory":
				changed = strings.Replace(changed, `"memory_bytes":"1048576",`, "", 1)
			case "unknown-profile":
				changed = strings.Replace(changed, `"profile":{`, `"profile":{"guess":true,`, 1)
			}
			var result Model
			require.Error(t, json.Unmarshal([]byte(changed), &result))
		})
	}
}
