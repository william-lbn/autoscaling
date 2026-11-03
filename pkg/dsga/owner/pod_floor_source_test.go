package owner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 真Store/WAL/Node CAS/Publisher/CM/claims读取；API/QMP/资格明确是既有夹具。
// 此处只证明Host floor来源闭环，不冒称真实K8s、PG或物理容量认证。
func TestHostPodFloorPublisherClaims(t *testing.T) {
	for _, mode := range []string{"ok", "unknown", "pre-drift", "post-drift", "recreated", "bad-reserved"} {
		t.Run(mode, func(t *testing.T) {
			api, store, coord := newAPI(t)
			client := vmCore(t)
			api.mu.Lock()
			cpuMode := vmv1.CpuScalingModeSysfs
			api.vm.Spec.CpuScalingMode, api.vm.Spec.ServiceAccountName = &cpuMode, "dsga-runner-test"
			api.vm.Spec.Guest.MemorySlotSize = resource.MustParse("256Mi")
			api.vm.Spec.Guest.MemorySlots = vmv1.MemorySlots{Min: 2, Use: 2, Max: 8}
			api.vm.Status.Node = "node-one"
			api.vm.Status.MemorySize = resource.NewQuantity(512<<20, resource.BinarySI)
			api.mu.Unlock()
			ctx := context.Background()
			pod, err := client.CoreV1().Pods(binding().Namespace).Get(ctx, "runner", metav1.GetOptions{})
			require.NoError(t, err)
			pod.Spec.Containers = []core.Container{{Name: "neonvm-runner", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("11Gi")}}}}
			for i := range pod.Spec.InitContainers {
				pod.Spec.InitContainers[i].Resources.Limits = core.ResourceList{core.ResourceMemory: resource.MustParse("11Gi")}
			}
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
			node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
			require.NoError(t, err)
			var model hostguard.Model
			require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &model))
			require.Empty(t, model.Holds)
			model.ProtocolVersion = 10
			for i := range *model.Placements {
				if (*model.Placements)[i].Managed {
					(*model.Placements)[i].PodMemoryLimitBytes = 11 << 30
				}
			}
			require.NoError(t, model.Validate())
			raw, err := json.Marshal(model)
			require.NoError(t, err)
			node.Annotations[hostguard.LedgerAnnotation] = string(raw)
			node.Status.Allocatable = core.ResourceList{core.ResourceMemory: resource.MustParse("16Gi")}
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), node, ""))
			tmpBase, err := filepath.EvalSymlinks("/tmp")
			require.NoError(t, err)
			dir, err := os.MkdirTemp(tmpBase, "host-floor-")
			require.NoError(t, err)
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			runner, err := vmguard.Initialize(ctx, store, client.CoreV1(), coord, vmguard.Config{StateDir: dir, PodName: "runner", PodUID: "runner-uid", NodeName: "node-one", MinimumBytes: 512 << 20, MaximumBytes: 2 << 30})
			require.NoError(t, err)
			peer := &vmQMPPeer{mu: sync.Mutex{}, uuid: runner.QMPArguments()[1], minimum: 512 << 20, maximum: 2 << 30, actual: 512 << 20, onResize: func(uint64) { t.Error("Host floor must not change QMP") }}
			vmPeer(t, filepath.Join(dir, "qmp.sock"), peer)
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				req := pgRequest(t, session.Actor())
				id := resourceguard.Identity{VMUID: req.Key.Instance.VMUID, ComputeInstanceID: req.Key.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
				s, err := store.Load(ctx)
				require.NoError(t, err)
				s, err = store.BindResource(ctx, s, session.Actor(), id)
				require.NoError(t, err)
				s, err = store.BindPG(ctx, s, session.Actor(), req.Key.Instance)
				require.NoError(t, err)
				prepared, _ := preparedGuest(t, req, id)
				outside := budget.OutsideReservation{ProtocolVersion: 1, Request: req, Identity: id, Incarnation: binding().Incarnation, ResidualProfileID: "explicit-qualified-fixture", SystemOutsidePG: prepared.SystemOutsidePG, Transition: prepared.Transition, Safety: prepared.Safety, HoldUntilExplicitSettlement: true}
				outsideRef := vmReservation(t, client, "outside-original", budgetclaims.OutsideKind, outside)
				prepared.OutsideClaim = &outsideRef
				_, _, err = store.Begin(ctx, s, session.Actor(), req, 1)
				require.NoError(t, err)
				priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.PrepareBudget, req.Key.Instance.ComputeInstanceID, prepared)
				require.NoError(t, runner.Tick(ctx))
				fault := ""
				if mode == "unknown" {
					fault = "publish-lost"
				}
				publisher, allocator := actualHostPublisher(t, client, coord, store, session, fault)
				if mode == "pre-drift" {
					changed := pod.DeepCopy()
					changed.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("12Gi")
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), changed, changed.Namespace))
				}
				ref, err := publisher.Reconcile(ctx)
				if mode == "pre-drift" {
					require.Error(t, err)
					require.Empty(t, ref)
					return nil
				}
				if mode == "unknown" {
					require.Error(t, err)
					require.Empty(t, ref)
					ref, err = publisher.Reconcile(ctx)
				}
				require.NoError(t, err)
				require.NoError(t, ref.Validate())
				held, err := allocator.Load(ctx)
				require.NoError(t, err)
				require.Len(t, held.State().Holds, 1)
				require.EqualValues(t, 11<<30, held.State().Holds[0].Plan.PodMemoryLimitBytes)
				peak, err := held.State().Holds[0].Plan.Bytes()
				require.NoError(t, err)
				require.EqualValues(t, 11<<30, peak)
				cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
				require.NoError(t, err)
				var h budget.HostReservation
				require.NoError(t, json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &h))
				require.EqualValues(t, 11<<30, h.ReservedMemoryBytes)
				reader, err := budgetclaims.New(client.CoreV1(), binding())
				require.NoError(t, err)
				s, err = store.Load(ctx)
				require.NoError(t, err)
				observed, err := reader.Read(ctx, s, prepared, ref)
				require.NoError(t, err)
				require.EqualValues(t, 11<<30, observed.Host.ReservedMemoryBytes)
				if mode == "post-drift" || mode == "recreated" {
					changed := pod.DeepCopy()
					if mode == "post-drift" {
						changed.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("12Gi")
					} else {
						changed.UID = "replacement-pod"
					}
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("pods"), changed, changed.Namespace))
				}
				if mode == "bad-reserved" {
					h.ReservedMemoryBytes -= 16 << 20
					raw, err := json.Marshal(h)
					require.NoError(t, err)
					cm.Data[budgetclaims.ClaimKey] = string(raw)
					require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
				}
				if mode == "post-drift" || mode == "recreated" || mode == "bad-reserved" {
					_, err = reader.Read(ctx, s, prepared, ref)
					require.Error(t, err)
				}
				latest, err := allocator.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, held.ResourceVersion(), latest.ResourceVersion(), "读取失败或CM丢回复恢复不得释放floor")
				peer.mu.Lock()
				require.Zero(t, peer.writes)
				peer.mu.Unlock()
				return nil
			})
			require.NoError(t, err)
		})
	}
}
