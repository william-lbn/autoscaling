package hostscheduler

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

func retainedManagedFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig, *core.Pod) {
	t.Helper()
	s, f, n, pod := schedulerPodFloorFixture(t)
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	store := s.domains[n.Binding.NodeName].store
	snapshot, err := store.Load(ctx)
	require.NoError(t, err)
	row, err := snapshot.State().Placement(string(pod.UID))
	require.NoError(t, err)
	// 保留一份原在途Host承诺：没有Claim不能投影成完成，也不能因事件恢复清除。
	request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: row.VMUID, ComputeInstanceID: "compute", PGBootID: 1}, IssuerEpoch: 1, Revision: 1, OperationID: control.OperationID{High: 1, Low: 1}}, Kind: control.Grow, ExpectedCapacityGeneration: 1, TargetBuffers: 1024, TargetVMMemoryBytes: 8 << 30, BudgetCommitmentID: "original-unknown-budget", DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Minute).UnixMilli())}
	_, _, err = store.Reserve(ctx, snapshot, hostguard.Plan{Namespace: row.Namespace, VMName: row.VMName, Incarnation: row.Incarnation, RunnerPodUID: row.PodUID, Request: request, BeforeBytes: row.MemoryBytes, HostOverheadBytes: row.HostOverheadBytes, PodMemoryLimitBytes: row.PodMemoryLimitBytes})
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		p := f.Pods["testing/runner"]
		stamp := metav1.Now()
		p.DeletionTimestamp, p.Status.Phase = &stamp, core.PodFailed
		p.Status.ContainerStatuses = []core.ContainerStatus{{Name: "app", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{Reason: "ContainerStatusUnknown"}}}}
		// 原VM/Birth可能已缺失或未知；只读原成本无需从它们制造新的执行事实。
		delete(f.VMs, "testing/vm")
	})
	// 真实调用者的对象来自Kube JSON读回。metav1.Time会去除内存中的
	// monotonic/纳秒，不应拿fixture内存对象冒充规范API对象作深等比较。
	pod, err = f.Core.CoreV1().Pods("testing").Get(ctx, "runner", metav1.GetOptions{})
	require.NoError(t, err)
	return s, f, n, pod
}

func TestSchedulerRetainedManagedUnknownObservesExistingCostWithoutGrant(t *testing.T) {
	for _, phase := range []core.PodPhase{core.PodFailed, core.PodRunning} {
		t.Run(string(phase), func(t *testing.T) {
			s, f, n, pod := retainedManagedFixture(t)
			ctx := context.Background()
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				f.Pods["testing/runner"].Status.Phase = phase
			})
			pod, err := f.Core.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			before, err := s.domains[n.Binding.NodeName].store.Load(ctx)
			require.NoError(t, err)
			require.Len(t, before.State().Holds, 1)
			require.Nil(t, before.State().Holds[0].Claim)
			writes := [3]int{}
			f.Mutate(func(f *dsgatest.SchedulerAPI) { writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites} })
			restarted, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
			require.NoError(t, err)
			canonical, held, err := restarted.ObserveRetainedManaged(ctx, n.Binding.NodeName, pod)
			require.NoError(t, err)
			original, err := before.State().Placement(string(pod.UID))
			require.NoError(t, err)
			require.Equal(t, original, held)
			require.Equal(t, pod, canonical, "只读观察不修传播注解，也不移除保护")
			bytes, err := held.Bytes()
			require.NoError(t, err)
			require.EqualValues(t, 11<<30, bytes)
			_, _, err = restarted.Reserve(ctx, n.Binding.NodeName, pod, 0)
			require.Error(t, err, "事件观察成功不能变成Framework Reserve成功")
			require.Error(t, restarted.CheckPlacement(ctx, n.Binding.NodeName, pod, 0))
			after, err := s.domains[n.Binding.NodeName].store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, before.State(), after.State(), "原峰值/水位/Unknown/完整模型不能改动")
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
			})
			// 新UID仍不能使用已占11Gi原成本；整个原域24Gi，系统域另占1Gi。
			candidate := boundedPod("cannot-use-retained-managed", "", "13Gi")
			f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/"+candidate.Name] = candidate.DeepCopy() })
			require.Error(t, restarted.CheckPlacement(ctx, n.Binding.NodeName, candidate, 0))
		})
	}
}

func TestSchedulerRetainedManagedRejectsMissingOrChangedAccounting(t *testing.T) {
	for _, mode := range []string{"no-row", "ordinary-row", "wrong-owner", "multiple-owners", "wrong-node", "wrong-scheduler", "unbound", "no-protection", "larger-cap", "smaller-cap", "missing-cap", "ephemeral", "live", "new-pod-uid", "new-node-uid", "new-lease-uid", "expired-lease", "short-node", "profile"} {
		t.Run(mode, func(t *testing.T) {
			_, f, n, pod := retainedManagedFixture(t)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p := f.Pods["testing/runner"]
				switch mode {
				case "no-row", "ordinary-row":
					var m hostguard.Model
					require.NoError(t, json.Unmarshal([]byte(f.Node.Annotations[hostguard.LedgerAnnotation]), &m))
					m.Holds = []hostguard.Hold{}
					if mode == "no-row" {
						*m.Placements = []hostguard.Placement{}
					} else {
						row := &(*m.Placements)[0]
						row.Managed, row.VMUID, row.VMName, row.Incarnation, row.PodMemoryLimitBytes = false, "", "", "", 0
						row.HostOverheadBytes = n.Profile.PodOverheadBytes
					}
					require.NoError(t, m.Validate())
					raw, err := json.Marshal(m)
					require.NoError(t, err)
					f.Node.Annotations[hostguard.LedgerAnnotation] = string(raw)
				case "wrong-owner":
					p.OwnerReferences[0].UID = "new-vm"
				case "multiple-owners":
					p.OwnerReferences = append(p.OwnerReferences, p.OwnerReferences[0])
				case "wrong-node":
					p.Spec.NodeName = "other-node"
				case "wrong-scheduler":
					p.Spec.SchedulerName = "other-scheduler"
				case "unbound":
					p.Spec.NodeName = ""
				case "no-protection":
					p.Finalizers = nil
				case "larger-cap":
					p.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("12Gi")
				case "smaller-cap":
					p.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("10Gi")
				case "missing-cap":
					delete(p.Spec.Containers[0].Resources.Limits, core.ResourceMemory)
				case "ephemeral":
					p.Spec.EphemeralContainers = []core.EphemeralContainer{{}}
				case "live":
					p.Status.Phase, p.DeletionTimestamp = core.PodRunning, nil
				case "new-pod-uid":
					p.UID = "replaced-pod"
				case "new-node-uid":
					f.Node.UID = "replaced-node"
				case "new-lease-uid":
					f.Lease.UID = "replaced-lease"
				case "expired-lease":
					stamp := metav1.NewMicroTime(time.Now().Add(-time.Hour))
					f.Lease.Spec.RenewTime = &stamp
				case "short-node":
					f.Node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("10Gi")
				case "profile":
					n.Profile.ID = "other-original-profile"
				}
			})
			if mode != "new-pod-uid" {
				var err error
				pod, err = f.Core.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
				require.NoError(t, err)
			}
			writes := [3]int{}
			var raw string
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites}
				raw = f.Node.Annotations[hostguard.LedgerAnnotation]
			})
			candidate, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
			require.NoError(t, err)
			_, _, err = candidate.ObserveRetainedManaged(context.Background(), n.Binding.NodeName, pod)
			require.Error(t, err)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
				require.Equal(t, raw, f.Node.Annotations[hostguard.LedgerAnnotation])
			})
		})
	}
}

type retainedManagedRaceCore struct {
	coreclient.CoreV1Interface
	onRead func(*core.Pod)
}
type retainedManagedRacePods struct {
	coreclient.PodInterface
	onRead func(*core.Pod)
}

func (c retainedManagedRaceCore) Pods(ns string) coreclient.PodInterface {
	return retainedManagedRacePods{c.CoreV1Interface.Pods(ns), c.onRead}
}
func (p retainedManagedRacePods) Get(ctx context.Context, name string, opts metav1.GetOptions) (*core.Pod, error) {
	pod, err := p.PodInterface.Get(ctx, name, opts)
	if err == nil {
		p.onRead(pod)
	}
	return pod, err
}

func TestSchedulerRetainedManagedRejectsReadRaces(t *testing.T) {
	for _, mode := range []string{"pod-rv", "node-rv", "node-uid"} {
		t.Run(mode, func(t *testing.T) {
			s, f, n, pod := retainedManagedFixture(t)
			snapshot, err := s.domains[n.Binding.NodeName].store.Load(context.Background())
			require.NoError(t, err)
			core := retainedManagedRaceCore{f.Core.CoreV1(), func(returned *core.Pod) {
				f.Mutate(func(f *dsgatest.SchedulerAPI) {
					if mode == "node-rv" {
						f.Node.ResourceVersion = "after-pod-read"
					}
					if mode == "node-uid" {
						f.Node.UID = "after-pod-read"
					}
				})
				if mode == "pod-rv" {
					returned.ResourceVersion = "replacement-read"
				}
			}}
			store, err := hostguard.NewStore(core, f.Core.CoordinationV1(), n.Binding, "another-reader-"+strings.Repeat("a", 8))
			require.NoError(t, err)
			_, err = store.ObserveRetainedManaged(context.Background(), snapshot, pod)
			require.Error(t, err, "原Pod双读或最终Node读漂移均不能确认旧占账")
			if mode == "pod-rv" {
				require.ErrorContains(t, err, "Pod changed across")
			} else if mode == "node-rv" {
				require.ErrorContains(t, err, "Node changed during")
			} else {
				require.ErrorContains(t, err, "Node UID/name changed")
			}
		})
	}
}
