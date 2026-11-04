package hostscheduler

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

func retainedOrdinaryFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig, *core.Pod) {
	t.Helper()
	s, f, n := schedulerFixture(t)
	p := boundedPod("old-ordinary", n.Binding.NodeName, "2Gi")
	p.Spec.SchedulerName = core.DefaultSchedulerName
	restart := core.ContainerRestartPolicyAlways
	p.Spec.InitContainers = []core.Container{
		{Name: "sidecar", RestartPolicy: &restart, Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("512Mi")}}},
		{Name: "init", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceMemory: resource.MustParse("3Gi")}}},
	}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/old-ordinary"] = p.DeepCopy() })
	require.NoError(t, s.reconcile(context.Background(), s.domains[n.Binding.NodeName]))
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		p := f.Pods["testing/old-ordinary"]
		p.Status.Phase = core.PodFailed
		p.Status.ContainerStatuses = []core.ContainerStatus{{Name: "app", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{Reason: "ContainerStatusUnknown"}}}}
		now := metav1.Now()
		p.DeletionTimestamp = &now
	})
	p, err := f.Core.CoreV1().Pods("testing").Get(context.Background(), "old-ordinary", metav1.GetOptions{})
	require.NoError(t, err)
	return s, f, n, p
}

func TestSchedulerUnknownOrdinaryRetainsOriginalAllocationAtStartup(t *testing.T) {
	s, f, n, p := retainedOrdinaryFixture(t)
	ctx := context.Background()
	before, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	old, err := before.State().Placement(string(p.UID))
	require.NoError(t, err)
	require.EqualValues(t, (3<<30)+(512<<20), old.MemoryBytes)
	_, err = hostguard.TerminalPodDigest(p)
	require.Error(t, err, "Unknown必须仍无退出证明")
	writes := [3]int{}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites} })
	restarted, err := New(f.Core.CoreV1(), f.Core.CoordinationV1(), f.VM, Config{Nodes: []NodeConfig{n}}, n.Profile.SchedulerName)
	require.NoError(t, err)
	canonical, held, err := restarted.Reserve(ctx, n.Binding.NodeName, p, 0)
	require.NoError(t, err, "同步初始事件须能确认完整旧占账，而非永久重试终止证明")
	require.Equal(t, old, held)
	require.Equal(t, p, canonical)
	after, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), after.State())
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
	})
	// 原owner循环也不再把未知普通Pod当作必须释放的动作。
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	next := boundedPod("replacement", "", "1Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/replacement"] = next.DeepCopy() })
	_, newHeld, err := restarted.Reserve(ctx, n.Binding.NodeName, next, 0)
	require.NoError(t, err)
	final, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.Len(t, *final.State().Placements, 2)
	oldAgain, err := final.State().Placement(string(p.UID))
	require.NoError(t, err)
	require.Equal(t, old, oldAgain)
	oldBytes, err := old.Bytes()
	require.NoError(t, err)
	newBytes, err := newHeld.Bytes()
	require.NoError(t, err)
	used, err := final.State().UsedBytes()
	require.NoError(t, err)
	require.Equal(t, n.SystemBytes+oldBytes+newBytes, used)
	tooBig := boundedPod("cannot-reuse-unknown", "", "4Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/cannot-reuse-unknown"] = tooBig.DeepCopy() })
	_, _, err = restarted.Reserve(ctx, n.Binding.NodeName, tooBig, 0)
	require.Error(t, err, "旧Unknown预算不得被新UID重复利用")
}

func TestSchedulerRetainedOrdinaryRejectsUnknownSourcesWithoutWrites(t *testing.T) {
	for _, scenario := range []string{"missing_row_without_protection", "replaced_uid", "retirement_intent", "vm_owner", "migration_owner", "node_drift", "scheduler_drift", "resources_drift", "missing_limit", "init_limit_missing", "ephemeral", "larger_limit", "lease_expired", "lease_replaced", "node_replaced", "node_capacity_short"} {
		t.Run(scenario, func(t *testing.T) {
			s, f, n, expected := retainedOrdinaryFixture(t)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p := f.Pods["testing/old-ordinary"]
				switch scenario {
				case "missing_row_without_protection", "replaced_uid", "retirement_intent":
					p.UID = "unknown-not-original-uid"
					if scenario == "missing_row_without_protection" {
						p.Finalizers = nil
					}
					if scenario == "retirement_intent" {
						p.Annotations = map[string]string{hostguard.RetirementAnnotation: "unconfirmed-intent"}
					}
				case "vm_owner":
					p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: "vm", UID: "vm-uid"}}
				case "migration_owner":
					p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachineMigration", Name: "migration", UID: "migration-uid"}}
				case "node_drift":
					p.Spec.NodeName = "node-other"
				case "scheduler_drift":
					p.Spec.SchedulerName = "changed-scheduler"
				case "resources_drift":
					p.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("1Gi")
				case "missing_limit":
					delete(p.Spec.Containers[0].Resources.Limits, core.ResourceMemory)
				case "init_limit_missing":
					delete(p.Spec.InitContainers[0].Resources.Limits, core.ResourceMemory)
				case "ephemeral":
					p.Spec.EphemeralContainers = []core.EphemeralContainer{{EphemeralContainerCommon: core.EphemeralContainerCommon{Name: "debug"}}}
				case "larger_limit":
					p.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("5Gi")
				case "lease_expired":
					stamp := metav1.NewMicroTime(time.Now().Add(-2 * time.Minute))
					f.Lease.Spec.RenewTime = &stamp
				case "lease_replaced":
					f.Lease.UID = "replacement-lease"
				case "node_replaced":
					f.Node.UID = "replacement-node"
				case "node_capacity_short":
					f.Node.Status.Allocatable[core.ResourceMemory] = resource.MustParse("1Gi")
				}
				// 对资源/owner负例必须走到fresh实际值的预算门；另单独保留watch漂移负例。
				if scenario != "scheduler_drift" && scenario != "resources_drift" && scenario != "node_drift" && scenario != "replaced_uid" {
					expected = p.DeepCopy()
				}
			})
			before, err := s.domains[n.Binding.NodeName].store.Load(context.Background())
			if scenario != "node_replaced" {
				require.NoError(t, err)
			}
			writes := [3]int{}
			f.Mutate(func(f *dsgatest.SchedulerAPI) { writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites} })
			_, _, err = s.Reserve(context.Background(), n.Binding.NodeName, expected, 0)
			require.Error(t, err)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
				if scenario != "retirement_intent" {
					require.Empty(t, f.Pods["testing/old-ordinary"].Annotations[hostguard.RetirementAnnotation])
				}
				if scenario != "missing_row_without_protection" {
					require.Contains(t, f.Pods["testing/old-ordinary"].Finalizers, hostguard.PodProtectionFinalizer)
				}
			})
			if before != nil {
				after, err := s.domains[n.Binding.NodeName].store.Load(context.Background())
				require.NoError(t, err)
				require.Equal(t, before.State(), after.State())
			}
		})
	}
}

func TestSchedulerRetainedOrdinaryDoesNotReplaceCompleteRetirement(t *testing.T) {
	s, f, n, p := retainedOrdinaryFixture(t)
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		p := f.Pods["testing/old-ordinary"]
		now := metav1.Now()
		p.Status.ContainerStatuses = []core.ContainerStatus{{Name: "app", ContainerID: "containerd://old-app", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: "containerd://old-app", StartedAt: now, FinishedAt: now}}}}
		p.Status.InitContainerStatuses = []core.ContainerStatus{
			{Name: "sidecar", ContainerID: "containerd://old-sidecar", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: "containerd://old-sidecar", StartedAt: now, FinishedAt: now}}},
			{Name: "init", ContainerID: "containerd://old-init", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: "containerd://old-init", StartedAt: now, FinishedAt: now}}},
		}
	})
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	after, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	_, err = after.State().Placement(string(p.UID))
	require.Error(t, err)
	actual, err := f.Core.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, hostguard.InspectRetiredPod(after.Node(), actual))
	require.NotContains(t, actual.Finalizers, hostguard.PodProtectionFinalizer)
}

// 模拟真实protect已确认、随后的Node占账未确认：原Pod仍在既定Node且受保护，
// 不能凭Unknown释放，也不能让漏记占用成为可再次授予的容量。
func protectedMissingOrdinaryFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig, *core.Pod) {
	t.Helper()
	s, f, n := schedulerFixture(t)
	require.NoError(t, s.reconcile(context.Background(), s.domains[n.Binding.NodeName]))
	p := boundedPod("protected-unknown-loader", n.Binding.NodeName, "10Mi")
	p.Spec.SchedulerName = core.DefaultSchedulerName
	p.Finalizers = []string{hostguard.PodProtectionFinalizer}
	p.Status.Phase = core.PodFailed
	p.Status.ContainerStatuses = []core.ContainerStatus{{Name: "app", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{Reason: "ContainerStatusUnknown"}}}}
	now := metav1.Now()
	p.DeletionTimestamp = &now
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[p.Namespace+"/"+p.Name] = p.DeepCopy() })
	return s, f, n, p
}

func TestSchedulerProtectedUnknownOrdinaryCompletesConservativeAccounting(t *testing.T) {
	for _, scenario := range []string{"confirmed", "lost_CAS_reply"} {
		t.Run(scenario, func(t *testing.T) {
			s, f, n, p := protectedMissingOrdinaryFixture(t)
			ctx := context.Background()
			before, err := s.domains[n.Binding.NodeName].store.Load(ctx)
			require.NoError(t, err)
			oldUsed, err := before.State().UsedBytes()
			require.NoError(t, err)
			writes := [3]int{}
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites}
				f.LoseNodeReply = scenario == "lost_CAS_reply"
			})
			_, held, err := s.Reserve(ctx, n.Binding.NodeName, p, 0)
			if scenario == "lost_CAS_reply" {
				var unknown *hostguard.UnconfirmedWrite
				require.ErrorAs(t, err, &unknown, "已发CAS丢回复必须保Unknown")
				_, held, err = s.Reserve(ctx, n.Binding.NodeName, p, 0)
			}
			require.NoError(t, err)
			require.EqualValues(t, 10<<20, held.MemoryBytes)
			require.EqualValues(t, 26<<20, mustPlacementBytes(t, held))
			final, err := s.domains[n.Binding.NodeName].store.Load(ctx)
			require.NoError(t, err)
			used, err := final.State().UsedBytes()
			require.NoError(t, err)
			require.Equal(t, oldUsed+mustPlacementBytes(t, held), used)
			require.Len(t, *final.State().Placements, 1)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Equal(t, writes[0]+1, f.NodeWrites)
				require.Equal(t, writes[1], f.PodWrites)
				require.Equal(t, writes[2], f.VMWrites)
				require.Empty(t, f.Pods[p.Namespace+"/"+p.Name].Annotations[hostguard.RetirementAnnotation])
				require.Contains(t, f.Pods[p.Namespace+"/"+p.Name].Finalizers, hostguard.PodProtectionFinalizer)
			})
			_, _, err = s.Reserve(ctx, n.Binding.NodeName, p, 0)
			require.NoError(t, err)
			again, err := s.domains[n.Binding.NodeName].store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, final.State(), again.State(), "原输入只读收敛，不新增第二行或降水位")
			_, err = hostguard.TerminalPodDigest(p)
			require.Error(t, err)
		})
	}
}

func mustPlacementBytes(t *testing.T, p hostguard.Placement) control.Uint64 {
	t.Helper()
	n, err := p.Bytes()
	require.NoError(t, err)
	return n
}

func TestSchedulerMissingOrdinaryAccountingKeepsCapacityAndOriginalSourceGates(t *testing.T) {
	for _, scenario := range []string{"unbound", "missing_finalizer", "retirement_intent", "unbounded", "VM", "migration", "expired_lease", "capacity", "stale_Node_RV"} {
		t.Run(scenario, func(t *testing.T) {
			s, f, n, p := protectedMissingOrdinaryFixture(t)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				q := f.Pods[p.Namespace+"/"+p.Name]
				switch scenario {
				case "unbound":
					q.Spec.NodeName = ""
				case "missing_finalizer":
					q.Finalizers = nil
				case "retirement_intent":
					q.Annotations = map[string]string{hostguard.RetirementAnnotation: "prior-intent"}
				case "unbounded":
					delete(q.Spec.Containers[0].Resources.Limits, core.ResourceMemory)
				case "VM":
					q.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: "vm", UID: "vm-uid"}}
				case "migration":
					q.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachineMigration", Name: "migration", UID: "migration-uid"}}
				case "expired_lease":
					stamp := metav1.NewMicroTime(time.Now().Add(-2 * time.Minute))
					f.Lease.Spec.RenewTime = &stamp
				case "capacity":
					q.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = resource.MustParse("8Gi")
				}
				p = q.DeepCopy()
			})
			ctx := context.Background()
			before, err := s.domains[n.Binding.NodeName].store.Load(ctx)
			require.NoError(t, err)
			writes := [3]int{}
			f.Mutate(func(f *dsgatest.SchedulerAPI) { writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites} })
			if scenario == "stale_Node_RV" {
				f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Node.ResourceVersion = "changed-after-snapshot" })
				_, err = s.domains[n.Binding.NodeName].store.ObserveRetainedOrdinary(ctx, before, p)
			} else {
				_, _, err = s.Reserve(ctx, n.Binding.NodeName, p, 0)
			}
			require.Error(t, err)
			after, err := s.domains[n.Binding.NodeName].store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, before.State(), after.State())
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
			})
		})
	}
}

func confirmedRetiredOrdinaryFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig, *core.Pod) {
	t.Helper()
	s, f, n := schedulerFixture(t)
	p := boundedPod("confirmed-retired", n.Binding.NodeName, "1Gi")
	p.Spec.SchedulerName = core.DefaultSchedulerName
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods[p.Namespace+"/"+p.Name] = p.DeepCopy() })
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		p := f.Pods[p.Namespace+"/"+p.Name]
		p.Status.Phase = core.PodSucceeded
		now := metav1.Now()
		p.Status.ContainerStatuses = []core.ContainerStatus{{Name: "app", ContainerID: "containerd://confirmed-retired", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: "containerd://confirmed-retired", StartedAt: now, FinishedAt: now}}}}
	})
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	actual, err := f.Core.CoreV1().Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	require.NoError(t, err)
	return s, f, n, actual
}

func TestSchedulerConfirmedRetiredOrdinaryInitialEventIsReadOnly(t *testing.T) {
	s, f, n, p := confirmedRetiredOrdinaryFixture(t)
	ctx := context.Background()
	before, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	writes := [3]int{}
	f.Mutate(func(f *dsgatest.SchedulerAPI) { writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites} })
	canonical, err := s.ObserveRetiredOrdinary(ctx, n.Binding.NodeName, p)
	require.NoError(t, err)
	require.Equal(t, p, canonical)
	after, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), after.State())
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
	})
	require.Empty(t, *after.State().Placements)
	// 后继真实holder可观察同一已闭合事实，不需要伪造旧关闭epoch。
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		holder := "new-real-holder"
		f.Lease.Spec.HolderIdentity = &holder
		*f.Lease.Spec.LeaseTransitions++
	})
	_, err = s.ObserveRetiredOrdinary(ctx, n.Binding.NodeName, p)
	require.NoError(t, err)
}

func TestSchedulerConfirmedRetiredOrdinaryRejectsIncompleteOrChangedProof(t *testing.T) {
	for _, scenario := range []string{"missing_intent", "unconfirmed_digest", "protection_present", "unknown_termination", "missing_row_alone", "different_UID", "different_Node", "different_scheduler", "VM_owner", "migration_owner", "expired_lease", "stale_Node_RV"} {
		t.Run(scenario, func(t *testing.T) {
			s, f, n, p := confirmedRetiredOrdinaryFixture(t)
			ctx := context.Background()
			expected := p.DeepCopy()
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				q := f.Pods[p.Namespace+"/"+p.Name]
				switch scenario {
				case "missing_intent", "missing_row_alone":
					delete(q.Annotations, hostguard.RetirementAnnotation)
				case "unconfirmed_digest":
					var v map[string]any
					require.NoError(t, json.Unmarshal([]byte(q.Annotations[hostguard.RetirementAnnotation]), &v))
					v["terminal_sha256"] = strings.Repeat("a", 64)
					raw, err := json.Marshal(v)
					require.NoError(t, err)
					q.Annotations[hostguard.RetirementAnnotation] = string(raw)
				case "protection_present":
					q.Finalizers = []string{hostguard.PodProtectionFinalizer}
				case "unknown_termination":
					q.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.Time{}
				case "different_UID":
					q.UID = "replaced-retired-UID"
				case "different_Node":
					q.Spec.NodeName = "other-Node"
				case "different_scheduler":
					q.Spec.SchedulerName = "changed-scheduler"
				case "VM_owner":
					q.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: "vm", UID: "vm-UID"}}
				case "migration_owner":
					q.OwnerReferences = []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachineMigration", Name: "migration", UID: "migration-UID"}}
				case "expired_lease":
					stamp := metav1.NewMicroTime(time.Now().Add(-2 * time.Minute))
					f.Lease.Spec.RenewTime = &stamp
				}
				if scenario != "different_UID" && scenario != "different_Node" && scenario != "different_scheduler" {
					expected = q.DeepCopy()
				}
			})
			before, err := s.domains[n.Binding.NodeName].store.Load(ctx)
			require.NoError(t, err)
			writes := [3]int{}
			f.Mutate(func(f *dsgatest.SchedulerAPI) { writes = [3]int{f.NodeWrites, f.PodWrites, f.VMWrites} })
			if scenario == "stale_Node_RV" {
				f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Node.ResourceVersion = "changed-after-retirement-read" })
				err = s.domains[n.Binding.NodeName].store.ObserveRetiredOrdinary(ctx, before, expected)
			} else {
				_, err = s.ObserveRetiredOrdinary(ctx, n.Binding.NodeName, expected)
			}
			require.Error(t, err)
			after, err := s.domains[n.Binding.NodeName].store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, before.State(), after.State())
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Equal(t, writes, [3]int{f.NodeWrites, f.PodWrites, f.VMWrites})
			})
		})
	}
}
