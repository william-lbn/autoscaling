package hostscheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"
	"testing"
	"time"
)

// 本夹具从未调用vmguard.Initialize，模拟发布出生前的校验失败；不能删除
// 已存在的受保护来源来伪造未出生。Pod终态来自显式Kube夹具，非真实KVM验收。
func preBirthRetirementFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig, *kubefake.Clientset, *core.Pod) {
	t.Helper()
	s, f, n := schedulerFixture(t)
	pod := legacyVM(t, f)
	mode, mount := vmv1.CpuScalingModeSysfs, true
	inc := "0123456789abcdef0123456789abcdef"
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		vm := f.VMs["testing/vm"]
		vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: inc, ComputeInstanceID: "compute-instance", VMUID: "", ConfigSecretUID: "", Ready: false}
		vm.Spec.CpuScalingMode, vm.Spec.ServiceAccountName = &mode, "original-runner"
		vm.Status = *new(vmv1.VirtualMachineStatus)
		pod = f.Pods["testing/runner"]
		// 原fixture也须覆盖既有完整VM上限+固定Host开销，不能用16Mi伪小floor。
		hardLimit := vm.Spec.Guest.MemorySlotSize.DeepCopy()
		hardLimit.Set(vm.Spec.Guest.MemorySlotSize.Value()*int64(vm.Spec.Guest.MemorySlots.Max) + int64(n.Profile.VMOverheadBytes))
		pod.Spec.Containers[0].Resources.Limits[core.ResourceMemory] = hardLimit
		pod.Spec.RestartPolicy, pod.Spec.ServiceAccountName, pod.Spec.AutomountServiceAccountToken = core.RestartPolicyNever, "original-runner", &mount
		pod.Spec.Containers[0].Name = "neonvm-runner"
		pod.Status.Phase = core.PodRunning
		pod.Status.ContainerStatuses = []core.ContainerStatus{{Name: "neonvm-runner", ContainerID: "containerd://original-runner", State: core.ContainerState{Running: &core.ContainerStateRunning{StartedAt: metav1.Now()}}}}
		pod = pod.DeepCopy()
	})
	store, err := journal.NewStore(f.VM.NeonvmV1().VirtualMachines("testing"), journal.Binding{Namespace: "testing", Name: "vm", VMUID: "vm-uid", Incarnation: inc})
	require.NoError(t, err)
	st, err := store.Initialize(context.Background(), "1", "original-vm-owner")
	require.NoError(t, err)
	_, err = store.ReleaseBootstrap(context.Background(), st.ResourceVersion(), "original-config")
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		vm := f.VMs["testing/vm"]
		vm.Status.Phase, vm.Status.PodName, vm.Status.Node = vmv1.VmRunning, "runner", n.Binding.NodeName
	})
	claims := kubefake.NewSimpleClientset()
	count := 0
	claims.PrependReactor("create", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
		cm := a.(kt.CreateAction).GetObject().(*core.ConfigMap).DeepCopy()
		count++
		cm.UID, cm.ResourceVersion = types.UID(fmt.Sprintf("source-%d", count)), "1"
		if err := claims.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace); err != nil {
			return true, nil, err
		}
		return true, nil, errors.New("fixture CREATE reply lost")
	})
	combined := retirementCore{CoreV1Interface: f.Core.CoreV1(), claims: claims.CoreV1()}
	s.core = combined
	require.NoError(t, s.reconcile(context.Background(), s.domains[n.Binding.NodeName]))

	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	_, err = store.Takeover(context.Background(), snapshot, snapshot.State().Owner, "second-owner-before-start")
	require.NoError(t, err)
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		stamp := metav1.Now()
		f.VMs["testing/vm"].DeletionTimestamp = &stamp
		p := f.Pods["testing/runner"]
		p.DeletionTimestamp = &stamp
		p.Status.Phase = core.PodFailed
		started := p.Status.ContainerStatuses[0].State.Running.StartedAt
		p.Status.ContainerStatuses[0].State = core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: p.Status.ContainerStatuses[0].ContainerID, StartedAt: started, FinishedAt: stamp, ExitCode: 1}}
		pod = p.DeepCopy()
	})
	return s, f, n, claims, pod
}

func TestSchedulerPreBirthRetirementRecoversOriginalClosure(t *testing.T) {
	for _, mode := range []string{"complete", "node-reply", "pod-reply", "vm-reply"} {
		t.Run(mode, func(t *testing.T) {
			s, f, n, claims, pod := preBirthRetirementFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			before, err := d.store.Load(ctx)
			require.NoError(t, err)
			original, err := before.State().Placement(string(pod.UID))
			require.NoError(t, err)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				f.LoseNodeReply = mode == "node-reply"
				f.LosePodReply = mode == "pod-reply"
				f.LoseVMReply = mode == "vm-reply"
			})
			first := s.retirePod(ctx, d, pod)
			if mode == "complete" {
				require.NoError(t, first)
			}
			require.NoError(t, s.retirePod(ctx, d, pod))
			after, err := d.store.Load(ctx)
			require.NoError(t, err)
			require.Empty(t, *after.State().Placements)
			require.Empty(t, after.State().Holds)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p := f.Pods["testing/runner"]
				r, err := hostguard.InspectRetirement(p)
				require.NoError(t, err)
				require.EqualValues(t, 3, r.ProtocolVersion)
				require.Nil(t, r.RunnerBirth)
				require.Nil(t, r.RunnerExit)
				require.NotNil(t, r.PreBirth)
				require.NotContains(t, p.Finalizers, hostguard.PodProtectionFinalizer)
				require.Equal(t, p.Annotations[hostguard.RetirementAnnotation], f.VMs["testing/vm"].Annotations[hostguard.VMRetirementAnnotation])
				require.NoError(t, hostguard.InspectRetirementCompletion(&f.Node, r))
			})
			list, err := claims.CoreV1().ConfigMaps("testing").List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			require.Empty(t, list.Items, "closure must not synthesize birth or exit claims")
			_, _, err = d.store.HoldPlacement(ctx, after, original)
			require.Error(t, err, "terminated original Pod cannot regain capacity")
			final, err := d.store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, after.State(), final.State())
		})
	}
}

func TestSchedulerPreBirthRetirementRejectsUnknownAndUsedSources(t *testing.T) {
	for _, mode := range []string{"not-deleting-vm", "replacement-vm", "restarted", "running", "missing-container", "birth-present", "state-present", "source-unavailable", "used-journal", "journal-changed-after-CAS", "state-present-after-CAS", "operation-hold"} {
		t.Run(mode, func(t *testing.T) {
			s, f, n, claims, pod := preBirthRetirementFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			if mode == "journal-changed-after-CAS" || mode == "state-present-after-CAS" {
				f.Mutate(func(f *dsgatest.SchedulerAPI) { f.LoseNodeReply = true })
				require.Error(t, s.retirePod(ctx, d, pod))
			}
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p := f.Pods["testing/runner"]
				vm := f.VMs["testing/vm"]
				switch mode {
				case "not-deleting-vm":
					vm.DeletionTimestamp = nil
				case "replacement-vm":
					vm.UID = "replacement"
				case "restarted":
					p.Status.ContainerStatuses[0].RestartCount = 1
				case "running":
					p.Status.ContainerStatuses[0].State = core.ContainerState{Running: &core.ContainerStateRunning{StartedAt: metav1.Now()}}
				case "missing-container":
					p.Spec.InitContainers = []core.Container{{Name: "unfinished-init"}}
				case "used-journal", "journal-changed-after-CAS":
					vm.Annotations[control.JournalAnnotation] = "unreadable-used-journal"
				}
			})
			if mode == "birth-present" || mode == "state-present" || mode == "state-present-after-CAS" || mode == "source-unavailable" {
				claims.PrependReactor("get", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
					name := a.(kt.GetAction).GetName()
					if mode == "source-unavailable" {
						return true, nil, errors.New("original source read unavailable")
					}
					if mode == "birth-present" && len(name) > 7 && name[:7] == "dsga-b-" || (mode == "state-present" || mode == "state-present-after-CAS") && len(name) > 7 && name[:7] == "dsga-s-" {
						return true, &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, UID: "unexpected-source"}}, nil
					}
					return false, nil, nil
				})
			}
			before, err := d.store.Load(ctx)
			require.NoError(t, err)
			if mode == "operation-hold" {
				original, err := before.State().Placement(string(pod.UID))
				require.NoError(t, err)
				request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: original.VMUID, ComputeInstanceID: "compute-instance", PGBootID: 1}, IssuerEpoch: 1, Revision: 1, OperationID: control.OperationID{High: 1, Low: 1}}, Kind: control.Grow, ExpectedCapacityGeneration: 1, TargetBuffers: 128, TargetVMMemoryBytes: 3 << 30, BudgetCommitmentID: "original-budget", DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Minute).UnixMilli())}
				_, _, err = d.store.Reserve(ctx, before, hostguard.Plan{Namespace: original.Namespace, VMName: original.VMName, Incarnation: original.Incarnation, RunnerPodUID: original.PodUID, Request: request, BeforeBytes: 1 << 30, HostOverheadBytes: original.HostOverheadBytes, PodMemoryLimitBytes: original.PodMemoryLimitBytes})
				require.NoError(t, err)
				before, err = d.store.Load(ctx)
				require.NoError(t, err)
			}
			require.Error(t, s.retirePod(ctx, d, pod))
			after, err := d.store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, before.State(), after.State(), "unknown proof must preserve original node state")
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Contains(t, f.Pods["testing/runner"].Finalizers, hostguard.PodProtectionFinalizer)
			})
		})
	}
}

func TestPreBirthRetirementRejectsUsedJournalEvenWhenActiveEmpty(t *testing.T) {
	original, err := journal.New("vm-uid", "0123456789abcdef0123456789abcdef", "owner")
	require.NoError(t, err)
	original.Sequence = 2
	original.Owner.Epoch = 2
	require.True(t, unusedPreBirthJournal(original), "owner fencing alone does not invent work")
	for _, mode := range []string{"expired", "high-water", "PG", "resource", "history", "active", "budget", "version"} {
		t.Run(mode, func(t *testing.T) {
			m := original.Clone()
			switch mode {
			case "expired":
				m.ExpiredResults = 1
			case "high-water":
				m.HighWater = &control.Key{}
			case "PG":
				m.PGInstance = &control.Instance{}
			case "resource":
				m.ProtocolVersion = 2
			case "history":
				m.History = []journal.Record{{}}
			case "active":
				m.Active = &journal.Operation{}
			case "budget":
				m.BudgetHeld = &journal.HeldBudget{}
			case "version":
				m.ProtocolVersion = 21
			}
			require.False(t, unusedPreBirthJournal(m))
		})
	}
}

func TestPreBirthRetirementProtocolCannotMasqueradeAsRunnerExit(t *testing.T) {
	s, _, n, _, pod := preBirthRetirementFixture(t)
	require.NoError(t, s.retirePod(context.Background(), s.domains[n.Binding.NodeName], pod))
	canonical, err := s.core.Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	retirement, err := hostguard.InspectRetirement(canonical)
	require.NoError(t, err)
	retirement.ProtocolVersion = 1
	raw, err := json.Marshal(retirement)
	require.NoError(t, err)
	var decoded hostguard.Retirement
	require.Error(t, json.Unmarshal(raw, &decoded), "legacy protocol must reject pre-birth closure")
	retirement.ProtocolVersion = 3
	retirement.PreBirth = nil
	require.Error(t, retirement.Validate())
}

// 在terminalRunner读完原VM之后，birth读取时注入身份变化；下一次GET必须
// 独立拒绝，不能沿用前一读取的UID、RV或bootstrap校验结果。
func TestSchedulerPreBirthRereadsVMIdentityAfterMissingBirth(t *testing.T) {
	for _, mode := range []string{"uid", "empty-rv", "missing-bootstrap", "incarnation"} {
		t.Run(mode, func(t *testing.T) {
			s, f, n, claims, pod := preBirthRetirementFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			changed := false
			claims.PrependReactor("get", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
				name := a.(kt.GetAction).GetName()
				if !changed && len(name) > 7 && name[:7] == "dsga-b-" {
					changed = true
					f.Mutate(func(f *dsgatest.SchedulerAPI) {
						vm := f.VMs["testing/vm"]
						switch mode {
						case "uid":
							vm.UID = "replacement-vm"
						case "empty-rv":
							vm.ResourceVersion = ""
						case "missing-bootstrap":
							vm.Spec.DSGABootstrap = nil
						case "incarnation":
							vm.Spec.DSGABootstrap.Incarnation = "abcdef0123456789abcdef0123456789"
						}
					})
				}
				return false, nil, nil
			})
			before, err := d.store.Load(ctx)
			require.NoError(t, err)
			require.Error(t, s.retirePod(ctx, d, pod))
			require.True(t, changed)
			after, err := d.store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, before.State(), after.State())
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				require.Contains(t, f.Pods["testing/runner"].Finalizers, hostguard.PodProtectionFinalizer)
				require.Empty(t, f.VMs["testing/vm"].Annotations[hostguard.VMRetirementAnnotation])
			})
		})
	}
}

func TestSchedulerEvictedPreBirthNeedsTrustedNodeStopAndUnusedVM(t *testing.T) {
	for _, mode := range []string{"complete", "missing", "ordinary-scope", "state-present", "failed-vm-not-deleting", "registered", "registered-published-state"} {
		t.Run(mode, func(t *testing.T) {
			s, f, n, claims, p := preBirthRetirementFixture(t)
			_, _, _, _, _, sourceExample := ordinaryTrustedStopFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			before, err := d.store.Load(ctx)
			require.NoError(t, err)
			row, err := before.State().Placement(string(p.UID))
			require.NoError(t, err)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p = f.Pods["testing/runner"]
				p.Status.Reason = "Evicted"
				p.Status.ContainerStatuses[0] = core.ContainerStatus{Name: "neonvm-runner", RestartCount: 1,
					State: core.ContainerState{Terminated: &core.ContainerStateTerminated{Reason: "ContainerStatusUnknown", ExitCode: 137}}}
				p = p.DeepCopy()
				if mode == "failed-vm-not-deleting" {
					f.VMs["testing/vm"].DeletionTimestamp = nil
				}
			})
			var source hostguard.OrdinaryStopSource
			require.NoError(t, json.Unmarshal([]byte(sourceExample.Data[hostguard.OrdinaryStopSourceKey]), &source))
			source.Scope = hostguard.ManagedPreBirthStopScope
			registered := mode == "registered" || mode == "registered-published-state"
			if registered {
				source.Scope = hostguard.UnpublishedBootStopScope
			}
			if mode == "ordinary-scope" {
				source.Scope = hostguard.OrdinaryStopScope
			}
			source.Binding, source.Origin, source.Placement = n.Binding, before.State().Origin, row
			source.ContainerNames = []string{"app/neonvm-runner"}
			source.Before = hostguard.OrdinaryStopPodFacts{Namespace: p.Namespace, Name: p.Name, UID: string(p.UID),
				NodeName: p.Spec.NodeName, Phase: p.Status.Phase, SpecSHA256: ordinaryTestCanonical(t, p.Spec), StatusSHA256: ordinaryTestCanonical(t, p.Status)}
			source.After = source.Before
			raw, err := json.Marshal(source)
			require.NoError(t, err)
			cm := sourceExample.DeepCopy()
			cm.Name = hostguard.OrdinaryStopClaimName(string(p.UID))
			if registered {
				cm.Name = hostguard.UnpublishedBootStopClaimName(string(p.UID))
				retainedUnpublishedRunnerFixture(t, claims, row, n, mode == "registered-published-state")
			}
			cm.Annotations[hostguard.OrdinaryStopSourceMarker] = source.Scope
			cm.Data[hostguard.OrdinaryStopSourceKey] = string(raw)
			require.NoError(t, claims.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
			if mode == "state-present" {
				b := journal.Binding{Namespace: row.Namespace, Name: row.VMName, VMUID: types.UID(row.VMUID), Incarnation: row.Incarnation}
				cm := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: vmguard.BirthSourceNames(b)[1], Namespace: b.Namespace}}
				require.NoError(t, claims.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
			}
			ref := hostguard.OrdinaryStopReference{Claim: budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)},
				SHA256: ordinaryTestHash(raw), SpecSHA256: source.After.SpecSHA256}
			refRaw, err := json.Marshal(ref)
			require.NoError(t, err)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p = f.Pods["testing/runner"]
				if mode != "missing" {
					key := hostguard.OrdinaryStopSourceAnnotation
					if registered {
						key = hostguard.UnpublishedBootStopSourceAnnotation
					}
					p.Annotations[key] = string(refRaw)
				}
				p = p.DeepCopy()
			})
			d.store, err = hostguard.NewStore(s.core, f.Core.CoordinationV1(), n.Binding, s.holder)
			require.NoError(t, err)
			s.domains[n.Binding.NodeName] = d
			err = s.reconcile(ctx, d)
			after, readErr := d.store.Load(ctx)
			require.NoError(t, readErr)
			if mode != "complete" && mode != "registered" {
				require.Error(t, err)
				original, err := after.State().Placement(string(p.UID))
				require.NoError(t, err)
				require.Equal(t, row, original)
				return
			}
			require.NoError(t, err)
			_, err = after.State().Placement(string(p.UID))
			require.Error(t, err)
			actual, err := s.core.Pods(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
			require.NoError(t, err)
			intent, err := hostguard.InspectRetirement(actual)
			require.NoError(t, err)
			if registered {
				require.EqualValues(t, 5, intent.ProtocolVersion)
				require.Equal(t, ref, intent.UnpublishedBoot.NodeStop)
				require.NotNil(t, intent.RunnerBirth)
				require.Nil(t, intent.RunnerExit)
			} else {
				require.EqualValues(t, 4, intent.ProtocolVersion)
				require.Equal(t, &ref, intent.ManagedPreBirthStopSource)
			}
			require.NotContains(t, actual.Finalizers, hostguard.PodProtectionFinalizer)
			require.NoError(t, s.retirePod(ctx, d, p))
		})
	}
}

func retainedUnpublishedRunnerFixture(t *testing.T, claims *kubefake.Clientset, p hostguard.Placement, n NodeConfig, published bool) {
	t.Helper()
	binding := journal.Binding{Namespace: p.Namespace, Name: p.VMName, VMUID: types.UID(p.VMUID), Incarnation: p.Incarnation}
	names := vmguard.BirthSourceNames(binding)
	birthRef, stateRef := budget.ObjectRef{Name: names[0], UID: "original-birth"}, budget.ObjectRef{Name: names[1], UID: "original-state"}
	identity := vmguard.Identity{VMUID: p.VMUID, Incarnation: p.Incarnation, RunnerPodUID: p.PodUID,
		NodeUID: string(n.Binding.NodeUID), RunnerBootID: "0123456789abcdef0123456789abcdef",
		MinimumBytes: 8 << 20, MaximumBytes: 16 << 20, BlockBytes: 8 << 20}
	birth := vmguard.Birth{ProtocolVersion: 1, Identity: identity, StateClaim: stateRef}
	state := vmguard.Publication{ProtocolVersion: 1, Identity: identity}
	if published {
		state.BindingClaim = &birthRef
	}
	controller := true
	for i, ref := range []budget.ObjectRef{birthRef, stateRef} {
		kind, immutable, value := "binding-v1", true, any(birth)
		if i == 1 {
			kind, immutable, value = "state-v1", false, state
		}
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		cm := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: binding.Namespace, Name: ref.Name, UID: types.UID(ref.UID), ResourceVersion: "1",
			Labels: map[string]string{vmguard.ClaimLabel: kind}, Finalizers: []string{vmguard.ProtectionFinalizer},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: binding.Name,
				UID: binding.VMUID, Controller: &controller, BlockOwnerDeletion: &controller}}},
			Immutable: &immutable, Data: map[string]string{vmguard.ClaimKey: string(raw)}}
		require.NoError(t, claims.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
	}
}
