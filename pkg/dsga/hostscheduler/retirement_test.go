package hostscheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	kt "k8s.io/client-go/testing"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

type retirementCore struct {
	coreclient.CoreV1Interface
	claims coreclient.CoreV1Interface
}

func (c retirementCore) ConfigMaps(namespace string) coreclient.ConfigMapInterface {
	return c.claims.ConfigMaps(namespace)
}

func TestSchedulerExitWaitChildHelper(t *testing.T) {
	if os.Getenv("DSGA_SCHEDULER_WAIT_CHILD") == "1" {
		os.Exit(0)
	}
}

// 实际Runner Start/Wait、本地WAL/退出CM、实际Scheduler owner路径和typed
// HTTP Node/Pod/VM UID-RV CAS组合。子进程是本测试二进制，CM/Pod终态由显式
// Kube夹具提供；不是QEMU/KVM、真实apiserver/RBAC或物理Host回收验收。
func retiredRunnerFixture(t *testing.T) (*Service, *dsgatest.SchedulerAPI, NodeConfig, *kubefake.Clientset, *core.Pod) {
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
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	runner, err := vmguard.Initialize(context.Background(), store, combined, f.Core.CoordinationV1(), vmguard.Config{StateDir: dir, PodName: pod.Name, PodUID: string(pod.UID), NodeName: n.Binding.NodeName, MinimumBytes: 1 << 30, MaximumBytes: 4 << 30})
	require.NoError(t, err)
	self, err := os.Executable()
	require.NoError(t, err)
	program := filepath.Join(t.TempDir(), "qemu-system-x86_64")
	require.NoError(t, os.Symlink(self, program))
	args := append([]string{"-test.run=^TestSchedulerExitWaitChildHelper$", "--"}, runner.QMPArguments()...)
	cmd := exec.Command(program, args...)
	cmd.Env = append(os.Environ(), "DSGA_SCHEDULER_WAIT_CHILD=1")
	require.NoError(t, runner.RunQEMU(context.Background(), cmd))
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		p := f.Pods["testing/runner"]
		started := p.Status.ContainerStatuses[0].State.Running.StartedAt
		p.Status.ContainerStatuses[0].State = core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: "containerd://original-runner", StartedAt: started, FinishedAt: metav1.Now()}}
		p.Status.Phase = core.PodSucceeded
		stamp := metav1.Now()
		p.DeletionTimestamp = &stamp
		pod = p.DeepCopy()
	})
	return s, f, n, claims, pod
}

func TestSchedulerWholePodRetirementRecoversOriginalUnknownWrites(t *testing.T) {
	for _, what := range []string{"complete", "node-reply", "pod-intent-reply", "vm-anchor-reply", "node-v6"} {
		t.Run(what, func(t *testing.T) {
			s, f, n, _, pod := retiredRunnerFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			if what == "node-v6" {
				// 已使用关闭Through语义的Node不能在另一个Pod退出时被降级。
				// 这是既有Node版本输入夹具；真正的Through生产由owner组合覆盖。
				f.Mutate(func(f *dsgatest.SchedulerAPI) {
					var model hostguard.Model
					require.NoError(t, json.Unmarshal([]byte(f.Node.Annotations[hostguard.LedgerAnnotation]), &model))
					model.ProtocolVersion = 6
					raw, err := json.Marshal(model)
					require.NoError(t, err)
					f.Node.Annotations[hostguard.LedgerAnnotation] = string(raw)
					f.Node.ResourceVersion += "-v6"
				})
			}
			before, err := d.store.Load(ctx)
			require.NoError(t, err)
			original, err := before.State().Placement(string(pod.UID))
			require.NoError(t, err)
			// 原Guest即使只启动1GiB，3GiB在途峰值仍可保留；停止后统一
			// 结清本Pod所有峰值，不能要求未知旧resize先被伪造成成功。
			request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: original.VMUID, ComputeInstanceID: "compute-instance", PGBootID: 1}, IssuerEpoch: 1, Revision: 1, OperationID: control.OperationID{High: 1, Low: 1}}, Kind: control.Grow, ExpectedCapacityGeneration: 1, TargetBuffers: 128, TargetVMMemoryBytes: 3 << 30, BudgetCommitmentID: "original-budget", DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Minute).UnixMilli())}
			_, _, err = d.store.Reserve(ctx, before, hostguard.Plan{Namespace: original.Namespace, VMName: original.VMName, Incarnation: original.Incarnation, RunnerPodUID: original.PodUID, Request: request, BeforeBytes: 1 << 30, HostOverheadBytes: original.HostOverheadBytes})
			require.NoError(t, err)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				f.LoseNodeReply = what == "node-reply"
				f.LosePodReply = what == "pod-intent-reply"
				f.LoseVMReply = what == "vm-anchor-reply"
			})
			err = s.retirePod(ctx, d, pod)
			if what == "node-reply" {
				var unknown *hostguard.UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
				f.Mutate(func(f *dsgatest.SchedulerAPI) {
					require.Contains(t, f.Pods["testing/runner"].Finalizers, hostguard.PodProtectionFinalizer)
					require.Empty(t, f.VMs["testing/vm"].Annotations[hostguard.VMRetirementAnnotation])
				})
				require.NoError(t, s.retirePod(ctx, d, pod))
			} else {
				require.NoError(t, err)
			}
			after, err := d.store.Load(ctx)
			require.NoError(t, err)
			if what == "node-v6" {
				require.EqualValues(t, 6, after.State().ProtocolVersion)
			} else {
				require.EqualValues(t, 5, after.State().ProtocolVersion)
			}
			require.Empty(t, *after.State().Placements)
			require.Empty(t, after.State().Holds)
			used, err := after.State().UsedBytes()
			require.NoError(t, err)
			require.Equal(t, n.SystemBytes, used)
			f.Mutate(func(f *dsgatest.SchedulerAPI) {
				p := f.Pods["testing/runner"]
				require.NotContains(t, p.Finalizers, hostguard.PodProtectionFinalizer)
				require.Equal(t, p.Annotations[hostguard.RetirementAnnotation], f.VMs["testing/vm"].Annotations[hostguard.VMRetirementAnnotation])
				require.NoError(t, hostguard.InspectRetiredPod(&f.Node, p))
			})
			require.NoError(t, s.retirePod(ctx, d, pod), "同原关闭恢复不增加墓碑或重复释放")
			_, _, err = d.store.HoldPlacement(ctx, after, original)
			require.Error(t, err, "v5最终容量写者拒绝已终止原Pod重放")
			waiting := boundedPod("waiting", "", "5Gi")
			f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/waiting"] = waiting })
			_, _, err = s.Reserve(ctx, n.Binding.NodeName, waiting, 0)
			require.NoError(t, err, "已结清容量可供新原Pod使用")
		})
	}
}

func TestSchedulerWholePodRetirementRejectsIncompleteProofs(t *testing.T) {
	for _, what := range []string{"missing-exit", "running-sidecar", "wrong-container", "replacement-after-cas"} {
		t.Run(what, func(t *testing.T) {
			s, f, n, claims, pod := retiredRunnerFixture(t)
			ctx := context.Background()
			d := s.domains[n.Binding.NodeName]
			list, err := claims.CoreV1().ConfigMaps("testing").List(ctx, metav1.ListOptions{})
			require.NoError(t, err)
			var exited *core.ConfigMap
			for i := range list.Items {
				if list.Items[i].Labels[vmguard.ClaimLabel] == "process-exit-v1" {
					exited = list.Items[i].DeepCopy()
				}
			}
			require.NotNil(t, exited)
			switch what {
			case "replacement-after-cas":
				f.Mutate(func(f *dsgatest.SchedulerAPI) { f.LoseNodeReply = true })
				require.Error(t, s.retirePod(ctx, d, pod))
				exited.UID = "replacement-exit-uid"
				require.NoError(t, claims.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), exited, exited.Namespace))
			case "missing-exit":
				require.NoError(t, claims.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), exited.Namespace, exited.Name))
			default:
				f.Mutate(func(f *dsgatest.SchedulerAPI) {
					p := f.Pods["testing/runner"]
					if what == "running-sidecar" {
						p.Spec.Containers = append(p.Spec.Containers, core.Container{Name: "extra"})
						p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, core.ContainerStatus{Name: "extra", State: core.ContainerState{Running: &core.ContainerStateRunning{}}})
					} else {
						p.Status.ContainerStatuses[0].ContainerID = "containerd://another-runner"
						p.Status.ContainerStatuses[0].State.Terminated.ContainerID = "containerd://another-runner"
					}
				})
			}
			before, err := d.store.Load(ctx)
			require.NoError(t, err)
			require.Error(t, s.retirePod(ctx, d, pod))
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

func TestSchedulerOrdinaryPodRetirementKeepsOtherAllocations(t *testing.T) {
	s, f, n := schedulerFixture(t)
	pod, other := boundedPod("done", n.Binding.NodeName, "1Gi"), boundedPod("other", n.Binding.NodeName, "2Gi")
	f.Mutate(func(f *dsgatest.SchedulerAPI) { f.Pods["testing/done"], f.Pods["testing/other"] = pod, other })
	ctx := context.Background()
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	f.Mutate(func(f *dsgatest.SchedulerAPI) {
		p := f.Pods["testing/done"]
		p.Status.Phase = core.PodFailed
		now := metav1.Now()
		p.Status.ContainerStatuses = []core.ContainerStatus{{Name: "app", ContainerID: "containerd://ordinary", State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ContainerID: "containerd://ordinary", StartedAt: now, FinishedAt: now}}}}
	})
	require.NoError(t, s.reconcile(ctx, s.domains[n.Binding.NodeName]))
	after, err := s.domains[n.Binding.NodeName].store.Load(ctx)
	require.NoError(t, err)
	require.Len(t, *after.State().Placements, 1)
	require.Equal(t, string(other.UID), (*after.State().Placements)[0].PodUID)
	raw, err := json.Marshal(after.State())
	require.NoError(t, err)
	var restored hostguard.Model
	require.NoError(t, json.Unmarshal(raw, &restored))
	restored.ProtocolVersion = 7
	raw, err = json.Marshal(restored)
	require.NoError(t, err)
	require.Error(t, json.Unmarshal(raw, &restored))
}
