package vmguard

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/stretchr/testify/require"
	coordination "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kt "k8s.io/client-go/testing"
)

// 只在typed fakeAPI夹具重建“原创建者尚未接管”的初始journal；不改生产来源，
// 不删除实际birth/WAL，也不把真实运行实例改回bootstrap。Runner出生来源保持原样。
func initialOwnerBootstrapFixture(t *testing.T, withLaunch bool) (*runnerFixture, *coordination.Lease) {
	t.Helper()
	f, _, _ := initialBootFixture(t, withLaunch)
	ctx := context.Background()
	binding := f.store.Binding()
	lease, err := f.core.CoordinationV1().Leases(binding.Namespace).Get(ctx, journal.LeaseName(binding), metav1.GetOptions{})
	require.NoError(t, err)
	vm, err := f.vms.NeonvmV1().VirtualMachines(binding.Namespace).Get(ctx, binding.Name, metav1.GetOptions{})
	require.NoError(t, err)
	originalStatus := vm.Status
	delete(vm.Annotations, control.ModeAnnotation)
	delete(vm.Annotations, control.JournalAnnotation)
	vm.Spec.DSGABootstrap.Ready = false
	vm.Spec.DSGABootstrap.VMUID = ""
	vm.Spec.DSGABootstrap.ConfigSecretUID = ""
	vm.Status = vmv1.VirtualMachineStatus{}
	vm, err = f.vms.NeonvmV1().VirtualMachines(binding.Namespace).Update(ctx, vm, metav1.UpdateOptions{})
	require.NoError(t, err)
	store, err := journal.NewStore(f.vms.NeonvmV1().VirtualMachines(binding.Namespace), binding)
	require.NoError(t, err)
	snapshot, err := store.Initialize(ctx, vm.ResourceVersion, "bootstrap-"+binding.Incarnation)
	require.NoError(t, err)
	snapshot, err = store.ReleaseBootstrap(ctx, snapshot.ResourceVersion(), "original-config-uid")
	require.NoError(t, err)
	vm = snapshot.VirtualMachine()
	vm.Status = originalStatus
	_, err = f.vms.NeonvmV1().VirtualMachines(binding.Namespace).Update(ctx, vm, metav1.UpdateOptions{})
	require.NoError(t, err)
	f.store, f.runner.store, f.actor = store, store, snapshot.State().Owner
	require.Equal(t, "bootstrap-"+binding.Incarnation, f.actor.HolderID)
	require.EqualValues(t, 1, f.actor.Epoch)
	return f, lease
}

func initialOwnerAssertNoLaunch(t *testing.T, f *runnerFixture) {
	t.Helper()
	_, err := os.Stat(filepath.Join(f.config.StateDir, "process-launch.json"))
	require.True(t, os.IsNotExist(err), "等待owner不能提前写launch或启动QEMU")
}

func TestInitialOwnerWaitMissingLeaseConvergesOnlyAfterOriginalTakeover(t *testing.T) {
	f, lease := initialOwnerBootstrapFixture(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	binding := f.store.Binding()
	require.NoError(t, f.core.CoordinationV1().Leases(binding.Namespace).Delete(ctx, lease.Name, metav1.DeleteOptions{}))
	ready, uid, err := f.runner.initialOwnerPoll(ctx, "")
	require.NoError(t, err)
	require.False(t, ready)
	require.Empty(t, uid)
	initialOwnerAssertNoLaunch(t, f)

	// 等待器本身不得创建Lease/接管journal；这里显式模拟生产者先CREATE、
	// 再经原Store.Takeover写同一owner。两个步骤之间必须仍然不授权。
	missingObserved := make(chan struct{})
	var once sync.Once
	f.core.PrependReactor("get", "leases", func(a kt.Action) (bool, runtime.Object, error) {
		if a.GetNamespace() == binding.Namespace && a.(kt.GetAction).GetName() == lease.Name {
			once.Do(func() { close(missingObserved) })
		}
		return false, nil, nil
	})
	coreBefore, vmBefore := len(f.core.Actions()), len(f.vms.Actions())
	done := make(chan error, 1)
	go func() { done <- f.runner.awaitInitialOwner(ctx, 2*time.Second, time.Millisecond) }()
	select {
	case <-missingObserved:
	case <-ctx.Done():
		t.Fatal("初始404尚未被原等待器读取", ctx.Err())
	}
	holder, transitions := "new-original-initial-owner", int32(1)
	lease.Spec.HolderIdentity, lease.Spec.LeaseTransitions = &holder, &transitions
	stamp := metav1.NewMicroTime(time.Now())
	lease.Spec.AcquireTime, lease.Spec.RenewTime = &stamp, &stamp
	_, err = f.core.CoordinationV1().Leases(binding.Namespace).Create(ctx, lease, metav1.CreateOptions{})
	require.NoError(t, err)
	ready, uid, err = f.runner.initialOwnerPoll(ctx, "")
	require.NoError(t, err)
	require.False(t, ready, "Lease已创建但原journal尚未接管，不能授权")
	require.Equal(t, string(lease.UID), uid)
	initialOwnerAssertNoLaunch(t, f)
	snapshot, err := f.store.Load(ctx)
	require.NoError(t, err)
	_, err = f.store.Takeover(ctx, snapshot, snapshot.State().Owner, holder)
	require.NoError(t, err)
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("原owner一致后等待器未收敛", ctx.Err())
	}
	_, err = f.runner.authorize(ctx)
	require.NoError(t, err, "成功仍须通过原完整authorize，不能只见到Lease就放行")
	initialOwnerAssertNoLaunch(t, f)
	producerCreates, producerPatches := 0, 0
	for _, a := range f.core.Actions()[coreBefore:] {
		if a.GetVerb() == "create" && a.GetResource().Resource == "leases" {
			producerCreates++
			continue
		}
		require.Equal(t, "get", a.GetVerb(), "等待器不得修改Pod/Node/出生/来源")
	}
	for _, a := range f.vms.Actions()[vmBefore:] {
		if a.GetVerb() == "patch" {
			producerPatches++
			continue
		}
		require.Equal(t, "get", a.GetVerb())
	}
	require.Equal(t, 1, producerCreates, "仅本测试明确模拟的原Elector CREATE")
	require.Equal(t, 1, producerPatches, "仅本测试明确调用的原Store.Takeover CAS")
}

func TestInitialOwnerWaitRejectsNonRetryableSources(t *testing.T) {
	for _, name := range []string{"forbidden", "malformed_lease", "replacement_lease_uid", "observed_lease_then_404", "terminal_pod", "already_launched", "original_exit", "real_owner_missing_lease", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var f *runnerFixture
			var lease *coordination.Lease
			if name == "real_owner_missing_lease" {
				f, _, _ = initialBootFixture(t, false)
				var err error
				lease, err = f.core.CoordinationV1().Leases(f.store.Binding().Namespace).Get(ctx, journal.LeaseName(f.store.Binding()), metav1.GetOptions{})
				require.NoError(t, err)
			} else {
				f, lease = initialOwnerBootstrapFixture(t, name == "already_launched" || name == "original_exit")
			}
			fixedUID := ""
			switch name {
			case "forbidden":
				f.core.PrependReactor("get", "leases", func(a kt.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, lease.Name, nil)
				})
			case "malformed_lease":
				lease.Annotations[journal.LeaseBindingAnnotation] = "not-the-original-binding"
				require.NoError(t, f.core.Tracker().Update(coordination.SchemeGroupVersion.WithResource("leases"), lease, lease.Namespace))
			case "replacement_lease_uid":
				fixedUID = string(lease.UID)
				lease.UID = "replacement-lease-uid"
				require.NoError(t, f.core.Tracker().Update(coordination.SchemeGroupVersion.WithResource("leases"), lease, lease.Namespace))
			case "observed_lease_then_404", "real_owner_missing_lease", "cancelled":
				if name == "observed_lease_then_404" {
					ready, observed, err := f.runner.initialOwnerPoll(ctx, "")
					require.NoError(t, err)
					require.False(t, ready)
					require.Equal(t, string(lease.UID), observed)
					fixedUID = observed
				}
				require.NoError(t, f.core.CoordinationV1().Leases(lease.Namespace).Delete(ctx, lease.Name, metav1.DeleteOptions{}))
			case "terminal_pod":
				pod, err := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(ctx, f.config.PodName, metav1.GetOptions{})
				require.NoError(t, err)
				pod.Status.Phase = corev1.PodFailed
				require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
			case "original_exit":
				raw, err := os.ReadFile(filepath.Join(f.config.StateDir, "process-launch.json"))
				require.NoError(t, err)
				var launch processLaunch
				require.NoError(t, json.Unmarshal(raw, &launch))
				_, err = f.runner.recordProcessExit(ctx, launch, 2, 0, 0, 0)
				require.NoError(t, err)
			}
			coreBefore, vmBefore := len(f.core.Actions()), len(f.vms.Actions())
			var err error
			if name == "cancelled" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				err = f.runner.awaitInitialOwner(cancelled, time.Second, time.Millisecond)
				require.ErrorIs(t, err, context.Canceled)
			} else {
				ready, _, pollErr := f.runner.initialOwnerPoll(ctx, fixedUID)
				err = pollErr
				require.False(t, ready)
				require.Error(t, err, "不能将非404bootstrap错误降为等待成功")
				switch name {
				case "forbidden":
					require.True(t, apierrors.IsForbidden(err))
				case "observed_lease_then_404", "real_owner_missing_lease":
					require.True(t, apierrors.IsNotFound(err))
				case "terminal_pod", "original_exit":
					require.ErrorIs(t, err, ErrStopped)
				}
			}
			for _, a := range f.core.Actions()[coreBefore:] {
				require.Equal(t, "get", a.GetVerb(), "拒绝不得创建/更新任何来源")
			}
			for _, a := range f.vms.Actions()[vmBefore:] {
				require.Equal(t, "get", a.GetVerb(), "拒绝不得接管或改写VM")
			}
			if name != "already_launched" && name != "original_exit" {
				initialOwnerAssertNoLaunch(t, f)
			}
		})
	}
}

// 本用例走真实RunQEMU前置流程，不直接调用finishProcess冒充业务关闭路径。
// 原fixture中的文件不是QEMU可执行镜像；任何真正Start都不属于本用例许可。
func TestInitialOwnerWaitRunQEMURecordsNoStartWithoutOverwritingLaunch(t *testing.T) {
	setRunningContainer := func(t *testing.T, f *runnerFixture) {
		t.Helper()
		pod, err := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(context.Background(), f.config.PodName, metav1.GetOptions{})
		require.NoError(t, err)
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "neonvm-runner", ContainerID: "containerd://model-only", RestartCount: 0,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Now())}},
		}}
		require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	}
	t.Run("owner_403_publishes_original_v2_no_start", func(t *testing.T) {
		f, _, cmd := initialBootFixture(t, false)
		setRunningContainer(t, f)
		f.core.PrependReactor("get", "leases", func(a kt.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, journal.LeaseName(f.store.Binding()), nil)
		})
		err := f.runner.RunQEMU(context.Background(), cmd)
		require.True(t, apierrors.IsForbidden(err), "原403必须保留，不能只凭NoStart或通用失败算成功")
		require.Nil(t, cmd.Process, "Owner拒绝不能进入QEMU Start")
		require.Nil(t, cmd.ProcessState)
		exited, err := ReadProcessExit(context.Background(), f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
		require.NoError(t, err, "必须经过原不可变退出CM的正式读回")
		require.EqualValues(t, 2, exited.ProtocolVersion)
		require.Zero(t, exited.ProcessID)
		require.Zero(t, exited.ExitCode)
		require.Zero(t, exited.Signal)
		require.Equal(t, f.runner.birth.Identity, exited.Identity)
		require.Equal(t, "containerd://model-only", exited.ContainerID)
		launchRaw, err := os.ReadFile(filepath.Join(f.config.StateDir, "process-launch.json"))
		require.NoError(t, err)
		var launch processLaunch
		require.NoError(t, json.Unmarshal(launchRaw, &launch))
		require.Equal(t, initialBootCanonical(launch), launchRaw, "关闭仍须原canonical write-once launch")
		require.Equal(t, exited.LaunchNonce, launch.Nonce)
		require.Equal(t, exited.CommandSHA256, launch.CommandSHA256)
	})
	t.Run("existing_launch_conflict_does_not_rewrite_or_publish_exit", func(t *testing.T) {
		f, _, cmd := initialBootFixture(t, true)
		setRunningContainer(t, f)
		path := filepath.Join(f.config.StateDir, "process-launch.json")
		before, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Error(t, f.runner.RunQEMU(context.Background(), cmd))
		require.Nil(t, cmd.Process)
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, before, after, "不能覆盖他次已持久launch后补签本次NoStart")
		_, err = os.Stat(filepath.Join(f.config.StateDir, "process-exit.json"))
		require.True(t, os.IsNotExist(err))
		_, err = ReadProcessExit(context.Background(), f.core.CoreV1(), f.store.Binding(), f.runner.birth, f.runner.engine.binding)
		require.True(t, apierrors.IsNotFound(err), "已有launch冲突不能凭当前无进程创建退出来源")
	})
}
