//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	vmtyped "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/typed/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"golang.org/x/sys/unix"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

func parseCPUUnsigned(raw []byte) (uint64, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" || strings.HasPrefix(text, "+") || strings.HasPrefix(text, "-") {
		return 0, fmt.Errorf("invalid unsigned CPU value")
	}
	value, err := strconv.ParseUint(text, 10, 64)
	return value, err
}

func parseCPUQuota(raw []byte) (uint64, uint64, error) {
	fields := strings.Fields(string(raw))
	if len(fields) != 2 {
		return 0, 0, fmt.Errorf("invalid CPU quota fields")
	}
	quota, err := parseCPUUnsigned([]byte(fields[0]))
	if err != nil {
		return 0, 0, err
	}
	period, err := parseCPUUnsigned([]byte(fields[1]))
	if err != nil {
		return 0, 0, err
	}
	observation := runnercpu.Observation{QuotaUS: quota, PeriodUS: period}
	if _, err := observation.Millicores(); err != nil {
		return 0, 0, err
	}
	return quota, period, nil
}

func parseCPUStatistics(stat, pressure []byte) (runnercpu.Statistics, error) {
	var result runnercpu.Statistics
	found := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(stat)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || found[fields[0]] {
			return result, fmt.Errorf("malformed or duplicate CPU statistic")
		}
		value, err := parseCPUUnsigned([]byte(fields[1]))
		if err != nil {
			return result, err
		}
		found[fields[0]] = true
		switch fields[0] {
		case "nr_periods":
			result.Periods = value
		case "nr_throttled":
			result.ThrottledPeriods = value
		case "throttled_usec":
			result.ThrottledUS = value
		}
	}
	if !found["nr_periods"] || !found["nr_throttled"] || !found["throttled_usec"] {
		return result, fmt.Errorf("CPU throttling statistics unavailable")
	}
	seenSome := false
	for _, line := range strings.Split(strings.TrimSpace(string(pressure)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return result, fmt.Errorf("CPU pressure unavailable")
		}
		if fields[0] != "some" {
			continue
		}
		if seenSome {
			return result, fmt.Errorf("duplicate CPU pressure")
		}
		seenSome = true
		totalFound := false
		for _, field := range fields[1:] {
			if !strings.HasPrefix(field, "total=") {
				continue
			}
			if totalFound {
				return result, fmt.Errorf("duplicate CPU pressure total")
			}
			totalFound = true
			total, err := parseCPUUnsigned([]byte(strings.TrimPrefix(field, "total=")))
			if err != nil {
				return result, err
			}
			result.PressureSomeTotalUS = total
		}
		if !totalFound {
			return result, fmt.Errorf("CPU pressure total unavailable")
		}
	}
	if !seenSome {
		return result, fmt.Errorf("CPU pressure source unavailable")
	}
	return result, nil
}

// 原 cgroup 路径只取 /proc/self/cgroup；逐段 O_NOFOLLOW，不能逃出原 mount 或移动进程。
type originalCPUGroup struct {
	fd           int
	source, path string
	dev, ino     uint64
	qemu         func(func(int, uint64) error) error
}

func unifiedCPUPath() (string, error) {
	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", err
	}
	found := ""
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "0::") {
			continue
		}
		if found != "" {
			return "", fmt.Errorf("ambiguous unified CPU cgroup")
		}
		found = strings.TrimPrefix(line, "0::")
	}
	if !strings.HasPrefix(found, "/") || strings.IndexByte(found, 0) >= 0 || strings.Contains(found, "\n") {
		return "", fmt.Errorf("original unified CPU cgroup unavailable")
	}
	for _, part := range strings.Split(found, "/") {
		if part == "." || part == ".." {
			return "", fmt.Errorf("CPU cgroup escapes original hierarchy")
		}
	}
	return found, nil
}

func openCPUGroupPath(source string) (int, error) {
	fd, err := unix.Open(cgroupMountPoint, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, err
	}
	for _, part := range strings.Split(source, "/") {
		if part == "" {
			continue
		}
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		unix.Close(fd)
		if openErr != nil {
			return -1, openErr
		}
		fd = next
	}
	var fs unix.Statfs_t
	if err = unix.Fstatfs(fd, &fs); err != nil || fs.Type != unix.CGROUP2_SUPER_MAGIC {
		unix.Close(fd)
		return -1, errors.Join(err, fmt.Errorf("CPU cgroup v2 filesystem required"))
	}
	return fd, nil
}

func openOriginalCPUGroup() (*originalCPUGroup, error) {
	return resolveOriginalCPUGroup(nil, "", "")
}

func (g *originalCPUGroup) close() error { return unix.Close(g.fd) }

func (g *originalCPUGroup) identity() (uint64, uint64, error) {
	return g.fixedIdentity()
}

func (g *originalCPUGroup) read(name string) ([]byte, error) {
	switch name {
	case "cpu.max", "cpu.weight", "cpu.stat", "cpu.pressure", "cgroup.procs":
	default:
		return nil, fmt.Errorf("CPU reader cannot read another resource")
	}
	if _, _, err := g.identity(); err != nil {
		return nil, err
	}
	return readCPUGroupFile(g.fd, name)
}

func readCPUGroupFile(groupFD int, name string) ([]byte, error) {
	fd, err := unix.Openat(groupFD, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "original-cpu-"+name)
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil {
		return nil, err
	}
	if len(raw) > 8192 {
		return nil, fmt.Errorf("CPU resource source exceeds bound")
	}
	return raw, nil
}

func (g *originalCPUGroup) writeMax(raw []byte) error {
	if _, _, err := g.identity(); err != nil {
		return err
	}
	if _, _, err := parseCPUQuota(raw); err != nil {
		return err
	}
	fd, err := unix.Openat(g.fd, "cpu.max", unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), "original-cpu-max")
	n, writeErr := file.Write(raw)
	closeErr := file.Close()
	if n != len(raw) {
		writeErr = errors.Join(writeErr, io.ErrShortWrite)
	}
	return errors.Join(writeErr, closeErr)
}

type cpuJournal struct{ fd int }

func openCPUJournal(path string) (*cpuJournal, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return &cpuJournal{fd: fd}, nil
}

func (j *cpuJournal) load() (*cpuIntent, error) {
	fd, err := unix.Openat(j.fd, "cpu-state.json", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "original-cpu-state")
	defer file.Close()
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) {
		return nil, errors.Join(err, fmt.Errorf("original CPU state is not a private regular source"))
	}
	raw, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(raw) > 4096 {
		return nil, errors.Join(err, fmt.Errorf("original CPU state exceeds bound"))
	}
	var state cpuIntent
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&state); err != nil {
		return nil, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("original CPU state has trailing content")
	}
	return &state, nil
}

func (j *cpuJournal) save(state cpuIntent) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	nonce := make([]byte, 16)
	if _, err = rand.Read(nonce); err != nil {
		return err
	}
	name := "cpu-state-new-" + hex.EncodeToString(nonce)
	fd, err := unix.Openat(j.fd, name, unix.O_WRONLY|unix.O_CLOEXEC|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), "original-cpu-state-next")
	n, writeErr := file.Write(raw)
	if n != len(raw) {
		writeErr = errors.Join(writeErr, io.ErrShortWrite)
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	writeErr = errors.Join(writeErr, file.Close())
	if writeErr != nil {
		unix.Unlinkat(j.fd, name, 0)
		return writeErr
	}
	if err = unix.Renameat(j.fd, name, j.fd, "cpu-state.json"); err != nil {
		unix.Unlinkat(j.fd, name, 0)
		return err
	}
	return unix.Fsync(j.fd)
}

// authorizeManagedCPU 固定使用原 InCluster 来源。VM generation/target 和原 Pod 都要直接复核。
// 不使用 HTTP 自报 generation 产生 floor，也不引入一份独立 Scheduler/预算。
func authorizeManagedCPU(id runnercpu.Identity, fixedMin, fixedMax vmv1.MilliCPU, fixedPodCPU string) (managedCPUSource, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return managedCPUSource{}, err
	}
	config = vmguard.RunnerClientConfig(config)
	vms, err := vmclient.NewForConfig(config)
	if err != nil {
		return managedCPUSource{}, err
	}
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		return managedCPUSource{}, err
	}
	namespace, name, podName := os.Getenv("K8S_NAMESPACE"), os.Getenv("DSGA_VM_NAME"), os.Getenv("K8S_POD_NAME")
	if namespace == "" || name == "" || podName == "" {
		return managedCPUSource{}, fmt.Errorf("original CPU Kubernetes source missing")
	}
	return managedCPUSource{
		authorize: managedCPUAuthority(id, fixedMin, fixedMax, fixedPodCPU, vms.NeonvmV1(), core.CoreV1(), namespace, name, podName),
		observe: func(ctx context.Context) (runnercpu.NodeCommitment, error) {
			var fact runnercpu.NodeCommitment
			read := managedCPUAuthorityMode(id, fixedMin, fixedMax, fixedPodCPU, vms.NeonvmV1(), core.CoreV1(), namespace, name, podName, false, &fact)
			err := read(ctx, runnercpu.Command{}, 0)
			return fact, err
		}}, nil
}

type managedCPUSource struct {
	authorize func(context.Context, runnercpu.Command, vmv1.MilliCPU) error
	observe   func(context.Context) (runnercpu.NodeCommitment, error)
}

func managedCPUAuthority(id runnercpu.Identity, fixedMin, fixedMax vmv1.MilliCPU, fixedPodCPU string,
	vms vmtyped.NeonvmV1Interface, core coreclient.CoreV1Interface, namespace, name, podName string) func(context.Context, runnercpu.Command, vmv1.MilliCPU) error {
	return managedCPUAuthorityMode(id, fixedMin, fixedMax, fixedPodCPU, vms, core, namespace, name, podName, true, nil)
}

func managedCPUAuthorityMode(id runnercpu.Identity, fixedMin, fixedMax vmv1.MilliCPU, fixedPodCPU string,
	vms vmtyped.NeonvmV1Interface, core coreclient.CoreV1Interface, namespace, name, podName string,
	requireCommand bool, capture *runnercpu.NodeCommitment) func(context.Context, runnercpu.Command, vmv1.MilliCPU) error {
	return func(ctx context.Context, c runnercpu.Command, cpu vmv1.MilliCPU) error {
		bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		vm, err := vms.VirtualMachines(namespace).Get(bounded, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		b := vm.Spec.DSGABootstrap
		held, heldErr := vm.DSGABootstrapHeld()
		if heldErr != nil || held || (vm.Status.Phase != vmv1.VmRunning && vm.Status.Phase != vmv1.VmScaling && (requireCommand || vm.Status.Phase != vmv1.VmPending)) {
			return errors.Join(heldErr, fmt.Errorf("CPU requires the original released non-migrating VM"))
		}
		if string(vm.UID) != id.VMUID || vm.DeletionTimestamp != nil || b == nil || !b.Ready ||
			b.RuntimeContractVersion != vmv1.RuntimeContractFixedCPU || b.VMUID != id.VMUID || b.Incarnation != id.Incarnation || b.ComputeInstanceID != id.ComputeInstanceID ||
			(requireCommand && (vm.Generation != c.Generation || c.Identity != id || vm.Spec.Guest.CPUs.Use != cpu)) ||
			vm.Spec.Guest.CPUs.Min != fixedMin || vm.Spec.Guest.CPUs.Max != fixedMax || vm.Spec.PodResources.Limits.Cpu().String() != fixedPodCPU ||
			vm.Spec.CpuScalingMode == nil || *vm.Spec.CpuScalingMode != vmv1.CpuScalingModeSysfs ||
			vm.Status.PodName != podName || vm.Status.RestartCount != 0 {
			return fmt.Errorf("CPU intent differs from current original VM")
		}

		// BootstrapHeld 只核外层 marker；原完整日志仍必须独立有界解码，不能把损坏/丢失当空闲。
		if _, err := journal.Inspect(vm, journal.Binding{Namespace: namespace, Name: name, VMUID: vm.UID, Incarnation: id.Incarnation}); err != nil {
			return fmt.Errorf("original CPU journal is unconfirmed: %w", err)
		}
		pod, err := core.Pods(namespace).Get(bounded, podName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		maximum, committed, budgetErr := vm.Spec.DSGARuntimeCPUBudget()
		if budgetErr != nil || maximum != uint32(fixedMax) {
			return errors.Join(budgetErr, fmt.Errorf("original CPU maximum contract changed"))
		}
		var runnerFound bool
		for _, container := range pod.Spec.Containers {
			if container.Name != "neonvm-runner" {
				continue
			}
			runnerFound = true
			request, reqErr := vmv1.ExactRuntimeMillicores(*container.Resources.Requests.Cpu())
			limit, limitErr := vmv1.ExactRuntimeMillicores(*container.Resources.Limits.Cpu())
			if reqErr != nil || limitErr != nil || request != committed || limit != committed {
				return fmt.Errorf("original actual Runner Pod CPU commitment changed")
			}
		}
		if !runnerFound {
			return fmt.Errorf("original Runner Pod CPU resources unavailable")
		}
		owner := metav1.GetControllerOf(pod)
		if string(pod.UID) != id.RunnerPodUID || pod.DeletionTimestamp != nil || owner == nil ||
			owner.UID != vm.UID || owner.Name != vm.Name || owner.Kind != "VirtualMachine" ||
			owner.APIVersion != vmv1.SchemeGroupVersion.String() || pod.Spec.NodeName == "" ||
			pod.Status.Phase != "Running" {
			return fmt.Errorf("CPU Runner Pod differs from original Birth")
		}
		foundContainer := false
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name == "neonvm-runner" {
				foundContainer = true
			}
			if container.Name == "neonvm-runner" && (container.RestartCount != 0 || container.State.Running == nil || container.ContainerID == "") {
				return fmt.Errorf("CPU Runner container restarted")
			}
		}
		if !foundContainer {
			return fmt.Errorf("original CPU Runner container unavailable")
		}
		node, err := core.Nodes().Get(bounded, pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil || string(node.UID) != id.NodeUID || node.DeletionTimestamp != nil {
			return errors.Join(err, fmt.Errorf("CPU Node differs from original Birth"))
		}
		fact, err := hostguard.InspectCPUCommitment(node, journal.Binding{Namespace: namespace, Name: name, VMUID: vm.UID,
			Incarnation: id.Incarnation}, podName, id.RunnerPodUID, maximum, committed)
		if err != nil {
			return err
		}
		requested, requestErr := hostguard.PodCPURequest(pod)
		if requestErr != nil || uint64(requested) > fact.CommittedMillicores {
			return fmt.Errorf("current whole-Pod CPU requests exceed original Node commitment")
		}
		if capture != nil {
			*capture = fact
		}
		return nil
	}
}

func initializeManagedCPU(spec *vmv1.VirtualMachineSpec, runner *vmguard.Runner) (*managedCPU, func(), error) {
	if spec.DSGABootstrap == nil || spec.DSGABootstrap.RuntimeContractVersion == 0 {
		return nil, func() {}, nil
	}
	birth, err := runner.OriginalIdentity()
	if err != nil {
		return nil, nil, err
	}
	id := runnercpu.Identity{VMUID: birth.VMUID, ComputeInstanceID: spec.DSGABootstrap.ComputeInstanceID,
		Incarnation: birth.Incarnation, RunnerPodUID: birth.RunnerPodUID, RunnerBootID: birth.RunnerBootID, NodeUID: birth.NodeUID}
	maxCPU, committed, budgetErr := spec.DSGARuntimeCPUBudget()
	if budgetErr != nil || id.Validate() != nil {
		return nil, nil, errors.Join(budgetErr, fmt.Errorf("managed CPU requires explicit original fixed Pod commitment"))
	}
	podLimit := spec.PodResources.Limits.Cpu()
	discoveryCtx, discoveryCancel := context.WithTimeout(context.Background(), 15*time.Second)
	container, qos, discoverErr := runner.OriginalCPUContainer(discoveryCtx)
	discoveryCancel()
	if discoverErr != nil {
		return nil, nil, discoverErr
	}
	group, err := resolveOriginalCPUGroup(&id, container, qos)
	if err != nil {
		return nil, nil, err
	}
	journal, err := openCPUJournal(vmguard.FixedStateDir)
	if err != nil {
		group.close()
		return nil, nil, err
	}
	cleanup := func() { group.close(); unix.Close(journal.fd) }
	authority, err := authorizeManagedCPU(id, spec.Guest.CPUs.Min, spec.Guest.CPUs.Max, podLimit.String())
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	// 出生CPU承诺只依赖原Node/Pod来源，不依赖PG完成初始化。
	initCtx, initCancel := context.WithTimeout(context.Background(), 15*time.Second)
	_, commitmentErr := authority.observe(initCtx)
	initCancel()
	if commitmentErr != nil {
		cleanup()
		return nil, nil, commitmentErr
	}
	intent := cpuIntent{MinimumMillicores: uint32(spec.Guest.CPUs.Min), ProtocolVersion: 1, Identity: id, MaximumMillicores: maxCPU, CommittedMillicores: committed,
		MaximumVCPUs: spec.Guest.CPUs.Max.RoundedUp(), CgroupDevice: group.dev, CgroupInode: group.ino,
		TargetMillicores: uint32(spec.Guest.CPUs.Use), Pending: true}
	previous, err := journal.load()
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	state := &managedCPU{kernel: group, intent: intent, persist: journal.save, authorize: authority.authorize, verifyCommitment: authority.observe,
		getGuest: getNeonvmDaemonCPU, setGuest: setNeonvmDaemonCPU}
	if previous != nil {
		if previous.ProtocolVersion != intent.ProtocolVersion || previous.Identity != intent.Identity ||
			previous.MinimumMillicores != intent.MinimumMillicores || previous.MaximumMillicores != intent.MaximumMillicores || previous.CommittedMillicores != intent.CommittedMillicores || previous.MaximumVCPUs != intent.MaximumVCPUs ||
			previous.CgroupDevice != intent.CgroupDevice || previous.CgroupInode != intent.CgroupInode ||
			previous.Generation < 0 || state.validateTarget(vmv1.MilliCPU(previous.TargetMillicores)) != nil {
			cleanup()
			return nil, nil, fmt.Errorf("CPU journal differs from original Birth, topology or cgroup")
		}
		state.intent = *previous
		state.durable = true
		// 只有原 CPU 日志恢复；不授权重启 QEMU，原 memory/Birth 屏障继续独立执行。
		group.qemu = runner.WithOriginalCPUProcess
		state.process = func() (int, uint64, error) {
			var pid int
			var ticks uint64
			err := runner.WithOriginalCPUProcess(func(p int, t uint64) error { pid, ticks = p, t; return nil })
			return pid, ticks, err
		}
		return state, cleanup, nil
	}
	if err = state.saveLocked(); err == nil {
		err = state.applyKernelLocked()
	}
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	group.qemu = runner.WithOriginalCPUProcess
	state.process = func() (int, uint64, error) {
		var pid int
		var ticks uint64
		err := runner.WithOriginalCPUProcess(func(p int, t uint64) error { pid, ticks = p, t; return nil })
		return pid, ticks, err
	}
	return state, cleanup, nil
}
