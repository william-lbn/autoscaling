//go:build linux

package vmguard

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"

	"golang.org/x/sys/unix"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// cpuProcessObserver 只观察原唯一 Start/Wait 的 child，既不发信号，也不接管退出消费。
type cpuProcessObserver struct {
	mu       sync.RWMutex
	child    *initialBootProcess
	captured bool
}

func (r *Runner) captureCPUProcess(cmd *exec.Cmd) {
	observer := r.cpuProcess
	if observer == nil {
		return
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.captured {
		return
	}
	observer.captured = true
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil || cmd.Process.Pid <= 0 {
		return
	}
	pidfd, err := unix.PidfdOpen(cmd.Process.Pid, 0)
	if err != nil {
		return
	}
	child := &initialBootProcess{pid: cmd.Process.Pid, pidfd: pidfd, creatorUID: uint32(os.Geteuid())}
	proc, err := unix.Open("/proc/"+strconv.Itoa(child.pid), unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		child.close()
		return
	}
	child.proc = os.NewFile(uintptr(proc), "original-cpu-qemu-proc")
	child.start, err = child.startTime()
	if err != nil || child.alive() != nil {
		child.close()
		return
	}
	observer.child = child
}

func (r *Runner) closeCPUProcess() {
	if r.cpuProcess == nil {
		return
	}
	r.cpuProcess.mu.Lock()
	defer r.cpuProcess.mu.Unlock()
	if r.cpuProcess.child != nil {
		r.cpuProcess.child.close()
		r.cpuProcess.child = nil
	}
}

// WithOriginalCPUProcess 在原 pidfd/procFD 存活窗口内观察固定 PID。
// numeric PID 不来自 HTTP，不按调用者给定 PID 重新打开，回调没有运行/释放授权。
func (r *Runner) WithOriginalCPUProcess(check func(int, uint64) error) error {
	if r == nil || r.cpuProcess == nil || check == nil {
		return fmt.Errorf("original QEMU CPU observer unavailable")
	}
	r.cpuProcess.mu.RLock()
	defer r.cpuProcess.mu.RUnlock()
	child := r.cpuProcess.child
	if child == nil || child.alive() != nil {
		return fmt.Errorf("original QEMU CPU process is not alive")
	}
	if err := check(child.pid, child.start); err != nil {
		return err
	}
	return child.alive()
}

// OriginalCPUContainer 复用原 Runner Pod/容器验证；QoS 仅用于有界发现 Host 叶组。
func (r *Runner) OriginalCPUContainer(ctx context.Context) (string, string, error) {
	if r == nil || r.core == nil || r.store == nil {
		return "", "", fmt.Errorf("original Runner unavailable")
	}
	container, err := r.originalContainer(ctx, false)
	if err != nil {
		return "", "", err
	}
	pod, err := r.core.Pods(r.store.Binding().Namespace).Get(ctx, r.pod, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}
	if err = originalPod(pod, r.store.Binding(), r.pod, r.birth.Identity.RunnerPodUID, r.node); err != nil {
		return "", "", err
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "neonvm-runner" && status.ContainerID == container && status.RestartCount == 0 && status.State.Running != nil {
			return container, string(pod.Status.QOSClass), nil
		}
	}
	return "", "", fmt.Errorf("original Runner CPU container changed during discovery")
}
