package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

var ErrStopped = errors.New("original QEMU child has exited; new memory actions are forbidden")

const exitKind = "process-exit-v1"

// ProcessExit只由同一Runner自己执行原QEMU的启动/等待流程产生，不接受远程
// 自报“进程已死”。v1是同一Start/Wait取得的退出事实；v2只表示已持久原启动
// 意图，但本调用尚未执行Start，或Start失败且没有返回任何Process/ProcessState。
// v2不伪造PID、退出码或信号；旧读者因不识别版本而拒绝，不把它当作v1的Wait。
// 原Birth/State UID、随机launch nonce、argv摘要和最后本地WAL摘要保留；
// 不将QMP断开、Pod404、某个可复用PID消失或没有Wait解释为此事实。
// 它只证明这个Guest进程已经退出，不结算原未知步骤或整Pod的其他Host开销。
type ProcessExit struct {
	ProtocolVersion uint32           `json:"protocol_version"`
	Identity        Identity         `json:"identity"`
	BirthClaim      budget.ObjectRef `json:"birth_claim"`
	StateClaim      budget.ObjectRef `json:"state_claim"`
	LaunchNonce     string           `json:"launch_nonce"`
	CommandSHA256   string           `json:"command_sha256"`
	ProcessID       uint32           `json:"process_id"`
	ExitCode        int32            `json:"exit_code"`
	Signal          int32            `json:"signal"`
	RecordSequence  control.Uint64   `json:"record_sequence"`
	RecordSHA256    string           `json:"record_sha256"`
	ContainerID     string           `json:"container_id"`
}

func (e ProcessExit) Validate() error {
	if e.Identity.Validate() != nil || e.BirthClaim.Validate() != nil || e.StateClaim.Validate() != nil || control.ValidateIncarnation(e.LaunchNonce) != nil || !digestValid(e.CommandSHA256) || e.RecordSequence == 0 || !digestValid(e.RecordSHA256) || control.ValidateID(e.ContainerID, "original Runner container") != nil {
		return fmt.Errorf("invalid original QEMU process exit")
	}
	if e.ProtocolVersion == 2 {
		if e.ProcessID != 0 || e.ExitCode != 0 || e.Signal != 0 {
			return fmt.Errorf("never-started QEMU cannot carry a child wait result")
		}
		return nil
	}
	if e.ProtocolVersion != 1 || e.ProcessID == 0 {
		return fmt.Errorf("invalid original QEMU wait version or PID")
	}
	if e.Signal == 0 && (e.ExitCode < 0 || e.ExitCode > 255) || e.Signal != 0 && (e.Signal < 1 || e.Signal > 127 || e.ExitCode != -1) {
		return fmt.Errorf("invalid original child wait result")
	}
	return nil
}

func (e *ProcessExit) UnmarshalJSON(raw []byte) error {
	type plain ProcessExit
	var v plain
	if err := control.UnmarshalObject(raw, &v); err != nil {
		return err
	}
	*e = ProcessExit(v)
	return e.Validate()
}

type processLaunch struct {
	Identity      Identity         `json:"identity"`
	BirthClaim    budget.ObjectRef `json:"birth_claim"`
	Nonce         string           `json:"nonce"`
	CommandSHA256 string           `json:"command_sha256"`
	ContainerID   string           `json:"container_id"`
}

// validateProcessCommand拒绝daemonize/迁移接收和第二套QMP/UUID。cgexec必须
// 是实际启动流程的exec式CPU组包装；只允许固定QEMU程序名。可执行文件/镜像
// 的资格来自可信部署，不把basename或此摘要当作任意文件的安全认证。
func (r *Runner) validateProcessCommand(cmd *exec.Cmd) (string, error) {
	if cmd == nil || cmd.Process != nil || !filepath.IsAbs(cmd.Path) || len(cmd.Args) < 2 {
		return "", fmt.Errorf("original unstarted QEMU command required")
	}
	name, args := filepath.Base(cmd.Path), cmd.Args[1:]
	if name == "cgexec" {
		if len(args) < 4 || args[0] != "-g" || !strings.HasPrefix(args[1], "cpu:") {
			return "", fmt.Errorf("unsupported QEMU execution wrapper")
		}
		name, args = filepath.Base(args[2]), args[3:]
	}
	if name != "qemu-system-x86_64" && name != "qemu-system-aarch64" {
		return "", fmt.Errorf("unsupported managed QEMU executable")
	}
	uuid, _ := r.birth.Identity.QEMUUUID()
	wantedQMP := "unix:" + filepath.Join(r.engine.dir, "qmp.sock") + ",server,wait=off"
	seenUUID, seenQMP := false, false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-daemonize", "-incoming":
			return "", fmt.Errorf("managed QEMU must remain the original foreground child")
		case "-uuid":
			if seenUUID || i+1 >= len(args) || args[i+1] != uuid {
				return "", fmt.Errorf("QEMU child UUID differs from original birth")
			}
			seenUUID = true
			i++
		case "-qmp":
			if seenQMP || i+1 >= len(args) || args[i+1] != wantedQMP {
				return "", fmt.Errorf("QEMU child private monitor differs")
			}
			seenQMP = true
			i++
		}
	}
	if !seenUUID || !seenQMP {
		return "", fmt.Errorf("QEMU child lacks original UUID/private monitor")
	}
	raw, err := json.Marshal([]any{cmd.Path, cmd.Args})
	if err != nil {
		return "", err
	}
	return SourceDigest(raw), nil
}

// launchContainer在Start前固定当前容器实例。Kubelet可能尚未发布Running，
// 有界等待只读取同一原Pod，绝不在退出后靠同名新容器补身份。退出本地落盘
// 不再依赖API可用性；Pod正在删除、容器已重启/终止时拒绝新启动。
func (r *Runner) launchContainer(ctx context.Context) (string, error) {
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		pod, err := r.core.Pods(r.store.Binding().Namespace).Get(callCtx, r.pod, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		if err := originalPod(pod, r.store.Binding(), r.pod, r.birth.Identity.RunnerPodUID, r.node); err != nil {
			return "", err
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != "neonvm-runner" {
				continue
			}
			if status.RestartCount != 0 || status.State.Terminated != nil {
				return "", fmt.Errorf("original Runner container is restarted or terminated")
			}
			if status.State.Running != nil && control.ValidateID(status.ContainerID, "original Runner container") == nil {
				return status.ContainerID, nil
			}
		}
		select {
		case <-callCtx.Done():
			return "", callCtx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// RunQEMU由实际Runner入口使用，完整持有Start/Wait的同一个exec.Cmd。启动
// 意图先fsync再Start；Wait期间不持VM文件锁，不阻塞正常内存串行执行。父进程
// 被杀或Wait未得到原ProcessState时不补造退出证据，容量继续保留供受控恢复。
// 调用方须独占cmd，不能另行Start/Wait或并发修改；该对象只交给此函数使用。
func (r *Runner) RunQEMU(ctx context.Context, cmd *exec.Cmd) error {
	digest, err := r.validateProcessCommand(cmd)
	if err != nil {
		return err
	}
	container, err := r.launchContainer(ctx)
	if err != nil {
		return err
	}
	nonce, err := randomProcessNonce()
	if err != nil {
		return err
	}
	launch := processLaunch{Identity: r.birth.Identity, BirthClaim: r.engine.binding, Nonce: nonce, CommandSHA256: digest, ContainerID: container}
	raw, err := json.Marshal(launch)
	if err != nil {
		return err
	}
	err = r.engine.withRecord(ctx, func(_ *record, s *lockedState) error { return s.writeOnce("process-launch.json", raw) })
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		// 原启动意图已同步，但本调用明确还没进入Start。即使调用者已取消，
		// 仍用独立有界上下文先保存关闭屏障；不能只退出并遗留永久容量持有。
		return r.finishProcess(launch, 2, 0, 0, 0, err)
	}
	if err := cmd.Start(); err != nil {
		// os/exec在Start成功时才返回Process；仍显式核两项防止以后封装或
		// Go实现变化把“可能已启动”写成NoStart。绝不调用远程PID/CRI猜测。
		// fork/exec失败由os.StartProcess同步处理，不留下可运行的QEMU。
		if cmd.Process != nil || cmd.ProcessState != nil {
			return errors.Join(err, fmt.Errorf("failed QEMU Start retained a process; controlled recovery required"))
		}
		return r.finishProcess(launch, 2, 0, 0, 0, err)
	}
	waitErr := cmd.Wait()
	if cmd.ProcessState == nil || cmd.ProcessState.Pid() != cmd.Process.Pid || cmd.Process.Pid <= 0 || uint64(cmd.Process.Pid) > math.MaxUint32 {
		return errors.Join(waitErr, fmt.Errorf("original QEMU child has no authoritative wait result"))
	}
	wait, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !wait.Exited() && !wait.Signaled() {
		return errors.Join(waitErr, fmt.Errorf("original QEMU child was not reaped"))
	}
	signal := int32(0)
	if wait.Signaled() {
		signal = int32(wait.Signal())
	}
	return r.finishProcess(launch, 1, uint32(cmd.Process.Pid), int32(wait.ExitStatus()), signal, waitErr)
}

// finishProcess只接收本地同步启动/Wait流程的结论，不对外暴露“上报已停止”入口。
// 锁序为原Runner文件锁→完整启动来源/原WAL→关闭文件fsync，释放文件锁后才
// 发布原不可变CM。关闭文件是执行屏障，不改内存Pending/Last/Sequence；启动
// 失败也不能把尚未查明的旧resize伪造为Completed/NoEffect。文件失败不发布，
// API失败只重发同一原文；即使调用者取消也不重启QEMU或删除来源。
func (r *Runner) finishProcess(launch processLaunch, version uint32, pid uint32, code, signal int32, cause error) error {
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exited, closeErr := r.recordProcessExit(closeCtx, launch, version, pid, code, signal)
	if closeErr == nil {
		// 只重发同一不可变退出事实，绝不重启QEMU或重发内存动作。API
		// 短暂故障/CREATE丢回复可在退出宽限内恢复，超时保留本地事实。
		for {
			closeErr = r.publishProcessExit(closeCtx, exited)
			if closeErr == nil || closeCtx.Err() != nil {
				break
			}
			select {
			case <-closeCtx.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return errors.Join(cause, closeErr)
}

func (r *Runner) recordProcessExit(ctx context.Context, launch processLaunch, version uint32, pid uint32, code, signal int32) (ProcessExit, error) {
	var exited ProcessExit
	err := r.engine.withRecordState(ctx, true, func(state *record, s *lockedState) error {
		actual, err := s.readProcessFile("process-launch.json")
		expected, encodeErr := json.Marshal(launch)
		if err != nil || encodeErr != nil || !bytes.Equal(actual, expected) || launch.Identity != r.birth.Identity || launch.BirthClaim != r.engine.binding {
			return fmt.Errorf("original process launch fact differs")
		}
		raw, err := json.Marshal(state)
		if err != nil {
			return err
		}
		exited = ProcessExit{ProtocolVersion: version, Identity: r.birth.Identity, BirthClaim: r.engine.binding, StateClaim: r.birth.StateClaim, LaunchNonce: launch.Nonce, CommandSHA256: launch.CommandSHA256, ProcessID: pid, ExitCode: code, Signal: signal, RecordSequence: state.Sequence, RecordSHA256: SourceDigest(raw), ContainerID: launch.ContainerID}
		if err := exited.Validate(); err != nil {
			return err
		}
		raw, err = json.Marshal(exited)
		if err != nil {
			return err
		}
		return s.writeOnce("process-exit.json", raw)
	})
	return exited, err
}

func ExitClaimName(binding journal.Binding, birth Birth) string {
	raw, _ := json.Marshal([]any{binding.Namespace, binding.Name, binding.VMUID, binding.Incarnation, birth.Identity})
	return "dsga-x-" + SourceDigest(raw)[:56]
}

func (r *Runner) publishProcessExit(ctx context.Context, exited ProcessExit) error {
	if exited.Validate() != nil || exited.Identity != r.birth.Identity || exited.BirthClaim != r.engine.binding || exited.StateClaim != r.birth.StateClaim {
		return fmt.Errorf("original process exit identity differs")
	}
	raw, err := json.Marshal(exited)
	if err != nil {
		return err
	}
	expected := newClaim(r.store.Binding(), ExitClaimName(r.store.Binding(), r.birth), exitKind, true, raw)
	_, createErr := r.core.ConfigMaps(expected.Namespace).Create(ctx, expected, metav1.CreateOptions{})
	got, err := r.core.ConfigMaps(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("original process exit CREATE unconfirmed: %w", errors.Join(createErr, err))
	}
	actual, err := InspectProcessExit(got, r.store.Binding(), r.birth, r.engine.binding)
	if err != nil || actual != exited {
		return errors.Join(fmt.Errorf("original process exit CREATE changed its complete fact"), err)
	}
	return nil
}

// ReadProcessExit只消费原Birth固定的退出来源；404只表示尚无证据，不等价于
// QEMU存活/已死。返回成功也不释放整Pod开销，Host仍须核终止状态和原Node CAS。
func ReadProcessExit(ctx context.Context, client coreclient.CoreV1Interface, binding journal.Binding, birth Birth, ref budget.ObjectRef) (ProcessExit, error) {
	if client == nil {
		return ProcessExit{}, fmt.Errorf("original process exit requires fixed source client")
	}
	cm, err := client.ConfigMaps(binding.Namespace).Get(ctx, ExitClaimName(binding, birth), metav1.GetOptions{})
	if err != nil {
		return ProcessExit{}, err
	}
	return InspectProcessExit(cm, binding, birth, ref)
}

// InspectProcessExit是直接APIReader的唯一纯校验规则；只接受仍受保护的原
// VM来源，即使GC已标记删除也不能同名换UID。传入对象本身不具有来源认证力。
func InspectProcessExit(cm *core.ConfigMap, binding journal.Binding, birth Birth, ref budget.ObjectRef) (ProcessExit, error) {
	var exited ProcessExit
	if birth.Validate() != nil || ref.Name != BindingName(binding) || ref.Validate() != nil || cm == nil || cm.Name != ExitClaimName(binding, birth) || birth.Identity.VMUID != string(binding.VMUID) || birth.Identity.Incarnation != binding.Incarnation {
		return exited, fmt.Errorf("original process exit binding differs")
	}
	claim := budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)}
	if validClaimState(cm, binding, claim, exitKind, true, true) != nil || json.Unmarshal([]byte(cm.Data[ClaimKey]), &exited) != nil || exited.Identity != birth.Identity || exited.BirthClaim != ref || exited.StateClaim != birth.StateClaim {
		return exited, fmt.Errorf("original process exit source is absent, replaced or malformed")
	}
	return exited, nil
}
