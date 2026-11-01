//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

type normalSystemRoleInstalledPhase uint8

const (
	normalSystemRoleInstalledStopped normalSystemRoleInstalledPhase = iota + 1
	normalSystemRoleInstalledDetached
	normalSystemRoleInstalledUnknown
)

// 仅原第二exec-stop的生产调用点可构造。task拥有原clone/proc的独立pin，
// SourceUID/GID是实际安装后的身份；不修改原Root Gate任务或按numeric PID重开。
// 当前类型/读取接口本身不构造资格；后继真实stop publisher必须设置所有私有源。
type normalSystemRoleInstalledMain struct {
	publication                                    *normalSystemRoleV2Publication
	mu                                             sync.Mutex
	phase                                          normalSystemRoleInstalledPhase
	g                                              *Guard
	system                                         *normalSystemLifecycleSource
	sourceSHA                                      string
	role, nonce                                    string
	binding                                        normalSystemLifecycleBinding
	identity                                       Identity
	floor                                          control.Uint64
	stateDevice, stateInode, lockDevice, lockInode uint64
	prefixes                                       [3]normalSystemRoleFileWitness
	child                                          *normalSystemRoleOriginalChild
	task                                           *normalSystemOriginalTask
	controls                                       normalSystemRoleV2ControlWitness
	stdio                                          *normalSystemRoleOriginalStdio
	creator                                        *normalSystemRoleOriginalCreatorPin
}

type normalSystemRoleInstalledLeaseInput struct {
	Role, Nonce                                    string
	Binding                                        normalSystemLifecycleBinding
	ResourceIdentity                               Identity
	ResourceFloor                                  control.Uint64
	StateDevice, StateInode, LockDevice, LockInode uint64
	Prefixes                                       [3]normalSystemRoleFileWitness
	Child                                          *normalSystemRoleOriginalChild
	// 借用原phase pin，backend不得关闭；需要独立所有权时用原Task的dup方法。
	Task *normalSystemOriginalTask
}

func (i *normalSystemRoleInstalledMain) requireOriginalStoppedCandidate() error {
	if i == nil {
		return ErrUnknown
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase != normalSystemRoleInstalledStopped {
		return ErrUnknown
	}
	return nil
}
func (i *normalSystemRoleInstalledMain) requireOriginalDetachedMain() error {
	if i == nil {
		return ErrUnknown
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase != normalSystemRoleInstalledDetached {
		return ErrUnknown
	}
	return nil
}

// 只能由同一个原stop控制流在真实Detach成功后调用，绝不接受request中的bool。
func (i *normalSystemRoleInstalledMain) originalDetachSucceeded() error {
	if i == nil {
		return ErrUnknown
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase != normalSystemRoleInstalledStopped {
		return ErrUnknown
	}
	i.phase = normalSystemRoleInstalledDetached
	return nil
}
func (i *normalSystemRoleInstalledMain) markOriginalRoleUnknown() {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.phase = normalSystemRoleInstalledUnknown
}

// 调用者已持同resource flock；本方法不拿broker/exec锁、不写或同步历史。
// 已退出的once角色可以核原历史，是否还活/creator/Wait由backend按独立phase核。
func (i *normalSystemRoleInstalledMain) originalRoleLeaseInputLocked(ctx context.Context, state *lockedState, g *Guard, system *normalSystemLifecycleSource) (normalSystemRoleInstalledLeaseInput, error) {
	return i.originalRoleLeaseInputForPhaseLocked(ctx, state, g, system, false)
}

// 仅供真实Detach后的pending-exit转移核原历史，绝不表示alive或Wait已完成。
// Child.Wait可已关闭原ChildFD；独立phase/creator pins由原publisher持续持有。
func (i *normalSystemRoleInstalledMain) originalRoleLeaseHistoryInputLocked(ctx context.Context, state *lockedState, g *Guard, system *normalSystemLifecycleSource) (normalSystemRoleInstalledLeaseInput, error) {
	return i.originalRoleLeaseInputForPhaseLocked(ctx, state, g, system, true)
}
func (i *normalSystemRoleInstalledMain) originalRoleLeaseInputForPhaseLocked(ctx context.Context, state *lockedState, g *Guard, system *normalSystemLifecycleSource, historyOnly bool) (normalSystemRoleInstalledLeaseInput, error) {
	var out normalSystemRoleInstalledLeaseInput
	if i == nil || ctx == nil || ctx.Err() != nil || state == nil || g == nil || system == nil {
		return out, ErrUnknown
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase == normalSystemRoleInstalledUnknown || i.phase == 0 || i.g != g || i.system != system || i.child == nil || i.task == nil || i.binding != system.binding || !normalSystemRoleGateDigest(i.nonce) {
		return out, ErrUnknown
	}
	if i.role == normalSystemRoleBrokerKind || i.role == "resource" || i.role == "shutdown" {
		return out, ErrUnknown
	}
	if i.role != "compute" {
		if _, e := NormalSystemOrdinaryV2RolePlan(i.role); e != nil {
			return out, e
		}
	}
	sha, e := system.referenceSHA256()
	if e != nil || sha != i.sourceSHA {
		return out, errors.Join(ErrUnknown, e)
	}
	var dir, lock unix.Stat_t
	if unix.Fstat(int(state.dir.Fd()), &dir) != nil || unix.Fstat(int(state.lock.Fd()), &lock) != nil || dir.Dev != i.stateDevice || dir.Ino != i.stateInode || lock.Dev != i.lockDevice || lock.Ino != i.lockInode {
		return out, ErrUnknown
	}
	for n, stage := range []string{"pending", "exec-stopped", "released"} {
		w, e := readNormalSystemRoleFileWitness(state, normalSystemRoleStageName(i.nonce, stage))
		if e != nil || w != i.prefixes[n] {
			return out, errors.Join(ErrUnknown, e, fmt.Errorf("original installed role durable prefix differs"))
		}
	}
	if historyOnly {
		if i.phase != normalSystemRoleInstalledDetached || i.controls == nil || i.stdio == nil || i.creator == nil {
			return out, ErrUnknown
		}
		if e := i.controls.recheckOriginalRoleControlHistory(ctx); e != nil {
			return out, e
		}
		if e := i.stdio.recheckOriginalHistoryLocked(ctx, state); e != nil {
			return out, e
		}
	} else if e := i.recheckOriginalInstalledSourcesLocked(ctx, state); e != nil {
		return out, e
	}
	return normalSystemRoleInstalledLeaseInput{Role: i.role, Nonce: i.nonce, Binding: i.binding, ResourceIdentity: i.identity, ResourceFloor: i.floor, StateDevice: i.stateDevice, StateInode: i.stateInode, LockDevice: i.lockDevice, LockInode: i.lockInode, Prefixes: i.prefixes, Child: i.child, Task: i.task}, nil
}

// 只dup原phase持有的kernel对象；不从pid/start/role枚举重开任务。
func (i *normalSystemRoleInstalledMain) duplicateOriginalRoleMainTask() (*normalSystemOriginalTask, error) {
	if i == nil {
		return nil, ErrUnknown
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.phase == 0 || i.phase == normalSystemRoleInstalledUnknown || i.task == nil {
		return nil, ErrUnknown
	}
	i.task.mu.Lock()
	defer i.task.mu.Unlock()
	if i.task.closed {
		return nil, ErrUnknown
	}
	source := i.task.source
	p, e := unix.FcntlInt(uintptr(i.task.pidfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	pf := os.NewFile(uintptr(p), "original-installed-role-pidfd-dup")
	defer pf.Close()
	d, e := unix.FcntlInt(uintptr(i.task.procfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	df := os.NewFile(uintptr(d), "original-installed-role-proc-dup")
	defer df.Close()
	source.PIDFD = pf
	source.ProcDirectory = df
	return newNormalSystemOriginalTask(source)
}

var errNormalSystemRoleOriginalStillRunning = errors.New("original role has not completed unique Wait")

type normalSystemRoleOriginalWaitResult struct {
	Status   syscall.WaitStatus
	ExitCode int
}

// 只从原Child唯一cmd.Wait和creator线程lease取得终态；pidfd可读自身不是Wait见证。
func (c *normalSystemRoleOriginalChild) originalRoleWaitConfirmed(pin *normalSystemRoleOriginalCreatorPin) (normalSystemRoleOriginalWaitResult, error) {
	var out normalSystemRoleOriginalWaitResult
	if c == nil || c.done == nil {
		return out, ErrUnknown
	}
	if pin == nil {
		return out, ErrUnknown
	}
	pin.mu.Lock()
	defer pin.mu.Unlock()
	if pin.closed || pin.child != c || pin.file == nil {
		return out, ErrUnknown
	}
	select {
	case <-c.done:
	default:
		return out, errNormalSystemRoleOriginalStillRunning
	}
	if c.creatorLeaseDone == nil {
		return out, ErrUnknown
	}
	select {
	case <-c.creatorLeaseDone:
	default:
		return out, ErrUnknown
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.waited || c.cmd == nil || c.cmd.ProcessState == nil || c.creatorLeaseErr != nil {
		return out, errors.Join(ErrUnknown, c.creatorLeaseErr)
	}
	if c.waitErr != nil {
		var exit *exec.ExitError
		if !errors.As(c.waitErr, &exit) || c.waitErr != exit || exit.ProcessState != c.cmd.ProcessState {
			return out, errors.Join(ErrUnknown, c.waitErr)
		}
	}
	status, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Exited() && !status.Signaled() {
		return out, ErrUnknown
	}
	poll := []unix.PollFd{{Fd: int32(pin.file.Fd()), Events: unix.POLLIN}}
	n, e := unix.Poll(poll, 0)
	if e != nil || n != 1 || poll[0].Revents&unix.POLLIN == 0 {
		return out, errors.Join(ErrUnknown, e)
	}
	return normalSystemRoleOriginalWaitResult{Status: status, ExitCode: c.cmd.ProcessState.ExitCode()}, nil
}

// 原creator关闭通知之前捕获的独立PIDFD_THREAD。不能用Wait后已close的原fd，
// 也不能把leaseDone通知自身当线程退出。只由原stopped candidate持有/关闭。
type normalSystemRoleOriginalCreatorPin struct {
	mu     sync.Mutex
	closed bool
	child  *normalSystemRoleOriginalChild
	file   *os.File
}

func (c *normalSystemRoleOriginalChild) pinOriginalCreatorForLease() (*normalSystemRoleOriginalCreatorPin, error) {
	if c == nil {
		return nil, ErrUnknown
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waited || c.creatorPIDFD == nil || c.creatorLeaseDone == nil || c.cmd == nil || c.task == nil {
		return nil, ErrUnknown
	}
	if e := normalSystemRoleGateThreadAlive(int(c.creatorPIDFD.Fd())); e != nil {
		return nil, e
	}
	fd, e := unix.FcntlInt(c.creatorPIDFD.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	return &normalSystemRoleOriginalCreatorPin{child: c, file: os.NewFile(uintptr(fd), "original-role-creator-thread-lease-pin")}, nil
}
func (p *normalSystemRoleOriginalCreatorPin) requireOriginalCreatorAlive() error {
	if p == nil {
		return ErrUnknown
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.child == nil || p.file == nil {
		return ErrUnknown
	}
	return normalSystemRoleGateThreadAlive(int(p.file.Fd()))
}
func (p *normalSystemRoleOriginalCreatorPin) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		if p.file != nil {
			p.file.Close()
		}
	}
}
