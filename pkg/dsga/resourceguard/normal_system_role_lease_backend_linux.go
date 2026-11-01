//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// 租约只保存本Root原Authority和原stop生产对象。索引不能从磁盘/JSON重建，
// Root重启后没有对象就Unknown。map的创建锁不保护租约；所有槽位始终由原
// resource flock→authority.producerMu保护，不反取broker.mu或execMu。
var normalSystemRoleLeasePublishers = struct {
	sync.Mutex
	byAuthority map[*normalSystemResourceAuthority]*normalSystemRoleLeasePublisher
}{byAuthority: map[*normalSystemResourceAuthority]*normalSystemRoleLeasePublisher{}}

type normalSystemRoleLeasePublisher struct {
	authority *normalSystemResourceAuthority
	roles     map[string]*normalSystemRoleLeaseCandidate
}

type normalSystemRoleLeasePhase uint8

const (
	normalSystemRoleLeaseCandidatePhase normalSystemRoleLeasePhase = iota + 1
	normalSystemRoleLeaseActivePhase
	normalSystemRoleLeasePendingExitPhase
	normalSystemRoleLeaseExitedPhase
	normalSystemRoleLeaseUnknownPhase
)

// 该转换表只约束私有对象的状态，不构造来源或许可。尤其Unknown不可恢复成
// active，退出也只能由原Child的唯一Wait与原creator线程真实退役共同确认。
// pending-exit只记原已detach对象已发出退出信号，绝不等价Wait/退役/授权。
func normalSystemRoleLeaseNextPhase(current, next normalSystemRoleLeasePhase) error {
	if current == normalSystemRoleLeaseCandidatePhase && (next == normalSystemRoleLeaseActivePhase || next == normalSystemRoleLeasePendingExitPhase || next == normalSystemRoleLeaseUnknownPhase) ||
		current == normalSystemRoleLeaseActivePhase && (next == normalSystemRoleLeaseExitedPhase || next == normalSystemRoleLeaseUnknownPhase) ||
		current == normalSystemRoleLeasePendingExitPhase && (next == normalSystemRoleLeaseExitedPhase || next == normalSystemRoleLeaseUnknownPhase) {
		return nil
	}
	return ErrUnknown
}

var errNormalSystemRoleLeasePendingOriginalExit = errors.New("original detached role awaits unique Wait and creator retirement")

type normalSystemRoleLeaseCandidate struct {
	publisher  *normalSystemRoleLeasePublisher
	installed  *normalSystemRoleInstalledMain
	input      normalSystemRoleInstalledLeaseInput
	task       *normalSystemOriginalTask
	creator    *normalSystemRoleOriginalCreatorPin
	phase      normalSystemRoleLeasePhase
	waitResult *normalSystemRoleOriginalWaitResult
}

type normalSystemRoleMainLeaseHandle struct {
	mu        sync.Mutex
	candidate *normalSystemRoleLeaseCandidate
	state     *lockedState
	record    *record
	task      *normalSystemOriginalTask
	closed    bool
}

func normalSystemRoleLeasePublisherFor(a *normalSystemResourceAuthority, create bool) (*normalSystemRoleLeasePublisher, error) {
	if a == nil {
		return nil, ErrUnknown
	}
	normalSystemRoleLeasePublishers.Lock()
	defer normalSystemRoleLeasePublishers.Unlock()
	p := normalSystemRoleLeasePublishers.byAuthority[a]
	if p == nil && create {
		if len(normalSystemRoleLeasePublishers.byAuthority) >= 64 {
			return nil, ErrUnknown
		}
		p = &normalSystemRoleLeasePublisher{authority: a, roles: map[string]*normalSystemRoleLeaseCandidate{}}
		normalSystemRoleLeasePublishers.byAuthority[a] = p
	}
	if p == nil || p.authority != a {
		return nil, ErrUnknown
	}
	return p, nil
}

// 本函数只比原引用的静态元数据，不产生task/Root/持久见证。真实调用者还必须
// 在同一flock内核原三WAL、当前Root身份和原task/creator内核对象。
func normalSystemRoleLeaseRecordMatches(in normalSystemRoleInstalledLeaseInput, r *record, stateDev, stateIno, lockDev, lockIno uint64) error {
	if r == nil || r.Identity != in.ResourceIdentity || r.Floor < in.ResourceFloor || in.StateDevice == 0 || in.StateInode == 0 || in.LockDevice == 0 || in.LockInode == 0 || stateDev != in.StateDevice || stateIno != in.StateInode || lockDev != in.LockDevice || lockIno != in.LockInode {
		return ErrUnknown
	}
	return nil
}
func normalSystemRoleLeasePrefixesMatch(state *lockedState, nonce string, want [3]normalSystemRoleFileWitness) error {
	if state == nil || state.dir == nil || !normalSystemRoleGateDigest(nonce) {
		return ErrUnknown
	}
	for n, stage := range []string{"pending", "exec-stopped", "released"} {
		got, e := readNormalSystemRoleFileWitness(state, normalSystemRoleStageName(nonce, stage))
		if e != nil || got != want[n] {
			return errors.Join(ErrUnknown, e, fmt.Errorf("original role lease durable prefix differs"))
		}
	}
	return nil
}

// 仅真正第二exec-stop/fullcontrol/stdio审计后的InstalledMain可以调用。Task是
// 已降为固定role UID/GID的新phase pin，旧UID0 Gate对象完全不修改；从raw
// observation/role枚举/PID或WAL重新open任务均无入口。candidate尚不可授权。
// caller已持原resource flock，producerMu只在本方法内部短持有。
func (a *normalSystemResourceAuthority) prepareOriginalRoleMainLeaseLocked(ctx context.Context, state *lockedState, r *record, i *normalSystemRoleInstalledMain) (out *normalSystemRoleLeaseCandidate, err error) {
	if a == nil || a.guard == nil || a.source == nil || i == nil || state == nil || state.dir == nil || state.lock == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrUnknown
	}
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	if e := i.requireOriginalStoppedCandidate(); e != nil {
		return nil, e
	}
	in, e := i.originalRoleLeaseInputLocked(ctx, state, a.guard, a.source)
	if e != nil {
		return nil, e
	}
	if e = normalSystemRoleLeaseAuthorityMatches(ctx, a, state, r, in); e != nil {
		return nil, e
	}
	p, e := normalSystemRoleLeasePublisherFor(a, true)
	if e != nil {
		return nil, e
	}
	if old := p.roles[in.Role]; old != nil {
		// 原Wait可能已经结束，但通知或pidfd可读不是退出确认。只有原Child唯一
		// Wait和本候选另dup的creator线程pin均真实终态，才允许同role新一代。
		if old.phase == normalSystemRoleLeaseExitedPhase {
			if old.waitResult == nil {
				return nil, ErrUnknown
			}
			if e = old.verifyOriginalHistoryBindingLocked(ctx, state, r); e != nil {
				return nil, e
			}
		} else if e = old.retireOriginalRoleMainLocked(ctx, state, r); e != nil {
			return nil, e
		}
	}
	task, e := i.duplicateOriginalRoleMainTask()
	if e != nil {
		return nil, e
	}
	creator, e := in.Child.pinOriginalCreatorForLease()
	if e != nil {
		task.close()
		return nil, e
	}
	candidate := &normalSystemRoleLeaseCandidate{publisher: p, installed: i, input: in, task: task, creator: creator, phase: normalSystemRoleLeaseCandidatePhase}
	if e = candidate.verifyOriginalPinsLocked(ctx); e != nil {
		task.close()
		creator.close()
		return nil, e
	}
	// 到这里才发布不可授权的candidate；后继detach/activate失败保持Unknown，
	// 不删除槽位或新造nonce洗掉已持久的旧角色记录。
	p.roles[in.Role] = candidate
	return candidate, nil
}

func normalSystemRoleLeaseAuthorityMatches(ctx context.Context, a *normalSystemResourceAuthority, state *lockedState, r *record, in normalSystemRoleInstalledLeaseInput) error {
	if a == nil || a.guard == nil || a.source == nil || a.admission == nil || !a.admission.businessAllowed() || lookupNormalSystemAuthority(a.guard) != a || in.Binding != a.source.binding || in.ResourceIdentity != a.origin.ResourceIdentity || in.ResourceFloor < a.origin.ResourceFloor || !normalSystemRoleGateDigest(in.Nonce) || in.Child == nil || in.Task == nil || state == nil || state.dir == nil || state.lock == nil || ctx == nil || ctx.Err() != nil {
		return ErrUnknown
	}
	if in.Role != "compute" {
		if _, e := NormalSystemOrdinaryV2RolePlan(in.Role); e != nil {
			return ErrUnknown
		}
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != in.Binding.RootConsumer || a.reference.Broker != current {
		return errors.Join(ErrUnknown, e)
	}
	var st, lock unix.Stat_t
	if unix.Fstat(int(state.dir.Fd()), &st) != nil || unix.Fstat(int(state.lock.Fd()), &lock) != nil || st.Dev != a.origin.StateDevice || st.Ino != a.origin.StateInode {
		return ErrUnknown
	}
	if e = normalSystemRoleLeaseRecordMatches(in, r, st.Dev, st.Ino, lock.Dev, lock.Ino); e != nil {
		return e
	}
	got, e := readNormalSystemRoleFileWitness(state, normalSystemAuthorityStageName(a.reference.InstanceNonce, "prepared"))
	if e != nil || got != a.prepared {
		return errors.Join(ErrUnknown, e)
	}
	return normalSystemRoleLeasePrefixesMatch(state, in.Nonce, in.Prefixes)
}

// 普通读取也从原Installed对象重核同Root/source引用与三WAL；引用发生变化
// 不以尚在的task/旧JSON补签。这里对已退出任务也不要求其重新alive。
func (c *normalSystemRoleLeaseCandidate) verifyOriginalBindingLocked(ctx context.Context, state *lockedState, r *record) error {
	if c == nil || c.publisher == nil || c.publisher.authority == nil || c.installed == nil {
		return ErrUnknown
	}
	a := c.publisher.authority
	in, e := c.installed.originalRoleLeaseInputLocked(ctx, state, a.guard, a.source)
	if e != nil || in != c.input {
		return errors.Join(ErrUnknown, e)
	}
	return normalSystemRoleLeaseAuthorityMatches(ctx, a, state, r, c.input)
}

// 身份切换产生新的phase task，不得把不同clone的同UID任务接到旧Child。
// 原Child/task只读，旧UID0不改；锁序Child→Task与原Wait关闭顺序一致。
func normalSystemRoleLeaseOriginalCloneMatches(task *normalSystemOriginalTask, child *normalSystemRoleOriginalChild) error {
	if task == nil || child == nil {
		return ErrUnknown
	}
	child.mu.Lock()
	defer child.mu.Unlock()
	if child.waited || child.pidfd == nil || child.task == nil {
		return ErrUnknown
	}
	child.task.mu.Lock()
	original := child.task.source
	closed := child.task.closed
	child.task.mu.Unlock()
	if closed {
		return ErrUnknown
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.closed || task.source.PID != original.PID || task.source.StartTicks != original.StartTicks || task.source.GuestBootID != original.GuestBootID {
		return ErrUnknown
	}
	fd := int(child.pidfd.Fd())
	if e := normalSystemRoleLeasePIDFS(fd); e != nil {
		return e
	}
	if e := normalSystemRoleLeasePIDFS(task.pidfd); e != nil {
		return e
	}
	var clone, phase unix.Stat_t
	if unix.Fstat(fd, &clone) != nil || unix.Fstat(task.pidfd, &phase) != nil || clone.Dev != phase.Dev || clone.Ino != phase.Ino {
		return ErrUnknown
	}
	return arrayRegistryPIDFDAlive(fd)
}

// 已退出task无法再读当前ELF/stdio，仍须原Installed历史opaque、原RO/control
// 来源和三份fsync见证；该入口本身既不判断alive也不判断Wait。
func (c *normalSystemRoleLeaseCandidate) verifyOriginalHistoryBindingLocked(ctx context.Context, state *lockedState, r *record) error {
	if c == nil || c.publisher == nil || c.publisher.authority == nil || c.installed == nil {
		return ErrUnknown
	}
	a := c.publisher.authority
	in, e := c.installed.originalRoleLeaseHistoryInputLocked(ctx, state, a.guard, a.source)
	if e != nil || in != c.input {
		return errors.Join(ErrUnknown, e)
	}
	return normalSystemRoleLeaseAuthorityMatches(ctx, a, state, r, c.input)
}

// poll是原持续持有的PIDFS对象的退出通知，不是Wait或物理free证明。
// 非PIDFS、closed/reused FD和任何不认识的poll flags均Unknown，不能转pending。
func normalSystemRoleLeaseExitPollResult(n int, flags int16, e error) (bool, error) {
	if e != nil {
		return false, errors.Join(ErrUnknown, e)
	}
	if n == 0 && flags == 0 {
		return false, nil
	}
	if n == 1 && flags&unix.POLLIN != 0 && flags & ^int16(unix.POLLIN|unix.POLLHUP) == 0 {
		return true, nil
	}
	return false, ErrUnknown
}
func (c *normalSystemRoleLeaseCandidate) originalMainExitPendingLocked(ctx context.Context) (bool, error) {
	if c == nil || c.task == nil || ctx == nil || ctx.Err() != nil {
		return false, ErrUnknown
	}
	c.task.mu.Lock()
	defer c.task.mu.Unlock()
	if c.task.closed {
		return false, ErrUnknown
	}
	if e := normalSystemRoleLeasePIDFS(c.task.pidfd); e != nil {
		return false, e
	}
	polls := []unix.PollFd{{Fd: int32(c.task.pidfd), Events: unix.POLLIN}}
	n, e := unix.Poll(polls, 0)
	return normalSystemRoleLeaseExitPollResult(n, polls[0].Revents, e)
}

func (c *normalSystemRoleLeaseCandidate) verifyOriginalPinsLocked(ctx context.Context) error {
	if c == nil || c.task == nil || c.creator == nil || c.installed == nil || ctx == nil || ctx.Err() != nil {
		return ErrUnknown
	}
	if e := c.creator.requireOriginalCreatorAlive(); e != nil {
		return e
	}
	if e := normalSystemRoleLeaseOriginalCloneMatches(c.task, c.input.Child); e != nil {
		return e
	}
	in := c.input
	select {
	case <-in.Child.done:
		return ErrUnknown
	default:
	}
	c.task.mu.Lock()
	defer c.task.mu.Unlock()
	if e := c.task.verify(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if c.task.source.GuestBootID != in.Binding.GuestBootID {
		return ErrUnknown
	}
	// 固定非Root身份必须与原构建表相等；Compute身份由原InstalledMain生产者
	// 绑定原postgres账户和完整审计。此处不从账号census替换任何旧task来源。
	if in.Role != "compute" {
		role, e := NormalSystemOrdinaryV2RolePlan(in.Role)
		if e != nil || c.task.source.UID != role.UID || c.task.source.GID != role.GID {
			return ErrUnknown
		}
	} else if c.task.source.UID == 0 || c.task.source.GID == 0 {
		return ErrUnknown
	}
	if e := normalSystemRoleLeasePIDFS(c.task.pidfd); e != nil {
		return e
	}
	return ctx.Err()
}

// 调用点仍持原flock且P2已完成真实PTRACE_DETACH。只读Installed phase，
// 本模块没有接受bool把stopped变detached的函数，不能用回调成功冒充detach。
func (c *normalSystemRoleLeaseCandidate) activateOriginalRoleMainLocked(ctx context.Context, state *lockedState, r *record) error {
	if c == nil || c.publisher == nil || c.publisher.authority == nil {
		return ErrUnknown
	}
	a := c.publisher.authority
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	fail := func(e error) error {
		if c.phase == normalSystemRoleLeaseCandidatePhase || c.phase == normalSystemRoleLeaseActivePhase || c.phase == normalSystemRoleLeasePendingExitPhase {
			c.phase = normalSystemRoleLeaseUnknownPhase
			c.installed.markOriginalRoleUnknown()
		}
		return errors.Join(ErrUnknown, e)
	}
	if c.publisher.roles[c.input.Role] != c || normalSystemRoleLeaseNextPhase(c.phase, normalSystemRoleLeaseActivePhase) != nil {
		return fail(ErrUnknown)
	}
	if e := c.installed.requireOriginalDetachedMain(); e != nil {
		return fail(e)
	}
	if e := c.verifyOriginalHistoryBindingLocked(ctx, state, r); e != nil {
		return fail(e)
	}
	pending := func() (bool, error) {
		exited, e := c.originalMainExitPendingLocked(ctx)
		if e != nil {
			return false, e
		}
		if !exited {
			return false, nil
		}
		if e = c.verifyOriginalHistoryBindingLocked(ctx, state, r); e != nil {
			return false, e
		}
		// history只能把同原candidate置为不可授权pending，绝不直接KnownExited。
		if e = normalSystemRoleLeaseNextPhase(c.phase, normalSystemRoleLeasePendingExitPhase); e != nil {
			return false, e
		}
		c.phase = normalSystemRoleLeasePendingExitPhase
		return true, nil
	}
	if yes, e := pending(); e != nil {
		return fail(e)
	} else if yes {
		return errNormalSystemRoleLeasePendingOriginalExit
	}
	if e := c.verifyOriginalBindingLocked(ctx, state, r); e != nil {
		if yes, x := pending(); x != nil {
			return fail(x)
		} else if yes {
			return errNormalSystemRoleLeasePendingOriginalExit
		}
		return fail(e)
	}
	if e := c.verifyOriginalPinsLocked(ctx); e != nil {
		if yes, x := pending(); x != nil {
			return fail(x)
		} else if yes {
			return errNormalSystemRoleLeasePendingOriginalExit
		}
		return fail(e)
	}
	c.phase = normalSystemRoleLeaseActivePhase
	return nil
}
func (c *normalSystemRoleLeaseCandidate) markUnknownLocked(cause error) {
	if c == nil || c.publisher == nil || c.publisher.authority == nil {
		return
	}
	a := c.publisher.authority
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	if c.phase == normalSystemRoleLeaseCandidatePhase || c.phase == normalSystemRoleLeaseActivePhase || c.phase == normalSystemRoleLeasePendingExitPhase {
		c.phase = normalSystemRoleLeaseUnknownPhase
		c.installed.markOriginalRoleUnknown()
	}
	// cause仅由原stop失败控制流提供；不落日志/正文，避免泄漏env或形成恢复开关。
	_ = cause
}
func (c *normalSystemRoleLeaseCandidate) retireOriginalRoleMainLocked(ctx context.Context, state *lockedState, r *record) error {
	if c == nil || (c.phase != normalSystemRoleLeaseActivePhase && c.phase != normalSystemRoleLeasePendingExitPhase) || c.input.Child == nil || c.creator == nil {
		return ErrUnknown
	}
	if e := c.verifyOriginalHistoryBindingLocked(ctx, state, r); e != nil {
		return e
	}
	wait, e := c.input.Child.originalRoleWaitConfirmed(c.creator)
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if e := normalSystemRoleLeaseNextPhase(c.phase, normalSystemRoleLeaseExitedPhase); e != nil {
		return e
	}
	c.waitResult = &wait
	c.phase = normalSystemRoleLeaseExitedPhase
	c.task.close()
	c.creator.close()
	return nil
}

// 固定Root服务每包在原resource flock内调本getter。这里不重新flock、不取
// broker/exec锁、不补fsync、无currentUID/PID枚举收养；返回独立dup仅本次用。
func (g *Guard) normalSystemOriginalRoleMainLeaseLocked(ctx context.Context, state *lockedState, r *record, role string) (normalSystemOriginalRoleMainLease, error) {
	a := lookupNormalSystemAuthority(g)
	if a == nil || a.guard != g || ctx == nil || ctx.Err() != nil {
		return nil, ErrUnknown
	}
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	p, e := normalSystemRoleLeasePublisherFor(a, false)
	if e != nil {
		return nil, e
	}
	c := p.roles[role]
	if c == nil || c.phase != normalSystemRoleLeaseActivePhase {
		return nil, ErrUnknown
	}
	if e = c.verifyOriginalBindingLocked(ctx, state, r); e != nil {
		return nil, e
	}
	if e = c.installed.requireOriginalDetachedMain(); e != nil {
		return nil, e
	}
	if e = c.verifyOriginalPinsLocked(ctx); e != nil {
		return nil, e
	}
	task, e := duplicateNormalSystemOriginalTask(c.task)
	if e != nil {
		return nil, e
	}
	return &normalSystemRoleMainLeaseHandle{candidate: c, state: state, record: r, task: task}, nil
}

// PIDFS的dev/inode才是本支持内核同一task对象的必要比较。Host6.8旧anoninode
// 的不同pidfd可能完全相同，必须明确拒不支持，绝不按numeric PID降级。
func normalSystemRoleLeasePIDFS(fd int) error {
	var fs unix.Statfs_t
	if fd < 0 || unix.Fstatfs(fd, &fs) != nil || uint64(fs.Type) != 0x50494446 {
		return errors.Join(ErrUnknown, fmt.Errorf("original main matcher requires PIDFS"))
	}
	return nil
}
func normalSystemRoleLeaseMatchPeer(task *normalSystemOriginalTask, cred *unix.Ucred, peer *os.File) error {
	if task == nil || cred == nil || peer == nil {
		return ErrUnknown
	}
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.closed || task.source.PID <= 1 || cred.Pid != int32(task.source.PID) || cred.Uid != task.source.UID || cred.Gid != task.source.GID {
		return ErrUnknown
	}
	fd := int(peer.Fd())
	if e := normalSystemRoleLeasePIDFS(fd); e != nil {
		return e
	}
	if e := normalSystemRoleLeasePIDFS(task.pidfd); e != nil {
		return e
	}
	var a, b unix.Stat_t
	if unix.Fstat(fd, &a) != nil || unix.Fstat(task.pidfd, &b) != nil || a.Dev != b.Dev || a.Ino != b.Ino {
		return ErrUnknown
	}
	if e := arrayRegistryPIDFDAlive(fd); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	// verify还核原proc start、所有四UID/GID、TGID=原main与Guestboot；单项inode
	// 相等不能覆盖进程退出、同socket fork发送或身份变化。
	return errors.Join(task.verify(), arrayRegistryPIDFDAlive(fd))
}
func (l *normalSystemRoleMainLeaseHandle) matchOriginalMainPacket(ctx context.Context, cred *unix.Ucred, peer *os.File) error {
	if l == nil {
		return ErrUnknown
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.candidate == nil || l.candidate.publisher == nil || ctx == nil || ctx.Err() != nil {
		return ErrUnknown
	}
	c := l.candidate
	a := c.publisher.authority
	if a == nil {
		return ErrUnknown
	}
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	if c.phase != normalSystemRoleLeaseActivePhase || c.publisher.roles[c.input.Role] != c {
		return ErrUnknown
	}
	if e := c.verifyOriginalBindingLocked(ctx, l.state, l.record); e != nil {
		return e
	}
	if e := c.installed.requireOriginalDetachedMain(); e != nil {
		return e
	}
	if e := c.verifyOriginalPinsLocked(ctx); e != nil {
		return e
	}
	return normalSystemRoleLeaseMatchPeer(l.task, cred, peer)
}
func (l *normalSystemRoleMainLeaseHandle) close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		if l.task != nil {
			l.task.close()
		}
	}
}

// 编译期确保实际Guard读者/handle满足冻结私有API。没有真实InstalledMain
// constructor/stop接线时getter只Unknown，不因为实现了interface而产生资格。
var _ normalSystemOriginalRoleMainLeaseReader = (*Guard)(nil)
var _ normalSystemOriginalRoleMainLease = (*normalSystemRoleMainLeaseHandle)(nil)
