//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
)

// 原Gate第二stop+三持久前缀绑定的Compute任务；不是预算或stdio许可。
type normalSystemOriginalComputeLease struct {
	authority *normalSystemResourceAuthority
	task      *normalSystemOriginalTask
	pending   normalSystemRoleJournalEntry
	stopped   normalSystemRoleObservation
	prefixes  [3]normalSystemRoleFileWitness
	done      <-chan struct{}
	child     *normalSystemRoleOriginalChild
}

func (l *normalSystemOriginalComputeLease) close() {
	if l != nil && l.task != nil {
		l.task.close()
	}
}
func duplicateNormalSystemOriginalTask(t *normalSystemOriginalTask) (*normalSystemOriginalTask, error) {
	if t == nil {
		return nil, ErrUnknown
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.verify(); e != nil {
		return nil, e
	}
	pf, e := unix.FcntlInt(uintptr(t.pidfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	p := os.NewFile(uintptr(pf), "original-compute-pidfd")
	defer p.Close()
	fd, e := unix.FcntlInt(uintptr(t.procfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), "original-compute-proc")
	defer f.Close()
	in := t.source
	in.PIDFD = p
	in.ProcDirectory = f
	return newNormalSystemOriginalTask(in)
}
func (a *normalSystemResourceAuthority) bindOriginalCompute(ctx context.Context, run *normalSystemRoleOriginalRun) error {
	if a == nil || run == nil || run.child == nil || run.pending.Role != "compute" || run.pending.Binding != a.source.binding || run.unknown || run.terminal || run.child.observed.Task.Role != "compute" || !run.child.observed.Released {
		return ErrUnknown
	}
	// 不取b.mu；monitor/finish尚未开始，调用点仍在原role exec序列。
	return a.guard.withRecord(ctx, func(r *record, state *lockedState) error {
		a.producerMu.Lock()
		defer a.producerMu.Unlock()
		if !a.admission.businessAllowed() || r.Identity != a.origin.ResourceIdentity || r.Floor < a.origin.ResourceFloor {
			return ErrUnknown
		}
		if a.compute != nil {
			select {
			case <-a.compute.done:
				if !normalOriginalComputeExitConfirmed(a.compute.child) {
					return ErrUnknown
				}
			default:
				return errors.Join(ErrUnknown, fmt.Errorf("original Compute lease remains active"))
			}
		}
		var prefixes [3]normalSystemRoleFileWitness
		for i, phase := range [3]string{"pending", "exec-stopped", "released"} {
			name := normalSystemRoleStageName(run.pending.Nonce, phase)
			want, ok := run.seen[name]
			if !ok {
				return ErrUnknown
			}
			actual, e := readNormalSystemRoleFileWitness(state, name)
			if e != nil || want != actual {
				return errors.Join(ErrUnknown, e)
			}
			prefixes[i] = actual
		}
		original, e := duplicateNormalSystemOriginalTask(run.child.task)
		if e != nil {
			return e
		}
		if _, e = a.source.inspectOriginalTask(ctx, original, "compute", a.declaration.compute.ComputeArgv, a.declaration.compute.ComputeEnv); e != nil {
			original.close()
			return e
		}
		next := &normalSystemOriginalComputeLease{authority: a, task: original, pending: run.pending, stopped: run.child.observed.Task, prefixes: prefixes, done: run.child.done, child: run.child}
		if a.compute != nil {
			a.compute.close()
		}
		a.compute = next
		return nil
	})
}

// 原resource锁→独立producerMu→原Task；不按PID重开，不取b.mu/execMu。
func (g *Guard) normalSystemOriginalComputeLeaseLocked(ctx context.Context, state *lockedState, r *record) (*normalSystemOriginalComputeLease, error) {
	a := lookupNormalSystemAuthority(g)
	if ctx == nil || ctx.Err() != nil || a == nil || state == nil || r == nil {
		return nil, ErrUnknown
	}
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	if e := normalSystemRoleV2ComputeAdmissionLocked(ctx, a, state, r); e != nil {
		return nil, e
	}
	l := a.compute
	if !a.admission.businessAllowed() || l == nil || l.authority != a || r.Identity != a.origin.ResourceIdentity || r.Floor < a.origin.ResourceFloor {
		return nil, ErrUnknown
	}
	select {
	case <-l.done:
		return nil, ErrUnknown
	default:
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != a.source.binding.RootConsumer {
		return nil, ErrUnknown
	}
	var st unix.Stat_t
	if unix.Fstat(int(state.dir.Fd()), &st) != nil || st.Dev != a.origin.StateDevice || st.Ino != a.origin.StateInode {
		return nil, ErrUnknown
	}
	if e := l.verifyOriginalPrefixesLocked(state); e != nil {
		return nil, e
	}
	task, e := duplicateNormalSystemOriginalTask(l.task)
	if e != nil {
		return nil, e
	}
	return &normalSystemOriginalComputeLease{authority: a, task: task, pending: l.pending, stopped: l.stopped, prefixes: l.prefixes, done: l.done, child: l.child}, nil
}

// 原resource锁和producerMu已由getter持有。只比较原三份持久见证，不执行
// fsync、补记录或重新收养；任一前缀被删、换inode或改字节均保持Unknown。
func (l *normalSystemOriginalComputeLease) verifyOriginalPrefixesLocked(state *lockedState) error {
	if l == nil || state == nil || state.dir == nil {
		return ErrUnknown
	}
	for i, phase := range [3]string{"pending", "exec-stopped", "released"} {
		got, e := readNormalSystemRoleFileWitness(state, normalSystemRoleStageName(l.pending.Nonce, phase))
		if e != nil || got != l.prefixes[i] {
			return errors.Join(ErrUnknown, e)
		}
	}
	return nil
}

// done只是Wait结束通知；若无原ProcessState/creator完成，不能换新Compute。
func normalOriginalComputeExitConfirmed(c *normalSystemRoleOriginalChild) bool {
	if c == nil || c.creatorLeaseDone == nil {
		return false
	}
	select {
	case <-c.creatorLeaseDone:
	default:
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waited && c.creatorLeaseErr == nil && c.cmd != nil && c.cmd.ProcessState != nil
}
