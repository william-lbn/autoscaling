//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"time"
)

// 仅表示原受控publisher尚未发布/应用尚未就绪；任何Unknown不进入等待重试。
var errNormalStartupUnpublished = errors.New("original fixed startup dependency not yet published")

// caller持原resource flock且已完成q.checkLocked。只借原publisher，不从WAL/PID补建。
func normalStartupPublisher(q *normalOrdinaryUdevV2) (*normalSystemRoleLeasePublisher, error) {
	if q == nil || q.authority == nil || q.authority.admission == nil || q.g == nil || lookupNormalSystemAuthority(q.g) != q.authority || !q.authority.admission.businessAllowed() {
		return nil, ErrUnknown
	}
	normalSystemRoleLeasePublishers.Lock()
	p := normalSystemRoleLeasePublishers.byAuthority[q.authority]
	normalSystemRoleLeasePublishers.Unlock()
	if p == nil {
		return nil, errNormalStartupUnpublished
	}
	if p.authority != q.authority {
		return nil, ErrUnknown
	}
	return p, nil
}
func normalStartupActiveLeaseLocked(ctx context.Context, q *normalOrdinaryUdevV2, state *lockedState, r *record, role string) (*normalSystemRoleMainLeaseHandle, error) {
	p, e := normalStartupPublisher(q)
	if e != nil {
		return nil, e
	}
	q.authority.producerMu.Lock()
	c := p.roles[role]
	if c == nil {
		q.authority.producerMu.Unlock()
		return nil, errNormalStartupUnpublished
	}
	active := c.phase == normalSystemRoleLeaseActivePhase
	q.authority.producerMu.Unlock()
	// prepare/activate原本同一resource事务；外部可见非Active不是可重试的未发布。
	if !active {
		return nil, ErrUnknown
	}
	lease, e := q.g.normalSystemOriginalRoleMainLeaseLocked(ctx, state, r, role)
	if e != nil {
		return nil, e
	}
	h, ok := lease.(*normalSystemRoleMainLeaseHandle)
	if !ok || h == nil || h.candidate != c {
		if lease != nil {
			lease.close()
		}
		return nil, ErrUnknown
	}
	return h, nil
}

// 生产等待在broker/resource/exec锁外；timer仅唤醒，绝不根据经过时间授ready。
func (q *normalOrdinaryActionsV2) awaitOriginalUdevReady(ctx context.Context, credential *unix.Ucred, peer *os.File) error {
	if q == nil || q.g == nil || ctx == nil || credential == nil || peer == nil {
		return ErrUnknown
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return errors.Join(ErrUnknown, ctx.Err())
		}
		e := q.g.withRecord(ctx, func(r *record, state *lockedState) error {
			source, e := q.originalUdevSourceLocked()
			if e != nil {
				return e
			}
			if e = source.checkLocked(ctx, state, r); e != nil {
				return e
			}
			if e = source.originalMainLocked(ctx, state, r, "udev-init", credential, peer); e != nil {
				return e
			}
			return source.ensureOriginalUdevReadyLocked(ctx, state, r)
		})
		if e == nil {
			return nil
		}
		if e != errNormalStartupUnpublished {
			return e
		}
		select {
		case <-ctx.Done():
			return errors.Join(ErrUnknown, ctx.Err())
		case <-ticker.C:
		}
	}
}

// caller已持broker.mu→resource；与原spawn/finish同序，不反向取broker或exec锁。
func (q *normalOrdinaryUdevV2) computeStartupLocked(ctx context.Context, state *lockedState, r *record, b *normalSystemRoleBrokerComponent) error {
	if q == nil || b == nil || b.guard != q.g {
		return ErrUnknown
	}
	if e := q.checkLocked(ctx, state, r); e != nil {
		return e
	}
	if e := q.ensureOriginalUdevReadyLocked(ctx, state, r); e != nil {
		return e
	}
	p, e := normalStartupPublisher(q)
	if e != nil {
		return e
	}
	q.authority.producerMu.Lock()
	defer q.authority.producerMu.Unlock()
	c := p.roles["udev-init"]
	if c == nil {
		return errNormalStartupUnpublished
	}
	run := b.byRole["udev-init"]
	if run == nil || run.child != c.input.Child || run.pending.Nonce != c.input.Nonce || c.publisher != p {
		return ErrUnknown
	}
	if run.unknown || c.phase == normalSystemRoleLeaseUnknownPhase {
		return ErrUnknown
	}
	if c.phase == normalSystemRoleLeaseActivePhase || c.phase == normalSystemRoleLeasePendingExitPhase {
		if e := c.verifyOriginalHistoryBindingLocked(ctx, state, r); e != nil {
			return e
		}
		return errNormalStartupUnpublished
	}
	if c.phase != normalSystemRoleLeaseExitedPhase || !run.terminal || c.waitResult == nil || !c.waitResult.Status.Exited() || c.waitResult.Status.ExitStatus() != 0 || c.waitResult.ExitCode != 0 {
		return ErrUnknown
	}
	if e := c.verifyOriginalHistoryBindingLocked(ctx, state, r); e != nil {
		return e
	}
	q.mu.Lock()
	cold := q.cold
	ready := !q.closed && q.ledger.failed == nil && cold != nil && cold.finished && cold.confirmed && cold.initCandidate == c
	q.mu.Unlock()
	if !ready {
		return ErrUnknown
	}
	name := normalSystemRoleStageName(run.pending.Nonce, "exited")
	expected, ok := run.seen[name]
	if !ok {
		return ErrUnknown
	}
	got, e := readNormalSystemRoleFileWitness(state, name)
	if e != nil || got != expected {
		return errors.Join(ErrUnknown, e)
	}
	return nil
}
func (v *normalSystemRoleService) awaitComputeStartup(ctx context.Context) error {
	if v == nil || v.source == nil {
		return ErrUnknown
	}
	if !normalSystemStockUdevSelected(v.source.manifest) {
		return nil
	}
	runtime, e := originalNormalSystemRoleV2Runtime(v.broker)
	if e != nil || runtime.stock == nil {
		return errors.Join(ErrUnknown, e)
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx == nil || ctx.Err() != nil {
			return ErrUnknown
		}
		v.broker.mu.Lock()
		e = v.broker.guard.withRecord(ctx, func(r *record, state *lockedState) error {
			return runtime.stock.computeStartupLocked(ctx, state, r, v.broker)
		})
		v.broker.mu.Unlock()
		if e == nil {
			return nil
		}
		if e != errNormalStartupUnpublished {
			return e
		}
		select {
		case <-ctx.Done():
			return errors.Join(ErrUnknown, ctx.Err())
		case <-ticker.C:
		}
	}
}
