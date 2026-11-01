//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"sync"
)

// candidate仅本原stop保留，不放入a.compute；所有PG消费者先拿resource锁。
// Detach成功后才能在同锁公布active。失败保Unknown tombstone，不补同名nonce。
type normalSystemRoleV2ComputePublication struct {
	authority       *normalSystemResourceAuthority
	installed       *normalSystemRoleInstalledMain
	lease, previous *normalSystemOriginalComputeLease
	phase           normalSystemRoleInstalledPhase
	pendingExit     bool
}

var normalSystemRoleV2Computes = struct {
	sync.Mutex
	byAuthority map[*normalSystemResourceAuthority]*normalSystemRoleV2ComputePublication
}{byAuthority: map[*normalSystemResourceAuthority]*normalSystemRoleV2ComputePublication{}}

func normalSystemRoleV2ComputePublicationFor(a *normalSystemResourceAuthority) *normalSystemRoleV2ComputePublication {
	normalSystemRoleV2Computes.Lock()
	defer normalSystemRoleV2Computes.Unlock()
	return normalSystemRoleV2Computes.byAuthority[a]
}
func (p *normalSystemRoleV2Publication) prepareOriginalComputeStopped(ctx context.Context) error {
	if p == nil || p.installed == nil || p.installed.role != "compute" || p.runtime == nil || p.journal == nil {
		return ErrUnknown
	}
	a := p.runtime.authority
	i := p.installed
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	if !a.admission.businessAllowed() || i.child == nil {
		return ErrUnknown
	}
	if old := normalSystemRoleV2ComputePublicationFor(a); old != nil {
		if old.phase == normalSystemRoleInstalledUnknown || old.installed == nil || (!old.pendingExit && old.lease != a.compute) || (old.pendingExit && old.previous != a.compute) {
			return ErrUnknown
		}
		if _, e := old.installed.child.originalRoleWaitConfirmed(old.installed.creator); e != nil {
			return e
		}
		if _, e := old.installed.originalRoleLeaseHistoryInputLocked(ctx, p.journal.state, a.guard, a.source); e != nil {
			return e
		}
		if old.pendingExit && old.lease != nil {
			old.lease.close()
		}
	} else if a.compute != nil {
		return ErrUnknown
	}
	input, e := i.originalRoleLeaseInputLocked(ctx, p.journal.state, a.guard, a.source)
	if e != nil {
		return e
	}
	original, e := i.duplicateOriginalRoleMainTask()
	if e != nil {
		return e
	}
	lease := &normalSystemOriginalComputeLease{authority: a, task: original, pending: p.journal.pending, stopped: i.child.observed.Task, prefixes: input.Prefixes, done: i.child.done, child: i.child}
	pub := &normalSystemRoleV2ComputePublication{authority: a, installed: i, lease: lease, previous: a.compute, phase: normalSystemRoleInstalledStopped}
	normalSystemRoleV2Computes.Lock()
	normalSystemRoleV2Computes.byAuthority[a] = pub
	normalSystemRoleV2Computes.Unlock()
	p.compute = pub
	return nil
}
func (p *normalSystemRoleV2ComputePublication) publishOriginalDetached(ctx context.Context, state *lockedState, r *record) error {
	if p == nil || p.authority == nil || p.installed == nil {
		return ErrUnknown
	}
	a := p.authority
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	if normalSystemRoleV2ComputePublicationFor(a) != p || p.phase != normalSystemRoleInstalledStopped || a.compute != p.previous {
		return ErrUnknown
	}
	if e := p.installed.requireOriginalDetachedMain(); e != nil {
		return e
	}
	input, e := p.installed.originalRoleLeaseInputLocked(ctx, state, a.guard, a.source)
	if e != nil {
		return e
	}
	if e = normalSystemRoleLeaseAuthorityMatches(ctx, a, state, r, input); e != nil {
		return e
	}
	a.compute = p.lease
	p.phase = normalSystemRoleInstalledDetached
	if p.previous != nil {
		p.previous.close()
	}
	return nil
}
func (p *normalSystemRoleV2ComputePublication) markUnknown() {
	if p == nil || p.authority == nil {
		return
	}
	p.authority.producerMu.Lock()
	p.phase = normalSystemRoleInstalledUnknown
	p.authority.producerMu.Unlock()
}

// 旧Compute getter只增此门。无mode2 reservation时旧1字节语义仍由原reader执行。
func normalSystemRoleV2ComputeAdmissionLocked(ctx context.Context, a *normalSystemResourceAuthority, state *lockedState, r *record) error {
	p := normalSystemRoleV2ComputePublicationFor(a)
	if p == nil {
		return nil
	}
	if p.phase != normalSystemRoleInstalledDetached || p.lease == nil || a.compute != p.lease || p.installed == nil {
		return ErrUnknown
	}
	_, e := p.installed.originalRoleLeaseInputLocked(ctx, state, a.guard, a.source)
	return e
}
func (a *normalSystemResourceAuthority) verifyOriginalComputeV2Published(ctx context.Context, run *normalSystemRoleOriginalRun) error {
	if a == nil || run == nil || run.child == nil || run.child.installedMain == nil {
		return ErrUnknown
	}
	return a.guard.withRecord(ctx, func(r *record, state *lockedState) error {
		a.producerMu.Lock()
		defer a.producerMu.Unlock()
		p := normalSystemRoleV2ComputePublicationFor(a)
		if p == nil || p.installed != run.child.installedMain || p.lease == nil || p.lease.child != run.child || p.lease.pending.Nonce != run.pending.Nonce {
			return ErrUnknown
		}
		if p.pendingExit {
			_, e := p.installed.originalRoleLeaseHistoryInputLocked(ctx, state, a.guard, a.source)
			return e // 不向a.compute发布，不代表原Wait已确认；client只等同原run。
		}
		if p.phase != normalSystemRoleInstalledDetached {
			return ErrUnknown
		}
		return errors.Join(normalSystemRoleV2ComputeAdmissionLocked(ctx, a, state, r))
	})
}

func (p *normalSystemRoleV2ComputePublication) originalExitPending(ctx context.Context, state *lockedState, r *record) error {
	if p == nil || p.authority == nil || p.installed == nil {
		return ErrUnknown
	}
	a := p.authority
	a.producerMu.Lock()
	defer a.producerMu.Unlock()
	if normalSystemRoleV2ComputePublicationFor(a) != p || p.phase != normalSystemRoleInstalledStopped || a.compute != p.previous {
		return ErrUnknown
	}
	input, e := p.installed.originalRoleLeaseHistoryInputLocked(ctx, state, a.guard, a.source)
	if e != nil {
		return e
	}
	if e = normalSystemRoleLeaseAuthorityMatches(ctx, a, state, r, input); e != nil {
		return e
	}
	p.pendingExit = true
	return nil
}
