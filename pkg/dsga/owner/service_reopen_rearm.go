package owner

import (
	"context"
	"errors"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// rearmServiceReopen在Run串行管理通道显式选择。先审完整原Root/预算/
// 出生/资格/Runner，再提高原生floor并观察准确过期CAS；Fence不续租。
// UID/RV写前确认后重审全部来源；任一未知不发送，不开放、不清Run latch。
// 再次过期允许更高owner选择最近恢复摘要，旧未知永久保留在有界前驱。
func (w *PGWorker) rearmServiceReopen(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() || m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil || m.ServiceClosure.Reopen.Closing == nil || m.ServiceClosure.Reopen.Closing.Close.Pending() || m.CheckStableServiceRecovery() != nil || !w.reopenSourcesAvailable() {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	r := m.ServiceClosure.Reopen
	_, _, _, selected := r.Closing.Close.Selection()
	if r.Rearm != nil {
		selected = r.Rearm.CommandSHA256
		if m.Owner.Epoch <= r.Rearm.Origin.Epoch {
			return pgexec.AwaitingFact, ErrServiceSafety
		}
	}
	if selector != selected {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	if err := w.inspectServiceRecovery(ctx, s); err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	instance, epoch := w.Binding()
	fence, err := w.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch})
	if err != nil || fence.Disposition != control.Fenced {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	before, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := w.checkReopenSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	s, dispatch, err := w.session.store.StartReopenLeaseRecovery(ctx, s, m.Owner, selector, before)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if err := w.checkRearmDispatch(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	if _, err := w.client.AdmissionLease(ctx, dispatch.Request); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := w.checkRearmDispatch(ctx, s); err != nil {
		// 已恢复期限也仍关闭；来源写后失效保留原Unknown，后继只读
		// 原应用回执。不能以setter成功跳过来源复核并开放业务。
		return pgexec.AwaitingFact, err
	}
	return w.advanceReopenLeaseRecovery(ctx, dispatch.CommandSHA256, false)
}

// 普通Run/显式observe只查准确原rearm-status，绝不重发setter。未命中
// 仍是Unknown；当前期限较新不能补旧成功。原owner显式重放亦须完整来源
// 及同RV，不换deadline、不收养旧SDK未知。网络后再次核Session才落账。
func (w *PGWorker) advanceReopenLeaseRecovery(ctx context.Context, selector string, replay bool) (pgexec.Progress, error) {
	if err := w.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() || m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil || m.ServiceClosure.Reopen.Rearm == nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	r := m.ServiceClosure.Reopen.Rearm
	if selector != r.CommandSHA256 || replay && r.Origin != m.Owner || r.Phase != journal.Unconfirmed {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	if replay {
		if err := w.checkRearmDispatch(ctx, s); err != nil {
			return pgexec.AwaitingFact, err
		}
		if _, err := w.client.AdmissionLease(ctx, r.Request); err != nil {
			return pgexec.AwaitingFact, err
		}
		if err := w.checkRearmDispatch(ctx, s); err != nil {
			return pgexec.AwaitingFact, err
		}
	}
	fact, err := w.client.AdmissionRearmStatus(ctx, r.Request)
	if err != nil || !fact.Retained {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if err := w.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	_, err = w.session.store.RecordReopenLeaseRecovery(ctx, s, m.Owner, fact)
	return pgexec.Recorded, err
}
