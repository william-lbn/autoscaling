package owner

import (
	"context"
	"errors"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 明确闭合续接只在正式 Run 串行入口执行。实际预算必须原样保留且重新
// 审计，随后才读新原生期限；写前/写后均复核会话、来源及完整 UID/RV。
// 不制造开放/关闭，不延长旧请求，不自动重试或隐式恢复 Unknown。
func (w *PGWorker) continueServiceRearm(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() || m.ServiceClosure == nil || m.ServiceClosure.Rearm == nil || m.ServiceClosure.Reopen != nil || m.ServiceClosure.Pending() || w.safety == nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	old := m.ServiceClosure.LatestRearm()
	if old.CommandSHA256 != selector || old.Phase != journal.Settled || m.Owner.Epoch <= old.Origin.Epoch {
		return pgexec.AwaitingFact, fmt.Errorf("closed rearm requires latest confirmed selector and higher owner")
	}
	if m.CheckStableServiceRecovery() != nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	if err := w.inspectServiceRecovery(ctx, s); err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	// 与既有再关闭恢复相同：正式 Fence 只提高 floor，不恢复期限。
	before, err := w.observeServiceRecoveryLease(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := w.checkReopenSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	s, dispatch, err := w.session.store.ContinueServiceRearm(ctx, s, m.Owner, selector, before)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if err := w.checkRearmDispatch(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	fact, sendErr := w.client.AdmissionLease(ctx, dispatch.Request)
	if sendErr != nil {
		return pgexec.AwaitingFact, sendErr
	}
	noteRecoveryRearmReply(ctx, fact)
	if err := w.checkRearmDispatch(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	return w.advanceServiceRearm(ctx, dispatch.CommandSHA256, false)
}
