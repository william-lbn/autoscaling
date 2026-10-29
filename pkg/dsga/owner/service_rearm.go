package owner

import (
	"context"
	"errors"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// 原生rearm只由本Run串行管理入口明确选择。先重审原Root/预算/资格/
// Runner，再观察原期限；UID/RV写前CAS固定完整原关闭、预算、原deadline。
// 写前丢回复不发送；写后重审失败也不发送，保留原选择等待查询/显式重放。
// 任何分支都不开放服务、不清原关闭、不变更预算或Run过期latch。
func (w *PGWorker) rearmServiceLease(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() || m.ServiceClosure == nil || m.ServiceClosure.Rearm != nil || m.ServiceClosure.Reopen != nil || m.ServiceClosure.Pending() || m.Active != nil || w.safety == nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	_, _, _, selected := m.ServiceClosure.Selection()
	if selector != selected {
		return pgexec.AwaitingFact, fmt.Errorf("rearm requires latest complete closure selector")
	}
	if err := w.inspectIdleService(ctx, s); err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	before, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	s, dispatch, err := w.session.store.StartServiceRearm(ctx, s, m.Owner, selector, before)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if err := w.checkRearmDispatch(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	if _, err := w.client.AdmissionLease(ctx, dispatch.Request); err != nil {
		return pgexec.AwaitingFact, err
	}
	return w.advanceServiceRearm(ctx, dispatch.CommandSHA256, false)
}

func (w *PGWorker) checkRearmDispatch(ctx context.Context, s *journal.Snapshot) error {
	if err := w.session.Check(ctx); err != nil {
		return err
	}
	if err := w.inspectIdleService(ctx, s); err != nil {
		return errors.Join(ErrServiceSafety, err)
	}
	current, err := w.session.store.Load(ctx)
	if err != nil {
		return err
	}
	if current.ResourceVersion() != s.ResourceVersion() || current.State().Owner != w.session.Actor() || !current.Dispatchable() {
		return ErrServiceSafety
	}
	return nil
}

// 普通轮询只查询准确原rearm回执。当前deadline变新不能完成旧命令，单槽
// 未命中仍保持Unconfirmed。显式重放仅限原派发owner/摘要，仍须完整来源
// 重审和同RV；只发送原CAS，不根据当前事实选择替代deadline或新owner。
func (w *PGWorker) advanceServiceRearm(ctx context.Context, selector string, replay bool) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() || m.ServiceClosure == nil || m.ServiceClosure.Rearm == nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	r := m.ServiceClosure.Rearm
	if r.CommandSHA256 != selector || replay && r.Origin != m.Owner {
		return pgexec.AwaitingFact, fmt.Errorf("rearm recovery differs from original owner or command")
	}
	if r.Phase != journal.Unconfirmed {
		return pgexec.Idle, ErrServiceSafety
	}
	if replay {
		if err := w.checkRearmDispatch(ctx, s); err != nil {
			return pgexec.AwaitingFact, err
		}
		if _, err := w.client.AdmissionLease(ctx, r.Request); err != nil {
			return pgexec.AwaitingFact, err
		}
	}
	fact, err := w.client.AdmissionRearmStatus(ctx, r.Request)
	if err != nil || !fact.Retained {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	_, err = w.session.store.RecordServiceRearm(ctx, s, m.Owner, fact)
	return pgexec.Recorded, err
}
