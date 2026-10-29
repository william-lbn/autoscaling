package owner

import (
	"context"
	"errors"

	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// retryServiceReopen只在Run原串行管理通道处理最近已确认再关闭。
// 必须重审完整原Root/预算/出生/资格/Runner及准确有效原生期限，再用
// UID/RV CAS冻结完整前驱；派发复用新开放的前后审计和异常独立再关闭。
// 没有本地锁跨网络；失租/取消/写前丢回复均不发送，不自动更换选择。
func (w *PGWorker) retryServiceReopen(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() || m.Active != nil || m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil || m.ServiceClosure.Reopen.Closing == nil || m.ServiceClosure.Reopen.Closing.Close.Pending() || !w.reopenSourcesAvailable() {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	_, _, _, selected := m.ServiceClosure.Reopen.Closing.Close.Selection()
	if selector != selected {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	if err := w.inspectIdleService(ctx, s); err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	before, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if err := w.checkReopenSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	s, _, err = w.session.store.RetryServiceReopen(ctx, s, m.Owner, selector, before)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	return w.sendServiceReopen(ctx, s)
}
