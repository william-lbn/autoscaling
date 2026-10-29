package owner

import (
	"context"
	"errors"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// advanceServiceReopen 只读当前已持久选择，再以原 UID/RV 落账；不选择
// 新开放/再关闭、不发送 setter、不续租、不解除 Run 过期 latch 或外层关闭。
// 只读恢复不要求坏来源先恢复，但仍受原 Session、boot、完整原命令约束。
// 各 native 事实由固定实例 SDK 核验，落账前再次检查 Session；网络期间
// 不持本地互斥锁，串行性仍由原 Run goroutine 和 Store CAS 保证。
func (w *PGWorker) advanceServiceReopen(ctx context.Context) (pgexec.Progress, error) {
	if err := w.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() {
		return pgexec.AwaitingFact, ErrLeadershipLost
	}
	if m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	r := m.ServiceClosure.Reopen
	if r.Rearm != nil {
		return w.advanceReopenLeaseRecovery(ctx, r.Rearm.CommandSHA256, false)
	}
	if r.Closing != nil {
		c := r.Closing.Close
		if !c.Pending() {
			return pgexec.AwaitingFact, ErrServiceSafety
		}
		if n := c.Continuation; n != nil {
			fact, err := w.client.AdmissionContinuationStatus(ctx, n.Request)
			if err != nil {
				return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
			}
			if err := w.session.Check(ctx); err != nil {
				return pgexec.AwaitingFact, err
			}
			_, err = w.session.store.RecordServiceReopenContinuation(ctx, s, m.Owner, fact, true)
			return pgexec.Recorded, err
		}
		request := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: r.Request, Close: c.Request}
		fact, err := w.client.AdmissionClosureStatus(ctx, request)
		if err != nil {
			return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
		}
		if err := w.session.Check(ctx); err != nil {
			return pgexec.AwaitingFact, err
		}
		_, err = w.session.store.RecordServiceReopenClosure(ctx, s, m.Owner, fact, true)
		return pgexec.Recorded, err
	}
	if r.Phase != journal.Unconfirmed {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	fact, err := w.client.AdmissionStatus(ctx, r.Request)
	if err != nil || !fact.Confirms() {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if err := w.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	_, err = w.session.store.RecordServiceReopen(ctx, s, m.Owner, fact, true)
	return pgexec.Recorded, err
}
