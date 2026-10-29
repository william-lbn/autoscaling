package owner

import (
	"context"
	"errors"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// reopenService 由受信管理端在 Run 原串行 goroutine 中显式选择。原
// 关闭/预算/首次未知均留在同一 WAL，不接受调用方制造的新原生命令。
// 先审 Root/出生/完整预算/资格/Runner，再固定当前有效原生期限；取得
// UID/RV 写前确认后必须再次审计，不能把第一次通过当成发送时的许可。
// 本入口只完成新原生开放，业务解锁仍须独立后继完成及生命周期归档。
func (w *PGWorker) reopenService(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	c := m.ServiceClosure
	if m.Owner != w.session.Actor() || c == nil || c.Reopen != nil || c.Pending() || m.CheckStableServiceRecovery() != nil || !w.reopenSourcesAvailable() {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	_, _, _, selected := c.Selection()
	if selector != selected {
		return pgexec.AwaitingFact, fmt.Errorf("reopen requires latest complete closure selector")
	}
	if err := w.inspectServiceRecovery(ctx, s); err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	before, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if err := w.checkReopenSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	s, _, err = w.session.store.StartServiceReopen(ctx, s, m.Owner, selector, before)
	if err != nil {
		// 即使服务端已写入而回复丢失，这里也没有派发票据。原 WAL
		// 只能查询，或由原 owner 持完整摘要显式重放；不重选 revision。
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	return w.sendServiceReopen(ctx, s)
}

func (w *PGWorker) reopenSourcesAvailable() bool {
	return w.safety != nil && w.safety.claims != nil && w.safety.runner != nil
}

// 稳定资源恢复比普通空闲服务多一项原PG终态对账。先审完整来源，再以
// 当前owner隔离并只读核验原终态，最后由调用方检查相同UID/RV。这个顺序
// 不修改原Unknown，也不允许资源仍在途的15～18借当前容量绕过原账本。
func (w *PGWorker) inspectServiceRecovery(ctx context.Context, s *journal.Snapshot) error {
	if err := w.inspectIdleService(ctx, s); err != nil {
		return err
	}
	if s.State().Active != nil {
		if err := w.executor.ReconcileServiceRecovery(ctx); err != nil {
			return err
		}
	}
	return nil
}

// 不持本地锁跨网络。当前 Session 及相同 VM UID/RV 是每次外部发送/落账
// 的最后本地屏障；Store 的 checked/apply 和原生签名 floor 继续独立认证。
// 不同 RV 即使只变了看似无关的字段，也不能沿用本次已审来源快照。
func (w *PGWorker) checkReopenSnapshot(ctx context.Context, s *journal.Snapshot) error {
	if err := w.session.Check(ctx); err != nil {
		return err
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

// auditServiceReopen 同时检查完整原预算来源和当前 PG 期限/容量/floor。
// 只接受原关闭或本次原开放两个准确边界；后者允许原 owner 幂等重放
// 已应用但丢回复的同一请求。当前较新 open 不能完成或授权旧请求。
// setter 确认成功后必须看到本次 open；普通错误仍可能尚未执行，因此
// requireOpen=false 时保留原关闭边界，不把未找到补成 NoEffect。
func (w *PGWorker) auditServiceReopen(ctx context.Context, s *journal.Snapshot, requireOpen bool) error {
	_, err := w.auditServiceReopenCurrent(ctx, s, requireOpen)
	return err
}

func (w *PGWorker) auditServiceReopenCurrent(ctx context.Context, s *journal.Snapshot, requireOpen bool) (control.AdmissionLeaseResponse, error) {
	m := s.State()
	if m.Owner != w.session.Actor() || m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil || !w.reopenSourcesAvailable() {
		return control.AdmissionLeaseResponse{}, ErrServiceSafety
	}
	r := m.ServiceClosure.Reopen
	if r.Closing != nil || m.CheckStableServiceRecovery() != nil {
		return control.AdmissionLeaseResponse{}, ErrServiceSafety
	}
	if err := w.inspectServiceRecovery(ctx, s); err != nil {
		return control.AdmissionLeaseResponse{}, errors.Join(ErrServiceSafety, err)
	}
	b, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return control.AdmissionLeaseResponse{}, errors.Join(ErrServiceSafety, err)
	}
	q, pg := r.Request, b.Observed
	opened := pg.AdmissionOpen && pg.AdmissionRevision == q.Revision
	closed := !pg.AdmissionOpen && pg.AdmissionRevision == r.Before.Observed.AdmissionRevision
	if !b.Armed || b.Expired || b.DeadlineNS < r.Before.DeadlineNS || b.LeaseEpoch < r.Before.LeaseEpoch || pg.IssuerFloorEpoch != m.Owner.Epoch || pg.CapacityGeneration != q.ExpectedCapacityGeneration || !opened && (!closed || requireOpen) {
		return control.AdmissionLeaseResponse{}, ErrServiceSafety
	}
	return b, w.checkReopenSnapshot(ctx, s)
}

func (w *PGWorker) sendServiceReopen(ctx context.Context, s *journal.Snapshot) (pgexec.Progress, error) {
	r := s.State().ServiceClosure.Reopen
	if r.Origin != w.session.Actor() || r.Phase != journal.Unconfirmed || r.Closing != nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	if err := w.auditServiceReopen(ctx, s, false); err != nil {
		return pgexec.AwaitingFact, err
	}
	fact, sendErr := w.client.SetAdmission(ctx, r.Request)
	// 发送失败也可能已执行。来源复核必须发生在错误分支之前；若原
	// 来源已坏，则先写独立关闭再派发，保留原开放 Phase/回执及所有
	// 旧未知，不将网络错误改成失败终态。取消/失租时不能另起脱离
	// Session 的后台重试；原 PG 自有期限负责独立有界失联关闭。
	if auditErr := w.auditServiceReopen(ctx, s, sendErr == nil && fact.Confirms()); auditErr != nil {
		progress, closeErr := w.closeServiceReopen(ctx, r.CommandSHA256, false)
		return progress, errors.Join(ErrServiceSafety, sendErr, auditErr, closeErr)
	}
	if sendErr != nil || !fact.Confirms() {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, sendErr)
	}
	_, err := w.session.store.RecordServiceReopen(ctx, s, w.session.Actor(), fact, false)
	return pgexec.Recorded, err
}

func (w *PGWorker) replayServiceReopen(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() || m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil || m.ServiceClosure.Reopen.CommandSHA256 != selector {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	return w.sendServiceReopen(ctx, s)
}

func (w *PGWorker) observeServiceReopen(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	c := s.State().ServiceClosure
	if c == nil || c.Reopen == nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	r := c.Reopen
	selected := r.CommandSHA256
	if r.Closing != nil {
		_, _, _, selected = r.Closing.Close.Selection()
	}
	if r.Rearm != nil {
		selected = r.Rearm.CommandSHA256
	}
	if selector != selected {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	return w.advanceServiceReopen(ctx)
}

// closeServiceReopen 不依赖已损坏的预算来源，不续租。先 fence 封住旧
// owner，再观察准确原 boot/floor/容量，UID/RV 确认后才发送独立关闭。
// continuation 仅供明确更高 owner 选择最近关闭，普通错误不消耗新
// revision。原开放即使未知也逐字保留；关门不证明排空或资源释放。
func (w *PGWorker) closeServiceReopen(ctx context.Context, selector string, continuation bool) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() || m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	r := m.ServiceClosure.Reopen
	if continuation {
		if r.Closing == nil || !r.Closing.Close.Pending() {
			return pgexec.AwaitingFact, ErrServiceSafety
		}
		origin, _, _, selected := r.Closing.Close.Selection()
		if selector != selected || m.Owner.Epoch <= origin.Epoch {
			return pgexec.AwaitingFact, ErrServiceSafety
		}
	} else if r.Closing != nil || selector != r.CommandSHA256 {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	instance, epoch := w.Binding()
	fence, err := w.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch})
	if err != nil || fence.Disposition != control.Fenced {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	before, err := w.observeAdmissionLease(ctx)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if err := w.checkReopenSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	if continuation {
		s, _, err = w.session.store.ContinueServiceReopenClosure(ctx, s, m.Owner, selector, before)
	} else {
		s, _, err = w.session.store.StartServiceReopenClosure(ctx, s, m.Owner, selector, before)
	}
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	return w.sendServiceReopenClosure(ctx, s)
}

func (w *PGWorker) sendServiceReopenClosure(ctx context.Context, s *journal.Snapshot) (pgexec.Progress, error) {
	m := s.State()
	r := m.ServiceClosure.Reopen
	c := r.Closing.Close
	origin, _, _, _ := c.Selection()
	if origin != w.session.Actor() || !c.Pending() {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	if err := w.checkReopenSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	if n := c.Continuation; n != nil {
		fact, err := w.client.ContinueAdmissionClosure(ctx, n.Request)
		if err != nil {
			return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
		}
		if err := w.checkReopenSnapshot(ctx, s); err != nil {
			return pgexec.AwaitingFact, err
		}
		_, err = w.session.store.RecordServiceReopenContinuation(ctx, s, m.Owner, fact, false)
		return pgexec.Recorded, err
	}
	fact, err := w.client.CloseAdmissionOriginal(ctx, control.AdmissionCloseRequest{ProtocolVersion: 1, Original: r.Request, Close: c.Request})
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if err := w.checkReopenSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	_, err = w.session.store.RecordServiceReopenClosure(ctx, s, m.Owner, fact, false)
	return pgexec.Recorded, err
}

func (w *PGWorker) replayServiceReopenClosure(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() || m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil || m.ServiceClosure.Reopen.Closing == nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	_, _, _, selected := m.ServiceClosure.Reopen.Closing.Close.Selection()
	if selector != selected {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	return w.sendServiceReopenClosure(ctx, s)
}
