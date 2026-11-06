package owner

import (
	"context"
	"encoding/json"
	"errors"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// completeServiceReopen只在Run原串行通道处理明确的业务完成选择。
// 原生Applied本身不解锁：重新审计完整原来源、原生有效期限和准确开放，
// 最后同Session/VM UID-RV CAS保存完整恢复证据并撤销当前关闭。审计失败
// 独立再关闭；取消/失租仍依赖原生有限期限，不启动脱离Session的后台任务。
func (w *PGWorker) completeServiceReopen(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() || m.CheckStableServiceRecovery() != nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	if m.ServiceClosure == nil {
		// 完成CAS已应用但回复丢失：仅重读最近完整证据并重新认证当前
		// 来源/准确开放。旧周期selector、已开始新资源动作或新关闭均拒绝，
		// 不能把幂等确认变成另一次setter或改写旧完成点。
		_, err := w.auditCompletedService(ctx, s, selector)
		return pgexec.Recorded, err
	}
	r := m.ServiceClosure.Reopen
	if r == nil || r.CommandSHA256 != selector || r.Phase != journal.Settled || r.Closing != nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	current, err := w.auditServiceReopenCurrent(ctx, s, true)
	if err != nil {
		progress, closeErr := w.closeServiceReopen(ctx, selector, false)
		return progress, errors.Join(ErrServiceSafety, err, closeErr)
	}
	_, err = w.session.store.CompleteServiceReopenObserved(ctx, s, m.Owner, selector, current)
	return pgexec.Recorded, err
}

// auditCompletedService认证最新持久完成后的当前服务，不改WAL或原生开放。
// 完整来源审计成功后，共用恢复层可对已认证的serving边界作原期限CAS续租，
// 承接Run解除本地过期等待；不rearm、不改固定TTL，也不自行清除过期latch。
// 旧完成、只恢复rearm或观察当前open均不够。上下文已由Model.Validate完整
// 严格解码，此处只取原新开放身份，不制造原命令、不恢复已退休的SDK pending。
func (w *PGWorker) auditCompletedService(ctx context.Context, s *journal.Snapshot, selector string) (control.ServiceAdmission, error) {
	m := s.State()
	l := m.ServiceLifecycle
	if m.Owner != w.session.Actor() || m.ServiceClosure != nil || m.Active != nil || l == nil || !w.reopenSourcesAvailable() {
		return control.ServiceAdmission{}, ErrServiceSafety
	}
	var frozen struct {
		ServiceClosure *journal.ServiceClosure `json:"service_closure"`
	}
	if json.Unmarshal(l.Context, &frozen) != nil || frozen.ServiceClosure == nil || frozen.ServiceClosure.Reopen == nil {
		return control.ServiceAdmission{}, ErrServiceSafety
	}
	r := frozen.ServiceClosure.Reopen
	if selector != "" && r.CommandSHA256 != selector {
		return control.ServiceAdmission{}, ErrServiceSafety
	}
	if err := w.inspectIdleService(ctx, s); err != nil {
		return control.ServiceAdmission{}, errors.Join(ErrServiceSafety, err)
	}
	current, err := w.observeServiceAdmission(ctx)
	if err != nil {
		return current, err
	}
	pg := current.Snapshot()
	if !current.ValidRead(r.Request.Instance, m.Owner.Epoch, false) || !current.AtLeast(l.Current) || !pg.AdmissionOpen || pg.AdmissionRevision != r.Request.Revision || pg.CapacityGeneration != r.Request.ExpectedCapacityGeneration || pg.IssuerFloorEpoch != m.Owner.Epoch {
		return current, ErrServiceSafety
	}
	return current, w.checkReopenSnapshot(ctx, s)
}
