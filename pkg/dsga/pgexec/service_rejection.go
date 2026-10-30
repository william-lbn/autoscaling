package pgexec

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// RecoverServiceRejection仅由显式服务恢复调用；普通Advance不保存此屏障。
// 原明确拒绝与当前owner严格分开，不自动换epoch，不重发Resize/Cancel。
// 先认证当前原生floor和原Key，后UID/RV保存独立证明；写后未知时Pending
// 仍保留。16/17的其他资源仍须另行恢复，不能以此屏障替代完整稳定预算。
func (e *Executor) RecoverServiceRejection(ctx context.Context, s *journal.Snapshot, selector string) (*journal.Snapshot, error) {
	m := s.State()
	if !s.Dispatchable() || m.Owner != e.actor || m.Active == nil || m.ServiceClosure == nil || m.ServiceClosure.Pending() {
		return nil, fmt.Errorf("PG rejection recovery requires original complete live closure")
	}
	_, _, _, selected := m.ServiceClosure.Selection()
	if selector != selected {
		return nil, fmt.Errorf("PG rejection recovery requires latest complete closure selector")
	}
	if m.ServiceClosure.PGRejection != nil {
		return s, nil
	}
	original, err := m.Active.OriginalPGRejection()
	if err != nil {
		// 这不是调用者可提交的宽松布尔：其他PG形状继续原完整终态路径，
		// 后继ReconcileServiceRecovery仍会严格拒绝缺失/损坏的原审计。
		return s, nil
	}
	if e.actor.Epoch <= m.Active.Request.Key.IssuerEpoch {
		return nil, fmt.Errorf("PG rejection recovery requires actual strictly newer owner")
	}
	fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return nil, err
	}
	status, err := e.client.Status(ctx, control.Query{ProtocolVersion: 1, Key: m.Active.Request.Key})
	if err != nil {
		return nil, err
	}
	if err := control.ValidateRejectedResizeBarrier(m.Active.Request, original, fence, status); err != nil {
		return nil, err
	}
	return e.store.RecordServicePGRejection(ctx, s, e.actor, selector, fence, status)
}

func (e *Executor) reconcileRejectedService(ctx context.Context, m journal.Model) error {
	p := m.ServiceClosure.PGRejection
	if p == nil {
		return fmt.Errorf("PG rejection recovery has no durable independent barrier")
	}
	original, err := m.Active.OriginalPGRejection()
	if err != nil {
		return err
	}
	fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return err
	}
	_, err = e.client.ReconcileRejected(ctx, m.Active.Request, original, p.Fence, p.Status, fence)
	return err
}
