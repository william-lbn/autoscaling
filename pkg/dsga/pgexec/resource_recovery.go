package pgexec

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// RecoverServicePG由显式恢复先保存独立原生终态/隔离证明，再重新Fence并查询
// 同一原Key，最后才清SDK同一Pending。普通轮询explicit=false只能复核已有
// 证明；没有证明时绝不自动选择恢复、换owner、Resize或Cancel原命令。
// 本域不写原Active/预算，不授予开放或其他资源的完成/释放许可。
func (e *Executor) RecoverServicePG(ctx context.Context, s *journal.Snapshot, selector string, explicit bool) (*journal.Snapshot, error) {
	m := s.State()
	c := m.ServiceClosure
	if !s.Dispatchable() || m.Owner != e.actor || m.Active == nil || c == nil || c.Pending() || c.Rearm != nil || c.Reopen != nil {
		return nil, fmt.Errorf("PG resource recovery requires original confirmed closed operation")
	}
	_, _, _, selected := c.Selection()
	if selector != selected {
		return nil, fmt.Errorf("PG resource recovery selected another closure")
	}
	if c.PGRejection != nil {
		return s, nil
	}
	if _, _, err := m.Active.OriginalServicePG(); err != nil {
		// 明确无原生终态的拒绝由独立拒绝路径处理，不能将其改成Unknown。
		if _, rejectionErr := m.Active.OriginalPGRejection(); rejectionErr == nil {
			return s, nil
		}
		return nil, err
	}
	if c.PGRecovery == nil {
		if !explicit {
			return s, fmt.Errorf("PG resource recovery has no durable independent selection")
		}
		fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
		if err != nil {
			return nil, err
		}
		status, err := e.client.Status(ctx, control.Query{ProtocolVersion: 1, Key: m.Active.Request.Key})
		if err != nil {
			return nil, err
		}
		if _, err := control.ValidateResizeRecovery(m.Active.Request, fence, status); err != nil {
			return nil, err
		}
		s, err = e.store.RecordServicePGRecovery(ctx, s, e.actor, selector, fence, status)
		if err != nil {
			// CAS即使实际落盘也不给已确认快照，原SDK Pending仍保留。
			return nil, err
		}
		m, c = s.State(), s.State().ServiceClosure
	}
	p := c.PGRecovery
	fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return nil, err
	}
	if fence.Observed.AdmissionOpen || fence.Observed.AdmissionRevision != p.Closed.Revision {
		return nil, fmt.Errorf("PG resource recovery current original gate changed")
	}
	current, err := e.store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if !current.Dispatchable() || current.ResourceVersion() != s.ResourceVersion() || current.State().Owner != e.actor {
		return nil, fmt.Errorf("PG resource recovery VM UID/RV or owner changed before reconciliation")
	}
	if pending := e.client.Pending(); pending != nil {
		status, err := e.client.ReconcileRecovery(ctx, m.Active.Request, p.Fence, p.Status, fence)
		if err != nil {
			return nil, err
		}
		if status.Observed.AdmissionOpen || status.Observed.AdmissionRevision != p.Closed.Revision {
			return nil, fmt.Errorf("PG resource recovery original gate changed during query")
		}
	} else {
		// 同一SDK已经对账时仍只读当前原Key；不得覆盖原独立证明，或因
		// Pending为空跳过当前floor/容量漂移检查。
		status, err := e.client.Status(ctx, control.Query{ProtocolVersion: 1, Key: m.Active.Request.Key})
		if err != nil {
			return nil, err
		}
		effect, err := control.ValidateResizeRecovery(m.Active.Request, fence, status)
		if err != nil || effect != p.Effect || status.Observed.AdmissionOpen || status.Observed.AdmissionRevision != p.Closed.Revision || !control.SameResizeRecoveryBoundary(p.Status.Observed, status.Observed) ||
			(p.Status.Receipt.Result != nil && (status.Receipt.Result == nil || !control.SameResizeRecoveryTerminal(*p.Status.Receipt.Result, *status.Receipt.Result))) {
			return nil, fmt.Errorf("PG resource recovery original native source changed")
		}
	}
	return s, nil
}
