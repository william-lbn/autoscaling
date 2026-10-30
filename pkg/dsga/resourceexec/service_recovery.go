package resourceexec

import (
	"context"
	"fmt"
	"reflect"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// RecoverServiceGuest是显式服务恢复中的单领域步骤。先保存独立取消/查询
// 选择，再允许Root取消；原GuestReady Unknown和原Abort标记均不改。explicit
// 为false的普通轮询只查已持久选择，绝不创建选择或发送Cancel/Apply/Replay。
// Root若已收到但部分应用，保留未知等待，不从旧Held或相同数值补成成功。
func (e *Executor) RecoverServiceGuest(ctx context.Context, s *journal.Snapshot, selector string, explicit bool) (*journal.Snapshot, pgexec.Progress, error) {
	m := s.State()
	c := m.ServiceClosure
	if !s.Dispatchable() || m.Owner != e.actor || c == nil || c.Pending() || c.Rearm != nil || c.Reopen != nil || m.ResourceIdentity == nil || *m.ResourceIdentity != e.id {
		return nil, pgexec.AwaitingFact, fmt.Errorf("guest service recovery requires original closed production domains")
	}
	_, _, _, selected := c.Selection()
	if selector != selected {
		return nil, pgexec.AwaitingFact, fmt.Errorf("guest service recovery selected another closure")
	}
	unknownGuest := false
	if m.Active != nil {
		for _, step := range m.Active.Steps {
			unknownGuest = unknownGuest || step.Action == journal.GuestReady && step.Phase == journal.Unconfirmed
		}
	}
	if !unknownGuest {
		return s, pgexec.ExternalStep, nil
	}
	command, _, err := m.OriginalServiceGuest()
	if err != nil {
		// 有原Guest未知却缺完整预算/命令时，不能伪装为本域无事可做。
		return nil, pgexec.AwaitingFact, err
	}
	if c.ResourceRecovery == nil {
		if !explicit {
			return s, pgexec.AwaitingFact, resourceguard.ErrUnknown
		}
		view, err := e.resources.InspectState(ctx)
		if err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		if err := e.checkRecoveryRoot(ctx, view, command); err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		cancelUnreceived := view.Original == nil || view.Original.Revision < command.Revision
		s, err = e.store.StartServiceGuestRecovery(ctx, s, e.actor, selector, cancelUnreceived)
		if err != nil {
			// CAS即使实际落盘也不给取消票据；本次没有Root资源写。
			return nil, pgexec.AwaitingFact, err
		}
		c = s.State().ServiceClosure
	}
	if c.ResourceRecovery.Guest == nil {
		return nil, pgexec.AwaitingFact, fmt.Errorf("service resource selection lost original Guest recovery")
	}
	if err := e.resources.RetainOriginal(command); err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	view, err := e.resources.InspectState(ctx)
	if err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	if err := e.checkRecoveryRoot(ctx, view, command); err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	g := c.ResourceRecovery.Guest
	query, effect := true, ""
	if view.Cancelled != nil && reflect.DeepEqual(*view.Cancelled, command) {
		proof, err := e.resources.ObserveCancellation(ctx, command)
		if err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		view, effect = proof.State, "cancelled"
	} else if view.Original != nil && reflect.DeepEqual(*view.Original, command) {
		if _, err := e.resources.Observe(ctx, command); err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		view, err = e.resources.InspectState(ctx)
		if err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		effect = "applied"
	} else {
		if !g.CancelUnreceived || !explicit || g.Phase != journal.Unconfirmed {
			return nil, pgexec.AwaitingFact, resourceguard.ErrUnknown
		}
		if err := e.checkRecoverySnapshot(ctx, s); err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		// 只取消完整原命令；Root最终文件锁决定Apply/Cancel谁先发生，
		// 不能把此处的较旧View当成NoEffect证明或重发Apply的理由。
		proof, err := e.resources.CancelOriginal(ctx, command)
		if err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		view, query, effect = proof.State, false, "cancelled"
	}
	if err := e.checkRecoveryRoot(ctx, view, command); err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	if g.Phase == journal.Settled {
		// 原证明不更新为本次较新观察。接管仍可查询同一原来源；Root
		// 改来源/限额、出生改变或部分应用都不能借旧Settled清理其他领域。
		before, beforeErr := budget.RootAggregateFromView(*g.State)
		after, afterErr := budget.RootAggregateFromView(view)
		if beforeErr != nil || afterErr != nil || !reflect.DeepEqual(before, after) || effect != g.Effect {
			return nil, pgexec.AwaitingFact, fmt.Errorf("independent Guest recovery source changed")
		}
		return s, pgexec.Reconciled, e.checkRecoverySnapshot(ctx, s)
	}
	if err := e.checkRecoverySnapshot(ctx, s); err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	result, err := e.store.RecordServiceGuestRecovery(ctx, s, e.actor, selector, effect, view, query)
	if err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	return result, pgexec.Recorded, nil
}

func (e *Executor) checkRecoveryRoot(ctx context.Context, view resourceguard.View, command resourceguard.Command) error {
	if view.Validate() != nil || view.Identity != e.id || view.IssuerFloorEpoch != e.actor.Epoch || view.Birth == nil || view.Birth.PostmasterPID != command.PostmasterPID || view.Birth.PostmasterStartTicks != command.PostmasterStartTicks || view.Birth.AddressSpaceMaxBytes != command.AddressSpaceMaxBytes {
		return fmt.Errorf("guest recovery original Root identity, floor or birth changed")
	}
	process, err := e.resources.InspectProcess(ctx)
	if err != nil {
		return err
	}
	if process != *view.Birth {
		return fmt.Errorf("guest recovery Postmaster process was replaced")
	}
	return nil
}

func (e *Executor) checkRecoverySnapshot(ctx context.Context, before *journal.Snapshot) error {
	// 不跨RPC持本地锁；PGWorker在原Session串行入口调用，最后重读原
	// VM的UID/RV/owner。真正Apply/Cancel竞争仍由Root自己的文件锁裁决。
	now, err := e.store.Load(ctx)
	if err != nil {
		return err
	}
	if now.ResourceVersion() != before.ResourceVersion() || now.State().Owner != e.actor || !now.Dispatchable() {
		return fmt.Errorf("guest recovery original VM UID/RV or owner changed")
	}
	return nil
}
