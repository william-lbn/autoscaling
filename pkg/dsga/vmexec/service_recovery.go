package vmexec

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// RecoverServiceVM只处理服务已关闭后的原VM领域。显式管理先用同UID/RV
// 保存选择；本执行器不发QMP、不改spec/原Abort，也不取消Runner外的资源。
// Runner从真实WAL看到选择，随后在原flock内观察已发送动作或永久封存未发
// 送原命令。普通轮询只能继续读取已持久选择；未知/部分效果仍原样保留。
func (e *Executor) RecoverServiceVM(ctx context.Context, s *journal.Snapshot, selector string, explicit bool) (*journal.Snapshot, pgexec.Progress, error) {
	m := s.State()
	c := m.ServiceClosure
	if !s.Dispatchable() || m.Owner != e.actor || c == nil || c.Pending() || c.Rearm != nil || c.Reopen != nil || m.ResourceIdentity == nil || *m.ResourceIdentity != e.id || m.PGInstance == nil || *m.PGInstance != e.instance {
		return nil, pgexec.AwaitingFact, fmt.Errorf("VM service recovery requires original closed production domains")
	}
	_, _, _, selected := c.Selection()
	if selected != selector {
		return nil, pgexec.AwaitingFact, fmt.Errorf("VM service recovery selected another closure")
	}
	dispatched := false
	if m.Active != nil {
		for _, step := range m.Active.Steps {
			// 原域封存已正式落账的NoEffect由完整预算读者重新认证，
			// 无须再制造独立选择或把该终态改写为未知。
			if step.Action == journal.ResizeVM && step.Phase == journal.Settled && step.Observation != nil && step.Observation.Result == journal.NoEffect {
				return s, pgexec.ExternalStep, nil
			}
			dispatched = dispatched || step.Action == journal.ResizeVM && step.Phase != journal.Planned
		}
	}
	if !dispatched {
		return s, pgexec.ExternalStep, nil
	}
	step, _, err := m.OriginalServiceVM()
	if err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	var command vmguard.Command
	if json.Unmarshal(step.Command, &command) != nil || command.Validate() != nil || command.Request.Key.Instance != e.instance || command.Identity.Incarnation != e.store.Binding().Incarnation {
		return nil, pgexec.AwaitingFact, fmt.Errorf("VM service recovery lost complete original native command")
	}
	if c.VMRecovery == nil {
		if !explicit {
			return s, pgexec.AwaitingFact, vmguard.ErrUnknown
		}
		birth, _, current, err := e.runner.Inspect(ctx, &command.BindingClaim, e.actor)
		if err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		if birth.Identity != command.Identity || current.State.Pending != nil && (current.State.Pending.Command != command || current.State.Pending.JournalSHA256 != step.CommandSHA256) {
			return nil, pgexec.AwaitingFact, fmt.Errorf("VM service recovery current Runner belongs to another original command")
		}
		if err := e.sameSnapshot(ctx, s); err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		s, err = e.store.StartServiceVMRecovery(ctx, s, e.actor, selector, step.Phase == journal.Unconfirmed)
		if err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		c = s.State().ServiceClosure
	}
	p := c.VMRecovery
	if p.Phase == journal.Settled {
		// 接管只重新认证已保存的准确来源，不把原观察者或取消见证换成
		// 当前owner。新floor未获Runner确认、原UID变化或实际漂移均拒绝。
		proof, err := e.runner.CheckCapacity(ctx, *p.Capacity, e.actor)
		if err != nil {
			return nil, pgexec.AwaitingFact, err
		}
		if !reflect.DeepEqual(vmRecoveryWitness(proof), p.Cancellation) {
			return nil, pgexec.AwaitingFact, fmt.Errorf("VM recovery original cancellation witness changed")
		}
		return s, pgexec.Reconciled, e.sameSnapshot(ctx, s)
	}
	source, proof, err := e.runner.CapacityForOriginal(ctx, command, step.CommandSHA256, e.actor)
	if err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	// 两次来源认证包围同VM快照检查；不是用一次requested-size或旧回执
	// 当完成。may_have_sent、not_sent、部分物理效果不会产生CapacitySource。
	if err := e.sameSnapshot(ctx, s); err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	again, err := e.runner.CheckCapacity(ctx, source, e.actor)
	if err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	if !reflect.DeepEqual(proof, again) {
		return nil, pgexec.AwaitingFact, fmt.Errorf("VM recovery original native receipt changed during verification")
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	result, err := e.store.RecordServiceVMRecovery(ctx, s, e.actor, selector, source, vmRecoveryWitness(proof))
	if err != nil {
		return nil, pgexec.AwaitingFact, err
	}
	return result, pgexec.Recorded, nil
}

func vmRecoveryWitness(p vmguard.Confirmed) *journal.ServiceVMCancellation {
	if p.Receipt.Cancellation == nil {
		return nil
	}
	w := p.Receipt.Cancellation
	return &journal.ServiceVMCancellation{Owner: w.Owner, VMSequence: w.VMSequence, VMResourceVersion: w.VMResourceVersion}
}
