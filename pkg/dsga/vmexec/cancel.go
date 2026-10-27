package vmexec

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// CancelledVM是一个原VM动作的NoEffect审计，不是整条伸缩取消完成。完整原
// 命令包含在不可变Receipt，Sources固定原Birth/State/Receipt UID及两域摘要。
// PG/Guest此前可能已生效，外部预算仍须完整持有，后续AbortReconcile另行结清。
type CancelledVM struct {
	ProtocolVersion uint32               `json:"protocol_version"`
	Sources         budget.RunnerSources `json:"sources"`
	Receipt         vmguard.Receipt      `json:"receipt"`
	Actual          vmguard.Observation  `json:"actual"`
}

// ValidateFor只校验原域审计格式/完整绑定，不认证来源。生产者必须先用固定
// Reader读两端原UID；历史消费者也不能用同目标/预算ID替代完整原命令与摘要。
func (a CancelledVM) ValidateFor(c vmguard.Command, outer string) error {
	if a.ProtocolVersion != 1 || a.Sources.Validate() != nil || a.Receipt.Validate() != nil || !a.Receipt.CancelledBeforeSend() ||
		a.Receipt.Intent.Command != c || a.Receipt.Intent.JournalSHA256 != outer || a.Sources.JournalSHA256 != outer || a.Sources.CommandSHA256 != a.Receipt.Intent.CommandSHA256 || a.Sources.BirthClaim != c.BindingClaim || a.Actual != a.Receipt.Actual {
		return fmt.Errorf("VM no-effect audit differs from exact original sealed cancellation")
	}
	return nil
}

func (a *CancelledVM) UnmarshalJSON(data []byte) error {
	type plain CancelledVM
	var decoded plain
	if err := control.UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*a = CancelledVM(decoded)
	return nil
}

// cancelledBeforeSend先只读原Runner，不让Host/Root暂失联诱发重发。仅消费原
// flock已永久封住的取消终态，普通not_sent或may_have_sent继续等待。Completed
// 返回原正常结算路径，绝不能把已经发生的动作写成NoEffect。两次读取包围
// 当前VM快照检查，最终原UID/RV Record CAS与接管/改意图竞争，不改原命令。
func (e *Executor) cancelledBeforeSend(ctx context.Context, s *journal.Snapshot, step journal.Step) (bool, pgexec.Progress, error) {
	op := s.State().Active
	var c vmguard.Command
	if op == nil || !op.AbortRequested || !s.Dispatchable() || json.Unmarshal(step.Command, &c) != nil || c.Validate() != nil || c.Request != op.Request || c.Request.Key.Instance != e.instance || step.DispatchedBy == nil || step.DispatchedBy.Epoch != c.Request.Key.IssuerEpoch || step.TargetID != c.Identity.RunnerPodUID {
		return true, pgexec.AwaitingFact, fmt.Errorf("VM cancellation changed full original command or live binding")
	}
	proof, err := e.runner.ObserveAbortOriginal(ctx, c, step.CommandSHA256, e.actor)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	if proof.Receipt.Complete() {
		return false, pgexec.ExternalStep, nil
	}
	witness := proof.Receipt.Cancellation
	if !proof.Receipt.CancelledBeforeSend() || witness == nil || witness.VMSequence > s.State().Sequence {
		return true, pgexec.AwaitingFact, fmt.Errorf("VM cancellation lacks original durable unsent terminal")
	}
	latest, err := e.runner.ObserveAbortOriginal(ctx, c, step.CommandSHA256, e.actor)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	if latest.Sources != proof.Sources || !latest.Receipt.CancelledBeforeSend() || latest.Actual != proof.Actual {
		return true, pgexec.AwaitingFact, fmt.Errorf("original VM cancellation source changed before record")
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return true, pgexec.AwaitingFact, err
	}
	audit := CancelledVM{ProtocolVersion: 1, Sources: proof.Sources, Receipt: proof.Receipt, Actual: proof.Actual}
	if err := audit.ValidateFor(c, step.CommandSHA256); err != nil {
		return true, pgexec.AwaitingFact, err
	}
	raw, err := json.Marshal(audit)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	_, err = e.store.Record(ctx, s, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.NoEffect, SourceID: step.TargetID, Data: raw})
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	return true, pgexec.Recorded, nil
}
