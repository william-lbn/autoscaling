package vmguard

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// CurrentBudgetVM保留当前完整预算的原完成与可选的新取消证据。仅封闭原VM
// 关闭链的消费者可使用，普通执行/Finalize读者仍用严格CheckRetained。
type CurrentBudgetVM struct {
	Budget       Confirmed
	Cancellation *Confirmed
}

// CheckCurrentBudget读取已关闭VM的永久预算。历史淘汰后仍从当前原State找到
// 不可变取消回执，必须处于真实VM关闭Through以内，且取消Witness序列晚于原
// 预算、早于关闭序列；不能仅按当前数值或State.Last的文字kind恢复旧来源。
func (r *Reader) CheckCurrentBudget(ctx context.Context, m journal.Model) (CurrentBudgetVM, error) {
	var out CurrentBudgetVM
	var final budget.FinalizationCommand
	if m.BudgetHeld == nil || json.Unmarshal(m.BudgetHeld.Finalization.Command, &final) != nil || final.Validate() != nil || final.Finalized.Sources == nil || final.Finalized.Sources.Runner == nil || m.CheckClosedBudget(final.Finalized.Request) != nil {
		return out, fmt.Errorf("current Runner budget requires original closed VM boundary")
	}
	sources, request := *final.Finalized.Sources.Runner, final.Finalized.Request
	_, _, publication, err := r.Inspect(ctx, &sources.BirthClaim, m.Owner)
	if err != nil {
		return out, err
	}
	if publication.State.Last != nil && publication.State.Last.Intent.Command.Request == request {
		confirmed, err := r.CheckRetained(ctx, sources, request, final.PreparedSHA256, final.GuestSHA256, final.Finalized.Sources.HostClaim, m.Owner)
		if err != nil {
			return out, err
		}
		return CurrentBudgetVM{Budget: confirmed, Cancellation: nil}, nil
	}
	last := publication.State.Last
	if m.ProtocolVersion < 7 || m.BudgetBoundary == nil || last == nil || last.Kind != cancelledBeforeSend || last.Cancellation == nil || last.Cancellation.VMSequence <= m.BudgetHeld.RecordedSequence || last.Cancellation.VMSequence >= m.BudgetBoundary.ThroughSequence || last.Intent.Command.Request.Key.Instance != request.Key.Instance || compareKey(last.Intent.Command.Request.Key, m.BudgetBoundary.ThroughKey) > 0 || compareKey(last.Intent.Command.Request.Key, m.BudgetBoundary.ThroughKey) == 0 && last.Intent.Command.Request.Key != m.BudgetBoundary.ThroughKey {
		return out, fmt.Errorf("current native cancellation is outside original closed VM budget chain")
	}
	joint, err := r.CheckRetainedAfterCancelled(ctx, sources, request, final.PreparedSHA256, final.GuestSHA256, final.Finalized.Sources.HostClaim, last.Intent.Command, last.Intent.JournalSHA256, m.Owner)
	if err != nil {
		return out, err
	}
	return CurrentBudgetVM{Budget: joint.Prior, Cancellation: &joint.Cancellation}, nil
}

// RetainedAfterCancel明确保留两个不同原意图的证明，不把较新取消回执替换成
// 旧Completed，也不让正常完成消费者自动接受State.Last已被改变的旧来源。
// 此返回值只描述Runner域；PG/Root/Host/Outside及原VM关闭链仍须协调器核验。
type RetainedAfterCancel struct {
	Prior        Confirmed
	Cancellation Confirmed
}

// CheckRetainedAfterCancelled用于当前取消已封成不可再发送终态后的原容量恢复。
// 普通not_sent、may_have_sent、另一boot/Pod/Node、同名新UID或当前数字等于
// before都不够。两端不可变回执与当前原State各重读一次，不持本地锁跨网络；
// 调用方最后还须以同VM UID/RV线性化，不能把本结果缓存为执行/回收许可。
func (r *Reader) CheckRetainedAfterCancelled(ctx context.Context, sources budget.RunnerSources, request control.Request, preparedSHA, guestSHA string, host budget.ObjectRef, cancelled Command, outer string, actor journal.Owner) (RetainedAfterCancel, error) {
	var out RetainedAfterCancel
	prior, err := r.retainedReceipt(ctx, sources, request, preparedSHA, guestSHA, host)
	if err != nil {
		return out, err
	}
	if !prior.Complete() || cancelled.Validate() != nil || cancelled.Request.Key.Instance != request.Key.Instance || compareKey(request.Key, cancelled.Request.Key) >= 0 || prior.Intent.Command.Identity != cancelled.Identity || sources.BirthClaim != cancelled.BindingClaim || cancelled.BeforeBytes != request.TargetVMMemoryBytes {
		return out, fmt.Errorf("original completed budget and later cancelled VM do not share one native capacity domain")
	}
	tail, err := r.ObserveAbortOriginal(ctx, cancelled, outer, actor)
	if err != nil {
		return out, err
	}
	if !tail.Receipt.CancelledBeforeSend() || tail.Birth.StateClaim != sources.StateClaim || tail.Birth.Identity != prior.Intent.Command.Identity || tail.Actual != prior.Actual || tail.Actual.RequestedBytes != request.TargetVMMemoryBytes || tail.Actual.ActualBytes != request.TargetVMMemoryBytes {
		return out, fmt.Errorf("later original VM cancellation does not preserve prior completed capacity")
	}
	priorAgain, err := r.retainedReceipt(ctx, sources, request, preparedSHA, guestSHA, host)
	if err != nil {
		return out, err
	}
	tailAgain, err := r.ObserveAbortOriginal(ctx, cancelled, outer, actor)
	if err != nil {
		return out, err
	}
	if !reflect.DeepEqual(prior, priorAgain) || !reflect.DeepEqual(tail, tailAgain) {
		return out, fmt.Errorf("original budget or cancellation source changed during joint readback")
	}
	return RetainedAfterCancel{Prior: Confirmed{Sources: sources, Birth: tail.Birth, Receipt: prior, Actual: tail.Actual}, Cancellation: tail}, nil
}
