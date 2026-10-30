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

// CurrentBudgetVM保留当前预算的原生证据；普通预算可另有较新取消尾证据，
// 实际预算自身也可来自不可再发送的原域取消。仅封闭原VM关闭链的消费者
// 可使用，普通执行/Finalize读者仍用严格CheckRetained。
type CurrentBudgetVM struct {
	Budget       Confirmed
	Cancellation *Confirmed
	// 与Budget互斥；原启动从未有命令时不能冒充普通Completed。
	Initial *InitialConfirmed
}

// CheckCurrentBudget读取已关闭VM的永久预算。历史淘汰后仍从当前原State找到
// 不可变取消回执，必须处于真实VM关闭Through以内，且取消Witness序列晚于原
// 预算、早于关闭序列；不能仅按当前数值或State.Last的文字kind恢复旧来源。
func (r *Reader) CheckCurrentBudget(ctx context.Context, m journal.Model) (CurrentBudgetVM, error) {
	var out CurrentBudgetVM
	if m.CapacityHeld != nil {
		capacity, err := m.CurrentCapacity()
		if err != nil || m.CheckClosedCapacity(capacity.Request) != nil {
			return out, fmt.Errorf("current Runner actual capacity lacks original closed permanent budget")
		}
		return r.checkHeldCapacity(ctx, capacity, m.Owner)
	}
	var final budget.FinalizationCommand
	if m.BudgetHeld == nil || json.Unmarshal(m.BudgetHeld.Finalization.Command, &final) != nil || final.Validate() != nil || final.Finalized.Sources == nil || final.Finalized.Sources.Runner == nil || m.CheckClosedBudget(final.Finalized.Request) != nil {
		return out, fmt.Errorf("current Runner budget requires original closed VM boundary")
	}
	return r.checkHeldFinalization(ctx, m, final, m.BudgetBoundary)
}

func (r *Reader) checkHeldFinalization(ctx context.Context, m journal.Model, final budget.FinalizationCommand, boundary *journal.ClosedBudgetBoundary) (CurrentBudgetVM, error) {
	var out CurrentBudgetVM
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
		return CurrentBudgetVM{Budget: confirmed, Cancellation: nil, Initial: nil}, nil
	}
	last := publication.State.Last
	if m.ProtocolVersion < 7 || boundary == nil || last == nil || last.Kind != cancelledBeforeSend || last.Cancellation == nil || last.Cancellation.VMSequence <= m.BudgetHeld.RecordedSequence || last.Cancellation.VMSequence >= boundary.ThroughSequence || last.Intent.Command.Request.Key.Instance != request.Key.Instance || compareKey(last.Intent.Command.Request.Key, boundary.ThroughKey) > 0 || compareKey(last.Intent.Command.Request.Key, boundary.ThroughKey) == 0 && last.Intent.Command.Request.Key != boundary.ThroughKey {
		return out, fmt.Errorf("current native cancellation is outside original closed VM budget chain")
	}
	joint, err := r.CheckRetainedAfterCancelled(ctx, sources, request, final.PreparedSHA256, final.GuestSHA256, final.Finalized.Sources.HostClaim, last.Intent.Command, last.Intent.JournalSHA256, m.Owner)
	if err != nil {
		return out, err
	}
	return CurrentBudgetVM{Budget: joint.Prior, Cancellation: &joint.Cancellation, Initial: nil}, nil
}

// CheckActiveHeldBudget只核新请求尚保留的旧完整永久预算。它不把Active
// 投影成已关闭Model，也不调用结清/释放接口；来源认证成功仍不是新派发许可。
// Begin尚未Start时使用原全局水位，Start后只使用原子捕获的Prior水位；
// 两者都必须逐字段绑定原预算，绝不以新Active的Key或目标数值补造关闭链。
func (r *Reader) CheckActiveHeldBudget(ctx context.Context, m journal.Model) (CurrentBudgetVM, error) {
	var out CurrentBudgetVM
	if err := m.Validate(); err != nil {
		return out, err
	}
	if m.Active == nil || m.Active.Cleanup != nil {
		return out, fmt.Errorf("active retained Runner budget requires original operation without cleanup")
	}
	source, err := m.CurrentRetainedSource()
	if err != nil {
		return out, err
	}
	if source.Initial != nil {
		return out, fmt.Errorf("initial retained budget requires original boot reader")
	}
	var final budget.FinalizationCommand
	var request control.Request
	if source.Capacity != nil {
		request = source.Capacity.Request
	} else if source.Finalization != nil && json.Unmarshal(source.Finalization.Command, &final) == nil && final.Validate() == nil && final.Finalized.Sources != nil && final.Finalized.Sources.Runner != nil {
		request = final.Finalized.Request
	} else {
		return out, fmt.Errorf("active retained Runner budget lacks complete original source")
	}
	boundary := m.BudgetBoundary
	if prior := m.Active.PriorBudgetBoundary; prior != nil {
		if boundary != nil {
			return out, fmt.Errorf("active retained Runner budget has ambiguous closed anchors")
		}
		boundary = prior
	} else if boundary != nil {
		// 全局旧水位只在Begin后、首次Start前仍有效。已派发操作不能
		// 将撤销过的全局水位重新挂回，以逃过原子捕获的Prior认证。
		for _, step := range m.Active.Steps {
			if step.Phase != journal.Planned {
				return out, fmt.Errorf("dispatched active Runner budget lacks captured original anchor")
			}
		}
	}
	if boundary == nil || boundary.BudgetKey != request.Key || boundary.FinalizationSHA256 != source.SourceSHA256 || boundary.RecordedSequence != source.RecordedSequence || boundary.ThroughKey.Instance != request.Key.Instance || compareKey(boundary.ThroughKey, m.Active.Request.Key) >= 0 {
		return out, fmt.Errorf("active retained Runner budget lacks exact original closed anchor")
	}
	if source.Capacity != nil {
		return r.checkHeldCapacity(ctx, *source.Capacity, m.Owner)
	}
	return r.checkHeldFinalization(ctx, m, final, boundary)
}

// 实际预算已固定原Completed/Cancelled或Initial来源；继续走各自严格读者，
// 不因新增Active入口放宽CheckCapacity的当前State.Last及不可变回执约束。
func (r *Reader) checkHeldCapacity(ctx context.Context, capacity budget.ReconciledCapacity, owner journal.Owner) (CurrentBudgetVM, error) {
	var out CurrentBudgetVM
	if capacity.InitialVM != nil {
		if capacity.InitialOutside == nil {
			return out, fmt.Errorf("initial VM capacity lost complete original boot budget")
		}
		proof, err := r.CheckInitialCapacity(ctx, *capacity.InitialVM, *capacity.InitialOutside, owner)
		if err != nil {
			return out, err
		}
		out.Initial = &proof
		return out, nil
	}
	proof, err := r.CheckCapacity(ctx, capacity.VM, owner)
	if err != nil {
		return out, err
	}
	return CurrentBudgetVM{Budget: proof, Cancellation: nil, Initial: nil}, nil
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
