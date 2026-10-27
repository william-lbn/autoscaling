package vmguard

import (
	"context"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// CapacitySource仅从已核验回执提取可永久保留的VM容量来源；结构仍可由Go调用
// 方构造，故消费者必须重新CheckCapacity，不能把构造成功当网络认证。完成和
// 未发送取消使用不同语义，普通not_sent/may_have_sent一律不产生容量结清源。
func CapacitySource(p Confirmed) (budget.NativeCapacity, error) {
	var out budget.NativeCapacity
	r, c := p.Receipt, p.Receipt.Intent.Command
	if p.Sources.Validate() != nil || p.Birth.Validate() != nil || r.Validate() != nil || (!r.Complete() && !r.CancelledBeforeSend()) ||
		p.Birth.Identity != c.Identity || p.Birth.StateClaim != p.Sources.StateClaim || c.BindingClaim != p.Sources.BirthClaim || r.Intent.CommandSHA256 != p.Sources.CommandSHA256 || r.Intent.JournalSHA256 != p.Sources.JournalSHA256 {
		return out, fmt.Errorf("native capacity lacks complete original Runner context")
	}
	out = budget.NativeCapacity{Sources: p.Sources, Request: c.Request, BeforeBytes: c.BeforeBytes, Cancelled: r.CancelledBeforeSend(), PreparedSHA256: c.PreparedSHA256, GuestSHA256: c.GuestSHA256, HostClaim: c.HostClaim}
	if out.Validate() != nil || p.Actual.validate(c.Identity) != nil || p.Actual.RequestedBytes != out.MemoryBytes() || p.Actual.ActualBytes != out.MemoryBytes() {
		return budget.NativeCapacity{}, fmt.Errorf("native original receipt does not match current actual capacity")
	}
	return out, nil
}

// CheckCapacity认证预算记录的准确原VM命令及当前State。取消只有不可再发送的
// cancelled_before_send可用；正常完成仍要求当前Last对应该完成。它不放宽
// CheckRetained，不拿当前数值相同替代旧命令未知，不派发QMP或清理任何来源。
func (r *Reader) CheckCapacity(ctx context.Context, source budget.NativeCapacity, actor journal.Owner) (Confirmed, error) {
	var none Confirmed
	if source.Validate() != nil || source.Request.Key.Instance.VMUID != string(r.binding.VMUID) {
		return none, fmt.Errorf("native capacity source differs from fixed original VM")
	}
	receipt, err := r.retainedReceipt(ctx, source.Sources, source.Request, source.PreparedSHA256, source.GuestSHA256, source.HostClaim)
	if err != nil {
		return none, err
	}
	if receipt.Intent.Command.BeforeBytes != source.BeforeBytes || source.Cancelled != receipt.CancelledBeforeSend() || (!source.Cancelled && !receipt.Complete()) {
		return none, fmt.Errorf("native capacity changed original completion or unsent cancellation")
	}
	wanted := completed
	if source.Cancelled {
		wanted = cancelledBeforeSend
	}
	proof, err := r.confirmKind(ctx, source.Sources, receipt.Intent.Command, actor, wanted)
	if err != nil {
		return none, err
	}
	checked, err := CapacitySource(proof)
	if err != nil || checked != source {
		return none, fmt.Errorf("native capacity current readback changed original budget source")
	}
	return proof, nil
}

// CapacityForOriginal只读当前原State，先核完整命令/外层WAL摘要再导出来源。
// 用于尚无外层Completed、但原Runner已达终态的恢复；它不会按目标新造一个
// 命令，也不会将只读取得的事实当作原业务请求所有步骤已完成。
func (r *Reader) CapacityForOriginal(ctx context.Context, command Command, outerSHA string, actor journal.Owner) (budget.NativeCapacity, Confirmed, error) {
	proof, err := r.ObserveAbortOriginal(ctx, command, outerSHA, actor)
	if err != nil {
		return budget.NativeCapacity{}, proof, err
	}
	source, err := CapacitySource(proof)
	return source, proof, err
}
