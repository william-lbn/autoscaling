package dsga

import "fmt"

// ValidateRejectedResizeBarrier认证“原执行明确拒绝、较高执行权已封住旧请求”的
// 独立屏障。它不制造原生Denied，不修改原拒绝，也不证明任何资源已释放。
// 仅NotFound从来不够：还须有原Resize的Busy/Rejected完整回执、严格较高的
// 原生floor、已退出的串行执行者、同boot且未前进的容量，以及屏障后的原Key
// 查询。这样晚到的旧POST只能被原生floor拒绝，不能再产生新的容量效果。
// 同epoch、结果淘汰、正在执行、已提交或未知原执行均拒绝；不能为通过此处
// 自动换owner。真实Session/UID/RV及预算来源由调用者另行认证并持久保存。
func ValidateRejectedResizeBarrier(intent Request, original ControlResponse, fence FenceResponse, status ControlResponse) error {
	instance := intent.Key.Instance
	if original.ValidateResize(intent, instance) != nil || original.Receipt.Result != nil ||
		(original.Receipt.Disposition != ReceiptBusy && original.Receipt.Disposition != ReceiptRejected) ||
		fence.RequestedEpoch <= intent.Key.IssuerEpoch || original.Observed.IssuerFloorEpoch > fence.RequestedEpoch {
		return fmt.Errorf("rejected resize barrier lacks original rejection and strictly newer owner")
	}
	request := FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: fence.RequestedEpoch}
	if fence.ValidateFor(request, instance) != nil || fence.Disposition != Fenced ||
		fence.Observed.CapacityGeneration != intent.ExpectedCapacityGeneration ||
		status.ValidateResize(intent, instance) != nil || status.Receipt.Result != nil ||
		(status.Receipt.Disposition != ReceiptNotFound && status.Receipt.Disposition != ReceiptStale) ||
		status.Observed.IssuerFloorEpoch != fence.RequestedEpoch || status.Observed.CapacityGeneration != intent.ExpectedCapacityGeneration {
		return fmt.Errorf("rejected resize barrier lacks current fenced unchanged original domain")
	}
	for _, snapshot := range []Snapshot{fence.Observed, status.Observed} {
		if op := snapshot.Operation; op != nil && (op.Phase == Preparing || op.Key == intent.Key) {
			return fmt.Errorf("rejected resize barrier contradicts native operation")
		}
	}
	return nil
}
