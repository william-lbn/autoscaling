package dsga

import "fmt"

// ResizeRecoveryEffect明确区分原生终态和独立隔离。Isolated保留原执行Unknown，
// 只说明旧执行权已退出且不能再产生未来效果；绝不是原请求NoEffect/成功。
// 这些值只进入协调器持久证据，不增加PG/Compute HTTP wire命令或版本。
type ResizeRecoveryEffect string

const (
	ResizeRecoveredCommitted ResizeRecoveryEffect = "committed"
	ResizeRecoveredDenied    ResizeRecoveryEffect = "denied"
	ResizeRecoveredIsolated  ResizeRecoveryEffect = "isolated_unknown"
)

// SameResizeRecoveryBoundary只比较稳定逻辑容量。floor、准入、Provider和
// 成本可能因接管、关门或后继清理推进，须由各自消费者重新认证，不能由
// 本比较发VM/预算释放许可。调用前必须完整Validate相同boot的原生快照。
func SameResizeRecoveryBoundary(a, b Snapshot) bool {
	return a.Instance == b.Instance && a.CapacityGeneration == b.CapacityGeneration &&
		a.LowBuffers == b.LowBuffers && a.HighBuffers == b.HighBuffers &&
		a.MaximumBuffers == b.MaximumBuffers && a.StartupSharedUpperBytes == b.StartupSharedUpperBytes
}

// SameResizeRecoveryTerminal比较原终态不可改变的完整请求/逻辑事实；只忽略
// 查询来源Replayed，不把后来的物理/成本快照覆盖原回执。
func SameResizeRecoveryTerminal(a, b Snapshot) bool {
	if !SameResizeRecoveryBoundary(a, b) || a.Operation == nil || b.Operation == nil {
		return false
	}
	x, y := *a.Operation, *b.Operation
	x.Replayed, y.Replayed = false, false
	return (x.Phase == Committed || x.Phase == Denied) && x == y
}

// ValidateResizeRecovery核验同一boot中原命令的独立恢复事实。FENCED必须已
// 取得PG resize串行权并完成遗留对账，不能拿PENDING/当前phase替代旧执行者退出。
// Fence→原Key查询期间必须保持同一稳定容量，且只允许原代次或原操作下一代。
//
// 有完整终态时认证原请求摘要；没有终态时只允许真实更高floor，保留原未知。
// 即使历史已淘汰，旧POST也被floor拒绝，已执行的效果则由当前完整容量重新
// 预算；因此它与“明确Rejected且无效果”的屏障分开，不会制造Denied。
func ValidateResizeRecovery(intent Request, fence FenceResponse, status ControlResponse) (ResizeRecoveryEffect, error) {
	instance := intent.Key.Instance
	q := FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: fence.RequestedEpoch}
	if intent.Validate() != nil || fence.RequestedEpoch < intent.Key.IssuerEpoch ||
		fence.ValidateFor(q, instance) != nil || fence.Disposition != Fenced ||
		status.ValidateResize(intent, instance) != nil || status.Observed.IssuerFloorEpoch != fence.RequestedEpoch ||
		!SameResizeRecoveryBoundary(fence.Observed, status.Observed) {
		return "", fmt.Errorf("resize recovery lacks exact current serialized native fence and original key")
	}
	s := status.Observed
	expected := intent.ExpectedCapacityGeneration
	advanced := s.CapacityGeneration > expected && s.CapacityGeneration-expected == 1
	if s.LowBuffers != s.HighBuffers || s.CapacityGeneration != expected && !advanced ||
		advanced && s.HighBuffers != intent.TargetBuffers ||
		s.Operation != nil && s.Operation.Phase == Preparing ||
		fence.Observed.Operation != nil && fence.Observed.Operation.Phase == Preparing {
		return "", fmt.Errorf("resize recovery has unstable or unrelated actual capacity")
	}
	if result := status.Receipt.Result; result != nil {
		if status.Receipt.Disposition != ReceiptReplayed || !SameResizeRecoveryBoundary(*result, s) || result.Operation == nil {
			return "", fmt.Errorf("resize recovery lacks original terminal at actual capacity")
		}
		switch result.Operation.Phase {
		case Committed:
			return ResizeRecoveredCommitted, nil
		case Denied:
			return ResizeRecoveredDenied, nil
		default:
			return "", fmt.Errorf("resize recovery original native operation remains in flight")
		}
	}
	if fence.RequestedEpoch <= intent.Key.IssuerEpoch ||
		(status.Receipt.Disposition != ReceiptStale && status.Receipt.Disposition != ReceiptNotFound && status.Receipt.Disposition != ReceiptResultExpired) {
		return "", fmt.Errorf("unknown resize recovery requires actual strictly newer native owner")
	}
	for _, snapshot := range []Snapshot{fence.Observed, s} {
		if op := snapshot.Operation; op != nil && op.Key == intent.Key {
			return "", fmt.Errorf("missing resize result contradicts native original operation")
		}
	}
	return ResizeRecoveredIsolated, nil
}
