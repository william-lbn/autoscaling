package journal

import (
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// currentCapacityRecord仅选择当前永久实际预算的原格式，不进行Model.Validate，
// 因为预算/关闭水位校验本身也需要本函数。它不是网络认证或资源释放许可。
// v1/v2逐字沿用原Cleanup预算及命令摘要；v3独立资源恢复只读自己的完整
// 预算证明，不能用零Cleanup伪造原成功，也不按当前资源值临时拼一份预算。
func (m Model) currentCapacityRecord() (budget.ReconciledCapacity, string, control.Uint64, error) {
	var capacity budget.ReconciledCapacity
	h := m.CapacityHeld
	if h == nil || h.RecordedSequence == 0 {
		return capacity, "", 0, fmt.Errorf("current actual capacity has no retained permanent header")
	}
	var sourceSHA string
	switch h.ProtocolVersion {
	case 1, 2:
		if h.ActiveRecovery != nil || (h.ProtocolVersion == 1) != (h.Recovery == nil) || h.Cleanup.DispatchedBy == nil {
			return capacity, "", 0, fmt.Errorf("legacy actual capacity mixed independent resource recovery")
		}
		var command budget.ReconciledAbortCommand
		if json.Unmarshal(h.Cleanup.Command, &command) != nil || command.ValidateFor(h.Cleanup.DispatchedBy.Epoch) != nil {
			return capacity, "", 0, fmt.Errorf("current actual capacity lost original complete Cleanup command")
		}
		capacity, sourceSHA = command.Capacity, h.Cleanup.CommandSHA256
	case 3:
		var noStep Step
		if h.ActiveRecovery == nil || h.Recovery != nil || !reflect.DeepEqual(h.Cleanup, noStep) {
			return capacity, "", 0, fmt.Errorf("independent actual capacity cannot project original Cleanup completion")
		}
		capacity, sourceSHA = h.ActiveRecovery.Proof.Capacity, h.ActiveRecovery.Proof.CapacitySHA256
	default:
		return capacity, "", 0, fmt.Errorf("unknown permanent actual-capacity format")
	}
	if !lifecycleDigest(sourceSHA) {
		return budget.ReconciledCapacity{}, "", 0, fmt.Errorf("current actual capacity source digest is invalid")
	}
	// v3预算直接存在Proof结构中；返回值必须拥有PGOperation、InitialOutside、
	// InitialVM及其嵌套指针/切片。统一有界严格解码也保留旧路径的拒绝语义，
	// 不让调用方改返回值后污染持久Proof或绕过实际预算自身的版本校验。
	raw, err := json.Marshal(capacity)
	if err != nil {
		return budget.ReconciledCapacity{}, "", 0, err
	}
	if h.ProtocolVersion == 3 && rawDigest(raw) != sourceSHA {
		return budget.ReconciledCapacity{}, "", 0, fmt.Errorf("independent actual-capacity digest differs from complete budget")
	}
	var copied budget.ReconciledCapacity
	// 使用类型自己的有界解码器；直接把带UnmarshalJSON的方法类型传给
	// UnmarshalObjectBounded会被其plain-alias防递归规则拒绝。该解码器
	// 同时保留旧版本64KiB/2048及独立v4来源的专属有界限制。
	if err := json.Unmarshal(raw, &copied); err != nil {
		return budget.ReconciledCapacity{}, "", 0, err
	}
	return copied, sourceSHA, h.RecordedSequence, nil
}

// CurrentCapacity返回整个当前模型已经校验的实际预算深副本。原Request保持
// 原目标，实际VM/PG容量另列；结构读取成功不能替代固定来源Reader的重新认证。
func (m Model) CurrentCapacity() (budget.ReconciledCapacity, error) {
	if err := m.Validate(); err != nil {
		return budget.ReconciledCapacity{}, err
	}
	capacity, _, _, err := m.currentCapacityRecord()
	return capacity, err
}

// CapacitySourceSHA256给永久关闭水位绑定准确的预算来源。旧头仍是原Cleanup
// 命令摘要，新头使用独立实际预算摘要；不得用原操作或Runner命令摘要代替。
func (m Model) CapacitySourceSHA256() (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	_, digest, _, err := m.currentCapacityRecord()
	return digest, err
}
