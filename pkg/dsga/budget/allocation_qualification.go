package budget

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

const (
	// 与原 budgetclaims.r.claim 的正式观察上限一致。结构校验和反序列化都
	// 使用同一界限，避免签发“字段数量合法但消费者永远读不下”的来源。
	MaxAllocationQualificationBytes  = 32 * 1024
	maxAllocationQualificationValues = 4096
)

// QualificationSelection 显式区分旧 VM-only 来源与新分配来源。v1 的 Source 与
// 原 QualificationSource 完全相同；v2 必须带确切分配选择，不允许缺失时按 VM
// bytes 回退。它只选择只读来源，不是 Request、Owner 接受记录或资源写入许可。
type QualificationSelection struct {
	ProtocolVersion uint32              `json:"protocol_version"`
	Source          QualificationSource `json:"source"`
	Allocation      *AllocationSelector `json:"allocation,omitempty"`
}

// AllocationSelector 在同一 VM 大小内仍区分完整 SB/LFC/normal/backing 向量。
// 原运行期 key、PG boot、cache/PG generation 和 journal Sequence 不由这个静态
// ID 替代；未来动作合同必须另外绑定它们，本切片不增加任何执行入口。
type AllocationSelector struct {
	VMMemoryBytes control.Uint64 `json:"vm_memory_bytes"`
	AllocationID  string         `json:"allocation_id"`
	VectorSHA256  string         `json:"vector_sha256"`
}

// QualifiedAllocation 描述受信签发者给出的候选分区，而不是从 SB 字节或 LFC
// 文件长度推算出来的物理事实。ArrayNormalUpperBytes 覆盖完整原数组域，不能
// 只算 buffers*8192；三个域互不重复且共同覆盖 Limits.MemoryMaxBytes。
// BackingProfileSHA256 指向独立固定的真实 backing/持续约束合同；摘要本身不
// 认证该合同。当前读者只校验引用与父包络，不能据此执行缓存或降低任何硬限。
type QualifiedAllocation struct {
	ID                        string               `json:"id"`
	VMMemoryBytes             control.Uint64       `json:"vm_memory_bytes"`
	Buffers                   uint32               `json:"buffers"`
	LFCLimitBytes             control.Uint64       `json:"lfc_limit_bytes"`
	ArrayNormalUpperBytes     control.Uint64       `json:"array_normal_upper_bytes"`
	NoncachePrivateUpperBytes control.Uint64       `json:"noncache_private_upper_bytes"`
	CacheNormalUpperBytes     control.Uint64       `json:"cache_normal_upper_bytes"`
	BackingProfileSHA256      string               `json:"backing_profile_sha256"`
	Limits                    resourceguard.Limits `json:"limits"`
}

// QualifiedAllocationTransition 明确列出允许核验的方向；两端各自存在不表示
// 反向边存在。第一版只描述同 VM、完整原父预算不变的只读候选，尚不能消费预计
// 回收量或降低峰值预算。真正释放和新分配仍需要独立 PG/Root/Owner 合同。
type QualifiedAllocationTransition struct {
	From       string               `json:"from"`
	To         string               `json:"to"`
	PeakLimits resourceguard.Limits `json:"peak_limits"`
}

// AllocationQualification v2 是独立的 immutable 来源，不修改旧平台资格 v1。
// Base 固定原资格对象 UID 和完整原文 SHA；MatchBase 逐项核其 VM/Guest/Runner
// 闭包和完整限额。它不把旧 q.Target(bytes) 扩成任选同大小分配，也不生成可供
// Outside/Host 消费的 Bound。未实现全旧引用释放前，RetainedTarget 始终保持
// 原父 normal/Outside/整 HugeTLB 上界，不能用新分区数值替换原持有预算。
type AllocationQualification struct {
	ProtocolVersion             uint32                          `json:"protocol_version"`
	VMUID                       string                          `json:"vm_uid"`
	Incarnation                 string                          `json:"incarnation"`
	ReportID                    string                          `json:"report_id"`
	Base                        QualificationSource             `json:"base"`
	HoldUntilExplicitSettlement bool                            `json:"hold_until_explicit_settlement"`
	Targets                     []QualifiedAllocation           `json:"targets"`
	Transitions                 []QualifiedAllocationTransition `json:"transitions"`
}

// AllocationObservation 是经过原读链核对的只读值，不是可兑换预算的票据。
// SourceID 与旧 QualifiedTarget.Bounds 分域并绑定原 UID/SHA 与完整向量；
// RetainedTarget 保存原平台整份上界，Candidate 不能被旧自动动作投影成 v1。
type AllocationObservation struct {
	Source         QualificationSource
	Base           QualificationSource
	Selector       AllocationSelector
	SourceID       string
	Candidate      QualifiedAllocation
	RetainedTarget QualifiedTarget
}

func (s QualificationSelection) Validate() error {
	if s.Source.Validate() != nil {
		return fmt.Errorf("invalid original qualification selection source")
	}
	if s.ProtocolVersion == 1 && s.Allocation == nil {
		return nil
	}
	if s.ProtocolVersion == 2 && s.Allocation != nil && s.Allocation.Validate() == nil {
		return nil
	}
	return fmt.Errorf("qualification selection version or allocation differs")
}

func (s AllocationSelector) Validate() error {
	if s.VMMemoryBytes == 0 || s.VMMemoryBytes > math.MaxInt64 || control.ValidateID(s.AllocationID, "allocation") != nil || !validDigest(s.VectorSHA256) {
		return fmt.Errorf("incomplete exact allocation selector")
	}
	return nil
}

func (a QualifiedAllocation) Validate() error {
	if control.ValidateID(a.ID, "allocation") != nil || a.VMMemoryBytes == 0 || a.VMMemoryBytes > math.MaxInt64 ||
		a.Buffers < 16 || a.Buffers > uint32((1<<31-1)/2) || !validDigest(a.BackingProfileSHA256) || a.Limits.Validate() != nil ||
		a.ArrayNormalUpperBytes < control.Uint64(a.Buffers)*8192 || a.NoncachePrivateUpperBytes == 0 {
		return fmt.Errorf("invalid exact qualified allocation")
	}
	total := uint64(a.ArrayNormalUpperBytes)
	for _, n := range []control.Uint64{a.NoncachePrivateUpperBytes, a.CacheNormalUpperBytes} {
		if uint64(n) > math.MaxUint64-total {
			return fmt.Errorf("allocation normal partition overflows")
		}
		total += uint64(n)
	}
	if total != uint64(a.Limits.MemoryMaxBytes) {
		return fmt.Errorf("allocation partition differs from complete normal limit")
	}
	return nil
}

func (a QualifiedAllocation) Selector() (AllocationSelector, error) {
	if err := a.Validate(); err != nil {
		return AllocationSelector{}, err
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return AllocationSelector{}, err
	}
	digest := sha256.Sum256(append([]byte("neon-dsga-qualified-allocation-vector-v2\x00"), raw...))
	return AllocationSelector{VMMemoryBytes: a.VMMemoryBytes, AllocationID: a.ID, VectorSHA256: hex.EncodeToString(digest[:])}, nil
}

func (q AllocationQualification) Validate() error {
	if q.ProtocolVersion != 2 || control.ValidateID(q.VMUID, "allocation VM") != nil || control.ValidateIncarnation(q.Incarnation) != nil ||
		control.ValidateID(q.ReportID, "allocation report") != nil || q.Base.Validate() != nil || !q.HoldUntilExplicitSettlement ||
		len(q.Targets) == 0 || len(q.Targets) > 32 || len(q.Transitions) > 128 {
		return fmt.Errorf("incomplete original allocation qualification")
	}
	byID := make(map[string]QualifiedAllocation, len(q.Targets))
	var previous control.Uint64
	for _, t := range q.Targets {
		if _, exists := byID[t.ID]; exists || t.Validate() != nil || t.VMMemoryBytes < previous {
			return fmt.Errorf("duplicate, invalid or unordered allocation")
		}
		byID[t.ID], previous = t, t.VMMemoryBytes
	}
	seen := make(map[[2]string]bool, len(q.Transitions))
	for _, edge := range q.Transitions {
		left, hasLeft := byID[edge.From]
		right, hasRight := byID[edge.To]
		key := [2]string{edge.From, edge.To}
		if !hasLeft || !hasRight || edge.From == edge.To || seen[key] || left.VMMemoryBytes != right.VMMemoryBytes || edge.PeakLimits.Validate() != nil {
			return fmt.Errorf("invalid exact same-VM allocation transition")
		}
		seen[key] = true
	}
	// 目标数与边数分别有界，不代表其组合可进入正式只读载荷。校验规范化的
	// 完整 typed 原文；wire 别名避免未来增加 MarshalJSON 时递归调用 Validate。
	type wire AllocationQualification
	raw, err := json.Marshal(wire(q))
	if err != nil {
		return err
	}
	if err := control.ValidateJSON(raw, MaxAllocationQualificationBytes, maxAllocationQualificationValues); err != nil {
		return fmt.Errorf("allocation qualification exceeds formal read budget: %w", err)
	}
	return nil
}

func (q AllocationQualification) MatchBase(source QualificationSource, base PlatformQualification) error {
	if q.Validate() != nil || source != q.Base || base.Validate() != nil || q.VMUID != base.Identity.VMUID || q.Incarnation != base.Incarnation {
		return fmt.Errorf("allocation differs from original platform qualification")
	}
	for _, t := range q.Targets {
		parent, err := base.Target(t.VMMemoryBytes)
		if err != nil {
			return err
		}
		// 同父上界只允许候选 normal 数值较小或相同，其余原 PIDs/AS/System/
		// Outside/整 HugeTLB 逐字保持。这里绝不将较小候选改成已生效的约束。
		expected := parent.Limits
		expected.MemoryMaxBytes = t.Limits.MemoryMaxBytes
		if !reflect.DeepEqual(expected, t.Limits) || t.Limits.MemoryMaxBytes > parent.Limits.MemoryMaxBytes {
			return fmt.Errorf("allocation changes retained complete parent limits")
		}
	}
	for _, edge := range q.Transitions {
		for _, t := range q.Targets {
			if t.ID == edge.From {
				parent, _ := base.Target(t.VMMemoryBytes)
				if !reflect.DeepEqual(edge.PeakLimits, parent.Limits) {
					return fmt.Errorf("allocation peak does not retain original complete budget")
				}
				break
			}
		}
	}
	return nil
}

// Observe 只做原来源与精确候选的闭合校验；source 的原 UID/raw SHA 必须由
// Kubernetes 读者重新 GET 验证。调用此方法不能自行认证客户端提交的资格。
func (q AllocationQualification) Observe(source QualificationSource, selector AllocationSelector, base PlatformQualification) (AllocationObservation, error) {
	if source.Validate() != nil || selector.Validate() != nil || q.MatchBase(q.Base, base) != nil {
		return AllocationObservation{}, fmt.Errorf("allocation observation lacks original complete sources")
	}
	for _, candidate := range q.Targets {
		actual, err := candidate.Selector()
		if err != nil || actual != selector {
			continue
		}
		parent, _ := base.Target(candidate.VMMemoryBytes)
		context := struct {
			Source   QualificationSource `json:"source"`
			Base     QualificationSource `json:"base"`
			Selector AllocationSelector  `json:"selector"`
		}{source, q.Base, selector}
		raw, err := json.Marshal(context)
		if err != nil {
			return AllocationObservation{}, err
		}
		digest := sha256.Sum256(append([]byte("neon-dsga-qualified-allocation-source-v2\x00"), raw...))
		candidate.Limits.HugePools = append([]resourceguard.HugePool(nil), candidate.Limits.HugePools...)
		parent.Limits.HugePools = append([]resourceguard.HugePool(nil), parent.Limits.HugePools...)
		return AllocationObservation{Source: source, Base: q.Base, Selector: selector, SourceID: "qualified-allocation-v2-" + hex.EncodeToString(digest[:]), Candidate: candidate, RetainedTarget: parent}, nil
	}
	return AllocationObservation{}, fmt.Errorf("exact VM/allocation/vector has no retained qualification")
}

func (s *QualificationSelection) UnmarshalJSON(raw []byte) error {
	type plain QualificationSelection
	var value plain
	if err := control.UnmarshalObject(raw, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if _, present := fields["allocation"]; value.ProtocolVersion == 1 && present {
		return fmt.Errorf("legacy qualification selection cannot carry allocation")
	}
	next := QualificationSelection(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*s = next
	return nil
}

func (s *AllocationSelector) UnmarshalJSON(raw []byte) error {
	type plain AllocationSelector
	var value plain
	if err := control.UnmarshalObject(raw, &value); err != nil {
		return err
	}
	next := AllocationSelector(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*s = next
	return nil
}

func (a *QualifiedAllocation) UnmarshalJSON(raw []byte) error {
	type plain QualifiedAllocation
	var value plain
	if err := control.UnmarshalObject(raw, &value); err != nil {
		return err
	}
	next := QualifiedAllocation(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*a = next
	return nil
}

func (e *QualifiedAllocationTransition) UnmarshalJSON(raw []byte) error {
	type plain QualifiedAllocationTransition
	var value plain
	if err := control.UnmarshalObject(raw, &value); err != nil {
		return err
	}
	*e = QualifiedAllocationTransition(value)
	return nil
}

func (q *AllocationQualification) UnmarshalJSON(raw []byte) error {
	type plain AllocationQualification
	var value plain
	if err := control.UnmarshalObjectBounded(raw, &value, MaxAllocationQualificationBytes, maxAllocationQualificationValues); err != nil {
		return err
	}
	next := AllocationQualification(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*q = next
	return nil
}
