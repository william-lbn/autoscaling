package budget

import (
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// QuiesceCommand 固定缩容前的 PG 准入状态和 Root 硬预算来源。旧版保存原关闭意图；
// 在线版只记录已开放的原 revision。丢回复后只能继续读取同一份原始来源。
// omitempty 告知严格读取器此字段可缺省，omitzero 使 Go 的值类型在在线协议中实际省略。
type QuiesceCommand struct {
	ProtocolVersion uint32                   `json:"protocol_version"`
	VMShrinkPermit  bool                     `json:"vm_shrink_permit"`
	Request         control.Request          `json:"request"`
	Close           control.AdmissionRequest `json:"close,omitempty,omitzero"`
	ServingRevision control.Uint64           `json:"serving_revision,omitempty"`
	PGAggregate     RootAggregate            `json:"pg_aggregate"`
}

func (c QuiesceCommand) Validate() error {
	r := c.Request
	switch c.ProtocolVersion {
	case 1:
		if c.ServingRevision != 0 || c.Close.Validate() != nil || c.Close.Open || c.Close.Instance != r.Key.Instance || c.Close.BudgetCommitmentID != r.BudgetCommitmentID || c.Close.ExpectedCapacityGeneration != r.ExpectedCapacityGeneration || c.Close.IssuerEpoch < r.Key.IssuerEpoch {
			return fmt.Errorf("invalid original quiesce close intent")
		}
	case 2:
		// 在线路径只核原生回执，不能伪造一个从未执行的开门/关门请求。
		if c.ServingRevision == 0 || c.Close != (control.AdmissionRequest{}) {
			return fmt.Errorf("online shrink requires a serving revision and no gate write")
		}
	default:
		return fmt.Errorf("unknown shrink preparation protocol")
	}
	if c.VMShrinkPermit || r.Validate() != nil || r.Kind != control.Shrink || c.PGAggregate.Validate() != nil || c.PGAggregate.Identity.VMUID != r.Key.Instance.VMUID || c.PGAggregate.Identity.ComputeInstanceID != r.Key.Instance.ComputeInstanceID {
		return fmt.Errorf("invalid complete original quiesce command")
	}
	return nil
}

func (c QuiesceCommand) ValidateFor(p Prepared, epoch control.Uint64) error {
	if c.Validate() != nil || p.ValidateFor(p.Request, p.Identity) != nil || c.Request != p.Request || c.Close.IssuerEpoch > epoch || c.PGAggregate.Identity != p.Identity {
		return fmt.Errorf("quiesce differs from original shrink/prepared/closing intent")
	}
	if (c.ProtocolVersion == 2) != (p.OnlineShrinkProtocolVersion == 1) {
		return fmt.Errorf("shrink checkpoint protocol differs from durable preparation")
	}
	if c.ProtocolVersion == 2 && (p.Shared.Observed.AdmissionRevision != c.ServingRevision || c.Request.Key.IssuerEpoch != epoch) {
		return fmt.Errorf("online shrink original serving revision or owner changed")
	}
	// 持续策略不能借一次current/RSS压低原normal承诺；该检查也覆盖显式
	// 重放，不因当前owner或新资源来源变更而重写原命令中的承诺。
	normal := c.PGAggregate.Bootstrap.Limits.MemoryMaxBytes
	if c.PGAggregate.Held != nil {
		normal = c.PGAggregate.Held.MemoryMaxBytes
	}
	if p.Limits.AddressSpaceMaxBytes != c.PGAggregate.Bootstrap.Limits.AddressSpaceMaxBytes || p.Limits.PidsMaxTasks != c.PGAggregate.Bootstrap.Limits.PidsMaxTasks || p.Limits.SystemMemoryMaxBytes != c.PGAggregate.Bootstrap.Limits.SystemMemoryMaxBytes {
		return fmt.Errorf("quiesce cannot change immutable task limit")
	}
	// 完整 normal1 Prepared 只允许准备和排水。硬限及 VM 下降仍需后段真实释放证明。
	if p.Limits.MemoryMaxBytes < normal && p.NormalQuotaProtocolVersion != 1 {
		return fmt.Errorf("continuous aggregate quiesce cannot authorize memory.max decrease")
	}
	return nil
}

func (c *QuiesceCommand) UnmarshalJSON(data []byte) error {
	type plain QuiesceCommand
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	value := QuiesceCommand(next)
	if err := value.Validate(); err != nil {
		return err
	}
	*c = value
	return nil
}

// Quiesced 记录同次 PG 观测。旧协议等待关闭和前台排空；在线协议核验原开放 revision，
// 由持续硬预算覆盖前台、后台、2PC、DSM 和会话私有内存。两条路径都保留这些成本。
// 本事实仅可推进PG数组排水；不能降低memory.max、开放准入、释放Host或缩VM。
// 完整外部成本仍来自同账本受信Prepared生产者，结构校验不认证自报来源。
type Quiesced struct {
	ProtocolVersion uint32                   `json:"protocol_version"`
	VMShrinkPermit  bool                     `json:"vm_shrink_permit"`
	Request         control.Request          `json:"request"`
	Close           control.AdmissionRequest `json:"close,omitempty,omitzero"`
	ServingRevision control.Uint64           `json:"serving_revision,omitempty"`
	DSM             control.DSMResponse      `json:"dsm"`
	PGAggregate     RootAggregate            `json:"pg_aggregate"`
}

// MatchesAdmission 核对当前 PG 与原缩容准备所绑定的门状态和 revision。
// 其它硬预算、真实数组释放以及 Runner 的完整证明仍须分别核对。
func (q Quiesced) MatchesAdmission(s control.Snapshot) bool {
	if s.Instance != q.Request.Key.Instance || s.AdmissionRevision == 0 {
		return false
	}
	switch q.ProtocolVersion {
	case 1:
		return !s.AdmissionOpen && s.AdmissionRevision == q.Close.Revision
	case 2:
		return s.AdmissionOpen && s.AdmissionRevision == q.ServingRevision
	default:
		return false
	}
}

func (q Quiesced) OriginalCommand() QuiesceCommand {
	return QuiesceCommand{ProtocolVersion: q.ProtocolVersion, VMShrinkPermit: q.VMShrinkPermit, Request: q.Request, Close: q.Close, ServingRevision: q.ServingRevision, PGAggregate: q.PGAggregate}
}

// ValidateFor 核对同 boot、原预算、原准入 revision 和原聚合来源；DSM 内嵌一次原生采样
// 的Prepared/Connection/Work，不能拼接不同时间的门禁ack和“空闲”。背景/2PC/
// DSM计数可以非零，只有持续原约束覆盖它们时才采用此策略，不能清原未知占用。
func (q Quiesced) ValidateFor(p Prepared, epoch control.Uint64) error {
	ready, err := q.ReadyFor(p, epoch)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("original close is not continuously acknowledged and foreground idle")
	}
	return nil
}

// ReadyFor 先核原准入、Root 和原生采样，再检查对应模式的 ACK / 前台条件。
// false,nil 是等待状态；ValidateFor 及持久 Record 仍严格拒绝。
func (q Quiesced) ReadyFor(p Prepared, epoch control.Uint64) (bool, error) {
	r := p.Request
	command := q.OriginalCommand()
	if err := command.ValidateFor(p, epoch); err != nil {
		return false, err
	}
	if q.DSM.ValidateFor(r.Key.Instance) != nil {
		return false, fmt.Errorf("quiesce lacks original aggregate or native accounting")
	}
	c := q.DSM.Prepared.Connections
	w := c.Work
	s := w.Observed
	if !c.Enabled || !c.PrivateManagementReady || !w.ObserverExcluded || s.IssuerFloorEpoch != epoch || s.CapacityGeneration != r.ExpectedCapacityGeneration || s.LowBuffers != p.Shared.Observed.LowBuffers || s.HighBuffers != s.LowBuffers || s.MaximumBuffers != p.Shared.Observed.MaximumBuffers || (s.Operation != nil && s.Operation.Phase == control.Preparing) {
		return false, fmt.Errorf("original close is not continuously acknowledged and foreground idle")
	}
	for _, a := range s.Arrays {
		if a.AllocationUnknown {
			return false, fmt.Errorf("quiesce native backing is unknown")
		}
	}
	if q.ProtocolVersion == 2 {
		if !q.MatchesAdmission(s) || !c.RequestedOpen || c.PublicConnectionsBlocked {
			return false, fmt.Errorf("online shrink original serving gate changed")
		}
		return c.Acknowledged, nil
	}
	if !q.MatchesAdmission(s) {
		return false, fmt.Errorf("original shrink close revision changed")
	}
	return c.Acknowledged && c.PublicConnectionsBlocked && w.ForegroundIdle, nil
}

func (q *Quiesced) UnmarshalJSON(data []byte) error {
	type plain Quiesced
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	value := Quiesced(next)
	// Close 仅在线 v2 协议可缺省；结构标签需要兼容它，不能因此让旧 v1
	// 丢失原关闭身份后静默解码为零值。这里核完整原命令而不核 ACK/空闲，
	// 使合法等待仍可读取，但缺失关闭、跨版本门状态和 VM 许可必须先拒绝。
	if err := value.OriginalCommand().Validate(); err != nil {
		return err
	}
	*q = value
	return nil
}
