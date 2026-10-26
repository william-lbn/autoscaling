package budget

import (
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// QuiesceCommand在关闭前持久完整原意图和当时实际约束来源。不能只记关闭
// revision而在丢回复后重新选一份root来源，哪怕新来源的数值完全相同。
type QuiesceCommand struct {
	ProtocolVersion uint32                   `json:"protocol_version"`
	VMShrinkPermit  bool                     `json:"vm_shrink_permit"`
	Request         control.Request          `json:"request"`
	Close           control.AdmissionRequest `json:"close"`
	PGAggregate     RootAggregate            `json:"pg_aggregate"`
}

func (c QuiesceCommand) Validate() error {
	r := c.Request
	if c.ProtocolVersion != 1 || c.VMShrinkPermit || r.Validate() != nil || r.Kind != control.Shrink || c.Close.Validate() != nil || c.Close.Open || c.Close.Instance != r.Key.Instance || c.Close.BudgetCommitmentID != r.BudgetCommitmentID || c.Close.ExpectedCapacityGeneration != r.ExpectedCapacityGeneration || c.Close.IssuerEpoch < r.Key.IssuerEpoch || c.PGAggregate.Validate() != nil || c.PGAggregate.Identity.VMUID != r.Key.Instance.VMUID || c.PGAggregate.Identity.ComputeInstanceID != r.Key.Instance.ComputeInstanceID {
		return fmt.Errorf("invalid complete original quiesce command")
	}
	return nil
}

func (c QuiesceCommand) ValidateFor(p Prepared, epoch control.Uint64) error {
	if c.Validate() != nil || p.ValidateFor(p.Request, p.Identity) != nil || c.Request != p.Request || c.Close.IssuerEpoch > epoch || c.PGAggregate.Identity != p.Identity {
		return fmt.Errorf("quiesce differs from original shrink/prepared/closing intent")
	}
	// 持续策略不能借一次current/RSS压低原normal承诺；该检查也覆盖显式
	// 重放，不因当前owner或新资源来源变更而重写原命令中的承诺。
	normal := c.PGAggregate.Bootstrap.Limits.MemoryMaxBytes
	if c.PGAggregate.Held != nil {
		normal = c.PGAggregate.Held.MemoryMaxBytes
	}
	if p.Limits.PidsMaxTasks != c.PGAggregate.Bootstrap.Limits.PidsMaxTasks || p.Limits.SystemMemoryMaxBytes != c.PGAggregate.Bootstrap.Limits.SystemMemoryMaxBytes {
		return fmt.Errorf("quiesce cannot change immutable task limit")
	}
	if p.Limits.MemoryMaxBytes < normal {
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

// Quiesced采用持续聚合约束策略：关掉公共新连接/新执行并等前台在途结束，
// 保留后台、2PC、DSM和会话私有内存的原聚合限额，不假装这些内存已清零。
// 本事实仅可推进PG数组排水；不能降低memory.max、开放准入、释放Host或缩VM。
// 完整外部成本仍来自同账本受信Prepared生产者，结构校验不认证自报来源。
type Quiesced struct {
	ProtocolVersion uint32                   `json:"protocol_version"`
	VMShrinkPermit  bool                     `json:"vm_shrink_permit"`
	Request         control.Request          `json:"request"`
	Close           control.AdmissionRequest `json:"close"`
	DSM             control.DSMResponse      `json:"dsm"`
	PGAggregate     RootAggregate            `json:"pg_aggregate"`
}

// ValidateFor核对同boot、原预算/关闭revision和原聚合来源；DSM内嵌一次原生采样
// 的Prepared/Connection/Work，不能拼接不同时间的门禁ack和“空闲”。背景/2PC/
// DSM计数可以非零，只有持续原约束覆盖它们时才采用此策略，不能清原未知占用。
func (q Quiesced) ValidateFor(p Prepared, epoch control.Uint64) error {
	r := p.Request
	command := QuiesceCommand{ProtocolVersion: q.ProtocolVersion, VMShrinkPermit: q.VMShrinkPermit, Request: q.Request, Close: q.Close, PGAggregate: q.PGAggregate}
	if err := command.ValidateFor(p, epoch); err != nil {
		return err
	}
	if q.DSM.ValidateFor(r.Key.Instance) != nil {
		return fmt.Errorf("quiesce lacks original aggregate or native accounting")
	}
	c := q.DSM.Prepared.Connections
	w := c.Work
	s := w.Observed
	if !c.Enabled || !c.PrivateManagementReady || !c.Acknowledged || !c.PublicConnectionsBlocked || !w.ObserverExcluded || !w.ForegroundIdle || s.AdmissionOpen || s.AdmissionRevision != q.Close.Revision || s.IssuerFloorEpoch != epoch || s.CapacityGeneration != r.ExpectedCapacityGeneration || s.LowBuffers != p.Shared.Observed.LowBuffers || s.HighBuffers != s.LowBuffers || s.MaximumBuffers != p.Shared.Observed.MaximumBuffers || (s.Operation != nil && s.Operation.Phase == control.Preparing) {
		return fmt.Errorf("original close is not continuously acknowledged and foreground idle")
	}
	for _, a := range s.Arrays {
		if a.AllocationUnknown {
			return fmt.Errorf("quiesce native backing is unknown")
		}
	}
	return nil
}

func (q *Quiesced) UnmarshalJSON(data []byte) error {
	type plain Quiesced
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	*q = Quiesced(next)
	return nil
}
