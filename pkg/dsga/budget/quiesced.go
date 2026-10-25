package budget

import (
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

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
	if p.ValidateFor(r, p.Identity) != nil || r.Kind != control.Shrink || q.ProtocolVersion != 1 || q.VMShrinkPermit || q.Request != r || q.Close.Validate() != nil || q.Close.Open || q.Close.Instance != r.Key.Instance || q.Close.BudgetCommitmentID != r.BudgetCommitmentID || q.Close.ExpectedCapacityGeneration != r.ExpectedCapacityGeneration || q.Close.IssuerEpoch < r.Key.IssuerEpoch || q.Close.IssuerEpoch > epoch {
		return fmt.Errorf("quiesce differs from original shrink/prepared/closing intent")
	}
	if q.PGAggregate.Validate() != nil || q.PGAggregate.Identity != p.Identity || q.DSM.ValidateFor(r.Key.Instance) != nil {
		return fmt.Errorf("quiesce lacks original aggregate or native accounting")
	}
	c := q.DSM.Prepared.Connections
	w := c.Work
	s := w.Observed
	if !c.Enabled || !c.PrivateManagementReady || !c.Acknowledged || !c.PublicConnectionsBlocked || !w.ObserverExcluded || !w.ForegroundIdle || s.AdmissionOpen || s.AdmissionRevision != q.Close.Revision || s.IssuerFloorEpoch != epoch || s.CapacityGeneration != r.ExpectedCapacityGeneration || s.LowBuffers != p.Shared.Observed.LowBuffers || s.HighBuffers != s.LowBuffers || s.MaximumBuffers != p.Shared.Observed.MaximumBuffers || (s.Operation != nil && s.Operation.Phase == control.Preparing) {
		return fmt.Errorf("original close is not continuously acknowledged and foreground idle")
	}
	// memory.max只是持续聚合约束，不证明降限免OOM。当前策略必须保留其原
	// normal额度；未来要降低必须另实现可执行私有/后台预算与安全降限协议。
	limits := q.PGAggregate.Bootstrap.Limits
	if q.PGAggregate.Held != nil {
		limits.MemoryMaxBytes = q.PGAggregate.Held.MemoryMaxBytes
	}
	if p.Limits.MemoryMaxBytes < limits.MemoryMaxBytes {
		return fmt.Errorf("continuous aggregate quiesce cannot authorize memory.max decrease")
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
