package dsga

import "fmt"

// DSMResponse仅观察同一PG boot经dsm.c创建的普通页payload承诺，不包含PTE/THP/
// Host或跨boot清理。未知仍占原额度，前台idle不能补零；本响应不给VM许可。
type DSMResponse struct {
	ProtocolVersion     uint32           `json:"protocol_version"`
	DSMAPIVersion       uint32           `json:"dsm_api_version"`
	VMShrinkPermit      bool             `json:"vm_shrink_permit"`
	CurrentBootOnly     bool             `json:"current_boot_only"`
	ReservedUpperBytes  Uint64           `json:"reserved_upper_bytes"`
	ControlUpperBytes   Uint64           `json:"control_upper_bytes"`
	ExternalBudgetBytes Uint64           `json:"external_budget_bytes"`
	ExternalSegments    uint32           `json:"external_segments"`
	PendingCreations    uint32           `json:"pending_creations"`
	UnknownSegments     uint32           `json:"unknown_segments"`
	PageBytes           uint32           `json:"page_bytes"`
	LimitEnforced       bool             `json:"limit_enforced"`
	AccountingValid     bool             `json:"accounting_valid"`
	Prepared            PreparedResponse `json:"prepared"`
}

func (r DSMResponse) ValidateFor(bound Instance) error {
	if err := r.Prepared.ValidateFor(bound); err != nil {
		return err
	}
	if r.ProtocolVersion != 1 || r.DSMAPIVersion != 1 || r.VMShrinkPermit || !r.CurrentBootOnly || !r.AccountingValid {
		return fmt.Errorf("DSM observation version, scope or accounting is unavailable")
	}
	if r.PageBytes == 0 || r.PageBytes&(r.PageBytes-1) != 0 || r.ExternalSegments > 1<<31-1 ||
		uint64(r.PendingCreations)+uint64(r.UnknownSegments) > uint64(r.ExternalSegments) {
		return fmt.Errorf("DSM slot accounting differs")
	}
	page, used, control, budget := uint64(r.PageBytes), uint64(r.ReservedUpperBytes), uint64(r.ControlUpperBytes), uint64(r.ExternalBudgetBytes)
	// 页为非零u32幂、槽不超过int32，其乘积不溢出uint64。主段payload已在main
	// 成本中计费，不能再叠加；独立control必须有页对齐成本，不接受客户端补0。
	if control == 0 || control%page != 0 || used%page != 0 || used < uint64(r.ExternalSegments)*page ||
		(used == 0) != (r.ExternalSegments == 0) {
		return fmt.Errorf("DSM payload alignment or occupancy differs")
	}
	// 固定KB额度的0代表未限制，仍可如实观察；完整预算生产者须另核验持续正cap。
	if r.LimitEnforced != (budget > 0) || budget > (1<<31-1)*1024 || budget%1024 != 0 || (r.LimitEnforced && used > budget) {
		return fmt.Errorf("DSM configured payload limit contradicts native reservation")
	}
	return nil
}

func (r *DSMResponse) UnmarshalJSON(data []byte) error {
	type plain DSMResponse
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	*r = DSMResponse(next)
	return nil
}
