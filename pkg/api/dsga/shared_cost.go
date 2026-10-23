package dsga

import (
	"fmt"
	"math"
)

// SharedCostResponse只覆盖PG固定主段和四个DSG数组。启动总量已经包含启动数组，
// 必须用固定主段+当前已登记数组上界去重；DSM/私有/LFC/Guest池等另由预算域核验。
type SharedCostResponse struct {
	ProtocolVersion                uint32   `json:"protocol_version"`
	VMShrinkPermit                 bool     `json:"vm_shrink_permit"`
	StartupMainUpperBytes          Uint64   `json:"startup_main_upper_bytes"`
	StartupArraysUpperBytes        Uint64   `json:"startup_arrays_upper_bytes"`
	CurrentMainAndArraysUpperBytes Uint64   `json:"current_main_and_arrays_upper_bytes"`
	Observed                       Snapshot `json:"observed"`
}

func (r SharedCostResponse) ValidateFor(bound Instance) error {
	if err := r.Observed.Validate(); err != nil {
		return err
	}
	if r.ProtocolVersion != 1 || r.VMShrinkPermit || r.Observed.Instance != bound {
		return fmt.Errorf("shared cost binding differs")
	}
	main, initial := uint64(r.StartupMainUpperBytes), uint64(r.StartupArraysUpperBytes)
	if main == 0 || initial == 0 || initial > math.MaxUint64-main || main+initial != uint64(r.Observed.StartupSharedUpperBytes) {
		return fmt.Errorf("shared startup cost differs or overflows")
	}
	current := main
	for _, array := range r.Observed.Arrays {
		if uint64(array.CommittedUpperBytes) > math.MaxUint64-current {
			return fmt.Errorf("shared current cost overflows")
		}
		current += uint64(array.CommittedUpperBytes)
	}
	if current != uint64(r.CurrentMainAndArraysUpperBytes) {
		return fmt.Errorf("shared current cost duplicates bootstrap or contradicts native fact")
	}
	return nil
}

func (r *SharedCostResponse) UnmarshalJSON(data []byte) error {
	type plain SharedCostResponse
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	*r = SharedCostResponse(next)
	return nil
}
