package dsga

import (
	"encoding/json"
	"fmt"
	"math"
)

// 每数组保留原fd事实与未知；allocated来自文件st_blocks，不能当Guest池/RSS释放。
type RegionProviderFact struct {
	Array               ArrayKind `json:"array"`
	Provider            string    `json:"provider"`
	GranuleBytes        Uint64    `json:"granule_bytes"`
	BackingDevice       Uint64    `json:"backing_device"`
	BackingInode        Uint64    `json:"backing_inode"`
	BackingPresent      bool      `json:"backing_present"`
	IdentityVerified    bool      `json:"identity_verified"`
	MappingNoreserve    bool      `json:"mapping_noreserve"`
	SystemErrno         int32     `json:"system_errno"`
	AllocatedBytesKnown bool      `json:"allocated_bytes_known"`
	AllocatedBytes      *Uint64   `json:"allocated_bytes"`
	ReservedBytesKnown  bool      `json:"reserved_bytes_known"`
	ReservedBytes       *Uint64   `json:"reserved_bytes"`
}
type ProviderCost struct {
	MinimumBuffers                 uint32               `json:"minimum_buffers"`
	MainIsHugetlb                  bool                 `json:"main_is_hugetlb"`
	NormalMainAndArraysUpperBytes  Uint64               `json:"normal_main_and_arrays_upper_bytes"`
	HugetlbMainAndArraysUpperBytes Uint64               `json:"hugetlb_main_and_arrays_upper_bytes"`
	Arrays                         []RegionProviderFact `json:"arrays"`
}
type ProviderCostResponse struct {
	ProtocolVersion        uint32        `json:"protocol_version"`
	VMShrinkPermit         bool          `json:"vm_shrink_permit"`
	Supported              bool          `json:"supported"`
	ProviderCostAPIVersion uint32        `json:"provider_cost_api_version"`
	Observed               *Snapshot     `json:"observed"`
	Cost                   *ProviderCost `json:"cost"`
}

func providerObject(raw []byte, target any, count int) error {
	if err := UnmarshalObject(raw, target); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if len(fields) != count {
		return fmt.Errorf("provider cost omitted original fields")
	}
	return nil
}
func (v *RegionProviderFact) UnmarshalJSON(raw []byte) error {
	type plain RegionProviderFact
	var p plain
	if e := providerObject(raw, &p, 13); e != nil {
		return e
	}
	*v = RegionProviderFact(p)
	return nil
}
func (v *ProviderCost) UnmarshalJSON(raw []byte) error {
	type plain ProviderCost
	var p plain
	if e := providerObject(raw, &p, 5); e != nil {
		return e
	}
	*v = ProviderCost(p)
	return nil
}
func (v *ProviderCostResponse) UnmarshalJSON(raw []byte) error {
	type plain ProviderCostResponse
	var p plain
	if e := providerObject(raw, &p, 6); e != nil {
		return e
	}
	*v = ProviderCostResponse(p)
	return nil
}
func (v ProviderCostResponse) ValidateFor(bound Instance) error {
	if v.ProtocolVersion != 1 || v.VMShrinkPermit {
		return fmt.Errorf("provider cost is not a VM permit")
	}
	if v.Observed != nil && (v.Observed.Validate() != nil || v.Observed.Instance != bound) {
		return fmt.Errorf("provider cost changed original PG snapshot")
	}
	if !v.Supported {
		if v.ProviderCostAPIVersion != 0 || v.Cost != nil {
			return fmt.Errorf("unsupported provider cost must be absent")
		}
		return nil
	}
	c, s := v.Cost, v.Observed
	if v.ProviderCostAPIVersion != 1 || c == nil || s == nil || c.MinimumBuffers < 16 || c.MinimumBuffers > s.LowBuffers || len(c.Arrays) != 4 {
		return fmt.Errorf("provider cost missing original ABI/minimum/snapshot")
	}
	var normal, huge uint64
	for i, a := range c.Arrays {
		if a.Array != s.Arrays[i].Array || a.GranuleBytes < 4096 || uint64(a.GranuleBytes)&(uint64(a.GranuleBytes)-1) != 0 || a.SystemErrno < 0 ||
			a.AllocatedBytesKnown != (a.AllocatedBytes != nil) || a.ReservedBytesKnown != (a.ReservedBytes != nil) ||
			a.IdentityVerified && (!a.BackingPresent || a.BackingDevice == 0 || a.BackingInode == 0) {
			return fmt.Errorf("provider facts lost identity or unknown")
		}
		var sum *uint64
		switch a.Provider {
		case "normal":
			if a.MappingNoreserve {
				return fmt.Errorf("normal cannot claim hugetlb reservation mode")
			}
			sum = &normal
		case "hugetlb":
			if a.GranuleBytes < 2<<20 {
				return fmt.Errorf("hugetlb granule invalid")
			}
			sum = &huge
		default:
			return fmt.Errorf("unknown provider")
		}
		if uint64(s.Arrays[i].CommittedUpperBytes) > math.MaxUint64-*sum {
			return fmt.Errorf("provider array cost overflow")
		}
		*sum += uint64(s.Arrays[i].CommittedUpperBytes)
	}
	n, h := uint64(c.NormalMainAndArraysUpperBytes), uint64(c.HugetlbMainAndArraysUpperBytes)
	if n < normal || h < huge || h > math.MaxUint64-n || n-normal+h-huge == 0 || !c.MainIsHugetlb && h != huge {
		return fmt.Errorf("provider cost omitted fixed main or array responsibility")
	}
	return nil
}
