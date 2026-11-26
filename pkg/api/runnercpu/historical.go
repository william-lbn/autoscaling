package runnercpu

import "fmt"

const HistoricalScope = "historical-original-guest-cpu-v1"

// Historical 仅说明原Guest实际整数在线CPU；没有quota/调度承诺，不构成B7权益资格。
type Historical struct {
	ProtocolVersion uint32   `json:"protocol_version"`
	Scope           string   `json:"scope"`
	Identity        Identity `json:"identity"`
	OnlineVCPUs     uint32   `json:"online_vcpus"`
	MaximumVCPUs    uint32   `json:"maximum_vcpus"`
	QEMUPID         uint32   `json:"qemu_pid"`
	QEMUStartTicks  uint64   `json:"qemu_start_ticks"`
}

func (o Historical) Validate() error {
	if o.ProtocolVersion != ProtocolVersion || o.Scope != HistoricalScope || o.Identity.Validate() != nil ||
		o.OnlineVCPUs == 0 || o.MaximumVCPUs == 0 || o.MaximumVCPUs > MaximumVCPUs ||
		o.OnlineVCPUs > o.MaximumVCPUs || o.QEMUPID == 0 || o.QEMUStartTicks == 0 {
		return fmt.Errorf("historical original Guest topology is unavailable")
	}
	return nil
}
