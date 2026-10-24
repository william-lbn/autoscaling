package dsga

import "fmt"

// WorkResponse的Work2包含Postmaster fork前至PGPROC交接的启动工作，且绑定同一PG实例。
// ForegroundIdle仅覆盖已登记前台，不认证启动前连接、后台/DSM、预算或VM完成；
// 背景/辅助存活计数不能解释为后台quiesce应答，也不能据此放行VM缩容。
type WorkResponse struct {
	ProtocolVersion      uint32   `json:"protocol_version"`
	WorkAPIVersion       uint32   `json:"work_api_version"`
	VMShrinkPermit       bool     `json:"vm_shrink_permit"`
	ClientProcesses      uint32   `json:"client_processes"`
	StartupClients       uint32   `json:"startup_clients"`
	IdleClients          uint32   `json:"idle_clients"`
	FrontendReads        uint32   `json:"frontend_reads"`
	FrontendExecutes     uint32   `json:"frontend_executes"`
	BoundedInputs        uint32   `json:"bounded_inputs"`
	RetainedTransactions uint32   `json:"retained_transactions"`
	BackgroundProcesses  uint32   `json:"background_processes"`
	AuxiliaryProcesses   uint32   `json:"auxiliary_processes"`
	ObserverExcluded     bool     `json:"observer_excluded"`
	ForegroundIdle       bool     `json:"foreground_idle"`
	Observed             Snapshot `json:"observed"`
}

func (r WorkResponse) ValidateFor(bound Instance) error {
	if err := r.Observed.Validate(); err != nil {
		return err
	}
	if r.ProtocolVersion != 1 || r.WorkAPIVersion != 2 || r.VMShrinkPermit || r.Observed.Instance != bound {
		return fmt.Errorf("work observation native instance or protocol differs")
	}
	// 固定五项阶段为互斥集合。先提升uint64求和，不能让uint32回绕伪造总数。
	total := uint64(r.StartupClients) + uint64(r.IdleClients) + uint64(r.FrontendReads) + uint64(r.FrontendExecutes) + uint64(r.BoundedInputs)
	if total != uint64(r.ClientProcesses) || r.RetainedTransactions > r.ClientProcesses {
		return fmt.Errorf("work phase or retained transaction counts contradict native totals")
	}
	idle := !r.Observed.AdmissionOpen && r.StartupClients == 0 && r.FrontendReads == 0 && r.FrontendExecutes == 0 && r.RetainedTransactions == 0
	if r.ForegroundIdle != idle {
		return fmt.Errorf("work idle contradicts native gate or activity")
	}
	return nil
}

func (r *WorkResponse) UnmarshalJSON(data []byte) error {
	type plain WorkResponse
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	*r = WorkResponse(next)
	return nil
}
