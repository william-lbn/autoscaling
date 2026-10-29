package dsga

import "fmt"

// AdmissionLeaseRequest的deadline仅由同一PG单调时钟产生；调用者不制造墙钟
// 期限。续租CAS不开放服务，不清原未知；rearm是明确的更高owner恢复选择。
type AdmissionLeaseRequest struct {
	ProtocolVersion            int      `json:"protocol_version"`
	Instance                   Instance `json:"instance"`
	IssuerEpoch                Uint64   `json:"issuer_epoch"`
	ExpectedCapacityGeneration Uint64   `json:"expected_capacity_generation"`
	ExpectedAdmissionRevision  Uint64   `json:"expected_admission_revision"`
	ExpectedDeadlineNS         Uint64   `json:"expected_deadline_ns"`
	Action                     string   `json:"action"`
}

func (r AdmissionLeaseRequest) Validate() error {
	if r.ProtocolVersion != 1 || ValidateID(r.Instance.VMUID, "VM") != nil || ValidateID(r.Instance.ComputeInstanceID, "Compute") != nil || r.Instance.PGBootID == 0 || r.IssuerEpoch == 0 || r.ExpectedCapacityGeneration == 0 || (r.Action != "observe" && r.Action != "renew" && r.Action != "rearm") || (r.Action != "observe" && r.ExpectedDeadlineNS == 0) {
		return fmt.Errorf("invalid native admission lease request")
	}
	return nil
}

type AdmissionLeaseResponse struct {
	ProtocolVersion int                   `json:"protocol_version"`
	VMShrinkPermit  bool                  `json:"vm_shrink_permit"`
	Request         AdmissionLeaseRequest `json:"request"`
	Renewed         bool                  `json:"renewed"`
	Armed           bool                  `json:"armed"`
	Expired         bool                  `json:"expired"`
	DeadlineNS      Uint64                `json:"deadline_ns"`
	LeaseEpoch      Uint64                `json:"lease_epoch"`
	TimeoutMS       int                   `json:"timeout_ms"`
	Observed        Snapshot              `json:"observed"`
}

func (r AdmissionLeaseResponse) ValidateFor(request AdmissionLeaseRequest, bound Instance) error {
	if request.Validate() != nil || r.Observed.Validate() != nil || r.ProtocolVersion != 1 || r.VMShrinkPermit || r.Request != request || request.Instance != bound || r.Observed.Instance != bound || r.TimeoutMS < 1000 || r.TimeoutMS > 60000 || r.DeadlineNS > Uint64(1<<63-1) || r.LeaseEpoch > r.Observed.IssuerFloorEpoch {
		return fmt.Errorf("admission lease response differs from bound native context")
	}
	if r.Armed && r.DeadlineNS == 0 || !r.Armed && (r.DeadlineNS != 0 || r.LeaseEpoch != 0 || r.Expired) || r.Expired && r.Observed.AdmissionOpen || r.Observed.AdmissionOpen && !r.Armed {
		return fmt.Errorf("invalid native admission lease lifetime")
	}
	if r.Renewed && (request.Action == "observe" || !r.Armed || r.Expired || r.LeaseEpoch != request.IssuerEpoch || r.Observed.IssuerFloorEpoch != request.IssuerEpoch || r.Observed.CapacityGeneration != request.ExpectedCapacityGeneration || r.Observed.AdmissionRevision != request.ExpectedAdmissionRevision || r.DeadlineNS <= request.ExpectedDeadlineNS || request.Action == "rearm" && r.Observed.AdmissionOpen) {
		return fmt.Errorf("renewal contradicts original native CAS")
	}
	return nil
}

func (r *AdmissionLeaseRequest) UnmarshalJSON(data []byte) error {
	type plain AdmissionLeaseRequest
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	if err := AdmissionLeaseRequest(next).Validate(); err != nil {
		return err
	}
	*r = AdmissionLeaseRequest(next)
	return nil
}

func (r *AdmissionLeaseResponse) UnmarshalJSON(data []byte) error {
	type plain AdmissionLeaseResponse
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	*r = AdmissionLeaseResponse(next)
	return nil
}

// AdmissionRearmStatus只确认完整原rearm的历史应用。Retained=false是未知，
// 不证明未执行；当前期限可能已过期或换owner，历史命中不授予开放/回收许可。
type AdmissionRearmStatus struct {
	ProtocolVersion   int                    `json:"protocol_version"`
	VMShrinkPermit    bool                   `json:"vm_shrink_permit"`
	Request           AdmissionLeaseRequest  `json:"request"`
	Retained          bool                   `json:"retained"`
	AppliedDeadlineNS Uint64                 `json:"applied_deadline_ns"`
	Current           AdmissionLeaseResponse `json:"current"`
}

func (r AdmissionRearmStatus) ValidateFor(request AdmissionLeaseRequest, bound Instance) error {
	if r.Current.ValidateFor(request, bound) != nil || request.Action != "rearm" || r.ProtocolVersion != 1 || r.VMShrinkPermit || r.Request != request || r.Current.Renewed {
		return fmt.Errorf("rearm status differs from original read-only selection")
	}
	if r.Retained {
		if r.AppliedDeadlineNS <= request.ExpectedDeadlineNS || r.AppliedDeadlineNS > r.Current.DeadlineNS || !r.Current.Armed || r.Current.LeaseEpoch < request.IssuerEpoch || r.Current.Observed.IssuerFloorEpoch < request.IssuerEpoch || r.Current.Observed.CapacityGeneration < request.ExpectedCapacityGeneration || r.Current.Observed.AdmissionRevision < request.ExpectedAdmissionRevision {
			return fmt.Errorf("retained rearm contradicts native monotonic lifetime")
		}
	} else if r.AppliedDeadlineNS != 0 {
		return fmt.Errorf("unknown rearm contains an applied deadline")
	}
	return nil
}

func (r *AdmissionRearmStatus) UnmarshalJSON(data []byte) error {
	type plain AdmissionRearmStatus
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	*r = AdmissionRearmStatus(next)
	return nil
}
