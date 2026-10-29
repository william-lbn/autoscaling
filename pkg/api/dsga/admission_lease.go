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
