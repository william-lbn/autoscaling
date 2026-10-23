package dsga

import "fmt"

// AdmissionRequest是PG最后准入回执的完整不可变身份。Revision独立于resize，
// budget_commitment_id仅引用外层持久承诺；PG开门事实本身不能证明预算或排空。
type AdmissionRequest struct {
	ProtocolVersion            uint32   `json:"protocol_version"`
	Instance                   Instance `json:"instance"`
	IssuerEpoch                Uint64   `json:"issuer_epoch"`
	Revision                   Uint64   `json:"revision"`
	ExpectedCapacityGeneration Uint64   `json:"expected_capacity_generation"`
	Open                       bool     `json:"open"`
	BudgetCommitmentID         string   `json:"budget_commitment_id"`
}

func (r AdmissionRequest) Validate() error {
	if r.ProtocolVersion != ProtocolVersion || r.IssuerEpoch == 0 || r.Revision == 0 || r.ExpectedCapacityGeneration == 0 {
		return fmt.Errorf("invalid admission protocol or identity")
	}
	if err := r.Instance.Validate(); err != nil {
		return err
	}
	return ValidateID(r.BudgetCommitmentID, "budget commitment")
}

func (r AdmissionRequest) ContextSHA256() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	return contextSHA256(r)
}

// ClosesOver只检查同boot更高关闭意图的关系；实际连续revision和floor必须由PG
// 执行并回读。不能将此静态关系当作旧请求未发生、VM可缩容或预算可释放证明。
func (r AdmissionRequest) ClosesOver(original AdmissionRequest) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if err := original.Validate(); err != nil {
		return err
	}
	if r.Open || r.Instance != original.Instance || !(r.IssuerEpoch > original.IssuerEpoch || (r.IssuerEpoch == original.IssuerEpoch && r.Revision > original.Revision)) {
		return fmt.Errorf("admission close does not fence original")
	}
	return nil
}

type AdmissionDisposition string

const (
	AdmissionApplied       AdmissionDisposition = "applied"
	AdmissionReplayed      AdmissionDisposition = "replayed"
	AdmissionBusy          AdmissionDisposition = "busy"
	AdmissionStale         AdmissionDisposition = "stale"
	AdmissionConflict      AdmissionDisposition = "conflict"
	AdmissionNotFound      AdmissionDisposition = "not_found"
	AdmissionResultExpired AdmissionDisposition = "result_expired"
)

type AdmissionResponse struct {
	ProtocolVersion uint32               `json:"protocol_version"`
	VMShrinkPermit  bool                 `json:"vm_shrink_permit"`
	Request         AdmissionRequest     `json:"request"`
	Disposition     AdmissionDisposition `json:"disposition"`
	ContextSHA256   string               `json:"context_sha256"`
	Observed        Snapshot             `json:"observed"`
}

func (r AdmissionResponse) Confirms() bool {
	return r.Disposition == AdmissionApplied || r.Disposition == AdmissionReplayed
}

func (r AdmissionResponse) ValidateFor(request AdmissionRequest, bound Instance, query bool) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := r.Observed.Validate(); err != nil {
		return err
	}
	digest, err := request.ContextSHA256()
	if err != nil || r.ProtocolVersion != ProtocolVersion || r.VMShrinkPermit || r.Request != request || r.ContextSHA256 != digest || r.Observed.Instance != bound {
		return fmt.Errorf("admission response differs from complete context or binding")
	}
	s := r.Observed
	sameBoot := request.Instance == bound
	valid := false
	switch r.Disposition {
	case AdmissionApplied:
		valid = !query && sameBoot && s.IssuerFloorEpoch == request.IssuerEpoch && s.CapacityGeneration == request.ExpectedCapacityGeneration && s.AdmissionRevision == request.Revision && s.AdmissionOpen == request.Open
	case AdmissionReplayed:
		valid = sameBoot && s.AdmissionRevision == request.Revision && s.AdmissionOpen == request.Open && s.IssuerFloorEpoch >= request.IssuerEpoch && s.CapacityGeneration >= request.ExpectedCapacityGeneration && (query || s.IssuerFloorEpoch == request.IssuerEpoch)
	case AdmissionNotFound:
		valid = query && sameBoot && request.Revision > s.AdmissionRevision
	case AdmissionResultExpired:
		valid = query && sameBoot && request.Revision < s.AdmissionRevision
	case AdmissionBusy:
		// PG开放先尝试resize锁，busy可早于boot/floor检查，仍不能当作已应用。
		valid = !query && request.Open
	case AdmissionConflict:
		valid = sameBoot && (request.Revision == s.AdmissionRevision || (!query && s.AdmissionRevision < Uint64(^uint64(0)) && request.Revision > s.AdmissionRevision+1))
	case AdmissionStale:
		valid = !sameBoot || (!query && (request.IssuerEpoch != s.IssuerFloorEpoch || request.Revision < s.AdmissionRevision || request.ExpectedCapacityGeneration != s.CapacityGeneration))
	}
	if !valid {
		return fmt.Errorf("admission disposition contradicts action or observed fact")
	}
	return nil
}

type AdmissionCloseRequest struct {
	ProtocolVersion uint32           `json:"protocol_version"`
	Original        AdmissionRequest `json:"original"`
	Close           AdmissionRequest `json:"close"`
}

func (r AdmissionCloseRequest) Validate() error {
	if r.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported admission close protocol")
	}
	return r.Close.ClosesOver(r.Original)
}

// AdmissionClosure只解除未来门禁写的未知栅栏；PriorMayHaveApplied恒true，
// 外层必须另外排空旧工作、核实完整预算及Guest/VM事实，禁止解释为NoEffect。
type AdmissionClosure struct {
	ProtocolVersion     uint32            `json:"protocol_version"`
	VMShrinkPermit      bool              `json:"vm_shrink_permit"`
	Original            AdmissionRequest  `json:"original"`
	ClosedBy            AdmissionResponse `json:"closed_by"`
	PriorMayHaveApplied bool              `json:"prior_may_have_applied"`
}

func (r AdmissionClosure) ValidateFor(request AdmissionCloseRequest, bound Instance, query bool) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if r.ProtocolVersion != ProtocolVersion || r.VMShrinkPermit || !r.PriorMayHaveApplied || r.Original != request.Original || !r.ClosedBy.Confirms() {
		return fmt.Errorf("invalid admission closure proof")
	}
	return r.ClosedBy.ValidateFor(request.Close, bound, query)
}

func (r *AdmissionRequest) UnmarshalJSON(data []byte) error {
	type plain AdmissionRequest
	var next plain
	if len(data) > 16*1024 {
		return fmt.Errorf("admission request exceeds 16KiB")
	}
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	if err := AdmissionRequest(next).Validate(); err != nil {
		return err
	}
	*r = AdmissionRequest(next)
	return nil
}

func (r *AdmissionResponse) UnmarshalJSON(data []byte) error {
	type plain AdmissionResponse
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	*r = AdmissionResponse(next)
	return nil
}

func (r *AdmissionCloseRequest) UnmarshalJSON(data []byte) error {
	type plain AdmissionCloseRequest
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	if err := AdmissionCloseRequest(next).Validate(); err != nil {
		return err
	}
	*r = AdmissionCloseRequest(next)
	return nil
}

func (r *AdmissionClosure) UnmarshalJSON(data []byte) error {
	type plain AdmissionClosure
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	*r = AdmissionClosure(next)
	return nil
}
