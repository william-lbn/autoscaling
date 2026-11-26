package dsga

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
)

// 原父操作的一个 LFC 子步骤。PG/LFC 代次分别绑定，不把缓存票据作为预算。
// step_ordinal 属于原 journal；同 Key 下先关闭、再开启是两个不同的原生意图。
type LFCAllocationRequest struct {
	ProtocolVersion            uint32 `json:"protocol_version"`
	Key                        Key    `json:"key"`
	StepOrdinal                Uint64 `json:"step_ordinal"`
	ExpectedCapacityGeneration Uint64 `json:"expected_capacity_generation"`
	ExpectedLFCGeneration      Uint64 `json:"expected_lfc_generation"`
	ExpectedLFCSequence        Uint64 `json:"expected_lfc_sequence"`
	FromLimitBytes             Uint64 `json:"from_limit_bytes"`
	ToLimitBytes               Uint64 `json:"to_limit_bytes"`
	AllocationPlanSHA256       string `json:"allocation_plan_sha256"`
	BudgetCommitmentID         string `json:"budget_commitment_id"`
	DeadlineUnixMillis         Uint64 `json:"deadline_unix_ms"`
}

func (r LFCAllocationRequest) Validate() error {
	digest, err := hex.DecodeString(r.AllocationPlanSHA256)
	if r.ProtocolVersion != 1 || r.Key.Validate() != nil || r.StepOrdinal == 0 ||
		r.ExpectedCapacityGeneration == 0 || r.DeadlineUnixMillis == 0 ||
		ValidateID(r.BudgetCommitmentID, "original allocation budget") != nil ||
		err != nil || len(digest) != 32 || hex.EncodeToString(digest) != r.AllocationPlanSHA256 ||
		r.FromLimitBytes > math.MaxInt64 || r.ToLimitBytes > math.MaxInt64 ||
		r.FromLimitBytes%(1<<20) != 0 || r.ToLimitBytes%(1<<20) != 0 {
		return fmt.Errorf("invalid original LFC allocation intent")
	}
	return nil
}

func (r *LFCAllocationRequest) UnmarshalJSON(raw []byte) error {
	type plain LFCAllocationRequest
	var p plain
	if err := UnmarshalObject(raw, &p); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	// 原 LFC generation/sequence 可以为零；缺字段不能借零值被收养。
	if len(fields) != 11 {
		return fmt.Errorf("incomplete original LFC allocation intent")
	}
	next := LFCAllocationRequest(p)
	if err := next.Validate(); err != nil {
		return err
	}
	*r = next
	return nil
}

func (r LFCAllocationRequest) ContextSHA256() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	return contextSHA256(r)
}

type LFCAllocationObservation struct {
	PGBootID                    Uint64 `json:"pg_boot_id"`
	CapacityGeneration          Uint64 `json:"capacity_generation"`
	IssuerFloorEpoch            Uint64 `json:"issuer_floor_epoch"`
	LFCGeneration               Uint64 `json:"lfc_generation"`
	LFCSequence                 Uint64 `json:"lfc_sequence"`
	SharedLimitBytes            Uint64 `json:"shared_limit_bytes"`
	MaxBytes                    Uint64 `json:"max_bytes"`
	ChunkBytes                  Uint64 `json:"chunk_bytes"`
	Controlled                  bool   `json:"controlled"`
	PrewarmActive               bool   `json:"prewarm_active"`
	DescriptorReferences        Uint64 `json:"descriptor_references"`
	DescriptorTrackingComplete  bool   `json:"descriptor_tracking_complete"`
	SynchronousOperations       Uint64 `json:"synchronous_operations"`
	SynchronousIOScopes         Uint64 `json:"synchronous_io_scopes"`
	SynchronousTrackingComplete bool   `json:"synchronous_tracking_complete"`
	OriginalCurrent             bool   `json:"original_current"`
	ReleaseAuthorized           bool   `json:"release_authorized"`
	KernelReleaseProven         bool   `json:"kernel_release_proven"`
}

func (o *LFCAllocationObservation) UnmarshalJSON(raw []byte) error {
	type plain LFCAllocationObservation
	var p plain
	if err := UnmarshalObject(raw, &p); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if len(fields) != 18 {
		return fmt.Errorf("incomplete LFC observation")
	}
	*o = LFCAllocationObservation(p)
	return nil
}

type LFCAllocationResponse struct {
	ProtocolVersion        uint32                   `json:"protocol_version"`
	Disposition            string                   `json:"disposition"`
	Phase                  string                   `json:"phase"`
	Original               *LFCAllocationRequest    `json:"original"`
	Requested              *LFCAllocationRequest    `json:"requested"`
	Observed               LFCAllocationObservation `json:"observed"`
	ContextFormat          string                   `json:"context_format"`
	OriginalContextSHA256  *string                  `json:"original_context_sha256"`
	RequestedContextSHA256 *string                  `json:"requested_context_sha256"`
}

func (r *LFCAllocationResponse) UnmarshalJSON(raw []byte) error {
	type plain LFCAllocationResponse
	var p plain
	if err := UnmarshalObject(raw, &p); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if len(fields) != 9 {
		return fmt.Errorf("incomplete LFC response")
	}
	*r = LFCAllocationResponse(p)
	return nil
}

func (f LFCAllocationResponse) ValidateFor(bound Instance) error {
	o := f.Observed
	if f.ProtocolVersion != 1 || f.ContextFormat != "postgres_jsonb_out_ascii_v1" || bound.Validate() != nil || o.PGBootID != bound.PGBootID ||
		o.CapacityGeneration == 0 || o.SharedLimitBytes > o.MaxBytes ||
		o.MaxBytes > math.MaxInt64 || o.MaxBytes%(1<<20) != 0 || o.SharedLimitBytes%(1<<20) != 0 ||
		o.ChunkBytes < 8192 || o.ChunkBytes&(o.ChunkBytes-1) != 0 || o.SharedLimitBytes%o.ChunkBytes != 0 ||
		o.ReleaseAuthorized || o.KernelReleaseProven {
		return fmt.Errorf("LFC observation changed original boot or claims physical release")
	}
	switch f.Disposition {
	case "observed", "applied", "replayed", "unconfirmed", "conflict", "busy", "not_found", "stale", "deadline_expired", "rejected":
	default:
		return fmt.Errorf("unknown LFC disposition")
	}
	if f.Disposition == "observed" && f.Requested != nil ||
		f.Disposition != "observed" && (f.Requested == nil || f.Requested.Validate() != nil || f.Requested.Key.Instance != bound) {
		return fmt.Errorf("LFC response lost its original request selection")
	}
	for _, pair := range []struct {
		request *LFCAllocationRequest
		hash    *string
	}{{f.Original, f.OriginalContextSHA256}, {f.Requested, f.RequestedContextSHA256}} {
		if pair.request == nil {
			if pair.hash != nil {
				return fmt.Errorf("LFC hash without original intent")
			}
			continue
		}
		expected, err := pair.request.ContextSHA256()
		if err != nil || pair.hash == nil || *pair.hash != expected {
			return fmt.Errorf("LFC native canonical context differs")
		}
	}
	switch f.Phase {
	case "none":
		if f.Original != nil || o.Controlled || o.OriginalCurrent {
			return fmt.Errorf("empty LFC record claims control")
		}
	case "applied", "unconfirmed":
		if f.Original == nil || f.Original.Validate() != nil || f.Original.Key.Instance != bound || !o.Controlled {
			return fmt.Errorf("LFC original control record is incomplete")
		}
		if f.Phase != "applied" && o.OriginalCurrent {
			return fmt.Errorf("unknown LFC record claims current result")
		}
	default:
		return fmt.Errorf("unknown LFC phase")
	}
	if (f.Disposition == "applied" || f.Disposition == "replayed") && (f.Phase != "applied" || f.Original == nil) {
		return fmt.Errorf("LFC success lacks its original execution record")
	}
	return nil
}

func (f LFCAllocationResponse) ValidateRequest(r LFCAllocationRequest) error {
	if err := f.ValidateFor(r.Key.Instance); err != nil {
		return err
	}
	if f.Requested == nil || *f.Requested != r {
		return fmt.Errorf("LFC response belongs to another original intent")
	}
	return nil
}

// AppliedFor 只确认本次原生目标，不确认内核释放或 normal 降限。
func (f LFCAllocationResponse) AppliedFor(r LFCAllocationRequest) bool {
	return f.ValidateFor(r.Key.Instance) == nil &&
		(f.Disposition == "observed" || f.Requested != nil && *f.Requested == r) &&
		f.Original != nil && *f.Original == r &&
		f.Phase == "applied" && f.Observed.OriginalCurrent &&
		f.Observed.SharedLimitBytes == r.ToLimitBytes && f.Observed.IssuerFloorEpoch >= r.Key.IssuerEpoch &&
		f.Observed.CapacityGeneration >= r.ExpectedCapacityGeneration &&
		(f.Disposition == "applied" || f.Disposition == "replayed" || f.Disposition == "observed")
}

func (f LFCAllocationResponse) SynchronousExitFor(r LFCAllocationRequest) bool {
	o := f.Observed
	return r.ToLimitBytes == 0 && f.AppliedFor(r) && !o.PrewarmActive &&
		o.DescriptorTrackingComplete && o.DescriptorReferences == 0 &&
		o.SynchronousTrackingComplete && o.SynchronousOperations == 0 && o.SynchronousIOScopes == 0
}
