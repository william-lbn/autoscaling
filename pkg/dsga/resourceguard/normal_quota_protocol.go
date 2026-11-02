package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"reflect"
)

// NormalQuotaEnvelope是受信控制器从已持久Completed ReleasePG事实重建的原信封。
// Command仍为v1原值；本信封不改变旧WAL、PG主版本API，也没有trusted/permit开关。
type NormalQuotaEnvelope struct {
	ProtocolVersion uint32                      `json:"protocol_version"`
	Phase           string                      `json:"phase"`
	Command         Command                     `json:"command"`
	PGRequest       control.Request             `json:"pg_request"`
	PGFence         control.FenceResponse       `json:"pg_fence"`
	PGResponse      *control.ControlResponse    `json:"pg_response,omitempty"`
	PGShared        *control.SharedCostResponse `json:"pg_shared,omitempty"`
}

func (v NormalQuotaEnvelope) Validate() error {
	c, q, f := v.Command, v.PGRequest, v.PGFence
	if v.ProtocolVersion != 1 || c.Validate() != nil || q.Validate() != nil ||
		c.Identity.VMUID != q.Key.Instance.VMUID || c.Identity.ComputeInstanceID != q.Key.Instance.ComputeInstanceID ||
		c.IssuerEpoch != q.Key.IssuerEpoch || c.BudgetCommitmentID != q.BudgetCommitmentID ||
		f.ValidateFor(control.FenceRequest{ProtocolVersion: 1, Instance: q.Key.Instance, IssuerEpoch: q.Key.IssuerEpoch}, q.Key.Instance) != nil || f.Disposition != control.Fenced {
		return fmt.Errorf("normal quota requires original request and actual fence")
	}
	if v.Phase == "pre-grow" {
		if q.Kind != control.Grow || v.PGResponse != nil || v.PGShared == nil || v.PGShared.ValidateFor(q.Key.Instance) != nil || !reflect.DeepEqual(f.Observed, v.PGShared.Observed) {
			return fmt.Errorf("normal quota pre-grow requires original prepared cost and fence")
		}
		s := v.PGShared.Observed
		if s.IssuerFloorEpoch != q.Key.IssuerEpoch || s.CapacityGeneration != q.ExpectedCapacityGeneration || s.LowBuffers != s.HighBuffers || q.TargetBuffers <= s.HighBuffers || q.TargetBuffers > s.MaximumBuffers || s.Operation != nil && s.Operation.Phase == control.Preparing {
			return fmt.Errorf("normal quota pre-grow baseline changed")
		}
		for _, a := range s.Arrays {
			if a.AllocationUnknown {
				return fmt.Errorf("normal quota pre-grow allocation unknown")
			}
		}
		return nil
	}
	if v.Phase != "released-shrink" || q.Kind != control.Shrink || v.PGShared != nil || v.PGResponse == nil {
		return fmt.Errorf("normal quota phase must select one original PG fact")
	}
	r := *v.PGResponse
	if r.ValidateResize(q, q.Key.Instance) != nil || r.Receipt.Result == nil || r.Receipt.Result.Operation == nil || r.Receipt.Result.Operation.Phase != control.Committed {
		return fmt.Errorf("normal quota requires completed PG shrink")
	}
	s := r.Observed
	hash, e := q.ContextSHA256()
	if e != nil || s.Operation == nil || s.Operation.Key != q.Key || s.Operation.Kind != q.Kind ||
		s.Operation.Phase != control.Committed || s.Operation.ContextSHA256 != hash ||
		s.Operation.ExpectedCapacityGeneration != q.ExpectedCapacityGeneration || s.HighBuffers != q.TargetBuffers ||
		s.LowBuffers != s.HighBuffers || s.CapacityGeneration != r.Receipt.Result.CapacityGeneration ||
		s.IssuerFloorEpoch != q.Key.IssuerEpoch || s.CapacityGeneration < f.Observed.CapacityGeneration || s.AdmissionOpen {
		return fmt.Errorf("normal quota PG completion is stale, open or changed")
	}
	for _, a := range s.Arrays {
		x := a.Provider
		if a.AllocationUnknown || a.ReleaseState != control.ReleaseVerified || a.ReleaseKey == nil || *a.ReleaseKey != q.Key ||
			a.ReleaseCapacityGeneration != s.CapacityGeneration || !x.HasResult || x.AllocationMayBePartial || x.ReleaseMayBePartial || x.SystemErrno != 0 {
			return fmt.Errorf("normal quota array release is not complete")
		}
		if x.ValidateReleasedTail() != nil {
			return fmt.Errorf("normal quota array physical release is unconfirmed")
		}
	}
	return nil
}
func (v *NormalQuotaEnvelope) UnmarshalJSON(raw []byte) error {
	type plain NormalQuotaEnvelope
	var x plain
	if e := control.UnmarshalObject(raw, &x); e != nil {
		return e
	}
	n := NormalQuotaEnvelope(x)
	if e := n.Validate(); e != nil {
		return e
	}
	*v = n
	return nil
}
func cloneNormalQuota(v NormalQuotaEnvelope) NormalQuotaEnvelope {
	raw, _ := json.Marshal(v)
	// 已验证的值只作深拷贝；解码失败不能产生新的授权。
	var out NormalQuotaEnvelope
	if json.Unmarshal(raw, &out) != nil {
		return NormalQuotaEnvelope{}
	}
	return out
}

type NormalQuotaEvidence struct {
	ProtocolVersion uint32   `json:"protocol_version"`
	VMShrinkPermit  bool     `json:"vm_shrink_permit"`
	EnvelopeSHA256  string   `json:"envelope_sha256"`
	Command         Command  `json:"command"`
	Actual          Evidence `json:"actual"`
}

func (v NormalQuotaEvidence) validateFor(q NormalQuotaEnvelope) error {
	raw, e := json.Marshal(q)
	if e != nil || q.Validate() != nil || v.ProtocolVersion != 1 || v.VMShrinkPermit || v.EnvelopeSHA256 != physicalDigest(raw) || !reflect.DeepEqual(v.Command, q.Command) {
		return fmt.Errorf("normal quota response differs from the original envelope")
	}
	return v.Actual.ValidateFor(q.Command)
}
func (v *NormalQuotaEvidence) UnmarshalJSON(raw []byte) error {
	type plain NormalQuotaEvidence
	var x plain
	if e := control.UnmarshalObject(raw, &x); e != nil {
		return e
	}
	*v = NormalQuotaEvidence(x)
	return nil
}
func normalQuotaPath(path string) bool {
	switch path {
	case "/dsg/v1/resource/normal-apply", "/dsg/v1/resource/normal-observe", "/dsg/v1/resource/normal-replay-original", "/dsg/v1/resource/normal-recover":
		return true
	}
	return false
}

type normalQuotaBackend interface {
	ApplyNormalQuota(context.Context, NormalQuotaEnvelope) (NormalQuotaEvidence, error)
	ObserveNormalQuota(context.Context, NormalQuotaEnvelope) (NormalQuotaEvidence, error)
	ReplayNormalQuota(context.Context, NormalQuotaEnvelope) (NormalQuotaEvidence, error)
	RecoverNormalQuota(context.Context, NormalQuotaEnvelope) (NormalQuotaEvidence, error)
}
