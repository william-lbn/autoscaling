package budget

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// ExactAllocationTransition 只能在原 immutable 资格与 Base 已重新认证后被执行器使用。
// 所有字段固定同一 source/base，不将 v2 观察投影成旧 VM-only 的执行许可。
type ExactAllocationTransition struct {
	Source     QualificationSource  `json:"source"`
	Base       QualificationSource  `json:"base"`
	From       QualifiedAllocation  `json:"from"`
	To         QualifiedAllocation  `json:"to"`
	Parent     QualifiedTarget      `json:"parent"`
	PeakLimits resourceguard.Limits `json:"peak_limits"`
}

func (q AllocationQualification) Transition(source QualificationSource, from, to AllocationSelector,
	base PlatformQualification) (ExactAllocationTransition, error) {
	a, err := q.Observe(source, from, base)
	if err != nil {
		return ExactAllocationTransition{}, err
	}
	b, err := q.Observe(source, to, base)
	if err != nil {
		return ExactAllocationTransition{}, err
	}
	for _, edge := range q.Transitions {
		if edge.From == from.AllocationID && edge.To == to.AllocationID {
			result := ExactAllocationTransition{source, q.Base, a.Candidate, b.Candidate, a.RetainedTarget, edge.PeakLimits}
			return result, result.Validate()
		}
	}
	return ExactAllocationTransition{}, fmt.Errorf("allocation direction has no original qualified edge")
}

func (t ExactAllocationTransition) Validate() error {
	if t.Source.Validate() != nil || t.Base.Validate() != nil || t.From.Validate() != nil || t.To.Validate() != nil ||
		t.From.ID == t.To.ID || t.From.VMMemoryBytes != t.To.VMMemoryBytes || t.Parent.VMMemoryBytes != t.To.VMMemoryBytes ||
		t.Parent.Limits.Validate() != nil || !reflect.DeepEqual(t.PeakLimits, t.Parent.Limits) ||
		t.From.BackingProfileSHA256 != t.To.BackingProfileSHA256 {
		return fmt.Errorf("allocation transition lacks exact same-VM original parent")
	}
	for _, item := range []QualifiedAllocation{t.From, t.To} {
		retained := t.Parent.Limits
		retained.MemoryMaxBytes = item.Limits.MemoryMaxBytes
		if !reflect.DeepEqual(retained, item.Limits) || item.Limits.MemoryMaxBytes > t.Parent.Limits.MemoryMaxBytes {
			return fmt.Errorf("allocation changes another retained budget domain")
		}
	}
	return nil
}

// AllocationPlan 属于原 Operation，一经 Begin 固定。当前普通页转换保留整份父
// normal/hugetlb/Outside/Host 上界；较低候选上界不授权 Root 降限，更不退款。
// 后续正常 VM 缩容仍走其独立的 normal_release/实际预算协议和完整资格。
type AllocationPlan struct {
	ProtocolVersion       uint32                    `json:"protocol_version"`
	Scope                 string                    `json:"scope"`
	Request               control.Request           `json:"request"`
	Resource              resourceguard.Identity    `json:"resource"`
	Transition            ExactAllocationTransition `json:"transition"`
	ExpectedLFCGeneration control.Uint64            `json:"expected_lfc_generation"`
	ExpectedLFCSequence   control.Uint64            `json:"expected_lfc_sequence"`
	LFCMaximumBytes       control.Uint64            `json:"lfc_maximum_bytes"`
	LFCGranuleBytes       control.Uint64            `json:"lfc_granule_bytes"`
	HoldCompleteParent    bool                      `json:"hold_complete_parent"`
}

func (p AllocationPlan) Validate() error {
	r, t := p.Request, p.Transition
	if p.ProtocolVersion != 1 || p.Scope != "retained_parent_normal" || r.Validate() != nil || p.Resource.Validate() != nil || t.Validate() != nil ||
		!p.HoldCompleteParent || r.Key.Instance.VMUID != p.Resource.VMUID ||
		r.Key.Instance.ComputeInstanceID != p.Resource.ComputeInstanceID ||
		r.TargetVMMemoryBytes != t.To.VMMemoryBytes || r.TargetBuffers != t.To.Buffers ||
		t.From.LFCLimitBytes%(1<<20) != 0 || t.To.LFCLimitBytes%(1<<20) != 0 ||
		p.LFCGranuleBytes < 8192 || p.LFCGranuleBytes&(p.LFCGranuleBytes-1) != 0 ||
		t.From.LFCLimitBytes%p.LFCGranuleBytes != 0 || t.To.LFCLimitBytes%p.LFCGranuleBytes != 0 ||
		t.From.LFCLimitBytes > p.LFCMaximumBytes || t.To.LFCLimitBytes > p.LFCMaximumBytes ||
		(r.Kind == control.Grow && t.To.Buffers < t.From.Buffers) ||
		(r.Kind == control.Shrink && t.To.Buffers > t.From.Buffers) {
		return fmt.Errorf("allocation plan differs from original request, resource or exact vector")
	}
	type plain AllocationPlan
	raw, err := json.Marshal(plain(p))
	if err != nil {
		return err
	}
	return control.ValidateJSON(raw, 16*1024, 2048)
}

func (p AllocationPlan) SHA256() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("neon-dsga-cache-allocation-plan-v1\x00"), raw...))
	return hex.EncodeToString(digest[:]), nil
}

// LFCRequest 固定原 native 子步骤；after 只能使用原关闭的完整回执和 PG 实际代次。
func (p AllocationPlan) LFCRequest(ordinal control.Uint64, after bool, capacity control.Uint64,
	previous *control.LFCAllocationResponse) (control.LFCAllocationRequest, error) {
	hash, err := p.SHA256()
	if err != nil {
		return control.LFCAllocationRequest{}, err
	}
	r := control.LFCAllocationRequest{ProtocolVersion: 1, Key: p.Request.Key, StepOrdinal: ordinal,
		ExpectedCapacityGeneration: p.Request.ExpectedCapacityGeneration,
		ExpectedLFCGeneration:      p.ExpectedLFCGeneration, ExpectedLFCSequence: p.ExpectedLFCSequence,
		FromLimitBytes: p.Transition.From.LFCLimitBytes, ToLimitBytes: 0,
		AllocationPlanSHA256: hash, BudgetCommitmentID: p.Request.BudgetCommitmentID,
		DeadlineUnixMillis: p.Request.DeadlineUnixMillis}
	if after {
		if previous == nil || previous.Original == nil || previous.Original.Key != p.Request.Key ||
			previous.Original.AllocationPlanSHA256 != hash || previous.Original.StepOrdinal >= ordinal ||
			!previous.SynchronousExitFor(*previous.Original) ||
			previous.Original.ExpectedLFCGeneration != p.ExpectedLFCGeneration ||
			previous.Original.ExpectedLFCSequence != p.ExpectedLFCSequence ||
			previous.Original.ExpectedCapacityGeneration != p.Request.ExpectedCapacityGeneration ||
			previous.Original.BudgetCommitmentID != p.Request.BudgetCommitmentID ||
			previous.Original.DeadlineUnixMillis != p.Request.DeadlineUnixMillis ||
			previous.Original.FromLimitBytes != p.Transition.From.LFCLimitBytes ||
			capacity < p.Request.ExpectedCapacityGeneration {
			return control.LFCAllocationRequest{}, fmt.Errorf("successor LFC step lacks original stopped cache and actual PG generation")
		}
		r.ExpectedCapacityGeneration = capacity
		r.ExpectedLFCGeneration = previous.Observed.LFCGeneration
		r.ExpectedLFCSequence = previous.Observed.LFCSequence
		r.FromLimitBytes, r.ToLimitBytes = 0, p.Transition.To.LFCLimitBytes
	} else if capacity != p.Request.ExpectedCapacityGeneration || previous != nil {
		return control.LFCAllocationRequest{}, fmt.Errorf("initial LFC step changed original capacity")
	}
	return r, r.Validate()
}

func (p *AllocationPlan) UnmarshalJSON(raw []byte) error {
	type plain AllocationPlan
	var value plain
	if err := control.UnmarshalObjectBounded(raw, &value, 16*1024, 2048); err != nil {
		return err
	}
	next := AllocationPlan(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*p = next
	return nil
}

func (p AllocationPlan) Clone() AllocationPlan {
	p.Transition.From.Limits.HugePools = append([]resourceguard.HugePool(nil), p.Transition.From.Limits.HugePools...)
	p.Transition.To.Limits.HugePools = append([]resourceguard.HugePool(nil), p.Transition.To.Limits.HugePools...)
	p.Transition.Parent.Limits.HugePools = append([]resourceguard.HugePool(nil), p.Transition.Parent.Limits.HugePools...)
	p.Transition.PeakLimits.HugePools = append([]resourceguard.HugePool(nil), p.Transition.PeakLimits.HugePools...)
	return p
}
