package journal

import (
	"context"
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"reflect"
)

const (
	RepartitionLFCBefore Action = "repartition_lfc_before"
	RepartitionLFCAfter  Action = "repartition_lfc_after"
)

// 新计划仍是同一原Operation。严格旧读者拒绝新字段和额外Action/顺序，
// 不能通过删字段/投影成旧VM-only计划继续执行。基础Request wire保持不变。
func (o Operation) actions() []Action {
	original := plan(o.Request.Kind)
	if o.AllocationPlan == nil {
		return original
	}
	actions := make([]Action, 0, len(original)+2)
	after := ResizePG
	if o.Request.Kind == control.Shrink {
		after = ReleasePG
	}
	for _, action := range original {
		if action == ResizePG {
			actions = append(actions, RepartitionLFCBefore)
		}
		actions = append(actions, action)
		if action == after {
			actions = append(actions, RepartitionLFCAfter)
		}
	}
	return actions
}

type CacheAllocationAudit struct {
	ProtocolVersion uint32                        `json:"protocol_version"`
	Response        control.LFCAllocationResponse `json:"response"`
	NormalIdentity  resourceguard.Identity        `json:"normal_identity"`
	NormalLimits    resourceguard.Limits          `json:"normal_limits"`
	RootStateSHA256 string                        `json:"root_state_sha256"`
}

func (o Operation) LFCCommand(s Step) (control.LFCAllocationRequest, error) {
	if o.AllocationPlan == nil || s.Action != RepartitionLFCBefore && s.Action != RepartitionLFCAfter {
		return control.LFCAllocationRequest{}, fmt.Errorf("not an original cache allocation step")
	}
	p := *o.AllocationPlan
	if s.Action == RepartitionLFCBefore {
		return p.LFCRequest(s.Ordinal, false, p.Request.ExpectedCapacityGeneration, nil)
	}
	var before *control.LFCAllocationResponse
	var capacity control.Uint64
	for _, previous := range o.Steps {
		if previous.Ordinal >= s.Ordinal {
			break
		}
		if previous.Action == RepartitionLFCBefore {
			var saved CacheAllocationAudit
			if previous.Phase != Settled || previous.Observation == nil || previous.Observation.Result != Completed ||
				control.UnmarshalObject(previous.Observation.Data, &saved) != nil {
				return control.LFCAllocationRequest{}, fmt.Errorf("original cache closure is not recorded")
			}
			response := saved.Response
			before = &response
		}
		if previous.Action == ResizePG {
			var saved struct {
				ProtocolVersion uint32                  `json:"protocol_version"`
				Fence           control.FenceResponse   `json:"issuer_fence"`
				Response        control.ControlResponse `json:"response"`
			}
			if previous.Phase != Settled || previous.Observation == nil || previous.Observation.Result != Completed ||
				control.UnmarshalObject(previous.Observation.Data, &saved) != nil || saved.ProtocolVersion != 1 ||
				saved.Response.ValidateResize(o.Request, o.Request.Key.Instance) != nil ||
				saved.Response.Receipt.Result == nil || saved.Response.Receipt.Result.Operation == nil ||
				saved.Response.Receipt.Result.Operation.Phase != control.Committed {
				return control.LFCAllocationRequest{}, fmt.Errorf("original PG capacity is not recorded")
			}
			capacity = saved.Response.Receipt.Result.CapacityGeneration
		}
	}
	if before == nil || capacity == 0 {
		return control.LFCAllocationRequest{}, fmt.Errorf("original cache/PG prefix is incomplete")
	}
	return p.LFCRequest(s.Ordinal, true, capacity, before)
}

func (o Operation) validateCacheStep(s Step) error {
	if s.Action != RepartitionLFCBefore && s.Action != RepartitionLFCAfter {
		return nil
	}
	if s.Phase == Planned {
		return nil
	}
	expected, err := o.LFCCommand(s)
	if err != nil {
		return err
	}
	var original control.LFCAllocationRequest
	if json.Unmarshal(s.Command, &original) != nil || original != expected {
		return fmt.Errorf("cache native command differs from frozen original vector")
	}
	if s.Phase == Settled {
		var audit CacheAllocationAudit
		if s.Observation == nil || control.UnmarshalObject(s.Observation.Data, &audit) != nil || audit.ProtocolVersion != 1 ||
			audit.Response.ValidateRequest(expected) != nil || audit.NormalIdentity != o.AllocationPlan.Resource ||
			!reflect.DeepEqual(audit.NormalLimits, o.AllocationPlan.Transition.PeakLimits) || !lifecycleDigest(audit.RootStateSHA256) {
			return fmt.Errorf("cache observation lost original Root identity or complete parent budget")
		}
		if s.Observation.Result == Completed {
			if !audit.Response.AppliedFor(expected) || s.Action == RepartitionLFCBefore && !audit.Response.SynchronousExitFor(expected) {
				return fmt.Errorf("cache completion lacks original native installation or synchronous exit")
			}
		} else if s.Observation.Result != Rejected ||
			(audit.Response.Disposition != "rejected" && audit.Response.Disposition != "busy" && audit.Response.Disposition != "deadline_expired") {
			return fmt.Errorf("cache unknown cannot be recorded as no effect")
		}
	}
	return nil
}

func (m Model) BeginAllocation(actor Owner, p budget.AllocationPlan, now uint64) (Model, Disposition, error) {
	if p.Validate() != nil || m.Validate() != nil || actor != m.Owner ||
		m.PGInstance == nil || *m.PGInstance != p.Request.Key.Instance || m.ResourceIdentity == nil || *m.ResourceIdentity != p.Resource {
		return m, Stale, fmt.Errorf("allocation requires original PG/resource and current owner")
	}
	// 先核原接受决定；迟到重试不依赖当前墙钟，不能用另一个向量覆盖相同父Key。
	var found *Operation
	if m.Active != nil && m.Active.Request.Key == p.Request.Key {
		found = m.Active
	}
	for i := range m.History {
		if m.History[i].Operation.Request.Key == p.Request.Key {
			found = &m.History[i].Operation
		}
	}
	if found != nil {
		left, err := p.SHA256()
		if err != nil || found.AllocationPlan == nil {
			return m, Conflict, nil
		}
		right, err := found.AllocationPlan.SHA256()
		if err != nil || left != right {
			return m, Conflict, nil
		}
		return m.Clone(), Replayed, nil
	}
	next, disposition, err := m.Begin(actor, p.Request, now)
	if err != nil || disposition != Accepted {
		return next, disposition, err
	}
	frozen := p.Clone()
	next.Active.AllocationPlan = &frozen
	actions := next.Active.actions()
	next.Active.Steps = make([]Step, len(actions))
	for i, action := range actions {
		next.Active.Steps[i] = Step{Ordinal: control.Uint64(i + 1), Action: action, Phase: Planned}
	}
	return next, Accepted, next.Validate()
}

func (s *Store) BeginAllocation(ctx context.Context, snapshot *Snapshot, actor Owner, p budget.AllocationPlan, now uint64) (*Snapshot, Disposition, error) {
	if err := s.checked(snapshot); err != nil {
		return nil, Stale, err
	}
	if !snapshot.Dispatchable() || snapshot.vm.Spec.DSGABootstrap.ComputeInstanceID != p.Request.Key.Instance.ComputeInstanceID {
		return nil, Stale, fmt.Errorf("allocation requires original ready running VM")
	}
	next, disposition, err := snapshot.model.BeginAllocation(actor, p, now)
	if err != nil {
		return nil, disposition, err
	}
	result, err := s.apply(ctx, snapshot, next)
	return result, disposition, err
}
