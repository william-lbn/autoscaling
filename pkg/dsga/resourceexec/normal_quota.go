package resourceexec

import (
	"context"
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"reflect"
)

// 只从同一原操作的已持久Completed ReleasePG读取，不拿一次当前PG查询重新
// 生成“原完成”。因此GuestReady仍写原Command1，丢ACK后可逐字重建同一信封。
func normalQuotaEnvelope(op *journal.Operation, c resourceguard.Command, p budget.Prepared) (resourceguard.NormalQuotaEnvelope, error) {
	var zero resourceguard.NormalQuotaEnvelope
	if op == nil || p.NormalQuotaProtocolVersion != 1 || p.NormalQuotaFence == nil || p.ValidateFor(op.Request, c.Identity) != nil {
		return zero, fmt.Errorf("normal quota original preparation absent")
	}
	if op.Request.Kind == control.Grow {
		shared := p.Shared
		v := resourceguard.NormalQuotaEnvelope{ProtocolVersion: 1, Phase: "pre-grow", Command: c, PGRequest: op.Request, PGFence: *p.NormalQuotaFence, PGShared: &shared}
		if e := v.Validate(); e != nil {
			return zero, e
		}
		return v, nil
	}
	for _, s := range op.Steps {
		if s.Action != journal.ReleasePG {
			continue
		}
		if s.Phase != journal.Settled || s.Observation == nil || s.Observation.Result != journal.Completed || s.TargetID != c.Identity.ComputeInstanceID {
			return zero, fmt.Errorf("normal quota original PG release is not completed")
		}
		var a struct {
			ProtocolVersion uint32                  `json:"protocol_version"`
			Fence           control.FenceResponse   `json:"issuer_fence"`
			Response        control.ControlResponse `json:"response"`
		}
		var original control.Request
		if json.Unmarshal(s.Command, &original) != nil || original != op.Request || control.UnmarshalObject(s.Observation.Data, &a) != nil || a.ProtocolVersion != 1 {
			return zero, fmt.Errorf("normal quota PG release differs from retained original")
		}
		v := resourceguard.NormalQuotaEnvelope{ProtocolVersion: 1, Phase: "released-shrink", Command: c, PGRequest: op.Request, PGFence: a.Fence, PGResponse: &a.Response}
		if e := v.Validate(); e != nil {
			return zero, e
		}
		return v, nil
	}
	return zero, fmt.Errorf("normal quota original PG release missing")
}

// RecoverNormalQuota是独立、显式的恢复选择；普通Advance绝不调用。仅完整原quota
// 可以被重新确认耐久性，不重写旧epoch/预算，不重发partial内核效果。成功后仍走
// 原Advance的PG/资源/VM fresh核验及原CAS，Root返回并不直接使GuestReady完成。
func (e *Executor) RecoverNormalQuota(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if key.Validate() != nil {
		return pgexec.AwaitingFact, fmt.Errorf("invalid normal recovery key")
	}
	snapshot, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := snapshot.State()
	if m.Owner != e.actor || m.Active == nil || m.Active.Request.Key != key || m.ResourceIdentity == nil || *m.ResourceIdentity != e.id {
		return pgexec.AwaitingFact, fmt.Errorf("normal recovery original owner or identity changed")
	}
	step, source := guestStep(m.Active)
	if step == nil || source == nil || step.Phase != journal.Unconfirmed || step.DispatchedBy == nil || step.TargetID != e.instance.ComputeInstanceID {
		return pgexec.AwaitingFact, fmt.Errorf("normal recovery original GuestReady unavailable")
	}
	var p budget.Prepared
	var c resourceguard.Command
	if json.Unmarshal(source.Observation.Data, &p) != nil || p.NormalQuotaProtocolVersion != 1 || p.ValidateFor(m.Active.Request, e.id) != nil || json.Unmarshal(step.Command, &c) != nil || c.Validate() != nil || c.IssuerEpoch != step.DispatchedBy.Epoch || c.IssuerEpoch > e.actor.Epoch || !commandMatches(c, p) {
		return pgexec.AwaitingFact, fmt.Errorf("normal recovery changed original command or budget")
	}
	envelope, err := normalQuotaEnvelope(m.Active, c, p)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if !e.resources.NormalQuotaEnabled() {
		if _, err = e.resources.InspectState(ctx); err != nil {
			return pgexec.AwaitingFact, err
		}
	}
	if err = e.resources.RetainNormalQuota(envelope); err != nil {
		return pgexec.AwaitingFact, err
	}
	if _, err = e.resources.RecoverNormalQuota(ctx, envelope); err != nil {
		return pgexec.AwaitingFact, err
	}
	return e.advanceSelected(ctx, nil, &envelope)
}

// 这是原恢复后继续核验的实际生产门，不是新的授权来源。匹配失败不改SDK
// Pending，不发Root Apply/Replay/Cancel，不接受同Key替换新目标或预算。
func normalQuotaContinuationMatches(expected *resourceguard.NormalQuotaEnvelope, op *journal.Operation, step *journal.Step, command resourceguard.Command) error {
	if expected == nil {
		return nil
	}
	if expected.Validate() != nil || op == nil || op.Request != expected.PGRequest || step == nil || step.Action != journal.GuestReady || step.Phase != journal.Unconfirmed || !reflect.DeepEqual(command, expected.Command) {
		return resourceguard.ErrUnknown
	}
	return nil
}
