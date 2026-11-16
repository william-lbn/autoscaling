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
		if p.OnlineShrinkProtocolVersion == 1 {
			v.ProtocolVersion = 2
			v.Phase = "online-released-shrink"
			v.PGFence = *p.NormalQuotaFence
		}
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
	original, err := e.normalQuotaRecoveryOriginal(ctx, key, false)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if err = e.confirmNormalQuotaOriginal(ctx, original.envelope, false); err != nil {
		return pgexec.AwaitingFact, err
	}
	return e.advanceSelected(ctx, nil, &original.envelope)
}

// ConfirmNormalQuotaDurability只同步原完整quota的Root耐久性；它不确认GuestReady，
// 不调用PG Fence/SharedCost或任何VM/准入写。调用方仍须持有原Session；本方法仅从
// 当前Store owner/原Key选择同一完整WAL，不授予新的owner、Source或准入许可。
// 成功也返回AwaitingFact；关闭/过期管理入口是否准入由原协调器规则独立决定。
func (e *Executor) ConfirmNormalQuotaDurability(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	original, err := e.normalQuotaRecoveryOriginal(ctx, key, true)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.AwaitingFact, e.confirmSelectedNormalQuotaDurability(ctx, original)
}

func (e *Executor) confirmSelectedNormalQuotaDurability(ctx context.Context, original normalQuotaRecoverySelection) error {
	if err := e.confirmNormalQuotaOriginal(ctx, original.envelope, original.closure != nil); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := e.store.Load(ctx)
	if err != nil {
		return err
	}
	m := current.State()
	// Root的fsync与外层CAS没有跨域原子性。期间换owner/原意图/取消或cleanup时
	// 保留原Unknown，不将Root确认解释为当前GuestReady完成，也不跟随新Active。
	if m.Owner != e.actor || m.ResourceIdentity == nil || *m.ResourceIdentity != e.id || m.Active == nil || !reflect.DeepEqual(*m.Active, original.operation) {
		return resourceguard.ErrUnknown
	}
	if original.closure != nil && (!current.Dispatchable() || !reflect.DeepEqual(m.ServiceClosure, original.closure)) {
		return resourceguard.ErrUnknown
	}
	return nil
}

// ReconcileClosedQuotaDurability 供新 Owner 在正常 Root 状态因同一持久见证失效而
// 无法读回时使用。先按已完成 GuestReady 和原持久关闭重新执行完整耐久回证，
// 再由调用方重新读 Root 并走正常 fence。这个入口没有 PG、VM 或预算执行器。
func ReconcileClosedQuotaDurability(ctx context.Context, store *journal.Store, resources *resourceguard.Client, actor journal.Owner, key control.Key) error {
	if store == nil || resources == nil {
		return resourceguard.ErrUnknown
	}
	id, epoch := resources.Binding()
	if epoch != actor.Epoch || id.VMUID != key.Instance.VMUID || id.ComputeInstanceID != key.Instance.ComputeInstanceID {
		return resourceguard.ErrUnknown
	}
	selection := Executor{store: store, resources: resources, actor: actor, instance: key.Instance, id: id}
	original, err := selection.normalQuotaRecoveryOriginal(ctx, key, true)
	if err != nil {
		return err
	}
	if original.closure == nil {
		return resourceguard.ErrUnknown
	}
	return selection.confirmSelectedNormalQuotaDurability(ctx, original)
}

type normalQuotaRecoverySelection struct {
	envelope  resourceguard.NormalQuotaEnvelope
	operation journal.Operation
	closure   *journal.ServiceClosure
}

// rootOnly只增加新确认入口的取消/实例门；旧Recover的选择及后验语义保持。
func (e *Executor) normalQuotaRecoveryOriginal(ctx context.Context, key control.Key, rootOnly bool) (normalQuotaRecoverySelection, error) {
	if key.Validate() != nil {
		return normalQuotaRecoverySelection{}, fmt.Errorf("invalid normal recovery key")
	}
	snapshot, err := e.store.Load(ctx)
	if err != nil {
		return normalQuotaRecoverySelection{}, err
	}
	m := snapshot.State()
	if m.Owner != e.actor || m.Active == nil || m.Active.Request.Key != key || m.ResourceIdentity == nil || *m.ResourceIdentity != e.id {
		return normalQuotaRecoverySelection{}, fmt.Errorf("normal recovery original owner or identity changed")
	}
	if rootOnly && (m.Active.AbortRequested || m.Active.Cleanup != nil || m.Active.Request.Key.Instance != e.instance) {
		return normalQuotaRecoverySelection{}, fmt.Errorf("normal durability confirmation cannot adopt cancelled or changed original operation")
	}
	step, source := guestStep(m.Active)
	var closure *journal.ServiceClosure
	// 已完成 GuestReady 后，Guard 的进程级 fsync 见证仍可能失效。只复用已结算
	// 关闸保护的原 Finalize 边界，不回写 GuestReady 或把原命令重发给 PG/VM。
	if step == nil && rootOnly && snapshot.Dispatchable() && m.ServiceClosure != nil &&
		!m.ServiceClosure.Pending() && m.ServiceClosure.Reason == "finalize_source_unconfirmed" {
		if _, err := m.PendingFinalizationGenerationForClosure(); err != nil {
			return normalQuotaRecoverySelection{}, err
		}
		for i := range m.Active.Steps {
			candidate := &m.Active.Steps[i]
			switch candidate.Action {
			case journal.PrepareBudget:
				source = candidate
			case journal.GuestReady:
				step = candidate
			}
		}
		closure = m.ServiceClosure
	}
	if step == nil || source == nil || step.DispatchedBy == nil || step.TargetID != e.instance.ComputeInstanceID ||
		(closure == nil && step.Phase != journal.Unconfirmed) || (closure != nil && step.Phase != journal.Settled) {
		return normalQuotaRecoverySelection{}, fmt.Errorf("normal recovery original GuestReady unavailable")
	}
	var p budget.Prepared
	var c resourceguard.Command
	if json.Unmarshal(source.Observation.Data, &p) != nil || p.NormalQuotaProtocolVersion != 1 || p.ValidateFor(m.Active.Request, e.id) != nil || json.Unmarshal(step.Command, &c) != nil || c.Validate() != nil || c.IssuerEpoch != step.DispatchedBy.Epoch || c.IssuerEpoch > e.actor.Epoch || !commandMatches(c, p) {
		return normalQuotaRecoverySelection{}, fmt.Errorf("normal recovery changed original command or budget")
	}
	if closure != nil {
		var original Audit
		if json.Unmarshal(step.Observation.Data, &original) != nil || original.ProtocolVersion != 2 ||
			!reflect.DeepEqual(original.Command, c) || original.Evidence.ValidateFor(c) != nil {
			return normalQuotaRecoverySelection{}, fmt.Errorf("normal recovery lacks settled original GuestReady evidence")
		}
	}
	envelope, err := normalQuotaEnvelope(m.Active, c, p)
	if err != nil {
		return normalQuotaRecoverySelection{}, err
	}
	return normalQuotaRecoverySelection{envelope: envelope, operation: *m.Active, closure: closure}, nil
}

func (e *Executor) confirmNormalQuotaOriginal(ctx context.Context, envelope resourceguard.NormalQuotaEnvelope, retainedFinalization bool) error {
	if !e.resources.NormalQuotaEnabled() && !retainedFinalization {
		if _, err := e.resources.InspectState(ctx); err != nil {
			return err
		}
	}
	if err := e.resources.RetainNormalQuota(envelope); err != nil {
		return err
	}
	if _, err := e.resources.RecoverNormalQuota(ctx, envelope); err != nil {
		return err
	}
	return nil
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
