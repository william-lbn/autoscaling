package admissionexec

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// CheckRetainedService为完整Finalize后、尚未Finish的最终开放安全检查，
// 包括尚未Start的Planned：Grow可能保留首次服务的开放，不能只拒绝新开放。
// 普通advance在原开放NotFound时会先返回，不能据此跳过真实来源审计，
// 否则迟到开放仍可能在坏预算上服务。这里复用正常开放的Root/Host/Outside/
// Runner完整核验，不发开放、不Record/Finish，也不清SDK的原开放未知。
// 网络不持本地锁；RV复核和真正关闭的后续写前CAS各自独立，未声称跨域原子。
func (e *Executor) CheckRetainedService(ctx context.Context, s *journal.Snapshot) error {
	return e.checkRetainedService(ctx, s, nil, false)
}

// CheckCommittedServiceRecovery 只审计 13/14 已完整结算、最终准入待恢复的原
// Active。它仍使用原步骤/持有的 Host 与 Runner 来源，不把 Active 投影为空闲
// 已结清预算；不开放、Finish、释放 Host，也不回写原 Resume 的未知结果。
func (e *Executor) CheckCommittedServiceRecovery(ctx context.Context, s *journal.Snapshot) error {
	return e.checkRetainedService(ctx, s, nil, true)
}

// CheckRetainedServiceWithAudit只为本次正常服务完整审计返回瞬时后继检查点。
// 保留旧只读/恢复API，无回调字段、缓存许可或跨物理写的快照复用。
func (e *Executor) CheckRetainedServiceWithAudit(ctx context.Context, s *journal.Snapshot, after func(context.Context, *journal.Snapshot, control.SharedCostResponse) error) error {
	if after == nil {
		return fmt.Errorf("normal retained service audit requires explicit completion callback")
	}
	return e.checkRetainedService(ctx, s, after, false)
}

func (e *Executor) checkRetainedService(ctx context.Context, s *journal.Snapshot, after func(context.Context, *journal.Snapshot, control.SharedCostResponse) error, recovering bool) error {
	if s == nil {
		return fmt.Errorf("retained admission check requires original snapshot")
	}
	m := s.State()
	if e.claims == nil || e.runner == nil || !s.Dispatchable() || m.Owner != e.actor || m.Active == nil {
		return fmt.Errorf("retained admission check requires original live production domains")
	}
	if recovering {
		// Reopen/Rearm 是原关闭之上的恢复包装：选择业务重开后外层格式会变成
		// v20，期限恢复后会变成 v19，但原 v13/v14 的完整资源结算语义必须保持。
		// 这里必须检查解包后的原关闭类别；否则 StartServiceReopen 写前 CAS 一旦
		// 成功，发送前的第二次来源审计就会把合法 v20 状态误判成“不再是 13/14”。
		// 后面的 CheckStableServiceRecovery 仍逐项验证原 Active、预算、关闭摘要；
		// 本改动只修正类别识别，不放宽任何 Root/Host/Runner 或准入安全门。
		version := m.ServiceClosureVersion()
		if m.ServiceClosure == nil || m.ServiceClosure.Pending() || version != 13 && version != 14 || m.CheckStableServiceRecovery() != nil {
			return fmt.Errorf("committed service audit requires original complete 13/14 budget closure")
		}
	} else if m.ServiceClosure != nil {
		return fmt.Errorf("retained admission check requires original live production domains")
	}
	if _, err := m.ResumeAdmissionForClosure(); err != nil {
		if _, err := m.PlannedResumeBudgetForClosure(); err != nil {
			return err
		}
	}
	f, err := m.BudgetFor(m.Active.Request)
	if err != nil {
		return err
	}
	fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return err
	}
	shared, err := e.client.SharedCost(ctx)
	if err != nil {
		return err
	}
	if recovering && !serviceActualGate(m.ServiceClosure, shared.Observed) {
		return fmt.Errorf("committed service audit changed its original native gate")
	}
	vm := s.VirtualMachine()
	if vm.Status.MemorySize == nil {
		return fmt.Errorf("retained admission lacks original actual VM memory")
	}
	actual, exact := vm.Status.MemorySize.AsInt64()
	if fence.Disposition != control.Fenced || shared.Observed.Instance != e.instance || shared.Observed.IssuerFloorEpoch != e.actor.Epoch || shared.Observed.CapacityGeneration < fence.Observed.CapacityGeneration || !exact || actual <= 0 || f.ValidateFor(m.Active.Request, shared, e.actor.Epoch, vm.Status.PodName, vm.Status.RestartCount, uint64(actual)) != nil {
		return fmt.Errorf("retained admission changed PG, VM or full budget")
	}
	aggregate, err := e.checkRoot(ctx, s, f)
	if err != nil {
		return err
	}
	observed, prepared, err := e.checkClaims(ctx, s, f, aggregate)
	if err != nil {
		return err
	}
	// 如正常写后审计，再读原Root与持久来源，原UID/RV/来源摘要不相同
	// 即保留异常。这里的成功仅允许继续原只读/正常结案，绝非新开放许可。
	if _, err := e.checkRoot(ctx, s, f); err != nil {
		return err
	}
	if err := e.claims.Recheck(ctx, s, prepared, f.Sources.HostClaim, observed); err != nil {
		return err
	}
	if _, _, err := e.checkClaims(ctx, s, f, aggregate); err != nil {
		return err
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return err
	}
	if after != nil {
		known := !m.Active.AbortRequested && m.Active.Cleanup == nil && m.Active.Origin == e.actor
		for _, step := range m.Active.Steps {
			if step.Phase == journal.Unconfirmed {
				known = false
			}
		}
		pending, closing := e.client.PendingAdmission()
		if known && e.client.Pending() == nil && pending == nil && closing == nil && e.resources != nil && e.resources.Pending() == nil {
			return after(ctx, s, shared)
		}
	}
	return nil
}
