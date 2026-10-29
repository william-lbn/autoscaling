package admissionexec

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// CheckRetainedService为最终开放Unknown/Completed但尚未Finish的安全检查。
// 普通advance在原开放NotFound时会先返回，不能据此跳过真实来源审计，
// 否则迟到开放仍可能在坏预算上服务。这里复用正常开放的Root/Host/Outside/
// Runner完整核验，不发开放、不Record/Finish，也不清SDK的原开放未知。
// 网络不持本地锁；RV复核和真正关闭的后续写前CAS各自独立，未声称跨域原子。
func (e *Executor) CheckRetainedService(ctx context.Context, s *journal.Snapshot) error {
	m := s.State()
	if e.claims == nil || e.runner == nil || !s.Dispatchable() || m.Owner != e.actor || m.Active == nil || m.ServiceClosure != nil {
		return fmt.Errorf("retained admission check requires original live production domains")
	}
	if _, err := m.ResumeAdmissionForClosure(); err != nil {
		return err
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
	return e.sameSnapshot(ctx, s)
}
