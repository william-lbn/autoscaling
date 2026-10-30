package budgetclaims

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// serviceActualPrepared只开放独立关闭后的来源认证；它不是Abort的兼容捷径。
// 原Active及Prepare必须逐字保持，不能通过临时改Abort或构造另一Model绕过
// 普通取消读者的前置条件。早期缺Prepare/Host的关闭由独立旧预算路径处理。
func (r *Reader) serviceActualPrepared(s *journal.Snapshot) (budget.Prepared, error) {
	var none budget.Prepared
	if s == nil || !s.Dispatchable() || s.VirtualMachine().UID != r.binding.VMUID || s.State().Incarnation != r.binding.Incarnation {
		return none, fmt.Errorf("service actual sources require original live VM")
	}
	m := s.State()
	c := m.ServiceClosure
	version := m.ClosureFormatVersion()
	if c != nil && version == 20 && c.Reopen != nil {
		version = c.Reopen.ClosureVersion
	}
	if c != nil && version == 19 && c.Rearm != nil {
		version = c.Rearm.ClosureVersion
	}
	if m.Active == nil || c == nil || c.Pending() || version != 16 && version != 17 || m.ResourceIdentity == nil || m.PGInstance == nil || m.Active.Request.Key.Instance != *m.PGInstance {
		return none, fmt.Errorf("service actual sources require original confirmed resource closure")
	}
	p, _, err := m.OriginalServicePrepared()
	return p, err
}

// serviceActualOutside只选择已经被原Prepared或永久预算固定的来源。VM仍大
// 时不能借新小规格Outside；VM已扩大也不能用旧小规格资格。Root normal
// 是否已采用目标另由Root聚合认证，不能据此选择错误的VM规格。
func serviceActualOutside(m journal.Model, p budget.Prepared, memory control.Uint64, ref budget.ObjectRef) (control.Uint64, error) {
	if p.Request.TargetVMMemoryBytes == memory && p.OutsideClaim != nil && *p.OutsideClaim == ref {
		return p.GuestUsableMemoryBytes, nil
	}
	if m.CapacityHeld != nil {
		// v3独立资源恢复头刻意没有Cleanup。统一入口先验证完整Model、
		// 原操作覆盖和预算摘要，再给出深副本；不能为兼容旧读取伪造
		// Cleanup成功，也不能因头格式变化丢掉仍覆盖实际VM的原来源。
		saved, err := m.CurrentCapacity()
		if err == nil && saved.InitialOutside == nil && saved.Request.Key.Instance == p.Request.Key.Instance && saved.VMMemoryBytes == memory && saved.OutsideClaim == ref {
			return saved.GuestUsableMemoryBytes, nil
		}
	}
	if m.BudgetHeld != nil {
		var saved budget.FinalizationCommand
		if json.Unmarshal(m.BudgetHeld.Finalization.Command, &saved) == nil && saved.Validate() == nil && saved.Finalized.Sources != nil && saved.Finalized.Request.Key.Instance == p.Request.Key.Instance && saved.Finalized.Request.TargetVMMemoryBytes == memory && saved.Finalized.Sources.OutsideClaim == ref {
			return saved.Finalized.GuestUsableMemoryBytes, nil
		}
	}
	return 0, fmt.Errorf("service actual outside is not an exact original prepared or retained VM source")
}

// ServiceActualReservations仅读取独立实际预算的构造原料。返回的可用内存
// 是原精确规格资格的持续保证，不是当前MemTotal/RSS；当前Root仍须另核
// 实际可用内存不低于它。Host始终取本操作原峰值，不在此退休任何持有。
// outside与initial严格互斥；Initial完整原值不被改成目标Resize声明。
func (r *Reader) ServiceActualReservations(ctx context.Context, s *journal.Snapshot, p budget.Prepared, memory control.Uint64, outside *budget.ObjectRef, initial *budget.Initial) (budget.OutsideReservation, budget.HostReservation, budget.ObjectRef, control.Uint64, error) {
	var out budget.OutsideReservation
	var host budget.HostReservation
	var ref budget.ObjectRef
	original, err := r.serviceActualPrepared(s)
	if err != nil || !reflect.DeepEqual(original, p) || (outside == nil) == (initial == nil) {
		return out, host, ref, 0, fmt.Errorf("service actual declarations lost original complete Prepared or exclusive outside source")
	}
	var usable control.Uint64
	if initial != nil {
		saved, err := s.State().RetainedInitialOutside()
		if err != nil || !reflect.DeepEqual(saved, *initial) || initial.VMMemoryBytes != memory {
			return out, host, ref, 0, fmt.Errorf("service actual initial outside replaced original retained qualification")
		}
		if _, err := r.ReadInitialBudget(ctx, s, *initial); err != nil {
			return out, host, ref, 0, err
		}
		usable = initial.GuestUsableMemoryBytes
	} else {
		usable, err = serviceActualOutside(s.State(), p, memory, *outside)
		if err != nil {
			return out, host, ref, 0, err
		}
		out, err = r.ReadOutside(ctx, *outside)
		if err != nil {
			return out, host, ref, 0, err
		}
		if out.Request.TargetVMMemoryBytes != memory || out.Request.Key.Instance != p.Request.Key.Instance || out.Identity != p.Identity || out.Incarnation != r.binding.Incarnation || out.VMShrinkPermit || !out.HoldUntilExplicitSettlement {
			return out, host, ref, 0, fmt.Errorf("service actual outside lost exact original VM specification")
		}
		if out.Qualification != nil {
			q, err := r.Qualification(ctx, *out.Qualification)
			if err != nil {
				return out, host, ref, 0, err
			}
			target, err := q.Target(memory)
			if err != nil || target.GuestUsableMemoryBytes != usable {
				return out, host, ref, 0, fmt.Errorf("service actual usable memory differs from exact original platform guarantee")
			}
		}
	}
	ref, err = r.HostReference(ctx, p.Request)
	if err != nil {
		return out, host, ref, 0, err
	}
	cm, err := r.claim(ctx, ref, HostKind)
	if err != nil {
		return out, host, ref, 0, err
	}
	if json.Unmarshal([]byte(cm.Data[ClaimKey]), &host) != nil || host.ProtocolVersion != 2 || host.ValidateFor(p, r.binding.Incarnation) != nil || initial != nil && (host.RunnerPodUID != initial.RunnerPodUID || host.NodeUID != initial.NodeUID || host.HostOverheadBytes != initial.HostOverheadBytes) {
		return out, host, ref, 0, fmt.Errorf("service actual Host lost original peak or placement")
	}
	return out, host, ref, usable, nil
}

// ReadServiceActual只接受确认关闭的原操作与v4独立预算，不要求或伪造Abort。
// 底层共用固定UID/完整body/资格/Pod/Node/原峰值的认证，避免新恢复读者
// 建立宽松替代通道。保存Proof不是一次性来源认证，每次开放前仍重读本层。
func (r *Reader) ReadServiceActual(ctx context.Context, s *journal.Snapshot, c budget.ReconciledCapacity) (*ReconciledSources, error) {
	p, err := r.serviceActualPrepared(s)
	if err != nil || c.ProtocolVersion != 4 || c.Validate() != nil || c.Request != p.Request || c.VMBeforeMemoryBytes != p.VMBeforeMemoryBytes {
		return nil, fmt.Errorf("service actual reader lost exact original capacity and closed operation")
	}
	var outside *budget.ObjectRef
	if c.InitialOutside == nil {
		ref := c.OutsideClaim
		outside = &ref
	}
	out, host, ref, usable, err := r.ServiceActualReservations(ctx, s, p, c.VMMemoryBytes, outside, c.InitialOutside)
	if err != nil {
		return nil, err
	}
	if ref != c.HostClaim || host.SourceID != c.HostReservationID || usable != c.GuestUsableMemoryBytes || c.InitialOutside == nil && (out.Request != c.OutsideRequest || out.SystemOutsidePG != c.SystemOutsidePG || out.Transition != c.Transition || out.Safety != c.Safety || out.ResidualProfileID != c.ResidualProfileID) {
		return nil, fmt.Errorf("service actual original declarations changed retained budget")
	}
	return r.readReconciledSources(ctx, s, c, nil)
}

// RecheckServiceActual包围PG/Root/Runner跨域读取。Node其它计划或心跳可
// 前进，但本VM原峰值、来源CM UID/RV/body和Pod/Node身份不得被替换。
func (r *Reader) RecheckServiceActual(ctx context.Context, s *journal.Snapshot, c budget.ReconciledCapacity, original *ReconciledSources) error {
	if original == nil {
		return fmt.Errorf("service actual source observation missing")
	}
	next, err := r.ReadServiceActual(ctx, s, c)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(next.Outside, original.Outside) || next.Host != original.Host || !sameClaimSnapshot(next.outside, original.outside) || !sameInitialSources(next.initial, original.initial) || next.host.UID != original.host.UID || next.host.ResourceVersion != original.host.ResourceVersion || !reflect.DeepEqual(next.host.Data, original.host.Data) || next.pod.UID != original.pod.UID || next.node.UID != original.node.UID {
		return fmt.Errorf("service actual original source changed during verification")
	}
	return nil
}
