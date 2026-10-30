package hostguard

import (
	"context"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// PreparedClosure只携带可信Publisher从原VM UID/RV快照认证的事实。它不接收
// Node来源自报值；SealPrepared从自己的原Node快照填写完整binding/Hold/Claim。
// 生产者必须先确认类别16原生关闭、Prepare Completed且其它资源步骤全Planned，
// 并在此Node快照之后重核同一原VM闭门/Lease；本Store不替代跨域来源审计。
type PreparedClosure struct {
	Plan              Plan
	PreparedSHA256    string
	OperationSHA256   string
	VMOwner           journal.Owner
	VMSequence        control.Uint64
	VMResourceVersion string
	ClosedSHA256      string
	ClosedSequence    control.Uint64
}

func barrierPlan(b budget.HostReservationBarrier) Plan {
	return Plan{Namespace: b.Namespace, VMName: b.VMName, Incarnation: b.Incarnation,
		RunnerPodUID: b.RunnerPodUID, Request: b.Request, BeforeBytes: b.BeforeMemoryBytes,
		HostOverheadBytes: b.HostOverheadBytes}
}

func samePreparedClosure(b budget.HostReservationBarrier, in PreparedClosure) bool {
	return barrierPlan(b) == in.Plan && b.PreparedSHA256 == in.PreparedSHA256 &&
		b.OperationSHA256 == in.OperationSHA256 &&
		b.VMOwner == (budget.BarrierOwner{HolderID: in.VMOwner.HolderID, Epoch: in.VMOwner.Epoch}) &&
		b.VMSequence == in.VMSequence && b.VMResourceVersion == in.VMResourceVersion &&
		b.ClosedSHA256 == in.ClosedSHA256 && b.ClosedSequence == in.ClosedSequence
}

// validatePreparedBarrier验证账本内的固定原身份，而非网络活性。Held是封存时
// 的历史观察，后续合法Settle/Initial退休可删除原Hold；水位仍须永久保留。
func (m Model) validatePreparedBarrier(p Placement) error {
	b := p.Barrier
	if b == nil {
		return nil
	}
	if m.ProtocolVersion < 9 || b.Validate() != nil || b.NodeName != m.Binding.NodeName ||
		b.NodeUID != string(m.Binding.NodeUID) || b.LeaseNamespace != m.Binding.Namespace ||
		b.LeaseName != m.Binding.LeaseName || b.LeaseUID != string(m.Binding.LeaseUID) ||
		b.NodeOrigin != m.Origin || b.NodeSequence > m.Sequence ||
		b.HostOwner.Epoch > m.LeaseFloor.Epoch ||
		b.HostOwner.Epoch == m.LeaseFloor.Epoch && b.HostOwner.HolderID != m.LeaseFloor.HolderID {
		return fmt.Errorf("prepared barrier changed original Node/Lease/origin/floor or v9 sequence")
	}
	if _, err := p.Bytes(); err != nil {
		return err
	}
	plan := barrierPlan(*b)
	if _, err := plan.Bytes(); err != nil || b.Held != nil && b.Held.SourceID != SourceID(m.Origin, plan) {
		return fmt.Errorf("prepared barrier changed complete original hold source")
	}
	if p.Settled != nil && p.Settled.Request.Key.Instance != b.Request.Key.Instance {
		return fmt.Errorf("prepared barrier and closed budget changed original PG instance")
	}
	for _, h := range m.Holds {
		if h.Plan.RunnerPodUID != p.PodUID {
			continue
		}
		if h.Plan.Request.Key.Instance != b.Request.Key.Instance {
			return fmt.Errorf("prepared barrier cannot cover another PG boot")
		}
		if compare(h.Plan.Request.Key, b.Request.Key) == 0 {
			if h.Plan != plan || b.Held == nil || h.SourceID != b.Held.SourceID || !reflect.DeepEqual(h.Claim, b.Held.Claim) {
				return fmt.Errorf("prepared barrier changed sealed original plan/claim UID")
			}
		}
	}
	return nil
}

// checkPreparedBarrier只用于执行资格。只读旧预算须显式调用独立保留读者，
// 不能把这里的ErrSealed吞掉后继续发布。比较前先固定完整Instance，同一
// epoch/revision换OperationID仍属于已封前缀，绝不能绕过水位。
func (m Model) checkPreparedBarrier(plan Plan) error {
	p, err := m.Placement(plan.RunnerPodUID)
	if err != nil || p.Barrier == nil {
		return nil
	}
	b := p.Barrier
	if plan.Namespace != p.Namespace || plan.VMName != p.VMName || plan.Incarnation != p.Incarnation ||
		plan.HostOverheadBytes != p.HostOverheadBytes || plan.Request.Key.Instance != b.Request.Key.Instance {
		return fmt.Errorf("Host execution changed sealed original VM/Pod/PG domain")
	}
	if compare(plan.Request.Key, b.Request.Key) <= 0 {
		return ErrSealed
	}
	return nil
}

func (p Placement) checkPlacementKey(key *control.Key, memory control.Uint64) error {
	if !p.Managed || p.Barrier == nil && p.Settled == nil {
		return nil
	}
	if key == nil {
		if memory > p.MemoryBytes {
			return fmt.Errorf("keyless placement cannot increase sealed or closed capacity")
		}
		return nil
	}
	if p.Barrier != nil && (key.Instance != p.Barrier.Request.Key.Instance || compare(*key, p.Barrier.Request.Key) <= 0) {
		return ErrSealed
	}
	if p.Settled != nil && (key.Instance != p.Settled.Request.Key.Instance || compare(*key, p.Settled.ClosedThrough()) <= 0) {
		return ErrSettled
	}
	return nil
}

// PreparedBarrier只返回精确原Plan的完整水位副本。调用方须重新GET原Node并
// 独立认证UID/Lease/floor，不能把旧Snapshot当最新来源；更高水位覆盖后不
// 为旧Key伪造一张新票据，也不回退水位。该值绝非Host完成或释放证明。
func (n *Snapshot) PreparedBarrier(plan Plan) (budget.HostReservationBarrier, error) {
	var empty budget.HostReservationBarrier
	if n == nil || n.model.Validate() != nil {
		return empty, fmt.Errorf("original Node barrier snapshot unavailable")
	}
	p, err := n.model.Placement(plan.RunnerPodUID)
	if err != nil || p.Barrier == nil || barrierPlan(*p.Barrier) != plan {
		return empty, fmt.Errorf("exact original prepared barrier not found")
	}
	return p.Barrier.Clone(), nil
}

// SealPrepared的线性化点仍为原Node UID/RV RFC6902 CAS。只封不放：所有原
// Hold、Claim与placement额度逐字保留。回复丢失不返回屏障；重建后只能GET
// 并读取原准确水位，不重造Key/Claim UID或更换原闭门选择。
func (s *Store) SealPrepared(ctx context.Context, n *Snapshot, in PreparedClosure) (*Snapshot, budget.HostReservationBarrier, error) {
	var empty budget.HostReservationBarrier
	if n == nil || n.model.Binding != s.binding || n.model.Validate() != nil || n.model.ProtocolVersion < 3 {
		return nil, empty, fmt.Errorf("prepared seal requires original complete allocation snapshot")
	}
	actor, err := s.authorize(ctx, n.node)
	if err != nil || actor != n.model.LeaseFloor {
		return nil, empty, fmt.Errorf("prepared seal requires current fenced Node owner")
	}
	p, err := n.model.Placement(in.Plan.RunnerPodUID)
	if err != nil || !p.Managed || p.Namespace != in.Plan.Namespace || p.VMName != in.Plan.VMName ||
		p.VMUID != in.Plan.Request.Key.Instance.VMUID || p.Incarnation != in.Plan.Incarnation ||
		p.HostOverheadBytes != in.Plan.HostOverheadBytes {
		return nil, empty, fmt.Errorf("prepared seal changed original managed placement")
	}
	if p.Barrier != nil {
		order := compare(in.Plan.Request.Key, p.Barrier.Request.Key)
		if in.Plan.Request.Key.Instance != p.Barrier.Request.Key.Instance || order < 0 {
			return nil, empty, fmt.Errorf("prepared seal cannot regress or change PG instance")
		}
		if order == 0 {
			if !samePreparedClosure(*p.Barrier, in) {
				return nil, empty, fmt.Errorf("same sealed key changed original closure/operation")
			}
			return n, p.Barrier.Clone(), nil
		}
		if in.VMSequence <= p.Barrier.VMSequence || in.ClosedSequence <= p.Barrier.ClosedSequence ||
			in.VMOwner.Epoch < p.Barrier.VMOwner.Epoch || in.VMOwner.Epoch == p.Barrier.VMOwner.Epoch && in.VMOwner.HolderID != p.Barrier.VMOwner.HolderID {
			return nil, empty, fmt.Errorf("new prepared seal regressed original VM closure source")
		}
	}
	if p.Settled != nil && (in.Plan.Request.Key.Instance != p.Settled.Request.Key.Instance || compare(in.Plan.Request.Key, p.Settled.ClosedThrough()) <= 0) {
		return nil, empty, fmt.Errorf("prepared seal is not newer than closed original budget")
	}
	if n.model.Sequence == control.Uint64(math.MaxUint64) {
		return nil, empty, fmt.Errorf("prepared seal sequence exhausted")
	}
	b := budget.HostReservationBarrier{ProtocolVersion: 1, Namespace: in.Plan.Namespace, VMName: in.Plan.VMName,
		Incarnation: in.Plan.Incarnation, RunnerPodUID: in.Plan.RunnerPodUID, Request: in.Plan.Request,
		BeforeMemoryBytes: in.Plan.BeforeBytes, HostOverheadBytes: in.Plan.HostOverheadBytes,
		PreparedSHA256: in.PreparedSHA256, OperationSHA256: in.OperationSHA256,
		VMOwner:    budget.BarrierOwner{HolderID: in.VMOwner.HolderID, Epoch: in.VMOwner.Epoch},
		VMSequence: in.VMSequence, VMResourceVersion: in.VMResourceVersion,
		ClosedSHA256: in.ClosedSHA256, ClosedSequence: in.ClosedSequence,
		NodeName: s.binding.NodeName, NodeUID: string(s.binding.NodeUID), LeaseNamespace: s.binding.Namespace,
		LeaseName: s.binding.LeaseName, LeaseUID: string(s.binding.LeaseUID), NodeOrigin: n.model.Origin,
		HostOwner:    budget.BarrierOwner{HolderID: actor.HolderID, Epoch: actor.Epoch},
		NodeSequence: n.model.Sequence + 1, NodeResourceVersion: n.ResourceVersion(), Held: nil}
	// 只从封存CAS实际使用的Node快照导出三态，不能将CM404或外层Planned
	// 当成没有Reserve；同位置别名必须报错，不能伪造Held=nil。
	for _, h := range n.model.Holds {
		if h.Plan.RunnerPodUID != in.Plan.RunnerPodUID {
			continue
		}
		if h.Plan.Request.Key.Instance != in.Plan.Request.Key.Instance {
			return nil, empty, fmt.Errorf("prepared seal changed held PG instance")
		}
		if compare(h.Plan.Request.Key, in.Plan.Request.Key) == 0 {
			if h.Plan != in.Plan {
				return nil, empty, fmt.Errorf("prepared seal changed existing full original plan")
			}
			b.Held = &budget.HostBarrierHold{SourceID: h.SourceID, Claim: nil}
			if h.Claim != nil {
				ref := *h.Claim
				b.Held.Claim = &ref
			}
		}
	}
	if b.Validate() != nil {
		return nil, empty, fmt.Errorf("prepared seal lacks complete original VM closure witness")
	}
	next := n.model.Clone()
	next.ProtocolVersion, next.Sequence = max(next.ProtocolVersion, 9), b.NodeSequence
	for i := range *next.Placements {
		if (*next.Placements)[i].PodUID == p.PodUID {
			copyBarrier := b.Clone()
			(*next.Placements)[i].Barrier = &copyBarrier
		}
	}
	result, err := s.write(ctx, n.node, next, false)
	if err != nil {
		return nil, empty, err
	}
	return result, b.Clone(), nil
}

// RetireSealedHolds仅供Initial完整恢复完成后的可信生产适配调用。生产者必须
// 在本Node快照之后认证原VM无Active/当前关闭、完整RetainedCoverage及当前
// Initial/Runner/Lease来源，并做最终重核。裸Barrier DTO绝不是这些来源证明。
// 普通/实际预算须使用Settle，不能借本入口删除当前永久Host来源。
// 这里按最新水位覆盖的整个前缀退休，修复上次清理中断后更早Hold的残留；
// 原placement与Barrier全部保留，绝不据此声称Guest/Host物理内存已归还。
func (s *Store) RetireSealedHolds(ctx context.Context, n *Snapshot, b budget.HostReservationBarrier) (*Snapshot, []Hold, error) {
	if n == nil || n.model.Binding != s.binding || n.model.ProtocolVersion < 9 || n.model.Validate() != nil || b.Validate() != nil {
		return nil, nil, fmt.Errorf("sealed retirement requires original complete v9 Node snapshot")
	}
	actor, err := s.authorize(ctx, n.node)
	if err != nil || actor != n.model.LeaseFloor {
		return nil, nil, fmt.Errorf("sealed retirement requires current fenced Node owner")
	}
	p, err := n.model.Placement(b.RunnerPodUID)
	if err != nil || p.Settled != nil || p.Barrier == nil || !reflect.DeepEqual(*p.Barrier, b) {
		return nil, nil, fmt.Errorf("sealed retirement requires exact latest Initial barrier, not settled Host budget")
	}
	next := n.model.Clone()
	retained, removed := make([]Hold, 0, len(next.Holds)), []Hold{}
	for _, h := range next.Holds {
		if h.Plan.RunnerPodUID != p.PodUID {
			retained = append(retained, h)
			continue
		}
		if h.Plan.Request.Key.Instance != b.Request.Key.Instance || h.Plan.Namespace != p.Namespace ||
			h.Plan.VMName != p.VMName || h.Plan.Incarnation != p.Incarnation ||
			h.Plan.HostOverheadBytes != p.HostOverheadBytes || h.SourceID != SourceID(next.Origin, h.Plan) {
			return nil, nil, fmt.Errorf("sealed retirement changed original prefix source")
		}
		order := compare(h.Plan.Request.Key, b.Request.Key)
		if order == 0 && h.Plan != barrierPlan(b) {
			return nil, nil, fmt.Errorf("sealed retirement changed original sealed key")
		}
		if order <= 0 {
			removed = append(removed, h)
		} else {
			retained = append(retained, h)
		}
	}
	if len(removed) == 0 {
		return n, removed, nil
	}
	if next.Sequence == control.Uint64(math.MaxUint64) {
		return nil, nil, fmt.Errorf("sealed retirement sequence exhausted")
	}
	next.Holds, next.Sequence = retained, next.Sequence+1
	result, err := s.write(ctx, n.node, next, false)
	if err != nil {
		return nil, nil, err
	}
	return result, removed, nil
}

// ReadPreparedBarrier真实双读原Host Lease并GET原Node。它只返回已经落账的
// 精确屏障，不调用Fence/Seal/Reserve，也不会因当前失租删除旧持有。Node
// floor须已由当前原holder认证，接管未fence前宁可等待Scheduler完成。
func (s *Store) ReadPreparedBarrier(ctx context.Context, plan Plan) (budget.HostReservationBarrier, error) {
	var empty budget.HostReservationBarrier
	first, err := s.leaseActor(ctx, false)
	if err != nil {
		return empty, err
	}
	n, err := s.Load(ctx)
	if err != nil {
		return empty, err
	}
	last, err := s.leaseActor(ctx, false)
	if err != nil || first != last || last != n.model.LeaseFloor {
		return empty, fmt.Errorf("original Node barrier Lease/floor changed during read")
	}
	used, useErr := n.model.UsedBytes()
	total, capErr := capacity(n.node)
	if useErr != nil || capErr != nil || used > total {
		return empty, fmt.Errorf("original Node barrier domain exceeds actual allocatable capacity")
	}
	return n.PreparedBarrier(plan)
}
