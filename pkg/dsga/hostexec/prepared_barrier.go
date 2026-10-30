package hostexec

import (
	"context"
	"encoding/json"
	"fmt"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// SealClosedHostRequest只撤销已确认类别16早期请求的Host执行资格。原
// Prepare已Completed而所有其它资源步骤全Planned；不制造Host完成、不改
// WAL Unknown/Abort、不删除持有、不发PG/QMP，也不自动重开服务。
func (p *Publisher) SealClosedHostRequest(ctx context.Context) (budget.HostReservationBarrier, error) {
	var empty budget.HostReservationBarrier
	s, err := p.vm.Load(ctx)
	if err != nil {
		return empty, err
	}
	m := s.State()
	closure, err := m.PreparedHostClosure()
	if err != nil || !s.Dispatchable() {
		return empty, fmt.Errorf("Host seal requires exact confirmed completed-Prepare closure")
	}
	n, err := p.node.Load(ctx)
	if err != nil {
		return empty, err
	}
	n, err = p.node.Fence(ctx, n)
	if err != nil {
		return empty, err
	}
	plan, err := p.before(ctx, s, closure.Prepared)
	if err != nil {
		return empty, err
	}
	if b, readErr := n.PreparedBarrier(plan); readErr == nil {
		// 原封存回复可能丢失，VM后续也可能已保存独立Proof；只读恢复
		// 原完整票据，不用当前更高RV/sequence重造同Key的封存来源。
		if m.CheckPreparedHostBarrier(b) != nil {
			return empty, fmt.Errorf("saved Host seal differs from original VM closure")
		}
		confirmed, err := p.node.ReadPreparedBarrier(ctx, plan)
		if err != nil || !reflect.DeepEqual(b, confirmed) {
			return empty, fmt.Errorf("saved Host seal source changed during read")
		}
		if err := p.unchanged(ctx, s); err != nil {
			return empty, err
		}
		return confirmed, nil
	}
	if m.ServiceClosure.Rearm != nil || m.ServiceClosure.Reopen != nil || m.ServiceClosure.RetainedRecovery != nil {
		return empty, fmt.Errorf("Host seal cannot be manufactured after retained recovery selection")
	}
	if err := p.unchanged(ctx, s); err != nil {
		return empty, err
	}
	input := hostguard.PreparedClosure{Plan: plan, PreparedSHA256: closure.PreparedSHA256,
		OperationSHA256: closure.OperationSHA256, VMOwner: m.Owner, VMSequence: m.Sequence,
		VMResourceVersion: s.ResourceVersion(), ClosedSHA256: closure.ClosedSHA256, ClosedSequence: closure.ClosedSequence}
	_, b, err := p.node.SealPrepared(ctx, n, input)
	if err != nil {
		return empty, err
	}
	if err := p.unchanged(ctx, s); err != nil {
		return empty, err
	}
	return b, nil
}

// retireInitialSealedHolds在已有Node快照之后完整核当前Initial/Runner与
// 永久覆盖槽，再以同UID/RV退休最新屏障覆盖的全部原Hold前缀。只清一个
// Key会漏掉上次清理未完成、随后被更高屏障覆盖的旧Hold；placement不动。
func (p *Publisher) retireInitialSealedHolds(ctx context.Context, s *journal.Snapshot, n *hostguard.Snapshot) (bool, error) {
	m := s.State()
	if m.RetainedCoverage == nil || m.RetainedCoverage.Record.ServiceCoverage == nil {
		return false, nil
	}
	proof := m.RetainedCoverage.Record.ServiceCoverage.RetainedRecovery
	if proof == nil || proof.PreparedHostBarrier == nil {
		return false, nil
	}
	b := proof.PreparedHostBarrier.Clone()
	if _, err := initialSourceWatermark(m); err != nil {
		return false, err
	}
	if _, _, _, err := p.initialCollectionSources(ctx, s); err != nil {
		return false, err
	}
	plan := hostguard.Plan{Namespace: b.Namespace, VMName: b.VMName, Incarnation: b.Incarnation, RunnerPodUID: b.RunnerPodUID, Request: b.Request, BeforeBytes: b.BeforeMemoryBytes, HostOverheadBytes: b.HostOverheadBytes}
	original, err := n.PreparedBarrier(plan)
	if err != nil || !reflect.DeepEqual(original, b) {
		return false, fmt.Errorf("Initial retirement lost latest exact original Node barrier")
	}
	latest, err := p.node.ReadPreparedBarrier(ctx, plan)
	if err != nil || !reflect.DeepEqual(latest, b) {
		return false, fmt.Errorf("Initial retirement Node/Lease source changed")
	}
	if err := p.unchanged(ctx, s); err != nil {
		return false, err
	}
	next, _, err := p.node.RetireSealedHolds(ctx, n, b)
	if err != nil {
		return false, err
	}
	if _, _, _, err := p.initialCollectionSources(ctx, s); err != nil {
		return false, err
	}
	if err := p.unchanged(ctx, s); err != nil {
		return false, err
	}
	return next.ResourceVersion() != n.ResourceVersion(), nil
}

// oldInitialSealedHost只在完整Initial覆盖并已退休Node前缀后使用。原Initial
// 的VM/Guest/Runner值没有被封存改变，故可重建每份旧请求的准确原Plan与
// SourceID；同位置换ID、别Node/Pod/boot、原文/UID引用或当前slot仍引用者
// 一律不删。Runner receipt与首次资格/Birth/State永不进入此Host分支。
func (p *Publisher) oldInitialSealedHost(cm *core.ConfigMap, m journal.Model, b budget.HostReservationBarrier, keep sourceReferences) (string, bool) {
	if cm == nil || cm.UID == "" || cm.ResourceVersion == "" || cm.Immutable == nil || !*cm.Immutable || len(cm.OwnerReferences) != 1 || len(cm.Data) != 1 || len(cm.BinaryData) != 0 || cm.Labels[budgetclaims.ClaimLabel] != budgetclaims.HostKind || cm.Labels[vmguard.ClaimLabel] != "" || m.InitialHeld == nil || m.ResourceIdentity == nil {
		return "", false
	}
	if _, referenced := keep[cm.Name]; referenced {
		return "", false
	}
	owner := metav1.GetControllerOf(cm)
	binding := p.vm.Binding()
	if cm.Namespace != binding.Namespace || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != binding.Name || owner.UID != binding.VMUID || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
		return "", false
	}
	var h budget.HostReservation
	raw := []byte(cm.Data[budgetclaims.ClaimKey])
	if len(raw) > journal.MaxObservationBytes || json.Unmarshal(raw, &h) != nil || h.ProtocolVersion != 2 || h.Request.Validate() != nil || h.VMShrinkPermit || !h.HoldUntilExplicitSettlement || h.Identity != *m.ResourceIdentity || h.Incarnation != binding.Incarnation || h.RunnerPodUID != b.RunnerPodUID || h.NodeName != b.NodeName || h.NodeUID != b.NodeUID || h.HostOverheadBytes != b.HostOverheadBytes || !sourceClosedThrough(h.Request.Key, b.Request.Key, 1) {
		return "", false
	}
	var initial budget.Initial
	if json.Unmarshal(m.InitialHeld.Budget, &initial) != nil || initial.Validate() != nil || h.RunnerPodName != initial.RunnerPodName || initial.VMMemoryBytes != b.BeforeMemoryBytes {
		return "", false
	}
	plan := hostguard.Plan{Namespace: binding.Namespace, VMName: binding.Name, Incarnation: binding.Incarnation, RunnerPodUID: b.RunnerPodUID, Request: h.Request, BeforeBytes: initial.VMMemoryBytes, HostOverheadBytes: b.HostOverheadBytes}
	size, err := plan.Bytes()
	name, nameErr := budgetclaims.HostClaimName(binding, h.Request)
	if err != nil || nameErr != nil || name != cm.Name || size != h.ReservedMemoryBytes || h.SourceID != hostguard.SourceID(b.NodeOrigin, plan) {
		return "", false
	}
	return budgetclaims.ProtectionFinalizer, true
}
