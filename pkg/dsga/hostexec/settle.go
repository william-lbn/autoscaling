package hostexec

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
)

// SettleCurrent只释放已成功闭合原操作的“峰值分配承诺”，目标与原预算来源继续
// 持有；不是QMP命令，也不认证Host物理空闲。平台仍须认证当前memory-backend-ram/
// virtio-mem及Host开销上界，不能把任意QMP数字当物理归还。顺序固定为Node
// 快照/Fence→原VM无Active/当前预算→原Runner持久完成与实际值→两端重读→
// 同Node UID/RV结清CAS。新授予在此窗口改变Node RV则冲突，未知不清事实。
func (p *Publisher) SettleCurrent(ctx context.Context) (bool, error) {
	n, err := p.node.Load(ctx)
	if err != nil {
		return false, err
	}
	if n.State().ProtocolVersion < 3 {
		return false, fmt.Errorf("host settlement requires complete allocation domain")
	}
	n, err = p.node.Fence(ctx, n)
	if err != nil {
		return false, err
	}
	s, err := p.vm.Load(ctx)
	if err != nil {
		return false, err
	}
	m := s.State()
	if m.Active != nil || m.BudgetHeld == nil {
		return false, nil
	}
	closed, err := hostguard.SettlementFor(m)
	if err != nil {
		return false, err
	}
	if !s.Dispatchable() {
		return false, fmt.Errorf("host settlement requires latest closed dispatchable original VM")
	}
	// 完整预算和成功关闭水位有独立永久寿命；后续零派发取消只推进该水位，
	// 不更改原预算/Runner目标。任何Start已先撤销证明，不能借旧预算结清。
	if err := m.CheckClosedBudget(closed.Request); err != nil {
		return false, err
	}
	if err := p.currentOwner(ctx, s); err != nil {
		return false, err
	}
	var command budget.FinalizationCommand
	if json.Unmarshal(m.BudgetHeld.Finalization.Command, &command) != nil || command.Validate() != nil {
		return false, fmt.Errorf("original retained finalization unavailable")
	}
	current, err := p.claims.ReadCurrent(ctx, s)
	if err != nil {
		return false, err
	}
	confirmed, err := p.runner.CheckRetained(ctx, closed.Runner, closed.Request, command.PreparedSHA256, command.GuestSHA256, closed.HostClaim, m.Owner)
	if err != nil {
		return false, err
	}
	id := confirmed.Birth.Identity
	if current.Closed != closed || id.RunnerPodUID != current.Host.RunnerPodUID || id.NodeUID != string(p.node.Binding().NodeUID) || current.Host.HostOverheadBytes != p.overhead || confirmed.Actual.RequestedBytes != closed.Request.TargetVMMemoryBytes || confirmed.Actual.ActualBytes != closed.Request.TargetVMMemoryBytes {
		return false, fmt.Errorf("host settlement original Runner/Node/target differs")
	}
	if err := p.claims.RecheckCurrent(ctx, s, current); err != nil {
		return false, err
	}
	if err := p.unchanged(ctx, s); err != nil {
		return false, err
	}
	result, _, err := p.node.Settle(ctx, n, id.RunnerPodUID, closed)
	if err != nil {
		return false, err
	}
	// CAS后仍核原来源，失败只报告未知，不撤回已持久高水位。即使回复
	// 丢失，后续同请求只读恢复；GC始终在水位确认后进行，不先删来源。
	if err := p.claims.RecheckCurrent(ctx, s, current); err != nil {
		return false, err
	}
	if err := p.unchanged(ctx, s); err != nil {
		return false, err
	}
	if err := p.collectOldClaims(ctx, s, closed, id.RunnerPodUID); err != nil {
		return false, err
	}
	return result.State().Sequence != n.State().Sequence, nil
}

// collectOldClaims可在结清CAS丢回复/进程重启后重新扫描恢复，不依赖丢失的本地
// retired列表。只清同原VM/incarnation/Node/Pod、同PG实例且严格早于持久水位
// 的Host CM；当前预算、另一Pod/Node/boot和Outside/Runner来源一律不动。
// 这里删除的是已失执行资格的垃圾，不将其同名新UID“收养”为任何原有效回执。
func (p *Publisher) collectOldClaims(ctx context.Context, s *journal.Snapshot, closed hostguard.Settlement, podUID string) error {
	options := metav1.ListOptions{LabelSelector: budgetclaims.ClaimLabel + "=" + budgetclaims.HostKind, Limit: 64}
	for {
		list, err := p.core.ConfigMaps(p.vm.Binding().Namespace).List(ctx, options)
		if err != nil {
			return err
		}
		for i := range list.Items {
			cm := &list.Items[i]
			if !p.oldClaim(cm, closed, podUID) {
				continue
			}
			if err := p.unchanged(ctx, s); err != nil {
				return err
			}
			// 再核持久水位，不能把另一个Node恢复/来源损坏当作清理资格。
			node, err := p.node.Load(ctx)
			if err != nil {
				return err
			}
			if _, err := p.node.Fence(ctx, node); err != nil {
				return err
			}
			row, err := node.State().Placement(podUID)
			if err != nil || row.Settled == nil || *row.Settled != closed {
				return fmt.Errorf("original Host GC settlement watermark changed")
			}
			if err := p.deleteOldClaim(ctx, cm, closed, podUID); err != nil {
				return err
			}
		}
		if list.Continue == "" {
			return nil
		}
		options.Continue = list.Continue
	}
}

func (p *Publisher) oldClaim(cm *core.ConfigMap, closed hostguard.Settlement, podUID string) bool {
	if cm == nil || cm.Name == closed.HostClaim.Name || cm.UID == "" || cm.ResourceVersion == "" || cm.Immutable == nil || !*cm.Immutable || len(cm.OwnerReferences) != 1 || len(cm.Data) != 1 || len(cm.BinaryData) != 0 || cm.Labels[budgetclaims.ClaimLabel] != budgetclaims.HostKind {
		return false
	}
	owner := metav1.GetControllerOf(cm)
	b := p.vm.Binding()
	if cm.Namespace != b.Namespace || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != b.Name || owner.UID != b.VMUID || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
		return false
	}
	var h budget.HostReservation
	if json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &h) != nil || h.ProtocolVersion != 2 || h.Request.Validate() != nil || h.VMShrinkPermit || !h.HoldUntilExplicitSettlement || h.Request.Key.Instance != closed.Request.Key.Instance || h.Incarnation != b.Incarnation || h.RunnerPodUID != podUID || h.NodeName != p.node.Binding().NodeName || h.NodeUID != string(p.node.Binding().NodeUID) || h.HostOverheadBytes != p.overhead {
		return false
	}
	a, last := h.Request.Key, closed.Request.Key
	return a.IssuerEpoch < last.IssuerEpoch || a.IssuerEpoch == last.IssuerEpoch && a.Revision < last.Revision
}

func (p *Publisher) deleteOldClaim(ctx context.Context, expected *core.ConfigMap, closed hostguard.Settlement, podUID string) error {
	cm, err := p.core.ConfigMaps(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if cm.UID != expected.UID || !p.oldClaim(cm, closed, podUID) {
		return fmt.Errorf("original Host GC object changed")
	}
	if slices.Contains(cm.Finalizers, budgetclaims.ProtectionFinalizer) {
		finalizers := slices.DeleteFunc(slices.Clone(cm.Finalizers), func(f string) bool { return f == budgetclaims.ProtectionFinalizer })
		ops := []patch.Operation{{Op: patch.OpTest, Path: "/metadata/uid", Value: cm.UID}, {Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: cm.ResourceVersion}, {Op: patch.OpReplace, Path: "/metadata/finalizers", Value: finalizers}}
		raw, err := json.Marshal(ops)
		if err != nil {
			return err
		}
		cm, err = p.core.ConfigMaps(cm.Namespace).Patch(ctx, cm.Name, types.JSONPatchType, raw, metav1.PatchOptions{})
		if err != nil {
			return err
		}
		if cm == nil || cm.UID != expected.UID || cm.ResourceVersion == "" || !p.oldClaim(cm, closed, podUID) || slices.Contains(cm.Finalizers, budgetclaims.ProtectionFinalizer) {
			return fmt.Errorf("host GC finalizer removal is unconfirmed")
		}
	}
	err = p.core.ConfigMaps(cm.Namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &cm.UID, ResourceVersion: &cm.ResourceVersion}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
