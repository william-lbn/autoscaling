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

// SettleCurrent只释放已有完整关闭证明的原操作的“峰值分配承诺”，实际容量与原预算来源继续
// 持有；不是QMP命令，也不认证Host物理空闲。平台仍须认证当前memory-backend-ram/
// virtio-mem及Host开销上界，不能把任意QMP数字当物理归还。顺序固定为Node
// 快照/Fence→原VM无Active/当前预算→原Runner持久完成或原域取消与实际值→两端重读→
// 同Node UID/RV结清CAS。新授予在此窗口改变Node RV则冲突，未知不清事实。
func (p *Publisher) SettleCurrent(ctx context.Context) (bool, error) {
	return p.SettleCurrentWithCollection(ctx, p.collection)
}

// SettleCurrentWithCollection允许Scheduler在重建Publisher时保留有界发现
// 游标。progress不是容量或许可事实，丢失只会重新扫描；每次仍完整认证原
// VM/Runner/预算/Lease并核Node结清水位。单个游标不可并发使用。
func (p *Publisher) SettleCurrentWithCollection(ctx context.Context, progress *CollectionProgress) (bool, error) {
	if progress == nil || !progress.running.CompareAndSwap(false, true) {
		return false, fmt.Errorf("original source collection progress is absent or already in use")
	}
	defer progress.running.Store(false)
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
	if m.Active == nil && m.InitialHeld != nil {
		// 首次预算持续消费原placement而无Resize结清格式。完整独立
		// 覆盖后不生成Settlement、不降低placement；带完整Node屏障
		// 的新分支先独立退休已封原Hold前缀，再精确回收无引用的迟到
		// Host/Outside。旧分支仍只回收Outside。未扫完将ErrCollectionPending
		// 传回Scheduler继续有界工作，changed仍只表示Node结清变动。
		if err := p.checkInitial(ctx, s); err != nil {
			return false, err
		}
		changed, err := p.retireInitialSealedHolds(ctx, s, n)
		if err != nil {
			return false, err
		}
		return changed, p.collectRetainedInitialSources(ctx, s, progress)
	}
	if m.Active != nil || m.BudgetHeld == nil && m.CapacityHeld == nil {
		return false, nil
	}
	closed, err := hostguard.SettlementFor(m)
	if err != nil {
		return false, err
	}
	if !s.Dispatchable() {
		return false, fmt.Errorf("host settlement requires latest closed dispatchable original VM")
	}
	// 普通/实际预算和关闭水位有独立永久寿命；后续零派发取消只推进水位。
	// 实际预算必须核独立Capacity关闭合同，不能投影为原目标成功。任何
	// Start已先撤销关闭证明，不能借旧预算结清。
	var check error
	if m.CapacityHeld != nil {
		check = m.CheckClosedCapacity(closed.Request)
	} else {
		check = m.CheckClosedBudget(closed.Request)
	}
	if err := check; err != nil {
		return false, err
	}
	if err := p.currentOwner(ctx, s); err != nil {
		return false, err
	}
	current, err := p.claims.ReadCurrent(ctx, s)
	if err != nil {
		return false, err
	}
	currentVM, err := p.runner.CheckCurrentBudget(ctx, m)
	if err != nil {
		return false, err
	}
	id, actual := currentVM.Budget.Birth.Identity, currentVM.Budget.Actual
	if currentVM.Initial != nil {
		// 启动来源从未有命令，不读取零值Budget的Receipt，更不为
		// 复用结清路径合成Completed；仍核原真实requested/actual。
		id, actual = currentVM.Initial.Birth.Identity, currentVM.Initial.Actual
	}
	if current.Closed != closed || id.RunnerPodUID != current.Host.RunnerPodUID || id.NodeUID != string(p.node.Binding().NodeUID) || current.Host.HostOverheadBytes != p.overhead || actual.RequestedBytes != closed.MemoryBytes() || actual.ActualBytes != closed.MemoryBytes() {
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
	changed := result.State().Sequence != n.State().Sequence
	// Host水位已经持久化后，才清不再被永久预算/有界恢复历史/Runner
	// 引用的旧Outside及回执。未扫完只请求下轮继续，不撤销已完成结清。
	return changed, p.collectSettledSources(ctx, s, closed, progress)
}

// collectOldClaims可在结清CAS丢回复/进程重启后重新扫描恢复，不依赖丢失的本地
// retired列表。只清同原VM/incarnation/Node/Pod、同PG实例且严格早于持久水位
// 的Host CM；当前预算、另一Pod/Node/boot和Outside/Runner来源一律不动。
// 这里删除的是已失执行资格的垃圾，不将其同名新UID“收养”为任何原有效回执。
func (p *Publisher) collectOldClaims(ctx context.Context, s *journal.Snapshot, closed hostguard.Settlement, podUID string) error {
	// 永久slot及原Unknown可能继续引用已封存Claim，水位关闭只撤执行
	// 资格，不取消这些审计/恢复来源的寿命，不能先按Key删掉再扫引用。
	birth := closed.Runner.BirthClaim
	if closed.InitialVM.BirthClaim != (budget.ObjectRef{}) {
		birth = closed.InitialVM.BirthClaim
	}
	_, _, published, err := p.runner.Inspect(ctx, &birth, s.State().Owner)
	if err != nil {
		return err
	}
	keep, err := retainedSourceNames(s.State(), published)
	if err != nil {
		return err
	}
	options := metav1.ListOptions{LabelSelector: budgetclaims.ClaimLabel + "=" + budgetclaims.HostKind, Limit: 64}
	for {
		list, err := p.core.ConfigMaps(p.vm.Binding().Namespace).List(ctx, options)
		if err != nil {
			return err
		}
		for i := range list.Items {
			cm := &list.Items[i]
			if _, referenced := keep[cm.Name]; referenced {
				continue
			}
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
	a, last := h.Request.Key, closed.ClosedThrough()
	return a.IssuerEpoch < last.IssuerEpoch || a.IssuerEpoch == last.IssuerEpoch && a.Revision < last.Revision || a == last && closed.ThroughSequence != 0
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
