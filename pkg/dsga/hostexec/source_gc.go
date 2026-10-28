package hostexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
)

// ErrCollectionPending只表示本轮有界发现尚未扫完，不表示Host结清失败。
// Scheduler保留游标下轮继续，不能为此回滚Node或按一分钟完成缓存跳过尾部。
var ErrCollectionPending = errors.New("original settled source collection has more work")

const (
	sourcePageSize  = 64
	sourceMaxPages  = 2
	sourceMaxWrites = 16
	sourceKinds     = 2
)

// CollectionProgress只保留发现游标和最多一页候选，永不保存执行/回收许可。
// 同一对象由单个调用占用，running防止直接调用方并发破坏游标；不持业务
// Mutex或PluginState.mu跨网络。水位/原VM变化立即丢弃游标，重启也可直接
// 重建；每个候选仍重新GET并核原VM/Node/Lease，不信LIST快照的资格。
type CollectionProgress struct {
	running      atomic.Bool
	binding      journal.Binding
	closed       hostguard.Settlement
	kind         int
	continuation string
	end          bool
	retry        bool
	pending      []collectionCandidate
}

// 队列只保存小引用和原文摘要，不把每页最多64个32KiB CM正文留给
// 每个Pod；否则256个Pod可仅因GC游标保留约512MiB。正文只在本轮
// LIST/单个GET中临时存在，最终删除仍认证新GET的完整协议和原摘要。
type collectionCandidate struct {
	Object    budget.ObjectRef
	Finalizer string
	SHA256    string
}

func sourceData(cm *core.ConfigMap, finalizer string) []byte {
	key := budgetclaims.ClaimKey
	if finalizer == vmguard.ProtectionFinalizer {
		key = vmguard.ClaimKey
	}
	return []byte(cm.Data[key])
}

func NewCollectionProgress() *CollectionProgress {
	var progress CollectionProgress
	return &progress
}

func (p *CollectionProgress) prepare(binding journal.Binding, closed hostguard.Settlement) {
	if p.binding != binding || p.closed != closed || p.kind == sourceKinds {
		p.binding, p.closed = binding, closed
		p.kind, p.continuation, p.end, p.retry, p.pending = 0, "", false, false, nil
	}
}

// sourceReferences只计算保留集合，不认证任何来源。命令/审计在journal中是
// []byte，直接marshal整个Model会把它们变成base64而漏掉内部引用，所以
// 必须显式遍历每个原payload；永久头独立扫描，不能依赖16条可淘汰历史。
// 对payload内ObjectRef的name/uid形状递归保守保留，新增嵌套引用也不会被
// 漏删。集合按名称保护：同名换UID属于受控恢复，GC不能替恢复路径清掉它。
type sourceReferences map[string]struct{}

func (refs sourceReferences) add(raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	if err := control.ValidateJSON(raw, journal.MaxObservationBytes, 2048); err != nil {
		return err
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	var walk func(any) error
	walk = func(v any) error {
		switch v := v.(type) {
		case map[string]any:
			name, hasName := v["name"]
			uid, hasUID := v["uid"]
			if hasName && hasUID {
				n, nameOK := name.(string)
				u, uidOK := uid.(string)
				if !nameOK || !uidOK || (budget.ObjectRef{Name: n, UID: u}).Validate() != nil {
					return fmt.Errorf("retained source reference is malformed")
				}
				refs[n] = struct{}{}
			}
			for _, child := range v {
				if err := walk(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range v {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(value)
}

func (refs sourceReferences) step(s journal.Step) error {
	if err := refs.add(s.Command); err != nil {
		return err
	}
	if s.Observation != nil {
		return refs.add(s.Observation.Data)
	}
	return nil
}

func retainedSourceNames(m journal.Model, published vmguard.Publication) (sourceReferences, error) {
	refs := make(sourceReferences)
	if m.Active != nil {
		return nil, fmt.Errorf("source collection cannot cross an active operation")
	}
	if h := m.BudgetHeld; h != nil {
		if err := refs.step(h.Finalization); err != nil {
			return nil, err
		}
	}
	if h := m.CapacityHeld; h != nil {
		if err := refs.step(h.Cleanup); err != nil {
			return nil, err
		}
	}
	if h := m.InitialHeld; h != nil {
		payloads := [][]byte{h.Budget}
		if h.Admission != nil {
			payloads = append(payloads, h.Admission.Command, h.Admission.Receipt)
		}
		for _, raw := range payloads {
			if err := refs.add(raw); err != nil {
				return nil, err
			}
		}
	}
	for _, h := range m.History {
		for _, s := range h.Operation.Steps {
			if err := refs.step(s); err != nil {
				return nil, err
			}
		}
		if s := h.Operation.Cleanup; s != nil {
			if err := refs.step(*s); err != nil {
				return nil, err
			}
		}
	}
	raw, err := json.Marshal(published)
	if err != nil {
		return nil, err
	}
	if err := refs.add(raw); err != nil {
		return nil, err
	}
	// 原Runner可能已经持久Last但其发布回复未知。即使当前预算的
	// 正常读者通常要求LastPublished，GC也不能删除该完整Last的
	// 内容寻址对象，迫使Runner改UID重建。这里仅增加保留，不赋许可。
	if published.State != nil && published.State.Last != nil {
		raw, err := json.Marshal(published.State.Last)
		if err != nil {
			return nil, err
		}
		refs[vmguard.ReceiptSourceName(raw)] = struct{}{}
	}
	return refs, nil
}

func sourceClosedBefore(key control.Key, closed hostguard.Settlement) bool {
	last := closed.ClosedThrough()
	return key.Instance == last.Instance && (key.IssuerEpoch < last.IssuerEpoch || key.IssuerEpoch == last.IssuerEpoch && key.Revision < last.Revision || key == last && closed.ThroughSequence != 0)
}

// oldSource只识别本原VM/Guest/Runner域中、持久关闭水位已覆盖且已无
// 恢复引用的两种操作来源。资格/Birth/State/Exit永不在活动VM中回收；
// 未知版本、畸形、另一Pod/boot/Node和同revision换ID一律保留。无保护
// finalizer仅允许恢复上次已移除保护但DELETE回复未知的同一原对象。
func (p *Publisher) oldSource(cm *core.ConfigMap, m journal.Model, birth vmguard.Birth, birthRef budget.ObjectRef, closed hostguard.Settlement, keep sourceReferences) (string, bool) {
	if cm == nil || cm.UID == "" || cm.ResourceVersion == "" || cm.Immutable == nil || !*cm.Immutable || len(cm.OwnerReferences) != 1 || len(cm.Data) != 1 || len(cm.BinaryData) != 0 {
		return "", false
	}
	if _, retained := keep[cm.Name]; retained {
		return "", false
	}
	b := p.vm.Binding()
	owner := metav1.GetControllerOf(cm)
	if cm.Namespace != b.Namespace || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != b.Name || owner.UID != b.VMUID || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
		return "", false
	}
	if cm.Labels[budgetclaims.ClaimLabel] == budgetclaims.OutsideKind && cm.Labels[vmguard.ClaimLabel] == "" {
		raw := []byte(cm.Data[budgetclaims.ClaimKey])
		var out budget.OutsideReservation
		if len(raw) > journal.MaxObservationBytes || json.Unmarshal(raw, &out) != nil || out.ValidateSource() != nil || out.Request.Validate() != nil || out.VMShrinkPermit || !out.HoldUntilExplicitSettlement || m.ResourceIdentity == nil || out.Identity != *m.ResourceIdentity || out.Incarnation != b.Incarnation || !sourceClosedBefore(out.Request.Key, closed) {
			return "", false
		}
		return budgetclaims.ProtectionFinalizer, true
	}
	if cm.Labels[vmguard.ClaimLabel] == vmguard.ReceiptKind && cm.Labels[budgetclaims.ClaimLabel] == "" {
		raw := []byte(cm.Data[vmguard.ClaimKey])
		var receipt vmguard.Receipt
		if len(raw) > journal.MaxObservationBytes || json.Unmarshal(raw, &receipt) != nil || cm.Name != vmguard.ReceiptSourceName(raw) || receipt.Intent.Command.Identity != birth.Identity || receipt.Intent.Command.BindingClaim != birthRef || !sourceClosedBefore(receipt.Intent.Command.Request.Key, closed) {
			return "", false
		}
		return vmguard.ProtectionFinalizer, true
	}
	return "", false
}

func (p *Publisher) sourceCollectionBoundary(ctx context.Context, s *journal.Snapshot, closed hostguard.Settlement, podUID string) error {
	if err := p.unchanged(ctx, s); err != nil {
		return err
	}
	n, err := p.node.Load(ctx)
	if err != nil {
		return err
	}
	if _, err := p.node.Fence(ctx, n); err != nil {
		return err
	}
	row, err := n.State().Placement(podUID)
	if err != nil || row.Settled == nil || *row.Settled != closed {
		return fmt.Errorf("original source collection settlement watermark changed")
	}
	return nil
}

func (p *Publisher) deleteSettledSource(ctx context.Context, expected collectionCandidate, s *journal.Snapshot, m journal.Model, birth vmguard.Birth, birthRef budget.ObjectRef, closed hostguard.Settlement, keep sourceReferences) error {
	cm, err := p.core.ConfigMaps(p.vm.Binding().Namespace).Get(ctx, expected.Object.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	f, eligible := p.oldSource(cm, m, birth, birthRef, closed, keep)
	if !eligible || string(cm.UID) != expected.Object.UID || f != expected.Finalizer || vmguard.SourceDigest(sourceData(cm, f)) != expected.SHA256 {
		return fmt.Errorf("original settled source changed during collection")
	}
	if err := p.sourceCollectionBoundary(ctx, s, closed, birth.Identity.RunnerPodUID); err != nil {
		return err
	}
	if slices.Contains(cm.Finalizers, f) {
		finalizers := slices.DeleteFunc(slices.Clone(cm.Finalizers), func(value string) bool { return value == f })
		ops := []patch.Operation{{Op: patch.OpTest, Path: "/metadata/uid", Value: cm.UID}, {Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: cm.ResourceVersion}, {Op: patch.OpReplace, Path: "/metadata/finalizers", Value: finalizers}}
		raw, err := json.Marshal(ops)
		if err != nil {
			return err
		}
		cm, err = p.core.ConfigMaps(p.vm.Binding().Namespace).Patch(ctx, expected.Object.Name, types.JSONPatchType, raw, metav1.PatchOptions{})
		if err != nil {
			return err
		}
		// 若原对象此前已进入删除且只剩自己的保护，移除后API可
		// 直接返回404/空对象；未知交回下一轮GET，不猜测已清理。
		_, eligible = p.oldSource(cm, m, birth, birthRef, closed, keep)
		if !eligible || string(cm.UID) != expected.Object.UID || vmguard.SourceDigest(sourceData(cm, f)) != expected.SHA256 || slices.Contains(cm.Finalizers, f) {
			return fmt.Errorf("settled source protection removal is unconfirmed")
		}
	}
	err = p.core.ConfigMaps(cm.Namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &cm.UID, ResourceVersion: &cm.ResourceVersion}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (p *Publisher) collectSettledSources(ctx context.Context, s *journal.Snapshot, closed hostguard.Settlement, progress *CollectionProgress) error {
	m := s.State()
	ref := closed.Runner.BirthClaim
	if closed.InitialVM.BirthClaim != (budget.ObjectRef{Name: "", UID: ""}) {
		ref = closed.InitialVM.BirthClaim
	}
	birth, birthRef, published, err := p.runner.Inspect(ctx, &ref, m.Owner)
	if err != nil {
		return err
	}
	if published.State.Pending != nil || birth.Identity.NodeUID != string(p.node.Binding().NodeUID) || published.Actual.RequestedBytes != closed.MemoryBytes() || published.Actual.ActualBytes != closed.MemoryBytes() {
		return fmt.Errorf("original settled Runner changed before source collection")
	}
	keep, err := retainedSourceNames(m, published)
	if err != nil {
		return err
	}
	progress.prepare(p.vm.Binding(), closed)
	pages, writes := 0, 0
	var failures []error
	for {
		if len(progress.pending) == 0 && progress.end {
			progress.kind++
			progress.continuation, progress.end = "", false
		}
		if progress.kind == sourceKinds {
			if progress.retry && len(failures) == 0 {
				// 前一分页已报告的失败不能在本页成功后被缓存为
				// 全部完成；先完成尾部，再重开一轮实际发现重试。
				return ErrCollectionPending
			}
			return errors.Join(failures...)
		}
		if ctx.Err() != nil {
			return errors.Join(append(failures, ctx.Err())...)
		}
		if writes == sourceMaxWrites {
			return collectionYield(failures)
		}
		if len(progress.pending) == 0 {
			if pages == sourceMaxPages {
				return collectionYield(failures)
			}
			selector := budgetclaims.ClaimLabel + "=" + budgetclaims.OutsideKind
			if progress.kind == 1 {
				selector = vmguard.ClaimLabel + "=" + vmguard.ReceiptKind
			}
			list, err := p.core.ConfigMaps(p.vm.Binding().Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: sourcePageSize, Continue: progress.continuation})
			if err != nil {
				if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
					// 只重置发现；原关闭事实和持有不改变，不以过期
					// LIST token释放或重新生成任何来源。
					progress.continuation, progress.end = "", false
				}
				return errors.Join(append(failures, err)...)
			}
			if list == nil || len(list.Items) > sourcePageSize || progress.continuation != "" && progress.continuation == list.Continue {
				return errors.Join(append(failures, fmt.Errorf("settled source page is absent, oversized or did not advance"))...)
			}
			pages++
			progress.continuation, progress.end = list.Continue, list.Continue == ""
			for i := range list.Items {
				cm := &list.Items[i]
				if f, eligible := p.oldSource(cm, m, birth, birthRef, closed, keep); eligible {
					progress.pending = append(progress.pending, collectionCandidate{Object: budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)}, Finalizer: f, SHA256: vmguard.SourceDigest(sourceData(cm, f))})
				}
			}
			continue
		}
		expected := progress.pending[0]
		progress.pending[0] = collectionCandidate{Object: budget.ObjectRef{Name: "", UID: ""}, Finalizer: "", SHA256: ""}
		progress.pending = progress.pending[1:]
		writes++
		// 尝试前出队，坏来源/丢回复不能在队首饿死其他旧来源；失败
		// 不记为完整扫描，下一轮完成尾部后再从实际API重新发现重试。
		if err := p.deleteSettledSource(ctx, expected, s, m, birth, birthRef, closed, keep); err != nil {
			progress.retry = true
			failures = append(failures, err)
		}
	}
}

func collectionYield(failures []error) error {
	if len(failures) == 0 {
		return ErrCollectionPending
	}
	return errors.Join(append(failures, ErrCollectionPending)...)
}
