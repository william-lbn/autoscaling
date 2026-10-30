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
	initial      initialCollectionWatermark
	kind         int
	continuation string
	end          bool
	retry        bool
	pending      []collectionCandidate
}

// 初始预算没有Resize Request或Host CM；来源GC使用独立的关闭事实，
// 不将它编码成Settlement，更不把该水位用于释放原Node placement。
// 当前Source与slot完整证明另由每次真实Model/网络读取认证；游标仅存有界标识。
type initialCollectionWatermark struct {
	SourceSHA256     string
	RecordedSequence control.Uint64
	ThroughKey       control.Key
	ClosedSequence   control.Uint64
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
	var noInitial initialCollectionWatermark
	if p.binding != binding || p.closed != closed || p.initial != noInitial || p.kind == sourceKinds {
		p.initial = noInitial
		p.binding, p.closed = binding, closed
		p.kind, p.continuation, p.end, p.retry, p.pending = 0, "", false, false, nil
	}
}

func (p *CollectionProgress) prepareInitial(binding journal.Binding, watermark initialCollectionWatermark) {
	if p.binding != binding || p.initial != watermark || p.kind == 1 {
		var noSettlement hostguard.Settlement
		p.binding, p.closed, p.initial = binding, noSettlement, watermark
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
	return refs.addBounded(raw, journal.MaxObservationBytes, 2048)
}

// 独立Proof具有多个完整领域，使用其持久合同的专属有界上限；普通Step
// payload继续使用原32KiB/2048边界，不因新增恢复来源放宽旧输入。
func (refs sourceReferences) addBounded(raw []byte, maxBytes, maxValues int) error {
	if len(raw) == 0 {
		return nil
	}
	if err := control.ValidateJSON(raw, maxBytes, maxValues); err != nil {
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

// RetainedRecovery的Source.Finalization也是Step，完整Proof JSON中的该
// raw payload会变成base64。必须另解其原命令/审计，尤其普通新预算已替换
// 当前头后，slot/有界History仍应保护旧Unknown引用的准确来源。
func (refs sourceReferences) retainedProof(proof *journal.ServiceRetainedRecovery) error {
	if proof == nil {
		return nil
	}
	if proof.Source.Finalization != nil {
		if err := refs.step(*proof.Source.Finalization); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	return refs.addBounded(raw, journal.MaxRawBytes/2, 16384)
}

func (refs sourceReferences) retainedRecord(record journal.Record) error {
	for _, step := range record.Operation.Steps {
		if err := refs.step(step); err != nil {
			return err
		}
	}
	if step := record.Operation.Cleanup; step != nil {
		if err := refs.step(*step); err != nil {
			return err
		}
	}
	if record.ServiceCoverage != nil {
		return refs.retainedProof(record.ServiceCoverage.RetainedRecovery)
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
		if h.Recovery != nil {
			// 独立结算保留的完整原操作寿命长于结果缓存；原Finalize
			// 未知仍在其中，不能只扫描新预算而提前回收原来源对象。
			for _, step := range h.Recovery.Original.Steps {
				if err := refs.step(step); err != nil {
					return nil, err
				}
			}
		}
	}
	if h := m.CapacityHeld; h != nil {
		if err := refs.step(h.Cleanup); err != nil {
			return nil, err
		}
		if h.Recovery != nil {
			// 独立实际预算保存原Cleanup未知，原资源前驱仍须可审计；
			// 结果缓存淘汰不等于这些持有来源已经可以删除。
			for _, step := range h.Recovery.Original.Steps {
				if err := refs.step(step); err != nil {
					return nil, err
				}
			}
			if step := h.Recovery.Original.Cleanup; step != nil {
				if err := refs.step(*step); err != nil {
					return nil, err
				}
			}
		}
		if h.ActiveRecovery != nil {
			// v3没有原Cleanup成功；原Unknown与独立实际预算分别保留。
			// 原Operation中的[]byte仍须逐份解码，Proof则扫描完整结构，
			// 使历史淘汰不能裁掉原VM取消屏障/实际预算仍引用的原来源。
			for _, step := range h.ActiveRecovery.Original.Steps {
				if err := refs.step(step); err != nil {
					return nil, err
				}
			}
			if step := h.ActiveRecovery.Original.Cleanup; step != nil {
				if err := refs.step(*step); err != nil {
					return nil, err
				}
			}
			raw, err := json.Marshal(h.ActiveRecovery.Proof)
			if err != nil {
				return nil, err
			}
			if err := refs.addBounded(raw, journal.MaxRawBytes/2, 16384); err != nil {
				return nil, err
			}
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
	if slot := m.RetainedCoverage; slot != nil {
		// 单slot寿命独立于当前Initial/Finalized/Capacity头及可淘汰结果
		// 缓存。Previous只有退休摘要，不嵌旧proof；不按近似名泛留CM。
		if err := refs.retainedRecord(slot.Record); err != nil {
			return nil, err
		}
	}
	for _, h := range m.History {
		if h.ServiceCoverage != nil {
			if err := refs.retainedProof(h.ServiceCoverage.RetainedRecovery); err != nil {
				return nil, err
			}
		}
		if h.ServiceCoverage != nil && h.ServiceCoverage.ActualRecovery != nil {
			raw, err := json.Marshal(h.ServiceCoverage.ActualRecovery)
			if err != nil {
				return nil, err
			}
			if err := refs.addBounded(raw, journal.MaxRawBytes/2, 16384); err != nil {
				return nil, err
			}
		}
		if h.ServiceCoverage != nil && h.ServiceCoverage.CleanupRecovery != nil && h.ServiceCoverage.CleanupRecovery.Finalization != nil {
			if err := refs.step(*h.ServiceCoverage.CleanupRecovery.Finalization); err != nil {
				return nil, err
			}
		}
		if h.ServiceCoverage != nil && h.ServiceCoverage.Finalization != nil {
			raw, err := json.Marshal(h.ServiceCoverage.Finalization.Command)
			if err != nil {
				return nil, err
			}
			if err := refs.add(raw); err != nil {
				return nil, err
			}
		}
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
	return sourceClosedThrough(key, closed.ClosedThrough(), closed.ThroughSequence)
}

// 最后一项要求完整Key相等；同epoch/revision换OperationID从不获得关闭资格。
func sourceClosedThrough(key, through control.Key, sequence control.Uint64) bool {
	return key.Instance == through.Instance && (key.IssuerEpoch < through.IssuerEpoch || key.IssuerEpoch == through.IssuerEpoch && key.Revision < through.Revision || key == through && sequence != 0)
}

// oldSource只识别本原VM/Guest/Runner域中、持久关闭水位已覆盖且已无
// 恢复引用的两种操作来源。资格/Birth/State/Exit永不在活动VM中回收；
// 未知版本、畸形、另一Pod/boot/Node和同revision换ID一律保留。无保护
// finalizer仅允许恢复上次已移除保护但DELETE回复未知的同一原对象。
func (p *Publisher) oldSource(cm *core.ConfigMap, m journal.Model, birth vmguard.Birth, birthRef budget.ObjectRef, closed hostguard.Settlement, keep sourceReferences) (string, bool) {
	return p.oldSourceThrough(cm, m, birth, birthRef, closed.ClosedThrough(), closed.ThroughSequence, keep, true)
}

// 初始GC和已结清GC只共用对象身份/保护/内容检查；授权边界由各自入口
// 认证。初始域只收Outside，不借旧Runner终态删除任何不可变回执。
func (p *Publisher) oldSourceThrough(cm *core.ConfigMap, m journal.Model, birth vmguard.Birth, birthRef budget.ObjectRef, through control.Key, sequence control.Uint64, keep sourceReferences, receipts bool) (string, bool) {
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
		if len(raw) > journal.MaxObservationBytes || json.Unmarshal(raw, &out) != nil || out.ValidateSource() != nil || out.Request.Validate() != nil || out.VMShrinkPermit || !out.HoldUntilExplicitSettlement || m.ResourceIdentity == nil || out.Identity != *m.ResourceIdentity || out.Incarnation != b.Incarnation || !sourceClosedThrough(out.Request.Key, through, sequence) {
			return "", false
		}
		return budgetclaims.ProtectionFinalizer, true
	}
	if receipts && cm.Labels[vmguard.ClaimLabel] == vmguard.ReceiptKind && cm.Labels[budgetclaims.ClaimLabel] == "" {
		raw := []byte(cm.Data[vmguard.ClaimKey])
		var receipt vmguard.Receipt
		if len(raw) > journal.MaxObservationBytes || json.Unmarshal(raw, &receipt) != nil || cm.Name != vmguard.ReceiptSourceName(raw) || receipt.Intent.Command.Identity != birth.Identity || receipt.Intent.Command.BindingClaim != birthRef || !sourceClosedThrough(receipt.Intent.Command.Request.Key, through, sequence) {
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
	return p.deleteCollectedSource(ctx, expected, func(cm *core.ConfigMap) (string, bool) {
		return p.oldSource(cm, m, birth, birthRef, closed, keep)
	}, func() error {
		return p.sourceCollectionBoundary(ctx, s, closed, birth.Identity.RunnerPodUID)
	})
}

// 两个GC入口只共享精确删除机制；传入的boundary每个候选都重新核对
// 原VM/Node/Lease。移除自身finalizer与DELETE分别带UID/RV屏障，绝不收养
// LIST后同名替换对象；丢回复留给下轮同原文/原UID只读恢复。
func (p *Publisher) deleteCollectedSource(ctx context.Context, expected collectionCandidate, eligibleSource func(*core.ConfigMap) (string, bool), boundary func() error) error {
	cm, err := p.core.ConfigMaps(p.vm.Binding().Namespace).Get(ctx, expected.Object.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	f, eligible := eligibleSource(cm)
	if !eligible || string(cm.UID) != expected.Object.UID || f != expected.Finalizer || vmguard.SourceDigest(sourceData(cm, f)) != expected.SHA256 {
		return fmt.Errorf("original settled source changed during collection")
	}
	if err := boundary(); err != nil {
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
		_, eligible = eligibleSource(cm)
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

// CollectRetainedInitialSources只回收独立覆盖已经退休的早期请求留下的
// Outside垃圾。它没有Host结清值，不修改Node placement/持有，不删除Runner
// 回执；没有完整v22证明时保持原Initial只读行为。直接调用也须占有游标，
// Scheduler的SettleCurrent已持有同游标时调用私有版本避免重入。
func (p *Publisher) CollectRetainedInitialSources(ctx context.Context, s *journal.Snapshot, progress *CollectionProgress) error {
	if progress == nil || !progress.running.CompareAndSwap(false, true) {
		return fmt.Errorf("initial source collection progress is absent or already in use")
	}
	defer progress.running.Store(false)
	return p.collectRetainedInitialSources(ctx, s, progress)
}

// initialSourceWatermark严格认证当前Initial叶子与独立slot；slot在普通预算
// 替换后仍供审计，但不能为另一当前头发垃圾回收资格。Through来自真实旧
// Active的完整退休记录，绝不为Initial合成Request或借HighWater猜关闭。
func initialSourceWatermark(m journal.Model) (initialCollectionWatermark, error) {
	var none initialCollectionWatermark
	if err := m.Validate(); err != nil {
		return none, err
	}
	if m.ProtocolVersion < 22 || m.Active != nil || m.ServiceClosure != nil || m.InitialHeld == nil || !m.InitialAdmissionReady() || m.RetainedCoverage == nil {
		return none, fmt.Errorf("initial source collection lacks complete retired early operation")
	}
	record := m.RetainedCoverage.Record
	coverage := record.ServiceCoverage
	if coverage == nil || coverage.RetainedRecovery == nil || coverage.RetainedRecovery.Source.Initial == nil || m.CheckRetainedSource(coverage.RetainedRecovery.Source) != nil || record.ClosedSequence == 0 || record.ClosedSequence > m.Sequence {
		return none, fmt.Errorf("initial source collection lost exact current permanent source")
	}
	source := coverage.RetainedRecovery.Source
	return initialCollectionWatermark{SourceSHA256: source.SourceSHA256, RecordedSequence: source.RecordedSequence, ThroughKey: record.Operation.Request.Key, ClosedSequence: record.ClosedSequence}, nil
}

// 预算来源与原Runner的当前终态分别认证。InitialVM必须从未有命令；合法
// NativeVM则保留原Completed或Cancelled及其原Witness，不将数值相同视作
// 无命令，不新发取消。Host仍只核原placement且不生成任何Settlement。
func (p *Publisher) initialCollectionSources(ctx context.Context, s *journal.Snapshot) (vmguard.Birth, budget.ObjectRef, vmguard.Publication, error) {
	var noBirth vmguard.Birth
	var noRef budget.ObjectRef
	var noPublication vmguard.Publication
	if err := p.checkInitial(ctx, s); err != nil {
		return noBirth, noRef, noPublication, err
	}
	m := s.State()
	proof := m.RetainedCoverage.Record.ServiceCoverage.RetainedRecovery
	initial := *proof.Source.Initial
	if proof.NativeVM != nil {
		checked, err := p.runner.CheckCapacity(ctx, *proof.NativeVM, m.Owner)
		if err != nil {
			return noBirth, noRef, noPublication, err
		}
		witness, saved := checked.Receipt.Cancellation, proof.NativeVMCancellation
		if (witness == nil) != (saved == nil) || witness != nil && (witness.Owner != saved.Owner || witness.VMSequence != saved.VMSequence || witness.VMResourceVersion != saved.VMResourceVersion) {
			return noBirth, noRef, noPublication, fmt.Errorf("initial source collection changed original native cancellation witness")
		}
	} else if proof.InitialVM == nil {
		return noBirth, noRef, noPublication, fmt.Errorf("initial source collection lacks exclusive original native source")
	} else if _, err := p.runner.CheckInitialCapacity(ctx, *proof.InitialVM, initial, m.Owner); err != nil {
		return noBirth, noRef, noPublication, err
	}
	birth, ref, publication, err := p.runner.Inspect(ctx, &initial.RunnerBirth, m.Owner)
	if err != nil {
		return noBirth, noRef, noPublication, err
	}
	if publication.State.Pending != nil || publication.Actual.RequestedBytes != initial.VMMemoryBytes || publication.Actual.ActualBytes != initial.VMMemoryBytes {
		return noBirth, noRef, noPublication, fmt.Errorf("initial source collection changed original actual VM capacity")
	}
	return birth, ref, publication, nil
}

func (p *Publisher) initialSourceCollectionBoundary(ctx context.Context, s *journal.Snapshot) error {
	if err := p.unchanged(ctx, s); err != nil {
		return err
	}
	node, err := p.node.Load(ctx)
	if err != nil {
		return err
	}
	if _, err := p.node.Fence(ctx, node); err != nil {
		return err
	}
	// 重新认证原资格/Pod/Node/placement。其他VM的正常Node RV推进不
	// 阻塞收集；本VM发生新Begin、换UID或owner变化则原VM/Lease检查拒绝。
	return p.checkInitial(ctx, s)
}

func (p *Publisher) collectRetainedInitialSources(ctx context.Context, s *journal.Snapshot, progress *CollectionProgress) error {
	if s == nil || !s.Dispatchable() {
		return fmt.Errorf("initial source collection requires original running VM snapshot")
	}
	m := s.State()
	if m.ProtocolVersion < 22 || m.RetainedCoverage == nil {
		return nil
	}
	watermark, err := initialSourceWatermark(m)
	if err != nil {
		return err
	}
	birth, ref, publication, err := p.initialCollectionSources(ctx, s)
	if err != nil {
		return err
	}
	keep, err := retainedSourceNames(m, publication)
	if err != nil {
		return err
	}
	progress.prepareInitial(p.vm.Binding(), watermark)
	eligible := func(cm *core.ConfigMap) (string, bool) {
		return p.oldSourceThrough(cm, m, birth, ref, watermark.ThroughKey, watermark.ClosedSequence, keep, false)
	}
	collected := p.collectSourcePages(ctx, progress, 1, eligible, func(candidate collectionCandidate) error {
		return p.deleteCollectedSource(ctx, candidate, eligible, func() error {
			return p.initialSourceCollectionBoundary(ctx, s)
		})
	})
	// 一轮有界写后双读来源与原VM/Lease；未知不撤回已经删除的同原UID
	// 垃圾，也不改Host账本，后续保留游标继续认证尚未处理的候选。
	_, _, _, sourceErr := p.initialCollectionSources(ctx, s)
	return errors.Join(collected, sourceErr, p.unchanged(ctx, s))
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
	return p.collectSourcePages(ctx, progress, sourceKinds, func(cm *core.ConfigMap) (string, bool) {
		return p.oldSource(cm, m, birth, birthRef, closed, keep)
	}, func(candidate collectionCandidate) error {
		return p.deleteSettledSource(ctx, candidate, s, m, birth, birthRef, closed, keep)
	})
}

// 分页游标没有删除权；每页只保留小引用，每轮最多两页/16次写尝试。
// 原预算或关闭Through一变即丢弃游标；Initial传1仅发现Outside，普通传2。
func (p *Publisher) collectSourcePages(ctx context.Context, progress *CollectionProgress, kinds int, eligibleSource func(*core.ConfigMap) (string, bool), remove func(collectionCandidate) error) error {
	pages, writes := 0, 0
	var failures []error
	for {
		if len(progress.pending) == 0 && progress.end {
			progress.kind++
			progress.continuation, progress.end = "", false
		}
		if progress.kind == kinds {
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
				if f, eligible := eligibleSource(cm); eligible {
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
		if err := remove(expected); err != nil {
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
