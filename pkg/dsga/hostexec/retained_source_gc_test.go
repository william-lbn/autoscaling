package hostexec

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 纯保留集合合同，不将刻意省略无关字段的Record冒充有效Model。当前
// slot、历史Proof、旧Source.Finalization里的raw字节必须分别显式解码。
func TestRetainedSourceNamesPreserveSingleSlotAcrossHistoryExpiryAndBudgetReplacement(t *testing.T) {
	raw, err := os.ReadFile("../admissionexec/testdata/reconciled-domain.json")
	require.NoError(t, err)
	var domain struct {
		Capacity budget.ReconciledCapacity `json:"capacity"`
	}
	require.NoError(t, json.Unmarshal(raw, &domain))
	step := func(name string) journal.Step {
		raw, err := json.Marshal(map[string]budget.ObjectRef{"original_source": {Name: name, UID: name + "-uid"}})
		require.NoError(t, err)
		var s journal.Step
		var audit journal.Observation
		s.Command, audit.Data = raw, raw
		s.Observation = &audit
		return s
	}
	record := func(prefix string) journal.Record {
		var r journal.Record
		r.Operation.Steps = []journal.Step{step(prefix + "-original-unknown")}
		cleanup := step(prefix + "-cleanup-unknown")
		r.Operation.Cleanup = &cleanup
		var coverage journal.CommittedServiceCoverage
		var proof journal.ServiceRetainedRecovery
		final := step(prefix + "-source-finalization")
		proof.Source.Finalization = &final
		vm := domain.Capacity.VM
		vm.Sources.ReceiptClaim = budget.ObjectRef{Name: prefix + "-native-receipt", UID: prefix + "-receipt-uid"}
		proof.NativeVM = &vm
		coverage.RetainedRecovery = &proof
		r.ServiceCoverage = &coverage
		return r
	}
	var m journal.Model
	var slot journal.RetainedServiceCoverage
	slot.Record = record("first")
	m.RetainedCoverage = &slot
	m.History = []journal.Record{record("history")}
	var publication vmguard.Publication
	refs, err := retainedSourceNames(m, publication)
	require.NoError(t, err)
	for _, name := range []string{"first-original-unknown", "first-cleanup-unknown", "first-source-finalization", "first-native-receipt", "history-original-unknown", "history-source-finalization", "history-native-receipt"} {
		require.Contains(t, refs, name)
	}
	m.History = nil
	var nextBudget journal.HeldBudget
	nextBudget.Finalization = step("new-normal-budget")
	m.BudgetHeld = &nextBudget
	refs, err = retainedSourceNames(m, publication)
	require.NoError(t, err)
	for _, name := range []string{"first-original-unknown", "first-cleanup-unknown", "first-source-finalization", "first-native-receipt", "new-normal-budget"} {
		require.Contains(t, refs, name, "预算替换不删除仍永久保留的独立原Unknown审计")
	}
	require.NotContains(t, refs, "history-native-receipt")
	old := slot.Record
	slot.Record = record("second")
	m.History = []journal.Record{old}
	refs, err = retainedSourceNames(m, publication)
	require.NoError(t, err)
	require.Contains(t, refs, "first-source-finalization", "覆盖slot后旧Proof暂由有界History保留")
	m.History = nil
	refs, err = retainedSourceNames(m, publication)
	require.NoError(t, err)
	require.NotContains(t, refs, "first-original-unknown")
	require.NotContains(t, refs, "first-source-finalization")
	require.Contains(t, refs, "second-original-unknown")
	require.Contains(t, refs, "new-normal-budget")
	slot.Record.ServiceCoverage.RetainedRecovery.Source.Finalization.Command = []byte(`{"broken":`)
	_, err = retainedSourceNames(m, publication)
	require.Error(t, err, "slot叶子raw损坏不能产生可删除集合")
}

func TestInitialSourceCandidatesUseRetiredKeyAndNeverCollectRunnerReceipts(t *testing.T) {
	for _, mode := range []string{"exact-retired-key", "earlier-key", "future-key", "same-position-new-id", "retained-name", "other-guest", "runner-receipt"} {
		t.Run(mode, func(t *testing.T) {
			p, m, birth, birthRef, closed, cm := collectionCandidateFixture(t)
			var out budget.OutsideReservation
			require.NoError(t, json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &out))
			out.Request.Key = closed.Request.Key
			keep := make(sourceReferences)
			switch mode {
			case "earlier-key":
				out.Request.Key.Revision--
			case "future-key":
				out.Request.Key.Revision++
			case "same-position-new-id":
				out.Request.Key.OperationID.Low++
			case "retained-name":
				keep[cm.Name] = struct{}{}
			case "other-guest":
				out.Identity.GuestBootID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
			case "runner-receipt":
				cm.Labels = map[string]string{vmguard.ClaimLabel: vmguard.ReceiptKind}
			}
			raw, err := json.Marshal(out)
			require.NoError(t, err)
			cm.Data[budgetclaims.ClaimKey] = string(raw)
			_, eligible := p.oldSourceThrough(cm, m, birth, birthRef, closed.Request.Key, 42, keep, false)
			require.Equal(t, mode == "exact-retired-key" || mode == "earlier-key", eligible)
		})
	}
}

// 真实Core client/fake对象存储执行生产删除函数。CAS由peer明确实现，
// 每个PATCH/DELETE核原UID/RV，丢回复保留已落库状态；不伪造删除成功。
func TestSourceCollectionDeleteKeepsExactUIDDigestAndUnknownReplies(t *testing.T) {
	for _, mode := range []string{"success", "replacement-uid", "changed-body", "changed-boundary", "lost-patch-reply", "lost-delete-reply"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			p, m, birth, birthRef, closed, cm := collectionCandidateFixture(t)
			cm.Finalizers = []string{budgetclaims.ProtectionFinalizer}
			client := kubefake.NewSimpleClientset(cm)
			p.core = client.CoreV1()
			candidate := collectionCandidate{Object: budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)}, Finalizer: budgetclaims.ProtectionFinalizer, SHA256: vmguard.SourceDigest(sourceData(cm, budgetclaims.ProtectionFinalizer))}
			lostPatch, lostDelete, writes := mode == "lost-patch-reply", mode == "lost-delete-reply", 0
			client.PrependReactor("patch", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
				patchAction := action.(kt.PatchAction)
				stored, err := client.Tracker().Get(core.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name)
				require.NoError(t, err)
				original, err := json.Marshal(stored)
				require.NoError(t, err)
				operations, err := jsonpatch.DecodePatch(patchAction.GetPatch())
				require.NoError(t, err)
				next, err := operations.Apply(original)
				require.NoError(t, err, "生产PATCH必须包含有效原UID/RV test")
				var changed core.ConfigMap
				require.NoError(t, json.Unmarshal(next, &changed))
				revision, err := strconv.Atoi(changed.ResourceVersion)
				require.NoError(t, err)
				changed.ResourceVersion = strconv.Itoa(revision + 1)
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), &changed, cm.Namespace))
				writes++
				if lostPatch {
					lostPatch = false
					return true, nil, errors.New("explicit PATCH committed but reply lost")
				}
				return true, changed.DeepCopy(), nil
			})
			client.PrependReactor("delete", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
				stored, err := client.Tracker().Get(core.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name)
				require.NoError(t, err)
				current := stored.(*core.ConfigMap)
				options := action.(kt.DeleteAction).GetDeleteOptions()
				require.NotNil(t, options.Preconditions)
				require.Equal(t, current.UID, *options.Preconditions.UID)
				require.Equal(t, current.ResourceVersion, *options.Preconditions.ResourceVersion)
				require.Empty(t, current.Finalizers)
				require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name))
				writes++
				if lostDelete {
					lostDelete = false
					return true, nil, errors.New("explicit DELETE committed but reply lost")
				}
				return true, nil, nil
			})
			if mode == "replacement-uid" || mode == "changed-body" {
				changed := cm.DeepCopy()
				if mode == "replacement-uid" {
					changed.UID += "-replacement"
				} else {
					var outside budget.OutsideReservation
					require.NoError(t, json.Unmarshal([]byte(changed.Data[budgetclaims.ClaimKey]), &outside))
					outside.ResidualProfileID += "-same-capacity"
					raw, err := json.Marshal(outside)
					require.NoError(t, err)
					changed.Data[budgetclaims.ClaimKey] = string(raw)
				}
				require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, cm.Namespace))
			}
			eligible := func(current *core.ConfigMap) (string, bool) {
				return p.oldSourceThrough(current, m, birth, birthRef, closed.Request.Key, 42, make(sourceReferences), false)
			}
			boundary := func() error {
				if mode == "changed-boundary" {
					return errors.New("explicit original VM RV changed")
				}
				return nil
			}
			err := p.deleteCollectedSource(ctx, candidate, eligible, boundary)
			if mode == "success" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if strings.HasPrefix(mode, "lost-") {
				require.NoError(t, p.deleteCollectedSource(ctx, candidate, eligible, boundary), "未知回复只按原UID重读恢复")
			}
			_, err = client.CoreV1().ConfigMaps(cm.Namespace).Get(ctx, cm.Name, metav1.GetOptions{})
			if mode == "success" || strings.HasPrefix(mode, "lost-") {
				require.True(t, apierrors.IsNotFound(err))
				require.Equal(t, 2, writes)
			} else {
				require.NoError(t, err)
				require.Zero(t, writes)
			}
		})
	}
}

func TestInitialCollectionProgressIsIndependentAndRestartsAfterBoundedScan(t *testing.T) {
	p, _, _, _, closed, _ := collectionCandidateFixture(t)
	progress := NewCollectionProgress()
	mark := initialCollectionWatermark{SourceSHA256: strings.Repeat("a", 64), RecordedSequence: 3, ThroughKey: closed.Request.Key, ClosedSequence: 42}
	progress.prepareInitial(p.vm.Binding(), mark)
	progress.continuation, progress.kind = "outside-tail", 0
	progress.pending = []collectionCandidate{{Object: budget.ObjectRef{Name: "late-original", UID: "late-original-uid"}, Finalizer: budgetclaims.ProtectionFinalizer, SHA256: strings.Repeat("b", 64)}}
	progress.prepareInitial(p.vm.Binding(), mark)
	require.Equal(t, "outside-tail", progress.continuation)
	require.Len(t, progress.pending, 1)
	mark.ClosedSequence++
	progress.prepareInitial(p.vm.Binding(), mark)
	require.Empty(t, progress.continuation)
	require.Empty(t, progress.pending)
	progress.kind = 1
	progress.prepareInitial(p.vm.Binding(), mark)
	require.Zero(t, progress.kind, "完整扫完后仍重启发现，允许迟到CREATE下轮被精确发现")
	// PG17-U-P1-011：带屏障的三段发现中，receipt段不是完成，不能
	// 丢分页尾游标；真正扫完Host段后必须重启，以发现更晚的CREATE。
	mark.HostSources = true
	progress.prepareInitial(p.vm.Binding(), mark)
	progress.kind, progress.continuation = 1, "intermediate-receipt-page"
	progress.prepareInitial(p.vm.Binding(), mark)
	require.Equal(t, 1, progress.kind)
	require.Equal(t, "intermediate-receipt-page", progress.continuation)
	progress.kind = 3
	progress.prepareInitial(p.vm.Binding(), mark)
	require.Zero(t, progress.kind)
	require.Empty(t, progress.continuation)
	progress.prepare(p.vm.Binding(), closed)
	var noInitial initialCollectionWatermark
	require.Equal(t, noInitial, progress.initial, "普通预算使用自己的Settlement游标")
}

func TestInitialSourceCollectionPagesYieldResumeAndDiscoverLateOutside(t *testing.T) {
	ctx := context.Background()
	p, m, birth, birthRef, closed, original := collectionCandidateFixture(t)
	client := kubefake.NewSimpleClientset()
	p.core = client.CoreV1()
	for i := 0; i < sourceMaxWrites+4; i++ {
		cm := original.DeepCopy()
		cm.Name, cm.UID = "late-outside-"+strconv.Itoa(i), original.UID+"-"+types.UID(strconv.Itoa(i))
		require.NoError(t, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
	}
	receipt := original.DeepCopy()
	receipt.Name, receipt.Labels = "must-keep-runner", map[string]string{vmguard.ClaimLabel: vmguard.ReceiptKind}
	require.NoError(t, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), receipt, receipt.Namespace))
	progress := NewCollectionProgress()
	mark := initialCollectionWatermark{SourceSHA256: strings.Repeat("a", 64), RecordedSequence: 3, ThroughKey: closed.Request.Key, ClosedSequence: 42}
	eligible := func(cm *core.ConfigMap) (string, bool) {
		return p.oldSourceThrough(cm, m, birth, birthRef, mark.ThroughKey, mark.ClosedSequence, make(sourceReferences), false)
	}
	removed := 0
	remove := func(candidate collectionCandidate) error {
		removed++
		return client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), original.Namespace, candidate.Object.Name)
	}
	progress.prepareInitial(p.vm.Binding(), mark)
	err := p.collectSourcePages(ctx, progress, 1, eligible, remove)
	require.ErrorIs(t, err, ErrCollectionPending)
	require.Equal(t, sourceMaxWrites, removed)
	require.Len(t, progress.pending, 4)
	progress.prepareInitial(p.vm.Binding(), mark)
	require.NoError(t, p.collectSourcePages(ctx, progress, 1, eligible, remove))
	require.Equal(t, sourceMaxWrites+4, removed)
	late := original.DeepCopy()
	late.Name, late.UID = "created-after-complete-scan", "late-after-scan-uid"
	require.NoError(t, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), late, late.Namespace))
	progress.prepareInitial(p.vm.Binding(), mark)
	require.NoError(t, p.collectSourcePages(ctx, progress, 1, eligible, remove))
	require.Equal(t, sourceMaxWrites+5, removed, "关闭很久后到达的CREATE仍由下轮有界重新发现")
	_, err = client.CoreV1().ConfigMaps(receipt.Namespace).Get(ctx, receipt.Name, metav1.GetOptions{})
	require.NoError(t, err, "Initial-only收集不触Runner回执")
	for _, action := range client.Actions() {
		if action.GetVerb() == "list" {
			selector := action.(kt.ListAction).GetListRestrictions().Labels.String()
			require.Equal(t, budgetclaims.ClaimLabel+"="+budgetclaims.OutsideKind, selector)
		}
	}
}
