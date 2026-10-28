package owner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	kt "k8s.io/client-go/testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 仅LIST分页是明确夹具：模拟apiserver的同一快照continue，而非从不断
// 删除的tracker按offset翻页（后者会漏项）。GET/PATCH/DELETE及真实
// Publisher/Node/VM/Lease验证仍走原组合。假条目用于批量GC，不代表
// 已执行过对应的物理Resize；当前预算与Runner来自本次实际生产执行链。
type collectionPages struct {
	mu    sync.Mutex
	pages map[string]*core.ConfigMapList
	reads []string
}

type collectionCore struct {
	coreclient.CoreV1Interface
	pages *collectionPages
}

func (c *collectionCore) ConfigMaps(ns string) coreclient.ConfigMapInterface {
	return &collectionMaps{ConfigMapInterface: c.CoreV1Interface.ConfigMaps(ns), pages: c.pages}
}

type collectionMaps struct {
	coreclient.ConfigMapInterface
	pages *collectionPages
}

func (c *collectionMaps) List(ctx context.Context, opts metav1.ListOptions) (*core.ConfigMapList, error) {
	key := opts.LabelSelector + "/" + opts.Continue
	c.pages.mu.Lock()
	page, ok := c.pages.pages[key]
	if ok {
		c.pages.reads = append(c.pages.reads, key)
	}
	c.pages.mu.Unlock()
	if !ok {
		return c.ConfigMapInterface.List(ctx, opts)
	}
	if opts.Limit != 64 {
		return nil, fmt.Errorf("fixture requires bounded source LIST")
	}
	return page.DeepCopy(), nil
}

func verifyClosedSourceCollection(t *testing.T, ctx context.Context, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, allocator *hostguard.Store, s *journal.Snapshot) {
	t.Helper()
	m := s.State()
	require.NotNil(t, m.BudgetHeld)
	var finalized budget.Finalized
	require.NoError(t, json.Unmarshal(m.BudgetHeld.Finalization.Observation.Data, &finalized))
	refs := finalized.Sources
	require.NotNil(t, refs)
	require.NotNil(t, refs.Runner)
	outCM, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, refs.OutsideClaim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var out budget.OutsideReservation
	require.NoError(t, json.Unmarshal([]byte(outCM.Data[budgetclaims.ClaimKey]), &out))
	require.Greater(t, out.Request.Key.Revision, control.Uint64(1))
	receiptCM, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, refs.Runner.ReceiptClaim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var receipt vmguard.Receipt
	require.NoError(t, json.Unmarshal([]byte(receiptCM.Data[vmguard.ClaimKey]), &receipt))
	outsideSelector := budgetclaims.ClaimLabel + "=" + budgetclaims.OutsideKind
	runnerSelector := vmguard.ClaimLabel + "=" + vmguard.ReceiptKind
	first, tail, results := &core.ConfigMapList{}, &core.ConfigMapList{}, &core.ConfigMapList{}
	first.Continue = "original-outside-tail"
	// 无可回收项的完整首64条，证明有界扫描不会每次从第一页重来。
	for i := 0; i < 64; i++ {
		foreign := outCM.DeepCopy()
		foreign.Name, foreign.UID = fmt.Sprintf("foreign-%02d", i), types.UID(fmt.Sprintf("foreign-%02d-uid", i))
		foreign.OwnerReferences[0].UID = "another-vm-uid"
		first.Items = append(first.Items, *foreign)
	}
	var candidates []*core.ConfigMap
	for i := 0; i < 20; i++ {
		old := out
		old.Request.Key.Revision--
		old.Request.Key.OperationID.Low += control.Uint64(100 + i)
		raw, err := json.Marshal(old)
		require.NoError(t, err)
		cm := outCM.DeepCopy()
		cm.Name, cm.UID, cm.ResourceVersion = fmt.Sprintf("dsga-old-outside-%02d", i), types.UID(fmt.Sprintf("old-outside-%02d-uid", i)), "old-source-rv"
		cm.Data = map[string]string{budgetclaims.ClaimKey: string(raw)}
		if i == 3 {
			cm.Finalizers = append(cm.Finalizers, "another.neon.tech/protection")
		}
		require.NoError(t, client.Tracker().Add(cm))
		candidates = append(candidates, cm)
		tail.Items = append(tail.Items, *cm.DeepCopy())
	}
	tail.Items = append(tail.Items, *outCM.DeepCopy())
	for i := 0; i < 3; i++ {
		old := receipt
		old.Intent.Command.Request.Key.Revision--
		old.Intent.Command.Request.Key.OperationID.Low += control.Uint64(200 + i)
		raw, err := json.Marshal(old.Intent.Command)
		require.NoError(t, err)
		hash := sha256.Sum256(append([]byte("neon-dsga-runner-memory-v1\x00"), raw...))
		old.Intent.CommandSHA256 = hex.EncodeToString(hash[:])
		require.NoError(t, old.Validate(), "批量回执先证明领域自洽，不能只测坏JSON被跳过")
		raw, err = json.Marshal(old)
		require.NoError(t, err)
		cm := receiptCM.DeepCopy()
		cm.Name, cm.UID, cm.ResourceVersion = vmguard.ReceiptSourceName(raw), types.UID(fmt.Sprintf("old-receipt-%d-uid", i)), "old-receipt-rv"
		cm.Data = map[string]string{vmguard.ClaimKey: string(raw)}
		require.NoError(t, client.Tracker().Add(cm))
		candidates = append(candidates, cm)
		results.Items = append(results.Items, *cm.DeepCopy())
	}
	results.Items = append(results.Items, *receiptCM.DeepCopy())
	pages := &collectionPages{mu: sync.Mutex{}, pages: map[string]*core.ConfigMapList{outsideSelector + "/": first, outsideSelector + "/original-outside-tail": tail, runnerSelector + "/": results}, reads: nil}
	wrapped := &collectionCore{CoreV1Interface: client.CoreV1(), pages: pages}
	var patchLost, deleteLost, replacement atomic.Bool
	client.PrependReactor("get", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
		if a.(kt.GetAction).GetName() != candidates[2].Name || !replacement.CompareAndSwap(false, true) {
			return false, nil, nil
		}
		changed := candidates[2].DeepCopy()
		changed.UID = "same-name-replacement-uid"
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
		return true, changed, nil
	})
	client.PrependReactor("patch", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
		if a.(kt.PatchAction).GetName() != candidates[0].Name || !patchLost.CompareAndSwap(false, true) {
			return false, nil, nil
		}
		// 明确模拟原JSONPatch已经提交、回复丢失。下一轮只读原
		// UID并继续DELETE，不补发任何业务命令或创建新来源。
		changed := candidates[0].DeepCopy()
		changed.Finalizers = nil
		changed.ResourceVersion = "protection-removed-reply-lost"
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
		return true, nil, errors.New("fixture source protection reply lost after commit")
	})
	client.PrependReactor("delete", "configmaps", func(a kt.Action) (bool, runtime.Object, error) {
		request := a.(kt.DeleteAction)
		if request.GetName() == candidates[1].Name && deleteLost.CompareAndSwap(false, true) {
			require.NoError(t, client.Tracker().Delete(core.SchemeGroupVersion.WithResource("configmaps"), a.GetNamespace(), request.GetName()))
			return true, nil, errors.New("fixture source DELETE reply lost after commit")
		}
		if request.GetName() == candidates[3].Name {
			obj, err := client.Tracker().Get(core.SchemeGroupVersion.WithResource("configmaps"), a.GetNamespace(), request.GetName())
			require.NoError(t, err)
			cm := obj.(*core.ConfigMap).DeepCopy()
			require.Equal(t, []string{"another.neon.tech/protection"}, cm.Finalizers)
			options := request.GetDeleteOptions()
			require.NotNil(t, options.Preconditions)
			require.Equal(t, cm.UID, *options.Preconditions.UID)
			require.Equal(t, cm.ResourceVersion, *options.Preconditions.ResourceVersion)
			stamp := metav1.Now()
			cm.DeletionTimestamp = &stamp
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
			return true, nil, nil
		}
		return false, nil, nil
	})
	before, err := allocator.Load(ctx)
	require.NoError(t, err)
	progress := hostexec.NewCollectionProgress()
	complete, rounds := false, 0
	for rounds < 12 {
		rounds++
		// 每轮真实重建Publisher，仅分页进度复用。源认证和原Session/
		// Lease不来自游标；这对应Scheduler每轮重建生产者的真实行为。
		publisher, err := hostexec.New(wrapped, coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
		require.NoError(t, err)
		changed, err := publisher.SettleCurrentWithCollection(ctx, progress)
		require.False(t, changed, "GC不能改变已结清的Node承诺")
		if rounds == 1 {
			require.ErrorContains(t, err, "source protection reply lost")
			require.ErrorContains(t, err, "source DELETE reply lost")
			require.ErrorContains(t, err, "source changed during collection")
			got, getErr := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, candidates[2].Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			require.EqualValues(t, "same-name-replacement-uid", got.UID)
			require.Contains(t, got.Finalizers, budgetclaims.ProtectionFinalizer)
			// 只在测试中恢复准确原对象，不能让生产清理收养替代UID。
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), candidates[2], binding().Namespace))
		} else if err == nil {
			complete = true
			break
		} else {
			require.ErrorIs(t, err, hostexec.ErrCollectionPending)
		}
	}
	require.True(t, complete, "有界游标必须最终扫到尾部并重试原失败")
	require.Greater(t, rounds, 1)
	require.True(t, patchLost.Load())
	require.True(t, deleteLost.Load())
	require.True(t, replacement.Load())
	for i, cm := range candidates {
		got, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, cm.Name, metav1.GetOptions{})
		if i == 3 {
			require.NoError(t, err)
			require.NotNil(t, got.DeletionTimestamp)
			require.Equal(t, []string{"another.neon.tech/protection"}, got.Finalizers)
		} else {
			require.True(t, apierrors.IsNotFound(err), cm.Name)
		}
	}
	for _, ref := range []budget.ObjectRef{refs.OutsideClaim, refs.HostClaim, refs.Runner.BirthClaim, refs.Runner.StateClaim, refs.Runner.ReceiptClaim, out.Qualification.Claim} {
		got, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.EqualValues(t, ref.UID, got.UID)
		require.Nil(t, got.DeletionTimestamp)
	}
	after, err := allocator.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.State(), after.State())
	latest, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, s.ResourceVersion(), latest.ResourceVersion(), "GC不改业务WAL")
	claims, err := budgetclaims.New(client.CoreV1(), store.Binding())
	require.NoError(t, err)
	_, err = claims.ReadCurrent(ctx, latest)
	require.NoError(t, err)
}
