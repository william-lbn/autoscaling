package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 复用实际Agent/Runner/私有QMP协议链，新增原域取消与消费验证；PG/Root/
// Prepare、平台资格与Kube仍为显式夹具。Shrink已有PG/Guest事实不被回滚，
// NoEffect只属于本条VM命令，不把它冒充整个操作清理或Host峰值释放。
func verifyVMCancelledBeforeSend(t *testing.T, ctx context.Context, api *ownerAPI, core *kubefake.Clientset, store *journal.Store, session *Session, worker *PGWorker, runner *vmguard.Runner, peer *vmQMPPeer, request control.Request, outside budget.ObjectRef, mode string) {
	t.Helper()
	if mode == "cancel-too-late" {
		verifyVMAlreadyCompletedCancellation(t, ctx, core, store, session, worker, runner, peer, request)
		return
	}
	if mode == "cancel-not-sent" {
		reads := 0
		core.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
			if action.(kt.GetAction).GetName() == outside.Name {
				reads++
				if reads == 3 {
					return true, nil, fmt.Errorf("fixture original source unavailable after durable prepared")
				}
			}
			return false, nil, nil
		})
		require.Error(t, runner.Tick(ctx))
		require.ErrorIs(t, runner.Tick(ctx), vmguard.ErrNotSent)
	}
	original, err := store.Load(ctx)
	require.NoError(t, err)
	before := original.State()
	vmStep := budgetStep(t, store, journal.ResizeVM)
	var command vmguard.Command
	require.NoError(t, json.Unmarshal(vmStep.Command, &command))
	node, err := core.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = store.RequestAbort(ctx, original, session.Actor(), request.Key)
	require.NoError(t, err)
	progress, err := worker.Advance(ctx)
	if mode == "cancel-not-sent" {
		require.ErrorIs(t, err, vmguard.ErrNotSent, "ordinary replayable not_sent is not a terminal cancellation")
	} else {
		require.ErrorIs(t, err, vmguard.ErrUnknown, "VM abort flag alone gives no NoEffect receipt")
	}
	require.Equal(t, pgexec.AwaitingFact, progress)
	require.Equal(t, vmStep, budgetStep(t, store, journal.ResizeVM))
	require.NoError(t, runner.Tick(ctx))
	reader, err := vmguard.NewReader(core.CoreV1(), binding(), string(session.Ownership().LeaseUID))
	require.NoError(t, err)
	proof, err := reader.ObserveAbortOriginal(ctx, command, vmStep.CommandSHA256, session.Actor())
	require.NoError(t, err)
	require.True(t, proof.Receipt.CancelledBeforeSend())
	require.False(t, proof.Receipt.Complete())
	require.False(t, proof.Receipt.NotSent())
	_, err = reader.ObserveOriginal(ctx, command, vmStep.CommandSHA256, session.Actor())
	require.ErrorIs(t, err, vmguard.ErrUnknown, "completed-only consumers cannot use a cancelled receipt")
	_, err = worker.ReplayVMOriginal(ctx, request.Key)
	require.Error(t, err, "VM/WAL cancellation must forbid explicit original replay")
	if mode == "cancel-receipt-gone" {
		require.NoError(t, core.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, proof.Sources.ReceiptClaim.Name))
	}
	if mode == "cancel-record-lost" {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	progress, err = worker.Advance(ctx)
	if mode == "cancel-receipt-gone" {
		require.Error(t, err)
		require.Equal(t, pgexec.AwaitingFact, progress)
		require.Equal(t, vmStep, budgetStep(t, store, journal.ResizeVM), "missing immutable cancellation source keeps original Unconfirmed")
	} else {
		if mode == "cancel-record-lost" {
			var unknown *journal.UnconfirmedWrite
			require.ErrorAs(t, err, &unknown)
			require.Equal(t, pgexec.AwaitingFact, progress)
		} else {
			require.NoError(t, err)
			require.Equal(t, pgexec.Recorded, progress)
		}
		settled := budgetStep(t, store, journal.ResizeVM)
		require.Equal(t, journal.Settled, settled.Phase)
		require.Equal(t, journal.NoEffect, settled.Observation.Result)
		require.Equal(t, vmStep.Command, settled.Command)
		require.Equal(t, vmStep.CommandSHA256, settled.CommandSHA256)
		var audit vmexec.CancelledVM
		require.NoError(t, json.Unmarshal(settled.Observation.Data, &audit))
		require.NoError(t, audit.ValidateFor(command, vmStep.CommandSHA256))
		require.Equal(t, proof.Sources, audit.Sources)
		bad := audit
		bad.Sources.ReceiptClaim.UID = "same-name-wrong-uid"
		// 纯结构校验不认证UID，实际Reader上面已经证明删原UID后不能落账；
		// 改原命令/摘要则无需网络即拒绝，不能把Validate当来源认证。
		bad.Sources.JournalSHA256 = "wrong-original-wal"
		require.Error(t, bad.ValidateFor(command, vmStep.CommandSHA256))
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.ExternalStep, progress, "VM domain closed; complete budget/admission AbortReconcile is still owed")
		require.Equal(t, settled, budgetStep(t, store, journal.ResizeVM), "retry must not replace original NoEffect audit")
	}
	latest, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest.State().Active)
	require.True(t, latest.State().Active.AbortRequested)
	require.Equal(t, before.BudgetHeld, latest.State().BudgetHeld)
	require.Equal(t, before.HighWater, latest.State().HighWater)
	for _, step := range before.Active.Steps {
		if step.Action != journal.ResizeVM {
			require.Equal(t, step, budgetStep(t, store, step.Action), "cancelling VM must preserve original PG/Guest/planned steps")
		}
	}
	_, err = store.Finish(ctx, latest, session.Actor(), request.Key)
	require.Error(t, err, "one VM NoEffect cannot close unconfirmed effects or omit complete abort reconciliation")
	currentNode, err := core.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, node.Annotations[hostguard.LedgerAnnotation], currentNode.Annotations[hostguard.LedgerAnnotation], "peak and initial capacity remain held")
	peer.mu.Lock()
	require.Zero(t, peer.writes)
	require.Equal(t, uint64(command.BeforeBytes), peer.actual)
	peer.mu.Unlock()
}

// 原QMP已完成而Agent还未落账，取消只能读回原完成并保留其真实效果。
// 这与VM未发送取消共用入口，专门证明不能为了关闭取消而伪造NoEffect。
func verifyVMAlreadyCompletedCancellation(t *testing.T, ctx context.Context, core *kubefake.Clientset, store *journal.Store, session *Session, worker *PGWorker, runner *vmguard.Runner, peer *vmQMPPeer, request control.Request) {
	t.Helper()
	original := budgetStep(t, store, journal.ResizeVM)
	node, err := core.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, runner.Tick(ctx))
	latest, err := store.Load(ctx)
	require.NoError(t, err)
	_, err = store.RequestAbort(ctx, latest, session.Actor(), request.Key)
	require.NoError(t, err)
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	settled := budgetStep(t, store, journal.ResizeVM)
	require.Equal(t, journal.Settled, settled.Phase)
	require.Equal(t, journal.Completed, settled.Observation.Result, "already completed mutation cannot be changed to NoEffect")
	require.Equal(t, original.Command, settled.Command)
	var actual budget.VMSettlement
	require.NoError(t, json.Unmarshal(settled.Observation.Data, &actual))
	require.Equal(t, request, actual.Request)
	require.Equal(t, request.TargetVMMemoryBytes, actual.ActualMemoryBytes)
	require.NoError(t, runner.Tick(ctx))
	latest, err = store.Load(ctx)
	require.NoError(t, err)
	require.True(t, latest.State().Active.AbortRequested)
	_, err = store.Finish(ctx, latest, session.Actor(), request.Key)
	require.Error(t, err, "original complete VM effect still requires full budget/admission reconciliation")
	currentNode, err := core.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, node.Annotations[hostguard.LedgerAnnotation], currentNode.Annotations[hostguard.LedgerAnnotation])
	peer.mu.Lock()
	require.Equal(t, 1, peer.writes)
	require.Equal(t, uint64(request.TargetVMMemoryBytes), peer.actual)
	peer.mu.Unlock()
}
