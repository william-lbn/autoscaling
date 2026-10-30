package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	kt "k8s.io/client-go/testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 同一次真实Agent/Runner缩容产生原BudgetHeld，再执行新Grow的未发送取消。
// 不手填原最终预算/取消清理/关闭水位；此前PG/Root内核、Prepare及平台资格
// 继承原组合的显式协议夹具。真实物理动作只有第一次缩容的QMP协议set一次。
func verifyUnchangedBudgetAbort(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, session *Session, worker *PGWorker, resources *ResourceWorker, config compute.Config, client *kubefake.Clientset, coord coordclient.CoordinationV1Interface, runner *vmguard.Runner, peer *vmQMPPeer, root *guestRootFixture, pg *committedAbortPG, shared control.SharedCostResponse, mode string) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	priorRequest := s.State().Active.Request
	var p budget.Prepared
	require.NoError(t, json.Unmarshal(s.State().Active.Steps[0].Observation.Data, &p))
	s, err = store.Finish(ctx, s, session.Actor(), priorRequest.Key)
	require.NoError(t, err)
	held := s.State().BudgetHeld
	f, err := s.State().BudgetFor(priorRequest)
	require.NoError(t, err)
	node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var initial hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &initial))
	allocator, err := hostguard.NewStore(client.CoreV1(), client.CoordinationV1(), initial.Binding, "host-fixture-holder")
	require.NoError(t, err)
	stopHostRenewal := keepUnchangedHostLease(t, ctx, client, initial.Binding)
	defer stopHostRenewal()
	publisher, err := hostexec.New(client.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	changed, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)

	req := priorRequest
	req.Kind, req.TargetBuffers, req.TargetVMMemoryBytes = control.Grow, 1024, 2<<30
	req.Key.Revision++
	req.Key.OperationID.Low++
	req.ExpectedCapacityGeneration = f.Shared.Observed.CapacityGeneration
	req.BudgetCommitmentID += "-cancelled-grow"
	p.Request = req
	p.Shared = pg.shared(shared)
	p.VMBeforeMemoryBytes = priorRequest.TargetVMMemoryBytes
	p.GuestUsableMemoryBytes = req.TargetVMMemoryBytes - (256 << 20)
	p.TargetSharedUpperBytes, err = budget.GrowSharedUpper(p.Shared, req.TargetBuffers)
	require.NoError(t, err)
	outside := budget.OutsideReservation{ProtocolVersion: 1, Request: req, Identity: p.Identity, Incarnation: binding().Incarnation, ResidualProfileID: "explicit-qualified-platform-fixture", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true, VMShrinkPermit: false}
	outsideRef := vmReservation(t, client, "outside-cancelled-grow", budgetclaims.OutsideKind, outside)
	p.OutsideClaim = &outsideRef
	require.NoError(t, p.ValidateFor(req, p.Identity))
	s, err = store.Load(ctx)
	require.NoError(t, err)
	priorBoundary := *s.State().BudgetBoundary
	_, _, err = store.Begin(ctx, s, session.Actor(), req, 0)
	require.NoError(t, err)
	priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.PrepareBudget, req.Key.Instance.ComputeInstanceID, p)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, &priorBoundary, s.State().Active.PriorBudgetBoundary)
	require.Nil(t, s.State().BudgetBoundary)
	require.EqualValues(t, 7, s.State().ProtocolVersion)
	_, err = publisher.Reconcile(ctx)
	require.NoError(t, err)
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress, "consume new Host reservation")
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress, "new VM write-ahead only; Runner has not sent")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	_, err = store.RequestAbort(ctx, s, session.Actor(), req.Key)
	require.NoError(t, err)
	require.NoError(t, runner.Tick(ctx))
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	vm := budgetStep(t, store, journal.ResizeVM)
	require.Equal(t, journal.NoEffect, vm.Observation.Result)
	var native vmguard.Command
	var cancellation vmexec.CancelledVM
	require.NoError(t, json.Unmarshal(vm.Command, &native))
	require.NoError(t, json.Unmarshal(vm.Observation.Data, &cancellation))
	reader, err := vmguard.NewReader(client.CoreV1(), binding(), string(session.Ownership().LeaseUID))
	require.NoError(t, err)
	var final budget.FinalizationCommand
	require.NoError(t, json.Unmarshal(held.Finalization.Command, &final))
	_, err = reader.CheckRetained(ctx, *f.Sources.Runner, priorRequest, final.PreparedSHA256, final.GuestSHA256, f.Sources.HostClaim, session.Actor())
	require.Error(t, err, "正常读者不能默默接受已被新取消替换的Last")
	joint, err := reader.CheckRetainedAfterCancelled(ctx, *f.Sources.Runner, priorRequest, final.PreparedSHA256, final.GuestSHA256, f.Sources.HostClaim, native, vm.CommandSHA256, session.Actor())
	require.NoError(t, err)
	require.Equal(t, cancellation.Sources, joint.Cancellation.Sources)
	require.True(t, joint.Prior.Receipt.Complete())
	require.True(t, joint.Cancellation.Receipt.CancelledBeforeSend())

	if mode == "restore-prior-negative" {
		var bad atomic.Pointer[string]
		client.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
			name := action.(kt.GetAction).GetName()
			selected := bad.Load()
			if selected == nil || name != *selected {
				return false, nil, nil
			}
			obj, err := client.Tracker().Get(core.SchemeGroupVersion.WithResource("configmaps"), binding().Namespace, name)
			if err != nil {
				return true, nil, err
			}
			cm := obj.(*core.ConfigMap).DeepCopy()
			cm.UID = "same-name-replacement-uid"
			return true, cm, nil
		})
		for _, name := range []string{f.Sources.Runner.ReceiptClaim.Name, cancellation.Sources.ReceiptClaim.Name, f.Sources.OutsideClaim.Name} {
			selected := name
			bad.Store(&selected)
			_, err = worker.Advance(ctx)
			require.Error(t, err, "同数值/同名但非原UID不得恢复预算")
			s, err = store.Load(ctx)
			require.NoError(t, err)
			require.Nil(t, s.State().Active.Cleanup)
			require.Equal(t, held, s.State().BudgetHeld)
		}
		bad.Store(nil)
		peer.mu.Lock()
		peer.actual = uint64(priorRequest.TargetVMMemoryBytes) + (256 << 20)
		peer.mu.Unlock()
		require.NoError(t, runner.Tick(ctx))
		_, err = worker.Advance(ctx)
		require.Error(t, err, "VM status仍旧时，当前QMP偏离也必须阻断旧预算恢复")
		peer.mu.Lock()
		peer.actual = uint64(priorRequest.TargetVMMemoryBytes)
		peer.mu.Unlock()
		require.NoError(t, runner.Tick(ctx))
	}

	if strings.HasPrefix(mode, "restore-prior-safety-") {
		readonly, settled := strings.Contains(mode, "readonly"), strings.HasSuffix(mode, "-settled")
		if readonly && !settled {
			api.mu.Lock()
			api.loseJournalReply = true
			api.mu.Unlock()
		}
		if readonly {
			_, err = worker.Advance(ctx)
		} else {
			pg.mu.Lock()
			pg.closedOverride, pg.allowNext, pg.loseAfter = true, true, !settled
			pg.mu.Unlock()
			_, err = worker.RecoverUnchangedAdmission(ctx, req.Key)
		}
		if settled {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
		verifyCapacityCleanupSourceFailure(t, ctx, store, session, resources, worker, config, client, root, pg, shared, priorRequest.BudgetCommitmentID, readonly, settled)
		if strings.Contains(mode, "-recover-") {
			verifyActualOrUnchangedCleanupRecovery(t, ctx, api, client, coord, store, session, resources, root, pg, shared)
		}
		peer.mu.Lock()
		require.Equal(t, 1, peer.writes, "取消未发送，安全关闭不增加VM反向写")
		peer.mu.Unlock()
		return
	}
	losses := mode == "restore-prior-replies"
	if losses {
		pg.mu.Lock()
		pg.closedOverride, pg.allowNext, pg.loseBefore, pg.loseAfter = true, true, true, true
		pg.mu.Unlock()
		_, err = worker.Advance(ctx)
		require.ErrorContains(t, err, "explicit trusted recovery selection", "普通取消不能覆盖外部关闭意图")
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Nil(t, s.State().Active.Cleanup)
		wrong := req.Key
		wrong.OperationID.Low++
		_, err = worker.RecoverUnchangedAdmission(ctx, wrong)
		require.Error(t, err)
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	if losses {
		selections := make(chan control.Key, 1)
		selections <- req.Key
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		err = worker.Run(loopCtx, PGLoopOptions{PollInterval: time.Second, Wake: nil, Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: selections, Observe: func(tick Tick) {
			var unknown *journal.UnconfirmedWrite
			if errors.As(tick.Err, &unknown) {
				stop()
			}
		}})
		require.ErrorIs(t, err, context.Canceled, "真实循环消费独立授权；Start回复未知后只重读")
	} else {
		_, err = worker.Advance(ctx)
	}
	if losses {
		require.Error(t, err, "Cleanup Start已落盘但回复丢失")
		rebuilt, rebuildErr := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
		require.NoError(t, rebuildErr)
		defer rebuilt.CloseIdleConnections()
		worker = rebuilt
		pending, _ := worker.client.PendingAdmission()
		require.NotNil(t, pending)
		require.Equal(t, priorRequest.BudgetCommitmentID, pending.BudgetCommitmentID)
		_, err = worker.Advance(ctx)
		require.NoError(t, err, "not-found只等待，不补发")
		_, err = worker.ReplayCommittedAbortOriginal(ctx, req.Key)
		require.Error(t, err, "setter执行前丢回复")
		_, err = worker.Advance(ctx)
		require.NoError(t, err)
		_, err = worker.ReplayCommittedAbortOriginal(ctx, req.Key)
		require.Error(t, err, "setter执行后丢回复")
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
		_, err = worker.Advance(ctx)
		require.Error(t, err, "Record已持久但回复丢失")
	} else {
		require.NoError(t, err)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.NoError(t, s.State().CheckUnchangedAbort())
	require.Equal(t, held, s.State().BudgetHeld)
	if losses {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	_, err = worker.Advance(ctx)
	if losses {
		require.Error(t, err, "Finish已持久但回复丢失")
	} else {
		require.NoError(t, err)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.Equal(t, journal.AbortedAfterReconcile, s.State().History[len(s.State().History)-1].Outcome)
	require.Equal(t, held, s.State().BudgetHeld)
	require.NoError(t, s.State().CheckClosedBudget(priorRequest))
	require.Equal(t, req.Key, s.State().BudgetBoundary.ThroughKey)
	if losses {
		s = expireUnchangedAbortHistory(t, ctx, api, store, session, worker, s)
	}
	currentVM, err := reader.CheckCurrentBudget(ctx, s.State())
	require.NoError(t, err)
	require.NotNil(t, currentVM.Cancellation, "当前预算读者必须保留新取消与旧完成的区别")
	n, err := allocator.Load(ctx)
	require.NoError(t, err)
	before, err := n.State().UsedBytes()
	require.NoError(t, err)
	changed, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	n, err = allocator.Load(ctx)
	require.NoError(t, err)
	after, err := n.State().UsedBytes()
	require.NoError(t, err)
	require.Less(t, after, before)
	row, err := n.State().Placement(native.Identity.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, priorRequest.TargetVMMemoryBytes, row.MemoryBytes)
	require.Equal(t, s.State().BudgetBoundary.ThroughKey, row.Settled.ClosedThrough())
	_, _, err = allocator.Reserve(ctx, n, hostguard.Plan{Namespace: binding().Namespace, VMName: binding().Name, Incarnation: binding().Incarnation, RunnerPodUID: native.Identity.RunnerPodUID, Request: req, BeforeBytes: native.BeforeBytes, HostOverheadBytes: 64 << 20})
	require.Error(t, err)
	_, err = client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, native.HostClaim.Name, metav1.GetOptions{})
	require.Error(t, err, "已失执行资格的新Host来源可清理，旧预算来源仍保存")
	peer.mu.Lock()
	require.Equal(t, 1, peer.writes)
	require.Equal(t, uint64(priorRequest.TargetVMMemoryBytes), peer.actual)
	peer.mu.Unlock()
	root.mu.Lock()
	require.Equal(t, 1, root.posts, "新取消不能发GuestReady改变原限额")
	root.mu.Unlock()
	pg.mu.Lock()
	want := 1
	if losses {
		want = 3
	}
	require.Equal(t, want, pg.posts, "只初次开放及受信显式重放，不发新目标PG resize")
	pg.mu.Unlock()
}

// 用真实codec字节限制淘汰旧成功和取消历史，再做两次真实零派发取消。永久
// 原预算/关闭链和Runner不可变来源仍须支持Host结清，不能借短期结果缓存。
func expireUnchangedAbortHistory(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, session *Session, worker *PGWorker, s *journal.Snapshot) *journal.Snapshot {
	t.Helper()
	m := s.State()
	request := m.History[len(m.History)-1].Operation.Request
	cancelledRevision := request.Key.Revision
	paddingKey := "test-only.example/unchanged-budget-annotation-room"
	var err error
	for round := range 2 {
		annotations := s.VirtualMachine().Annotations
		room := 256*1024 - len(control.JournalAnnotation)
		for key, value := range annotations {
			if key != control.JournalAnnotation && key != paddingKey {
				room -= len(key) + len(value)
			}
		}
		// 这里是实际压缩envelope长度；Active预留4096字节。每轮把
		// 净额度收紧到只差1字节，让真实codec淘汰最早终态，不写假历史。
		padding := room - len(paddingKey) - (len(annotations[control.JournalAnnotation]) + 4095)
		require.Positive(t, padding)
		api.mu.Lock()
		api.vm.Annotations[paddingKey] = strings.Repeat("x", padding)
		api.vmVersion++
		api.vm.ResourceVersion = fmt.Sprintf("vm-rv-%d", api.vmVersion)
		api.mu.Unlock()
		s, err = store.Load(ctx)
		require.NoError(t, err)
		request.Key.Revision = s.State().HighWater.Revision + 1
		request.Key.OperationID.Low++
		request.BudgetCommitmentID = fmt.Sprintf("unchanged-unstarted-%d", round)
		s, _, err = store.Begin(ctx, s, session.Actor(), request, 0)
		require.NoError(t, err)
		_, err = store.RequestAbort(ctx, s, session.Actor(), request.Key)
		require.NoError(t, err)
		_, err = worker.Advance(ctx)
		require.NoError(t, err)
		s, err = store.Load(ctx)
		require.NoError(t, err)
	}
	require.NotZero(t, s.State().ExpiredResults)
	for _, record := range s.State().History {
		require.Greater(t, record.Operation.Request.Key.Revision, cancelledRevision, "先前原预算/VM取消结果已真正淘汰")
	}
	return s
}

// 真实Scheduler会持续续约原节点holder。本组合跨两次操作和多次故障，不能
// 沿用只创建60秒Lease却没有续约循环的短用例夹具；仍由生产Store检查过期。
func keepUnchangedHostLease(t *testing.T, parent context.Context, client *kubefake.Clientset, binding hostguard.Binding) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	renewalErrors := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lease, err := client.CoordinationV1().Leases(binding.Namespace).Get(ctx, binding.LeaseName, metav1.GetOptions{})
				if err == nil {
					now := metav1.NewMicroTime(time.Now())
					lease.Spec.RenewTime = &now
					_, err = client.CoordinationV1().Leases(binding.Namespace).Update(ctx, lease, metav1.UpdateOptions{})
				}
				if err != nil && ctx.Err() == nil {
					renewalErrors <- err
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
		select {
		case err := <-renewalErrors:
			require.NoError(t, err, "原Host续约失败不能被忽略")
		default:
		}
	}
}
