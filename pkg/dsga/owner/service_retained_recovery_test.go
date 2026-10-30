package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

func TestProductionServiceRetainedInitialRecovery(t *testing.T) {
	for _, mode := range []string{"planned", "unknown"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "retained-safety-initial-"+mode) })
	}
}

func TestProductionServiceRetainedFinalizedRecovery(t *testing.T) {
	productionInitialService(t, "retained-safety-finalized-unknown")
}

func TestProductionServiceRetainedCapacityRecovery(t *testing.T) {
	productionInitialService(t, "retained-safety-capacity-planned-takeover")
}

// 只记录本根的阶段成本，不调整生产QPS/期限。现API.calls包含VM及Lease
// HTTP总量，VM版本差只代表写入；Core Actions另计，避免把全部API当VM GET。
func retainedStage(t *testing.T, api *ownerAPI, core *kubefake.Clientset, name string) func() {
	t.Helper()
	started := time.Now()
	api.mu.Lock()
	calls, writes := api.calls, api.vmVersion
	api.mu.Unlock()
	actions := len(core.Actions())
	t.Logf("阶段开始：%s", name)
	return func() {
		api.mu.Lock()
		callDelta, writeDelta := api.calls-calls, api.vmVersion-writes
		api.mu.Unlock()
		t.Logf("阶段结束：%s elapsed=%s VM/LeaseHTTP=%d VM版本推进=%d CoreAPI=%d", name, time.Since(started), callDelta, writeDelta, len(core.Actions())-actions)
	}
}

// 前驱全部走正式Initial/正常Grow/完整实际恢复生产者。本请求只有Prepare
// 可能未知，其它资源动作仍Planned，故只认证旧永久预算而不补新Host/Prepared。
// Session/SDK/文件WAL/typed CAS真实执行；Root/PG/QMP/Kube仍为协议peer。
func verifyProductionRetainedSuccessor(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, coord coordclient.CoordinationV1Interface, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, qualification budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner, qmp *vmQMPPeer, transfer *businessOwnerTransfer) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	if strings.Contains(mode, "finalized") {
		finished := retainedStage(t, api, core, "正常Grow前驱Finish")
		progress, err := worker.Advance(ctx)
		finished()
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress, "正常Grow由正式worker结案")
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, s.State().BudgetHeld)
	} else {
		require.NotNil(t, s.State().CapacityHeld.ActiveRecovery)
		require.EqualValues(t, 3, s.State().CapacityHeld.ProtocolVersion)
	}
	require.Nil(t, s.State().Active)
	publisher, _ := retainedHostPublisher(t, ctx, core, coord, store, session)
	finished := retainedStage(t, api, core, "原永久预算Host结清")
	_, err = publisher.SettleCurrent(ctx)
	finished()
	require.NoError(t, err)
	t.Log("正式旧永久预算前驱及Host结清已确认")
	current := pg.shared()
	next := pg.request
	next.Key.Revision++
	next.Key.OperationID.Low++
	next.Key.IssuerEpoch = session.Actor().Epoch
	next.ExpectedCapacityGeneration = current.Observed.CapacityGeneration
	next.BudgetCommitmentID += "-early-retained-target"
	if strings.Contains(mode, "finalized") {
		// 同1GiB原资格且normal上限不降；这里只准备一个不同的新目标，
		// 原Shrink尚未执行，不能借它重算旧1GiB预算或提前释放Host。
		next.Kind, next.TargetBuffers = control.Shrink, 512
	}
	pg.mu.Lock()
	pg.request, pg.before, pg.after, pg.resized = next, current, current, false
	pg.mu.Unlock()
	_, _, err = store.Begin(ctx, s, session.Actor(), next, 1)
	require.NoError(t, err)
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), qualification)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	verifyProductionServiceRetainedRecovery(t, ctx, mode, api, store, core, coord, session, resources, worker, config, qualification, pg, root, runner, qmp, transfer)
}

func retainedHostPublisher(t *testing.T, ctx context.Context, core *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session) (*hostexec.Publisher, *hostguard.Store) {
	t.Helper()
	node, err := core.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var ledger hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
	allocator, err := hostguard.NewStore(core.CoreV1(), core.CoordinationV1(), ledger.Binding, "host-fixture-holder")
	require.NoError(t, err)
	publisher, err := hostexec.New(core.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	return publisher, allocator
}

func verifyProductionServiceRetainedRecovery(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, coord coordclient.CoordinationV1Interface, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, qualification budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner, qmp *vmQMPPeer, transfer *businessOwnerTransfer) {
	t.Helper()
	initial := strings.Contains(mode, "initial")
	unknown := strings.Contains(mode, "unknown")
	lateCreate := initial && unknown
	before, err := store.Load(ctx)
	require.NoError(t, err)
	source, err := before.State().CurrentRetainedSource()
	require.NoError(t, err)
	oldID, err := source.BudgetCommitmentID()
	require.NoError(t, err)
	require.NotEqual(t, oldID, before.State().Active.Request.BudgetCommitmentID)
	var late *corev1.ConfigMap
	var releaseOnce sync.Once
	release, persisted := make(chan struct{}), make(chan error, 1)
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	if lateCreate {
		// 明确网络peer：fake reactor不能持自身锁等待完整恢复，否则会
		// 锁住其它Core GET。先返回Unknown，原CREATE payload由独立peer
		// goroutine在显式释放后经Tracker持久；不冒充真实Kube网络行为。
		core.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
			cm := action.(kt.CreateAction).GetObject().(*corev1.ConfigMap)
			if cm.Labels[budgetclaims.ClaimLabel] != budgetclaims.OutsideKind || late != nil {
				return false, nil, nil
			}
			late = cm.DeepCopy()
			late.UID, late.ResourceVersion = "retained-late-outside-uid", "retained-late-outside-rv"
			payload := late.DeepCopy()
			go func() {
				<-release
				persisted <- core.Tracker().Create(corev1.SchemeGroupVersion.WithResource("configmaps"), payload, payload.Namespace)
			}()
			return true, nil, fmt.Errorf("explicit network peer retained original CREATE payload after reply became unknown")
		})
	} else if unknown {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	if unknown {
		finished := retainedStage(t, api, core, "早期Prepare未知")
		_, err = worker.Advance(ctx)
		finished()
		if lateCreate {
			require.ErrorIs(t, err, resourceguard.ErrUnknown)
			require.NotNil(t, late)
		} else {
			var lost *journal.UnconfirmedWrite
			require.ErrorAs(t, err, &lost, "Prepare Start真实落盘丢回复，而非其它前置错误")
		}
	}
	before, err = store.Load(ctx)
	require.NoError(t, err)
	original := before.State()
	wantedPhase := journal.Planned
	if unknown {
		wantedPhase = journal.Unconfirmed
	}
	require.Equal(t, wantedPhase, original.Active.Steps[0].Phase)
	for _, step := range original.Active.Steps[1:] {
		require.Equal(t, journal.Planned, step.Phase)
	}
	t.Logf("早期原请求已固定：source=%s Prepare=%s", source.Kind, wantedPhase)
	_, _, pgWrites := pg.counts()
	root.mu.Lock()
	rootWrites, cancellations := root.posts, root.cancellations
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	qmp.mu.Lock()
	vmWrites := qmp.writes
	qmp.mu.Unlock()
	finished := retainedStage(t, api, core, "原来源异常关闭16")
	_, err = worker.Advance(ctx)
	finished()
	require.NoError(t, err, "原来源异常由生产入口保存并确认16关闭")
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 16, s.State().ClosureFormatVersion())
	require.False(t, s.State().ServiceClosure.Pending())
	require.Equal(t, original.Active, s.State().Active)
	_, _, closed, selector := s.State().ServiceClosure.Selection()
	require.Equal(t, original.Active.Request.BudgetCommitmentID, closed.BudgetCommitmentID)
	t.Log("生产异常关闭16已确认，原目标关闭ID保持")
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	var reopening serviceReopenPG
	reopening.mode, reopening.api, reopening.root = "normal", api, root
	pg.mu.Lock()
	pg.reopenFixture = &reopening
	pg.mu.Unlock()
	if source.Finalization != nil {
		var command budget.FinalizationCommand
		require.NoError(t, json.Unmarshal(source.Finalization.Command, &command))
		ref := command.Finalized.Sources.HostClaim
		cm, err := core.CoreV1().ConfigMaps(store.Binding().Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		require.NoError(t, err)
		replaced := cm.DeepCopy()
		replaced.UID = "retained-same-name-host-replacement"
		require.NoError(t, core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), replaced, replaced.Namespace))
		_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
		require.ErrorIs(t, err, ErrServiceSafety)
		rejected, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, s.State(), rejected.State(), "同名新UID拒绝不能保存来源Proof或改WAL")
		require.NoError(t, core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
	}
	finished = retainedStage(t, api, core, "完整旧来源Proof及旧ID新开放")
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
	finished()
	require.NoError(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	proof := s.State().ServiceClosure.RetainedRecovery
	require.NotNil(t, proof)
	require.Equal(t, source, proof.Source)
	require.Equal(t, original.Active, s.State().Active)
	require.Equal(t, original.InitialHeld, s.State().InitialHeld)
	require.Equal(t, original.BudgetHeld, s.State().BudgetHeld)
	require.Equal(t, original.CapacityHeld, s.State().CapacityHeld)
	r := s.State().ServiceClosure.Reopen
	require.NotNil(t, r)
	require.Equal(t, journal.Settled, r.Phase)
	require.Equal(t, oldID, r.Request.BudgetCommitmentID, "新开放采用旧准确预算ID，不改变原新目标关闭")
	require.Equal(t, closed, s.State().ServiceClosure.Request)
	t.Log("完整原来源Proof及旧预算ID新开放已确认")
	if strings.HasSuffix(mode, "takeover") {
		require.NotNil(t, transfer)
		previous := session.Actor()
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, qualification)
		require.Greater(t, session.Actor().Epoch, previous.Epoch)
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, proof, s.State().ServiceClosure.RetainedRecovery)
		require.Equal(t, r, s.State().ServiceClosure.Reopen)
		require.ErrorIs(t, worker.admission.CheckServiceRetained(ctx, s), vmguard.ErrUnknown)
		unchanged, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, s.State(), unchanged.State())
		require.NoError(t, runner.Tick(ctx))
		require.NoError(t, worker.admission.CheckServiceRetained(ctx, unchanged))
	}
	if lateCreate {
		api.mu.Lock()
		api.loseJournalReply = true
		api.mu.Unlock()
	}
	finished = retainedStage(t, api, core, "早期覆盖Complete")
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, r.CommandSHA256))
	finished()
	if lateCreate {
		var lost *journal.UnconfirmedWrite
		require.ErrorAs(t, err, &lost)
		worker.CloseIdleConnections()
		worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), qualification)
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, r.CommandSHA256))
	}
	require.NoError(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	done := s.State()
	require.Nil(t, done.Active)
	require.Nil(t, done.ServiceClosure)
	require.Equal(t, original.InitialHeld, done.InitialHeld)
	require.Equal(t, original.BudgetHeld, done.BudgetHeld)
	require.Equal(t, original.CapacityHeld, done.CapacityHeld)
	again, err := done.CurrentRetainedSource()
	require.NoError(t, err)
	require.Equal(t, source, again, "完成只覆盖早期原操作，旧永久预算完整叶子/序列保持")
	require.NotNil(t, done.RetainedCoverage)
	record := done.RetainedCoverage.Record
	require.Equal(t, journal.RecoveredWithRetainedBudget, record.Outcome)
	require.Equal(t, *original.Active, record.Operation)
	require.Equal(t, proof, record.ServiceCoverage.RetainedRecovery)
	require.Equal(t, r.Request, record.ServiceCoverage.Request)
	t.Log("正式Complete及原操作覆盖已确认，旧永久头保持")
	if source.Finalization != nil {
		var command budget.FinalizationCommand
		require.NoError(t, json.Unmarshal(source.Finalization.Command, &command))
		ref := command.Finalized.Sources.Runner.ReceiptClaim
		cm, err := core.CoreV1().ConfigMaps(store.Binding().Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		require.NoError(t, err)
		require.NoError(t, core.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("configmaps"), cm.Namespace, cm.Name))
		require.Error(t, worker.inspectIdleService(ctx, s), "Birth和当前数值相同仍须读取原不可变Runner receipt")
		unchanged, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, done, unchanged.State(), "原receipt缺失的只读拒绝不能退休或改写覆盖")
		require.NoError(t, core.Tracker().Create(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
		require.NoError(t, worker.inspectIdleService(ctx, unchanged))
	}
	if lateCreate {
		unblock()
		require.NoError(t, <-persisted)
		cm, err := core.CoreV1().ConfigMaps(late.Namespace).Get(ctx, late.Name, metav1.GetOptions{})
		require.NoError(t, err, "完成后原网络CREATE仍可能迟到，不能假定未收到")
		require.Equal(t, late.UID, cm.UID)
		var projection struct {
			Prepared budget.Prepared `json:"prepared"`
		}
		step := original.Active.Steps[0]
		require.NoError(t, json.Unmarshal(step.Command, &projection))
		ref := budget.ObjectRef{Name: late.Name, UID: string(late.UID)}
		projection.Prepared.OutsideClaim = &ref
		raw, err := json.Marshal(projection.Prepared)
		require.NoError(t, err)
		_, err = store.Record(ctx, before, session.Actor(), original.Active.Request.Key, journal.Observation{Key: original.Active.Request.Key, RequestSHA256: original.Active.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: step.TargetID, Data: raw})
		require.Error(t, err, "覆盖完成后的原旧RV/Prepare不能再落成Completed")
		current, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, done, current.State())
	}
	var publisher *hostexec.Publisher
	var allocator *hostguard.Store
	if initial {
		publisher, allocator = actualHostPublisher(t, core, coord, store, session)
	} else {
		publisher, allocator = retainedHostPublisher(t, ctx, core, coord, store, session)
	}
	finished = retainedStage(t, api, core, "恢复完成后的Host及GC")
	for range 4 {
		_, err = publisher.SettleCurrent(ctx)
		if !errors.Is(err, hostexec.ErrCollectionPending) {
			break
		}
	}
	finished()
	require.NoError(t, err)
	settled, err := allocator.Load(ctx)
	require.NoError(t, err)
	placement, err := settled.State().Placement("runner-uid")
	require.NoError(t, err)
	memory, err := source.VMMemoryBytes()
	require.NoError(t, err)
	if initial {
		// 初始placement可按认证平台保守持有大于实际VM的额度；早期恢复
		// 不生成Settlement，也无权把这份原Host持有压成当前VM字节数。
		require.Equal(t, source.Initial.HostMemoryBytes, placement.MemoryBytes)
		require.GreaterOrEqual(t, placement.MemoryBytes, memory)
		require.Nil(t, done.BudgetBoundary, "Initial不合成Resize关闭水位")
		require.Nil(t, placement.Settled, "Initial不合成Host Settlement")
		for _, ref := range []budget.ObjectRef{source.Initial.Qualification.Claim, source.Initial.RunnerBirth} {
			cm, err := core.CoreV1().ConfigMaps(store.Binding().Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, ref.UID, string(cm.UID))
		}
	} else {
		require.Equal(t, memory, placement.MemoryBytes)
		require.NotNil(t, done.BudgetBoundary)
		require.Equal(t, original.Active.Request.Key, done.BudgetBoundary.ThroughKey)
		require.NotNil(t, placement.Settled)
		require.Equal(t, original.Active.Request.Key, placement.Settled.ClosedThrough())
	}
	if lateCreate {
		_, err = core.CoreV1().ConfigMaps(late.Namespace).Get(ctx, late.Name, metav1.GetOptions{})
		require.True(t, apierrors.IsNotFound(err), "完整初始覆盖后仅回收原Key迟到Outside，原资格/出生继续持有")
	}
	t.Log("Host原实际预算/初始持有与精确来源回收已确认")
	root.mu.Lock()
	require.Equal(t, rootWrites, root.posts)
	require.Equal(t, cancellations, root.cancellations)
	root.mu.Unlock()
	_, _, resizes := pg.counts()
	require.Equal(t, pgWrites, resizes)
	pg.mu.Lock()
	require.Equal(t, 1, pg.reopenFixture.posts)
	pg.reopenFixture = nil
	pg.mu.Unlock()
	qmp.mu.Lock()
	require.Equal(t, vmWrites, qmp.writes, "早期恢复没有原VM命令，不补发或合成取消")
	qmp.mu.Unlock()
	if strings.Contains(mode, "finalized") {
		verifyRetainedNormalSuccessor(t, ctx, api, store, core, session, resources, worker, qualification, pg, runner, publisher)
	}
}

func verifyRetainedNormalSuccessor(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, qualification budget.QualificationSource, pg *initialServicePG, runner *vmguard.Runner, publisher *hostexec.Publisher) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	current := pg.shared()
	request := pg.request
	request.Kind, request.TargetBuffers = control.Grow, 2048
	request.Key.Revision++
	request.Key.OperationID.Low++
	request.ExpectedCapacityGeneration = current.Observed.CapacityGeneration
	request.BudgetCommitmentID += "-normal-successor"
	after := current
	after.Observed.CapacityGeneration = request.ExpectedCapacityGeneration + 1
	after.Observed.LowBuffers, after.Observed.HighBuffers = request.TargetBuffers, request.TargetBuffers
	digest, err := request.ContextSHA256()
	require.NoError(t, err)
	after.Observed.Operation = &control.OperationFact{Key: request.Key, Kind: request.Kind, Phase: control.Committed, Reason: control.ReasonNone, TargetBuffers: request.TargetBuffers, ExpectedCapacityGeneration: request.ExpectedCapacityGeneration, ContextSHA256: digest, CancelRequested: false, CancelClosed: true, Replayed: false}
	require.NoError(t, after.ValidateFor(request.Key.Instance))
	pg.mu.Lock()
	pg.request, pg.before, pg.after, pg.resized = request, current, after, false
	pg.mu.Unlock()
	_, _, err = store.Begin(ctx, s, session.Actor(), request, 1)
	require.NoError(t, err)
	worker.CloseIdleConnections()
	// 旧pgResponse静态样本只代表首次Grow。后继peer按完整新Request
	// 生成准确下一代终态，不能把静态旧expected/kind套进本次原生事实。
	config := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dsg/v1/resize" {
			pg.serve(t, w, r)
			return
		}
		var received control.Request
		require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
		require.Equal(t, request, received)
		require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizePG).Phase)
		pg.mu.Lock()
		pg.resized = true
		pg.resizePosts++
		current := pg.current().Observed
		response := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: request.Key, Disposition: control.ReceiptReplayed, HasResult: true, Result: &current}, Observed: current}
		require.NoError(t, response.ValidateResize(request, request.Key.Instance))
		pg.resizeReceipt = &response
		pg.mu.Unlock()
		pgReply(w, response)
	}, func() control.Snapshot { return pg.shared().Observed })
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), qualification)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	for _, action := range []journal.Action{journal.PrepareBudget, journal.ReserveHost, journal.ResizeVM, journal.ObserveVM, journal.GuestReady, journal.ResizePG, journal.FinalizeBudget, journal.ResumeAdmission} {
		finished := retainedStage(t, api, core, "正式后继 "+string(action))
		// 仅本新case的诊断门槛：每个完整外层动作最多30秒，保留
		// 原生产单RPC期限/QPS及全根总期限，避免无阶段输出等满6分钟。
		progress, err := func() (pgexec.Progress, error) {
			actionCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if action == journal.ReserveHost {
				if _, err := publisher.Reconcile(actionCtx); err != nil {
					return pgexec.AwaitingFact, err
				}
			}
			if action == journal.ObserveVM {
				if err := runner.Tick(actionCtx); err != nil {
					return pgexec.AwaitingFact, err
				}
			}
			return worker.Advance(actionCtx)
		}()
		finished()
		require.NoError(t, err, string(action))
		require.Equal(t, pgexec.Recorded, progress)
		t.Logf("正式后继已确认：%s", action)
	}
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress, "后继完整原预算由正式worker Finish")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	require.Equal(t, journal.Succeeded, s.State().History[len(s.State().History)-1].Outcome)
	require.NoError(t, s.State().CheckClosedBudget(request))
	_, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
}
