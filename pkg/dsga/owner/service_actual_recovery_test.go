package owner

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	kubev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

func TestProductionServiceActualRecovery(t *testing.T) {
	for _, mode := range []string{"vm-unreceived", "guest-unreceived", "pg-late-commit"} {
		t.Run(mode, func(t *testing.T) { productionInitialService(t, "actual-safety-recover-"+mode) })
	}
}

func TestProductionActualBudgetHeaderConsumers(t *testing.T) {
	productionInitialService(t, "actual-safety-recover-guest-unreceived-consumer")
}

func TestProductionActualRecoveryAfterRetainedPGTerminalExpires(t *testing.T) {
	productionInitialService(t, "actual-safety-recover-pg-late-commit-expired")
}

func TestProductionActualRecoveryFromUndispatchedVM(t *testing.T) {
	productionInitialService(t, "actual-safety-recover-vm-planned")
}

func TestProductionActualRecoveryAfterRealLeaseTakeover(t *testing.T) {
	productionInitialService(t, "actual-safety-recover-vm-unreceived-takeover")
}

// 原首次预算/Prepare/Host/VM以及Guest写前全部来自正式执行器；异常关闭
// 后真实管理选择独立恢复、实际预算、新开放与完成CAS。Lease、SDK和typed
// CAS真实运行；Root/PG/QMP/Kube仍为明确peer，不外推Linux物理返还。
func verifyProductionServiceActualRecovery(t *testing.T, ctx context.Context, mode string, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, coord coordclient.CoordinationV1Interface, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner, qmp *vmQMPPeer, transfer *businessOwnerTransfer) {
	t.Helper()
	guest := strings.Contains(mode, "guest-unreceived")
	latePG := strings.Contains(mode, "pg-late-commit")
	expiredPG := latePG && strings.HasSuffix(mode, "-expired")
	vmPlanned := strings.HasSuffix(mode, "vm-planned")
	takeover := strings.HasSuffix(mode, "-takeover")
	before, err := store.Load(ctx)
	require.NoError(t, err)
	original := before.State()
	require.NotNil(t, original.InitialHeld)
	root.mu.Lock()
	oldRoot, err := budget.RootAggregateFromView(root.view)
	require.NoError(t, err)
	rootWrites := root.posts
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.NoError(t, err)
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().ServiceClosure)
	require.False(t, s.State().ServiceClosure.Pending())
	require.Equal(t, original.Active, s.State().Active)
	_, _, _, selector := s.State().ServiceClosure.Selection()
	// 错误来源保持关闭，不能创建独立预算或发新开放。
	if guest {
		// Guest取消peer只提供合法限额下的完整取消回执。这里恢复限额，
		// 改变实际进程出生以在任何取消RPC前验证来源拒绝；不能要求
		// peer把不合法的当前限额签成有效取消证明。
		root.mu.Lock()
		root.view.Actual.MemoryMaxBytes--
		process := *root.view.Birth
		process.PostmasterStartTicks++
		root.processOverride = &process
		root.mu.Unlock()
	}
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
	require.ErrorIs(t, err, ErrServiceSafety)
	rejected, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, rejected.State().ServiceClosure.ActualRecovery)
	require.Nil(t, rejected.State().ServiceClosure.Reopen)
	root.mu.Lock()
	if guest {
		root.processOverride = nil
	} else {
		root.view.Actual.MemoryMaxBytes--
	}
	root.mu.Unlock()
	if latePG {
		// 原Resize已收到且回复未知；原关闭完整确认之后才推进该原
		// 执行的效果。旧close.expected保持原代次，独立PG证明记录
		// 当前下一代容量；恢复绝不向peer发送第二个Resize。
		pg.mu.Lock()
		require.Equal(t, 1, pg.resizePosts)
		pg.resized = true
		response := pgResponse(t, original.Active.Request)
		response.Observed = pg.current().Observed
		*response.Receipt.Result = response.Observed
		pg.resizeReceipt = &response
		pg.mu.Unlock()
	}
	var retainedPG *journal.ServicePGRecovery
	if expiredPG {
		// 先通过正式单域执行器保存完整原终态；当前SDK此时可以解除同
		// Pending。后继SDK必须仍从原WAL Unknown恢复，不能靠旧内存槽
		// 已清来绕过历史淘汰后的完整实际预算持久屏障。
		s, err = store.Load(ctx)
		require.NoError(t, err)
		s, err = worker.executor.RecoverServicePG(ctx, s, selector, true)
		require.NoError(t, err)
		retainedPG = s.State().ServiceClosure.PGRecovery
		require.NotNil(t, retainedPG)
		require.NotNil(t, retainedPG.Status.Receipt.Result)
		require.Equal(t, control.ResizeRecoveredCommitted, retainedPG.Effect)
		require.Nil(t, worker.client.Pending())
		// 较新同容量终态只模拟明确PG协议peer的历史保留，不是允许
		// 生产协调器越过Pending另发请求，也不称作真实PG缓存淘汰。
		pg.mu.Lock()
		successor := original.Active.Request
		successor.Key.Revision++
		successor.Key.OperationID.Low++
		successor.ExpectedCapacityGeneration = pg.after.Observed.CapacityGeneration
		successor.BudgetCommitmentID += "-explicit-cache-eviction-peer"
		digest, hashErr := successor.ContextSHA256()
		require.NoError(t, hashErr)
		pg.after.Observed.Operation = &control.OperationFact{Key: successor.Key, Kind: successor.Kind, Phase: control.Denied, Reason: control.ReasonTimeout, TargetBuffers: successor.TargetBuffers, ExpectedCapacityGeneration: successor.ExpectedCapacityGeneration, ContextSHA256: digest, CancelRequested: false, CancelClosed: true, Replayed: false}
		pg.mu.Unlock()
	}
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	if expiredPG {
		require.Equal(t, &original.Active.Request, worker.client.Pending(), "SDK重建必须保留原Unknown")
		s, err = store.Load(ctx)
		require.NoError(t, err)
		_, err = worker.executor.RecoverServicePG(ctx, s, selector, false)
		require.Error(t, err, "旧单域严格对账不能把完整已保存终态替换成缺历史隔离")
		require.NotNil(t, worker.client.Pending())
	}
	pg.mu.Lock()
	var reopening serviceReopenPG
	reopening.mode, reopening.api, reopening.root = "normal", api, root
	pg.reopenFixture = &reopening
	pg.mu.Unlock()
	if !guest && !latePG && !vmPlanned {
		// 未收到原VM时，首次显式选择只能写入独立取消；Runner在
		// 原flock内看真实WAL封存后才可构造实际before预算。
		_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
		require.ErrorIs(t, err, ErrServiceSafety)
		selected, err := store.Load(ctx)
		require.NoError(t, err)
		require.NotNil(t, selected.State().ServiceClosure.VMRecovery)
		require.Equal(t, journal.Unconfirmed, selected.State().ServiceClosure.VMRecovery.Phase)
		require.Nil(t, selected.State().ServiceClosure.ActualRecovery)
		require.NoError(t, runner.Tick(ctx))
	}
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationReopenService, selector))
	require.NoError(t, err, "完整来源认证后允许准确实际容量服务恢复")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	selected := s.State()
	if expiredPG {
		require.Equal(t, retainedPG, selected.ServiceClosure.PGRecovery, "完整原终态证明不被缓存淘汰覆盖")
		require.Nil(t, worker.client.Pending(), "Actual CAS确认及当前原Key/完整预算复核之后才清原SDK")
		require.NotNil(t, selected.ServiceClosure.ActualRecovery.Capacity.RecoveryPG.Status.Receipt.Result)
	}
	require.Equal(t, original.Active, selected.Active)
	require.Equal(t, original.InitialHeld, selected.InitialHeld)
	actual := selected.ServiceClosure.ActualRecovery
	require.NotNil(t, actual)
	require.EqualValues(t, 4, actual.Capacity.ProtocolVersion)
	require.Equal(t, oldRoot, actual.Root, "Guest未执行时必须保留实际old Root")
	if guest || latePG {
		require.Nil(t, actual.Capacity.InitialOutside)
		p, _, err := selected.OriginalServicePrepared()
		require.NoError(t, err)
		require.Equal(t, *p.OutsideClaim, actual.Capacity.OutsideClaim)
		require.Equal(t, p.GuestUsableMemoryBytes, actual.Capacity.GuestUsableMemoryBytes)
		require.Equal(t, original.Active.Request.TargetVMMemoryBytes, actual.Capacity.VMMemoryBytes)
		if guest {
			require.EqualValues(t, 40<<20, actual.Capacity.SystemOutsidePG.UpperBytes+actual.Capacity.Transition.UpperBytes+actual.Capacity.Safety.UpperBytes)
			require.EqualValues(t, 32<<20, oldRoot.Bootstrap.Limits.OutsideUpperBytes)
			require.Equal(t, "cancelled", actual.Guest.Guest.Effect)
		} else {
			require.NotNil(t, actual.Capacity.RecoveryPG)
			require.Equal(t, selected.ServiceClosure.Request.ExpectedCapacityGeneration+1, actual.Capacity.CapacityGeneration)
			require.Equal(t, original.Active.Request.ExpectedCapacityGeneration, selected.ServiceClosure.Request.ExpectedCapacityGeneration, "旧关闭expected原代次不得改写")
		}
	} else {
		require.NotNil(t, actual.Capacity.InitialOutside)
		var initial budget.Initial
		require.NoError(t, json.Unmarshal(original.InitialHeld.Budget, &initial))
		require.Equal(t, initial, *actual.Capacity.InitialOutside)
		require.Equal(t, actual.Capacity.InitialOutside.VMMemoryBytes, actual.Capacity.VMMemoryBytes)
		require.Equal(t, actual.Capacity.InitialOutside.GuestUsableMemoryBytes, actual.Capacity.GuestUsableMemoryBytes)
		if vmPlanned {
			require.Nil(t, actual.VM)
			require.NotNil(t, actual.Capacity.InitialVM)
			var noNative budget.NativeCapacity
			require.Equal(t, noNative, actual.Capacity.VM, "未派发VM不能合成原生命令或取消回执")
		}
	}
	r := selected.ServiceClosure.Reopen
	require.NotNil(t, r)
	require.Equal(t, journal.Settled, r.Phase)
	require.Equal(t, actual.Capacity.CapacityGeneration, r.Request.ExpectedCapacityGeneration)
	if takeover {
		// 不手改Owner、不删除或缩短Lease。原owner真实续约失效退出，
		// 同Lease的新Session接管；原新开放命令、Actual观察者和VM取消
		// 见证始终保持旧owner，不借接管改epoch重发任何原setter。
		require.NotNil(t, transfer)
		previousOwner := session.Actor()
		session, resources, worker = transfer.takeoverClosure(t, ctx, session, resources, worker, config, core, source)
		require.Greater(t, session.Actor().Epoch, previousOwner.Epoch)
		s, err = store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, actual, s.State().ServiceClosure.ActualRecovery)
		require.Equal(t, r, s.State().ServiceClosure.Reopen)
		_, _, published, err := vmguard.ReadPublished(ctx, core.CoreV1(), store.Binding(), nil)
		require.NoError(t, err)
		require.Less(t, published.State.Floor.Epoch, session.Actor().Epoch)
		// 直接只读预算审计可验证旧floor拒绝而不选择异常再关闭。
		// 普通Complete失败会依法再关门，不拿它制造另一次开放来过测试。
		require.ErrorIs(t, worker.admission.CheckServiceActual(ctx, s), vmguard.ErrUnknown)
		unchanged, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, s.State(), unchanged.State())
		require.NoError(t, runner.Tick(ctx))
		require.NoError(t, worker.admission.CheckServiceActual(ctx, unchanged))
	}
	// 完成CAS丢回复必须同时安装独立永久预算、覆盖终态和Host水位；
	// SDK重建只确认这同一完整证据，不补发开放/资源命令。
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, r.CommandSHA256))
	var lost *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &lost)
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	_, err = worker.executeControl(ctx, closureControl(session, config.Instance, control.CoordinationCompleteServiceReopen, r.CommandSHA256))
	require.NoError(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	done := s.State()
	require.Nil(t, done.Active)
	require.Nil(t, done.ServiceClosure)
	require.Nil(t, done.InitialHeld)
	require.NotNil(t, done.CapacityHeld.ActiveRecovery)
	require.Equal(t, *original.Active, done.CapacityHeld.ActiveRecovery.Original)
	require.Equal(t, *actual, done.CapacityHeld.ActiveRecovery.Proof)
	require.NoError(t, done.CheckClosedCapacity(original.Active.Request))
	record := done.History[len(done.History)-1]
	require.Equal(t, journal.RecoveredWithActualBudget, record.Outcome)
	require.Equal(t, *original.Active, record.Operation)
	if takeover {
		require.Equal(t, session.Actor(), record.ServiceCoverage.Origin)
		require.Greater(t, record.ServiceCoverage.Origin.Epoch, actual.Origin.Epoch)
		require.Equal(t, actual.Origin, done.CapacityHeld.ActiveRecovery.Proof.Origin)
		require.Equal(t, actual.VM.Cancellation, done.CapacityHeld.ActiveRecovery.Proof.VM.Cancellation)
		require.Equal(t, r.Request, record.ServiceCoverage.Request)
		require.Greater(t, done.BudgetBoundary.SucceededSequence, done.CapacityHeld.RecordedSequence)
	}
	root.mu.Lock()
	require.Equal(t, rootWrites, root.posts)
	require.Equal(t, map[bool]int{false: 0, true: 1}[guest], root.cancellations)
	root.mu.Unlock()
	pg.mu.Lock()
	require.Equal(t, 1, pg.reopenFixture.posts)
	require.Equal(t, map[bool]int{false: 0, true: 1}[latePG], pg.resizePosts)
	pg.reopenFixture = nil
	pg.mu.Unlock()
	qmp.mu.Lock()
	require.Equal(t, map[bool]int{false: 0, true: 1}[guest || latePG], qmp.writes)
	qmp.mu.Unlock()
	node, err := core.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var ledger hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
	allocator, err := hostguard.NewStore(core.CoreV1(), core.CoordinationV1(), ledger.Binding, "host-fixture-holder")
	require.NoError(t, err)
	publisher, err := hostexec.New(core.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	changed, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, changed)
	settled, err := allocator.Load(ctx)
	require.NoError(t, err)
	birth, _, _, err := vmguard.ReadPublished(ctx, core.CoreV1(), store.Binding(), nil)
	require.NoError(t, err)
	placement, err := settled.State().Placement(birth.Identity.RunnerPodUID)
	require.NoError(t, err)
	require.Equal(t, actual.Capacity.VMMemoryBytes, placement.MemoryBytes)
	if vmPlanned {
		// Reserve从真实持久VM/Pod重新执行allocation。这里必须选择
		// InitialVM.BirthClaim，零Runner来源不得被当成一个原生命令。
		vmAPI, err := vmclient.NewForConfig(api.config)
		require.NoError(t, err)
		configuration := hostscheduler.Config{Nodes: []hostscheduler.NodeConfig{{Binding: ledger.Binding, Profile: *ledger.Profile, SystemBytes: ledger.OutsideBytes, InitializeAtResourceVersion: ""}}}
		scheduler, err := hostscheduler.New(core.CoreV1(), core.CoordinationV1(), vmAPI, configuration, ledger.Profile.SchedulerName)
		require.NoError(t, err)
		pod, err := core.CoreV1().Pods(store.Binding().Namespace).Get(ctx, "runner", metav1.GetOptions{})
		require.NoError(t, err)
		pod.Spec.SchedulerName = ledger.Profile.SchedulerName
		require.NoError(t, core.Tracker().Update(kubev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
		_, allocated, err := scheduler.Reserve(ctx, ledger.Binding.NodeName, pod, 0)
		require.NoError(t, err)
		require.Equal(t, actual.Capacity.VMMemoryBytes, allocated.MemoryBytes)
		require.NotNil(t, allocated.Settled)
		require.Equal(t, *actual.Capacity.InitialVM, allocated.Settled.InitialVM)
		var noRunner budget.RunnerSources
		require.Equal(t, noRunner, allocated.Settled.Runner)
	}
	if !guest && !latePG && !vmPlanned && !takeover {
		verifyActualServiceNormalSuccessor(t, ctx, store, core, session, resources, worker, config, source, pg, runner, publisher)
	}
	if strings.HasSuffix(mode, "-consumer") {
		verifyProductionActualHeaderSourceReuse(t, ctx, api, store, core, session, resources, worker, config, source, pg, root, qmp, publisher)
	}
}

// 前驱必须由上面的正式管理/Complete生成真实v3永久头，再经后继Begin/
// Prepare/Host形成新关闭。当前VM仍为旧1GiB，新目标2GiB；新collector须
// 读取原v3头的Root/Runner/1GiB Outside，不能因没有Cleanup改借新2GiB
// 来源或丢掉原Unknown。这里不执行后继VM/Guest/PG，不扩展正常矩阵。
func verifyProductionActualHeaderSourceReuse(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, qmp *vmQMPPeer, publisher *hostexec.Publisher) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	previous := s.State()
	require.NotNil(t, previous.CapacityHeld.ActiveRecovery)
	retained, err := previous.CurrentCapacity()
	require.NoError(t, err)
	require.Nil(t, retained.InitialOutside)
	request := pg.request
	request.Key.Revision++
	request.Key.OperationID.Low++
	request.TargetVMMemoryBytes = 2 << 30
	request.BudgetCommitmentID += "-retained-actual-source"
	request.ExpectedCapacityGeneration = retained.CapacityGeneration
	_, _, err = store.Begin(ctx, s, session.Actor(), request, 1)
	require.NoError(t, err)
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	_, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, journal.Settled, budgetStep(t, store, journal.PrepareBudget).Phase)
	require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResizeVM).Phase)
	_, err = publisher.Reconcile(ctx)
	require.NoError(t, err)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.NoError(t, err)
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	s, err = store.Load(ctx)
	require.NoError(t, err)
	before := s.State()
	require.NotNil(t, before.ServiceClosure)
	require.False(t, before.ServiceClosure.Pending())
	require.Equal(t, previous.CapacityHeld, before.CapacityHeld)
	_, _, _, selector := before.ServiceClosure.Selection()
	require.NoError(t, worker.admission.RecoverServiceActual(ctx, s, selector))
	s, err = store.Load(ctx)
	require.NoError(t, err)
	current := s.State()
	proof := current.ServiceClosure.ActualRecovery
	require.NotNil(t, proof)
	require.Equal(t, retained.OutsideClaim, proof.Capacity.OutsideClaim)
	require.Equal(t, retained.RootSHA256, proof.Capacity.RootSHA256)
	require.Equal(t, retained.VM, proof.Capacity.VM)
	require.Equal(t, retained.VMMemoryBytes, proof.Capacity.VMMemoryBytes)
	require.Equal(t, retained.GuestUsableMemoryBytes, proof.Capacity.GuestUsableMemoryBytes)
	require.Equal(t, before.Active, current.Active)
	require.Equal(t, previous.CapacityHeld, current.CapacityHeld, "第二次Proof不提前替换旧永久头")
	require.NoError(t, worker.admission.CheckServiceActual(ctx, s))
	// 已保存完整Proof仍须每轮认证原资格UID；同名同正文的新对象不
	// 获得旧预算的持有寿命，失败只保留Proof/关闭，不更新原来源。
	qualified, err := core.CoreV1().ConfigMaps(store.Binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	replaced := qualified.DeepCopy()
	replaced.UID = "same-name-new-actual-qualified-uid"
	require.NoError(t, core.Tracker().Update(kubev1.SchemeGroupVersion.WithResource("configmaps"), replaced, replaced.Namespace))
	require.Error(t, worker.admission.CheckServiceActual(ctx, s))
	unchanged, err := store.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, current, unchanged.State())
	require.NoError(t, core.Tracker().Update(kubev1.SchemeGroupVersion.WithResource("configmaps"), qualified, qualified.Namespace))
	require.Nil(t, current.ServiceClosure.Reopen)
	require.False(t, pg.shared().Observed.AdmissionOpen)
	qmp.mu.Lock()
	require.Equal(t, 1, qmp.writes)
	qmp.mu.Unlock()
	// 这里的VM status只作额外断言；正向Check已由固定Reader认证原
	// Runner回执与当前实际值，不能拿此数值单独替代原生来源。
	api.mu.Lock()
	vmMemory := api.vm.Status.MemorySize.Value()
	api.mu.Unlock()
	require.EqualValues(t, retained.VMMemoryBytes, vmMemory)
	t.Log("真实v3实际预算头的后继Planned VM按旧Root/Runner/精确Outside再次对账通过")
}

// 独立实际永久头必须被后继正式Prepare/Host/Runner/Guest/PG/Finalize消费；
// 不能只证明完成CAS后可读JSON。只重跑一个相关Grow，不扩展未改策略矩阵。
func verifyActualServiceNormalSuccessor(t *testing.T, ctx context.Context, store *journal.Store, core *kubefake.Clientset, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, runner *vmguard.Runner, publisher *hostexec.Publisher) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	request := pg.request
	request.Key.Revision++
	request.Key.OperationID.Low++
	request.BudgetCommitmentID += "-after-actual-service"
	current := pg.shared()
	request.ExpectedCapacityGeneration = current.Observed.CapacityGeneration
	pg.mu.Lock()
	pg.request, pg.before, pg.after, pg.resized = request, current, current, false
	pg.after.Observed = pgResponse(t, request).Observed
	pg.after.CurrentMainAndArraysUpperBytes = pg.after.StartupMainUpperBytes
	for _, array := range pg.after.Observed.Arrays {
		pg.after.CurrentMainAndArraysUpperBytes += array.CommittedUpperBytes
	}
	pg.mu.Unlock()
	_, _, err = store.Begin(ctx, s, session.Actor(), request, 1)
	require.NoError(t, err)
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, config, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	_, err = publisher.Reconcile(ctx)
	require.NoError(t, err)
	for range 2 {
		progress, err = worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
	}
	require.NoError(t, runner.Tick(ctx))
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	for _, action := range []journal.Action{journal.GuestReady, journal.ResizePG, journal.FinalizeBudget, journal.ResumeAdmission} {
		progress, err = worker.Advance(ctx)
		require.NoError(t, err, string(action))
		require.Equal(t, pgexec.Recorded, progress)
	}
	s, err = store.Load(ctx)
	require.NoError(t, err)
	_, err = store.Finish(ctx, s, session.Actor(), request.Key)
	require.NoError(t, err)
	_, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	t.Log("完整实际预算恢复后正式后继Grow与Host结清通过")
}
