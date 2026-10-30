package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

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
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 新组合显式增加有限任务/系统组协议事实；只改变本用例，不悄悄让所有旧
// 夹具冒充新平台资格。数字均是测试配置，不是生产默认或Linux物理认证。
func qualifiedRootFixture(root *guestRootFixture) {
	root.mu.Lock()
	defer root.mu.Unlock()
	id := root.view.Identity
	id.SystemCgroupDevice, id.SystemCgroupInode = id.CgroupDevice, 4
	root.view.Identity, root.view.Bootstrap.Identity, root.view.Birth.Identity, root.view.Actual.Identity = id, id, id, id
	root.view.Bootstrap.Limits.PidsMaxTasks = 128
	root.view.Bootstrap.Limits.SystemMemoryMaxBytes = 16 << 20
	root.view.SystemPlacementObserved = true
	root.view.Actual.TaskLimit = &resourceguard.TaskLimitEvidence{MaxTasks: 128, CurrentTasks: 5}
	root.view.Actual.System = &resourceguard.SystemEvidence{Scope: resourceguard.SystemChargedScope, MemoryMaxBytes: 16 << 20, MemoryCurrentBytes: 4 << 20, SwapMaxBytes: 0, SwapCurrentBytes: 0, ProcessCount: 3, PID1Member: true, RootUserspaceCount: 0}
}

func trustedQualificationFixture(t *testing.T, ctx context.Context, client *kubefake.Clientset, root *guestRootFixture, p budget.Prepared) budget.QualificationSource {
	t.Helper()
	birth, birthRef, _, err := vmguard.ReadPublished(ctx, client.CoreV1(), binding(), nil)
	require.NoError(t, err)
	runnerRaw, err := json.Marshal(birth.Identity)
	require.NoError(t, err)
	root.mu.Lock()
	bootstrapRaw, err := json.Marshal(root.view.Bootstrap)
	root.mu.Unlock()
	require.NoError(t, err)
	q := budget.PlatformQualification{ProtocolVersion: 1, Identity: p.Identity, Incarnation: binding().Incarnation, ReportID: "explicit-qualified-platform-fixture", RootBootstrapSHA256: vmguard.SourceDigest(bootstrapRaw), RunnerBirth: birthRef, RunnerIdentitySHA256: vmguard.SourceDigest(runnerRaw), RunnerPodName: p.RunnerPodName, RunnerPodUID: birth.Identity.RunnerPodUID, NodeName: "node-one", NodeUID: birth.Identity.NodeUID, RunnerRestartCount: p.RunnerRestartCount, HoldUntilExplicitSettlement: true, Targets: []budget.QualifiedTarget{{VMMemoryBytes: p.Request.TargetVMMemoryBytes, GuestUsableMemoryBytes: p.GuestUsableMemoryBytes, Limits: p.Limits, SystemOutsidePGBytes: p.SystemOutsidePG.UpperBytes, TransitionBytes: p.Transition.UpperBytes, SafetyBytes: p.Safety.UpperBytes}}}
	require.NoError(t, q.Validate())
	raw, err := json.Marshal(q)
	require.NoError(t, err)
	ref := vmReservation(t, client, "original-platform-qualification", budgetclaims.QualificationKind, q)
	return budget.QualificationSource{Claim: ref, SHA256: vmguard.SourceDigest(raw)}
}

// 一条组合复用真实Agent/Runner扩容后半链：Start回复丢失时零Create，普通
// 查询不重发；独立原重放的Create提交后丢回复，随后Record提交后丢回复，
// SDK重建仍固定同一UID。没有手填Prepare Completed或Outside承诺。
func runTrustedPrepareReplies(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, request control.Request) (*PGWorker, budget.Prepared, budget.ObjectRef) {
	t.Helper()
	creates := 0
	loseCreate := true
	client.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		cm := action.(kt.CreateAction).GetObject().(*core.ConfigMap)
		if cm.Labels[budgetclaims.ClaimLabel] != budgetclaims.OutsideKind {
			return false, nil, nil
		}
		creates++
		require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.PrepareBudget).Phase, "Outside创建前必须真实原WAL已持久")
		if !loseCreate {
			return false, nil, nil
		}
		loseCreate = false
		stored := cm.DeepCopy()
		stored.UID, stored.ResourceVersion = "original-outside-created-uid", "original-outside-rv"
		require.NoError(t, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), stored, stored.Namespace))
		return true, nil, fmt.Errorf("explicit fixture lost Outside Create reply after commit")
	})
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err := worker.Advance(ctx)
	require.Error(t, err)
	require.Zero(t, creates)
	for i := 0; i < 2; i++ {
		_, err = worker.Advance(ctx)
		require.ErrorIs(t, err, resourceguard.ErrUnknown)
		require.Zero(t, creates, "Start未知且对象缺失，普通轮询不能创建")
	}
	wrong := request.Key
	wrong.Revision++
	_, err = worker.ReplayPrepareOriginal(ctx, wrong)
	require.Error(t, err)
	require.Zero(t, creates)
	_, err = worker.ReplayPrepareOriginal(ctx, request.Key)
	require.ErrorIs(t, err, resourceguard.ErrUnknown)
	require.Equal(t, 1, creates)
	require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.PrepareBudget).Phase)
	api.mu.Lock()
	api.loseJournalReply = true
	api.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.Error(t, err, "Record实际提交后丢回复，仍须从原WAL恢复")
	var write *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &write, "此处必须到达真正Record CAS，不能把前置失败当丢回复")
	require.Equal(t, 1, creates)
	worker.CloseIdleConnections()
	rebuilt, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1(), source)
	require.NoError(t, err)
	step := budgetStep(t, store, journal.PrepareBudget)
	require.Equal(t, journal.Settled, step.Phase)
	var p budget.Prepared
	require.NoError(t, json.Unmarshal(step.Observation.Data, &p))
	require.NotNil(t, p.OutsideClaim)
	require.Equal(t, "original-outside-created-uid", p.OutsideClaim.UID)
	require.NoError(t, p.ValidateFor(request, p.Identity))
	claims, err := budgetclaims.New(client.CoreV1(), binding())
	require.NoError(t, err)
	require.NoError(t, claims.CheckOutside(ctx, p))
	cm, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, source.Claim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	changed := cm.DeepCopy()
	changed.UID = "same-name-replacement"
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
	require.Error(t, claims.CheckOutside(ctx, p), "后续消费必须重读原资格UID")
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), cm, cm.Namespace))
	resourcesSnapshot, err := resources.client.InspectState(ctx)
	require.NoError(t, err)
	require.Nil(t, resourcesSnapshot.Original, "Prepare不能提前执行GuestReady")
	return rebuilt, p, *p.OutsideClaim
}

// 正常全链完成后再经永久预算读者、真实Publisher结清，检查资格释放中会
// 阻断长期消费；恢复原来源后才能继续。此处不重复其他取消/历史淘汰场景。
func finishTrustedPrepareHost(t *testing.T, ctx context.Context, api *ownerAPI, client *kubefake.Clientset, store *journal.Store, session *Session, worker *PGWorker, p budget.Prepared, collectSources ...bool) {
	t.Helper()
	s, err := store.Load(ctx)
	require.NoError(t, err)
	// 旧组合保留显式结案的历史验证范围。新增自动结案组合已经由正式
	// worker完成，不再重复Finish；后续Host读取/结清与下一Prepare复用。
	if s.State().Active != nil {
		s, err = store.Finish(ctx, s, session.Actor(), p.Request.Key)
		require.NoError(t, err)
	}
	require.Nil(t, s.State().Active)
	claims, err := budgetclaims.New(client.CoreV1(), binding())
	require.NoError(t, err)
	_, err = claims.ReadCurrent(ctx, s)
	require.NoError(t, err)
	out, err := claims.ReadOutside(ctx, *p.OutsideClaim)
	require.NoError(t, err)
	q, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, out.Qualification.Claim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	changed := q.DeepCopy()
	stamp := metav1.Now()
	changed.DeletionTimestamp = &stamp
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
	_, err = claims.ReadCurrent(ctx, s)
	require.Error(t, err, "长期预算不能忽略原资格已进入释放")
	require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), q, q.Namespace))
	node, err := client.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var ledger hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
	allocator, err := hostguard.NewStore(client.CoreV1(), client.CoordinationV1(), ledger.Binding, "host-fixture-holder")
	require.NoError(t, err)
	coord, err := coordclient.NewForConfig(api.config)
	require.NoError(t, err)
	publisher, err := hostexec.New(client.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	settled, err := publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	require.True(t, settled)
	_, err = claims.ReadCurrent(ctx, s)
	require.NoError(t, err)
	if len(collectSources) == 1 && collectSources[0] {
		verifyClosedSourceCollection(t, ctx, client, coord, store, session, allocator, s)
	}
	// 后继Shrink复用同一确切1GiB资格：普通Start确认后实际Create成功，
	// 覆盖前一请求因丢回复只走显式Create重放的另一条生产路径。仅执行
	// 新Prepare，不重复整套缩容；后续动作均须保持Planned。
	next := p.Request
	next.Key.Revision++
	next.Key.OperationID.Low++
	next.Kind, next.TargetBuffers = control.Shrink, 512
	next.ExpectedCapacityGeneration++
	next.BudgetCommitmentID = "trusted-next-shrink"
	s, _, err = store.Begin(ctx, s, session.Actor(), next, 0)
	require.NoError(t, err)
	// 普通永久预算与实际预算都必须区分空闲结清和在途原持有。
	// Begin未派发时仍有原全局anchor；Prepare Start后改用同CAS捕获的
	// PriorBudgetBoundary。两者只认证旧来源，不能给当前操作补结清。
	_, err = claims.ReadCurrent(ctx, s)
	require.Error(t, err, "普通永久预算的空闲读者不得跨过新Active")
	held, err := claims.ReadActiveHeld(ctx, s)
	require.NoError(t, err)
	require.NoError(t, claims.RecheckActiveHeld(ctx, s, held))
	t.Run("ordinary-active-held-rejects-host-replacement", func(t *testing.T) {
		original, err := client.CoreV1().ConfigMaps(binding().Namespace).Get(ctx, held.Closed.HostClaim.Name, metav1.GetOptions{})
		require.NoError(t, err)
		changed := original.DeepCopy()
		changed.UID = "ordinary-active-held-same-name-new-uid"
		require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), changed, changed.Namespace))
		defer func() {
			require.NoError(t, client.Tracker().Update(core.SchemeGroupVersion.WithResource("configmaps"), original, original.Namespace))
		}()
		require.Error(t, claims.RecheckActiveHeld(ctx, s, held), "同数值、同名Host不能取代原UID")
		current, err := store.Load(ctx)
		require.NoError(t, err)
		require.Equal(t, s.State(), current.State(), "读者拒绝不得改变原WAL或结清水位")
	})
	_, err = worker.Advance(ctx)
	require.NoError(t, err)
	prepared := budgetStep(t, store, journal.PrepareBudget)
	require.Equal(t, journal.Settled, prepared.Phase)
	var shrink budget.Prepared
	require.NoError(t, json.Unmarshal(prepared.Observation.Data, &shrink))
	require.Equal(t, next, shrink.Request)
	require.Equal(t, p.Request.TargetVMMemoryBytes, shrink.VMBeforeMemoryBytes)
	require.NoError(t, claims.CheckOutside(ctx, shrink))
	latest, err := store.Load(ctx)
	require.NoError(t, err)
	_, err = claims.ReadCurrent(ctx, latest)
	require.Error(t, err, "Prepare已完成也不恢复旧全局结清资格")
	held, err = claims.ReadActiveHeld(ctx, latest)
	require.NoError(t, err)
	require.NoError(t, claims.RecheckActiveHeld(ctx, latest, held))
	for _, step := range latest.State().Active.Steps[1:] {
		require.Equal(t, journal.Planned, step.Phase)
	}
}
