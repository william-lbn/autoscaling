package owner

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

func TestProductionShrinkActualRecoveryKeepsLargerVMAndOriginalUnknown(t *testing.T) {
	productionInitialService(t, "actual-shrink-recover")
}

// 前驱Grow和本Shrink的Prepare/Quiesce/PG/Release/Guest均走生产执行器，
// 不手填Completed。只让明确PG peer提供原生事实；Runner文件WAL/真实
// SDK/Session/管理/UID-RV CAS参与组合，不冒充Linux物理释放。本轮normal
// 保持512MiB，安全降低normal仍属P2，不借该窗口宣称降限已经实现。
func verifyProductionShrinkActualRecovery(t *testing.T, ctx context.Context, api *ownerAPI, core *kubefake.Clientset, coord coordclient.CoordinationV1Interface, store *journal.Store, session *Session, resources *ResourceWorker, worker *PGWorker, source budget.QualificationSource, grow *initialServicePG, root *guestRootFixture, runner *vmguard.Runner, qmp *vmQMPPeer) {
	t.Helper()
	progress, err := worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress, "原正常Grow由生产worker Finish")
	s, err := store.Load(ctx)
	require.NoError(t, err)
	require.Nil(t, s.State().Active)
	first := s.State().History[len(s.State().History)-1].Operation.Request
	full, err := s.State().BudgetFor(first)
	require.NoError(t, err)
	require.EqualValues(t, 2<<30, full.Request.TargetVMMemoryBytes)
	node, err := core.CoreV1().Nodes().Get(ctx, "node-one", metav1.GetOptions{})
	require.NoError(t, err)
	var ledger hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
	allocator, err := hostguard.NewStore(core.CoreV1(), core.CoordinationV1(), ledger.Binding, "host-fixture-holder")
	require.NoError(t, err)
	publisher, err := hostexec.New(core.CoreV1(), coord, store, allocator, session.Ownership().LeaseUID, 64<<20)
	require.NoError(t, err)
	_, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	request := first
	request.Kind, request.TargetBuffers, request.TargetVMMemoryBytes = control.Shrink, 512, 1<<30
	request.Key.Revision++
	request.Key.OperationID.Low++
	request.ExpectedCapacityGeneration = full.Shared.Observed.CapacityGeneration
	request.BudgetCommitmentID += "-closed-shrink-actual"
	_, _, err = store.Begin(ctx, s, session.Actor(), request, 1)
	require.NoError(t, err)
	// Prepare沿原平台资格生产，不能拷贝一份旧Prepared或注入 observation。
	progress, err = worker.Advance(ctx)
	require.NoError(t, err)
	require.Equal(t, pgexec.Recorded, progress)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	prepared, _, err := s.State().OriginalServicePrepared()
	require.NoError(t, err)
	require.Equal(t, request, prepared.Request)
	require.EqualValues(t, 512<<20, prepared.Limits.MemoryMaxBytes)
	var shaping partialPGPeer
	grow.mu.Lock()
	shaping.admission = *grow.gate
	grow.mu.Unlock()
	shaping.configure(t, prepared)
	var native initialServicePG
	native.request, native.before, native.after, native.store = request, shaping.before, shaping.after, store
	opening := shaping.admission
	native.gate = &opening
	shrinkConfig := pgConfig(t, request, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dsg/v1/work/dsm":
			pgReply(w, vmDSM(native.shared().Observed))
		case "/dsg/v1/resize":
			var received control.Request
			require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
			require.Equal(t, request, received)
			require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizePG).Phase)
			native.mu.Lock()
			native.resized = true
			native.resizePosts++
			current := native.current().Observed
			response := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: request.Key, Disposition: control.ReceiptReplayed, HasResult: true, Result: &current}, Observed: current}
			require.NoError(t, response.ValidateResize(request, request.Key.Instance))
			native.resizeReceipt = &response
			native.mu.Unlock()
			pgReply(w, response)
		default:
			native.serve(t, w, r)
		}
	}, func() control.Snapshot { return native.shared().Observed })
	worker.CloseIdleConnections()
	worker, err = session.NewProductionPGWorker(ctx, shrinkConfig, resources, core.CoreV1(), source)
	require.NoError(t, err)
	defer worker.CloseIdleConnections()
	_, err = publisher.Reconcile(ctx)
	require.NoError(t, err)
	for _, action := range []journal.Action{journal.Quiesce, journal.ResizePG, journal.ReleasePG, journal.GuestReady} {
		advancePartialStep(t, ctx, store, worker, action, journal.Settled)
	}
	advancePartialStep(t, ctx, store, worker, journal.ResizeVM, journal.Unconfirmed)
	before, err := store.Load(ctx)
	require.NoError(t, err)
	original := before.State()
	root.mu.Lock()
	rootWrites := root.posts
	root.view.Actual.MemoryMaxBytes++
	root.mu.Unlock()
	_, err = worker.Advance(ctx)
	require.NoError(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 17, s.State().ClosureFormatVersion())
	require.False(t, s.State().ServiceClosure.Pending())
	require.Equal(t, original.Active, s.State().Active)
	_, _, _, selector := s.State().ServiceClosure.Selection()
	root.mu.Lock()
	root.view.Actual.MemoryMaxBytes--
	root.mu.Unlock()
	var reopening serviceReopenPG
	reopening.mode, reopening.api, reopening.root = "normal", api, root
	native.mu.Lock()
	native.reopenFixture = &reopening
	native.mu.Unlock()
	_, err = worker.executeControl(ctx, closureControl(session, shrinkConfig.Instance, control.CoordinationReopenService, selector))
	require.ErrorIs(t, err, ErrServiceSafety, "原VM未收到时只保存独立取消选择")
	s, err = store.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, s.State().ServiceClosure.PGRecovery)
	require.NotNil(t, s.State().ServiceClosure.VMRecovery)
	require.Nil(t, s.State().ServiceClosure.ActualRecovery)
	require.NoError(t, runner.Tick(ctx))
	_, err = worker.executeControl(ctx, closureControl(session, shrinkConfig.Instance, control.CoordinationReopenService, selector))
	require.NoError(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	selected := s.State()
	actual := selected.ServiceClosure.ActualRecovery
	require.NotNil(t, actual)
	require.Equal(t, original.Active, selected.Active)
	require.Equal(t, original.BudgetHeld, selected.BudgetHeld)
	require.EqualValues(t, 2<<30, actual.Capacity.VMMemoryBytes)
	require.EqualValues(t, 1<<30, actual.Capacity.Request.TargetVMMemoryBytes)
	require.EqualValues(t, 512, actual.Capacity.Buffers)
	require.Equal(t, request.ExpectedCapacityGeneration+1, actual.Capacity.CapacityGeneration)
	require.Equal(t, full.Sources.OutsideClaim, actual.Capacity.OutsideClaim, "实际VM仍大，只能采用旧大VM规格Outside")
	require.NotEqual(t, *prepared.OutsideClaim, actual.Capacity.OutsideClaim)
	require.Equal(t, full.GuestUsableMemoryBytes, actual.Capacity.GuestUsableMemoryBytes)
	require.True(t, actual.Capacity.VM.Cancelled)
	require.NotEmpty(t, actual.Capacity.VM.GuestSHA256, "Shrink原VM命令必须绑定原Guest审计")
	require.Nil(t, actual.Guest, "原Guest已完整执行，不能另造Guest取消")
	require.Equal(t, journal.Settled, selected.ServiceClosure.Reopen.Phase)
	r := selected.ServiceClosure.Reopen
	_, err = worker.executeControl(ctx, closureControl(session, shrinkConfig.Instance, control.CoordinationCompleteServiceReopen, r.CommandSHA256))
	require.NoError(t, err)
	s, err = store.Load(ctx)
	require.NoError(t, err)
	done := s.State()
	require.Nil(t, done.Active)
	require.Nil(t, done.ServiceClosure)
	require.Nil(t, done.BudgetHeld)
	require.Equal(t, *original.Active, done.CapacityHeld.ActiveRecovery.Original)
	require.Equal(t, *actual, done.CapacityHeld.ActiveRecovery.Proof)
	require.Equal(t, journal.RecoveredWithActualBudget, done.History[len(done.History)-1].Outcome)
	require.NoError(t, done.CheckClosedCapacity(request))
	_, err = publisher.SettleCurrent(ctx)
	require.NoError(t, err)
	settled, err := allocator.Load(ctx)
	require.NoError(t, err)
	placement, err := settled.State().Placement("runner-uid")
	require.NoError(t, err)
	require.EqualValues(t, 2<<30, placement.MemoryBytes, "PG缩小不授权按未执行的较小VM目标退Host")
	root.mu.Lock()
	require.Equal(t, rootWrites, root.posts)
	require.Zero(t, root.cancellations)
	root.mu.Unlock()
	native.mu.Lock()
	require.Equal(t, 1, native.resizePosts)
	require.Equal(t, 1, native.reopenFixture.posts)
	native.mu.Unlock()
	qmp.mu.Lock()
	require.Equal(t, 1, qmp.writes, "仅原Grow有QMP写，Shrink取消不重发")
	require.EqualValues(t, 2<<30, qmp.actual)
	qmp.mu.Unlock()
}
