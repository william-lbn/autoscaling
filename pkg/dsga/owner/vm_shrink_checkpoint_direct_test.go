package owner

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

// 复用原vmCore/Root/PG/Host/Unix-QMP helper；只新增本checkpoint直接断言。
// 完整PG释放前驱为原协议peer，回调nil只是组件顺序观察，不签Owner完整来源/物理E。
func TestVMShrinkAndObserveAuditCheckpointsDirect(t *testing.T) {
	for _, mode := range []string{"complete", "resize-pre-reject", "resize-post-reject", "observe-pre-reject", "observe-post-reject", "runner-unknown", "gate-reopened", "root-drift", "resize-pre-rv", "resize-post-rv", "observe-pre-rv", "observe-post-rv", "resize-pre-actor", "resize-post-actor", "resize-pre-cancel", "resize-post-cancel", "legacy-nil"} {
		t.Run(mode, func(t *testing.T) { runVMShrinkCheckpoint(t, mode) })
	}
}
func runVMShrinkCheckpoint(t *testing.T, mode string) {
	t.Helper()
	api, store, coord := newAPI(t)
	client := vmCore(t)
	api.mu.Lock()
	cpu := vmv1.CpuScalingModeSysfs
	api.vm.Spec.CpuScalingMode = &cpu
	api.vm.Spec.ServiceAccountName = "dsga-runner-test"
	api.vm.Spec.Guest.MemorySlotSize = *resource.NewQuantity(256<<20, resource.BinarySI)
	api.vm.Spec.Guest.MemorySlots.Min = 2
	api.vm.Spec.Guest.MemorySlots.Max = 8
	api.vm.Spec.Guest.MemorySlots.Use = 8
	api.vm.Status.Node = "node-one"
	api.vm.Status.MemorySize = resource.NewQuantity(2<<30, resource.BinarySI)
	api.mu.Unlock()
	dir := ownerSocketTempDir(t, "dsga-vm-checkpoint-")
	runner, err := vmguard.Initialize(context.Background(), store, client.CoreV1(), coord, vmguard.Config{StateDir: dir, PodName: "runner", PodUID: "runner-uid", NodeName: "node-one", MinimumBytes: 512 << 20, MaximumBytes: 2 << 30})
	require.NoError(t, err)
	root, resourceConfig := guestRoot(t, store)
	root.mu.Lock()
	root.view.Bootstrap.Limits.MemoryMaxBytes = 512 << 20
	root.view.Actual.MemoryMaxBytes = 512 << 20
	root.view.Actual.GuestMemTotalBytes = (2 << 30) - (256 << 20)
	root.mu.Unlock()
	args := runner.QMPArguments()
	peer := &vmQMPPeer{uuid: args[1], minimum: 512 << 20, maximum: 2 << 30, actual: 2 << 30, mu: sync.Mutex{}, writes: 0, lose: false, onResize: func(actual uint64) {
		require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
		root.mu.Lock()
		root.view.Actual.GuestMemTotalBytes = control.Uint64(actual - (256 << 20))
		root.mu.Unlock()
		api.mu.Lock()
		api.vm.Status.MemorySize = resource.NewQuantity(int64(actual), resource.BinarySI)
		api.vmVersion++
		api.vm.ResourceVersion = fmt.Sprintf("vm-checkpoint-%d", api.vmVersion)
		api.mu.Unlock()
	}}
	vmPeer(t, filepath.Join(dir, "qmp.sock"), peer)
	elector, err := New(coord, store, DefaultOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		resources, err := session.NewResourceWorker(ctx, resourceConfig)
		require.NoError(t, err)
		defer resources.CloseIdleConnections()
		req := pgRequest(t, session.Actor())
		req.Kind = control.Shrink
		req.TargetBuffers = 512
		p, current := preparedGuest(t, req, root.view.Identity)
		p.Shared.Observed.AdmissionOpen = false
		p.Shared.Observed.AdmissionRevision = 5
		current.Observed.AdmissionOpen = false
		current.Observed.AdmissionRevision = 5
		var reopen atomic.Bool
		config := pgConfig(t, req, func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/dsg/v1/budget/shared":
				pgReply(w, current)
			case "/dsg/v1/work/dsm":
				d := vmDSM(current.Observed)
				if reopen.Load() {
					c := &d.Prepared.Connections
					c.RequestedSerial = 3
					c.AppliedSerial = 3
					c.RequestedOpen = true
					c.AppliedOpen = true
					c.PublicConnectionsBlocked = false
					c.Work.Observed.AdmissionOpen = true
					c.Work.ForegroundIdle = false
				}
				pgReply(w, d)
			default:
				t.Errorf("unexpected VM checkpoint PG route %s", r.URL.Path)
				w.WriteHeader(500)
			}
		}, func() control.Snapshot { return current.Observed })
		worker, err := session.NewProductionPGWorker(ctx, config, resources, client.CoreV1())
		require.NoError(t, err)
		defer worker.CloseIdleConnections()
		require.NoError(t, runner.Tick(ctx))
		outside := budget.OutsideReservation{ProtocolVersion: 1, Request: req, Identity: p.Identity, Incarnation: binding().Incarnation, ResidualProfileID: "explicit-qualified-platform-fixture", SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true}
		outsideRef := vmReservation(t, client, "outside-original", budgetclaims.OutsideKind, outside)
		p.OutsideClaim = &outsideRef
		snapshot, err := store.Load(ctx)
		require.NoError(t, err)
		_, _, err = store.Begin(ctx, snapshot, session.Actor(), req, 1)
		require.NoError(t, err)
		priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.PrepareBudget, req.Key.Instance.ComputeInstanceID, p)
		publishVMHost(t, ctx, client, coord, store, session, "complete")
		root.mu.Lock()
		aggregate, err := budget.RootAggregateFromView(root.view)
		root.mu.Unlock()
		require.NoError(t, err)
		closed := budget.Quiesced{ProtocolVersion: 1, Request: req, Close: control.AdmissionRequest{ProtocolVersion: 1, Instance: req.Key.Instance, IssuerEpoch: req.Key.IssuerEpoch, Revision: 5, ExpectedCapacityGeneration: req.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: req.BudgetCommitmentID}, DSM: vmDSM(p.Shared.Observed), PGAggregate: aggregate}
		require.NoError(t, closed.ValidateFor(p, session.Actor().Epoch))
		priorBudgetDomain(t, ctx, store, session.Actor(), req, journal.Quiesce, req.Key.Instance.ComputeInstanceID, closed)
		for _, a := range []journal.Action{journal.ResizePG, journal.ReleasePG} {
			priorBudgetDomain(t, ctx, store, session.Actor(), req, a, req.Key.Instance.ComputeInstanceID, map[string]bool{"explicit_prior_native_release_fixture": true})
		}
		progress, err := worker.Advance(ctx)
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress, "original actual GuestReady")
		calls := 0
		trace := []string{}
		reject := errors.New("complete source callback rejected")
		deny := true
		stepCtx, stepCancel := context.WithCancel(ctx)
		defer stepCancel()
		after := func(callCtx context.Context, s *journal.Snapshot, shared control.SharedCostResponse) error {
			require.NoError(t, callCtx.Err())
			require.NoError(t, shared.ValidateFor(req.Key.Instance))
			calls++
			var cursor journal.Step
			for _, x := range s.State().Active.Steps {
				if x.Phase != journal.Settled {
					cursor = x
					break
				}
			}
			trace = append(trace, string(cursor.Action)+"/"+string(cursor.Phase))
			at := string(cursor.Action) + "-"
			if cursor.Phase == journal.Planned {
				at += "pre"
			} else {
				at += "post"
			}
			at = strings.ReplaceAll(at, "resize_vm", "resize")
			at = strings.ReplaceAll(at, "observe_vm", "observe")
			if !deny {
				return nil
			}
			if mode == at+"-reject" {
				return reject
			}
			if mode == at+"-rv" {
				api.mu.Lock()
				api.vmVersion++
				api.vm.ResourceVersion = fmt.Sprintf("checkpoint-rv-%d", api.vmVersion)
				api.mu.Unlock()
			}
			if mode == at+"-actor" {
				_, e := store.Takeover(callCtx, s, s.State().Owner, "successor")
				require.NoError(t, e)
			}
			if mode == at+"-cancel" {
				stepCancel()
			}
			return nil
		}
		advance := func() (pgexec.Progress, error) {
			if mode == "legacy-nil" {
				return worker.vm.Advance(stepCtx)
			}
			return worker.vm.AdvanceWithAudit(stepCtx, after)
		}
		if mode == "gate-reopened" {
			reopen.Store(true)
		}
		progress, err = advance()
		if strings.HasPrefix(mode, "resize-pre-") || mode == "gate-reopened" {
			require.Error(t, err)
			if strings.HasSuffix(mode, "-reject") {
				require.ErrorIs(t, err, reject)
			}
			require.Equal(t, journal.Planned, budgetStep(t, store, journal.ResizeVM).Phase)
			peer.mu.Lock()
			require.Zero(t, peer.writes)
			peer.mu.Unlock()
			if mode == "gate-reopened" {
				require.Zero(t, calls)
			}
			return nil
		}
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		if mode == "runner-unknown" {
			before := calls
			_, err = advance()
			require.Error(t, err)
			require.Equal(t, before, calls)
			require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
			peer.mu.Lock()
			require.Zero(t, peer.writes)
			peer.mu.Unlock()
		}
		require.NoError(t, runner.Tick(ctx))
		if mode == "root-drift" {
			root.mu.Lock()
			root.view.Actual.MemoryMaxBytes++
			root.mu.Unlock()
		}
		progress, err = advance()
		if strings.HasPrefix(mode, "resize-post-") || mode == "root-drift" {
			require.Error(t, err)
			require.Equal(t, journal.Unconfirmed, budgetStep(t, store, journal.ResizeVM).Phase)
			peer.mu.Lock()
			require.Equal(t, 1, peer.writes)
			peer.mu.Unlock()
			if mode == "resize-post-reject" {
				require.ErrorIs(t, err, reject)
				deny = false
				progress, err = advance()
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
				peer.mu.Lock()
				require.Equal(t, 1, peer.writes)
				peer.mu.Unlock()
			}
			return nil
		}
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		progress, err = advance()
		if strings.HasPrefix(mode, "observe-pre-") || strings.HasPrefix(mode, "observe-post-") {
			require.Error(t, err)
			phase := journal.Planned
			if strings.HasPrefix(mode, "observe-post-") {
				phase = journal.Unconfirmed
			}
			require.Equal(t, phase, budgetStep(t, store, journal.ObserveVM).Phase)
			if mode == "observe-post-reject" {
				require.ErrorIs(t, err, reject)
				deny = false
				progress, err = advance()
				require.NoError(t, err)
				require.Equal(t, pgexec.Recorded, progress)
			}
			peer.mu.Lock()
			require.Equal(t, 1, peer.writes)
			peer.mu.Unlock()
			return nil
		}
		require.NoError(t, err)
		require.Equal(t, pgexec.Recorded, progress)
		require.Equal(t, journal.Settled, budgetStep(t, store, journal.ObserveVM).Phase)
		peer.mu.Lock()
		require.Equal(t, 1, peer.writes)
		peer.mu.Unlock()
		if mode == "legacy-nil" {
			require.Zero(t, calls)
		} else if calls != 4 {
			t.Fatalf("original Shrink/Observe full-audit checkpoints missing: callbacks=%d trace=%v", calls, trace)
		}
		return nil
	})
	if strings.HasSuffix(mode, "-actor") {
		require.True(t, err == nil || errors.Is(err, ErrLeadershipLost), "only original Actor loss expected")
	} else {
		require.NoError(t, err)
	}
}
