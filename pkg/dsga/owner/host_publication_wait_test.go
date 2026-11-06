package owner

import (
	"context"
	"encoding/json"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	ktesting "k8s.io/client-go/testing"
	"testing"
	"time"
)

func TestProductionHostPublicationWaitRunsFreshLeaseAudit(t *testing.T) {
	checkHostPublicationWait(t, "host-publication-wait", []string{"healthy", "outside-missing", "other-host-unknown"})
}

func TestProductionEnforcedBudgetWaitsForHost(t *testing.T) {
	checkHostPublicationWait(t, "enforced-budget-host-publication", []string{"healthy", "outside-timeout", "outside-missing", "other-host-unknown"})
}

func checkHostPublicationWait(t *testing.T, fixture string, modes []string) {
	t.Helper()
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			productionInitialServiceWithFixtureHook(t, fixture, func(t *testing.T, ctx context.Context, api *ownerAPI, store *journal.Store, core *kubefake.Clientset, coord coordclient.CoordinationV1Interface, session *Session, resources *ResourceWorker, worker *PGWorker, config compute.Config, source budget.QualificationSource, pg *initialServicePG, root *guestRootFixture, runner *vmguard.Runner, qmp *vmQMPPeer, request control.Request) bool {
				before, err := store.Load(ctx)
				require.NoError(t, err)
				require.NotNil(t, before.State().InitialHeld)
				require.Equal(t, journal.Settled, budgetStep(t, store, journal.PrepareBudget).Phase)
				require.Equal(t, journal.Completed, budgetStep(t, store, journal.PrepareBudget).Observation.Result)
				require.Equal(t, journal.Planned, budgetStep(t, store, journal.ReserveHost).Phase)
				name, err := budgetclaims.HostClaimName(session.binding, request)
				require.NoError(t, err)
				_, err = core.CoreV1().ConfigMaps(session.binding.Namespace).Get(ctx, name, metav1.GetOptions{})
				require.True(t, apierrors.IsNotFound(err), "new Host publication must really be absent")
				f := &guestAuditFixture{ctx: ctx, api: api, store: store, core: core, session: session, worker: worker, pg: pg, root: root, runner: runner, qmp: qmp}
				enforced := fixture == "enforced-budget-host-publication"
				if enforced {
					require.Equal(t, control.AdmissionPolicyEnforcedBudget, worker.client.AdmissionPolicy())
				} else {
					armGuestAuditClock(f, "healthy")
				}
				pg.mu.Lock()
				readsBefore, postsBefore := pg.budgetMemoryReads, pg.posts
				pg.mu.Unlock()
				var prepared budget.Prepared
				require.NoError(t, json.Unmarshal(budgetStep(t, store, journal.PrepareBudget).Observation.Data, &prepared))
				require.NotNil(t, prepared.OutsideClaim)
				if mode == "outside-missing" || mode == "outside-timeout" {
					missing := prepared.OutsideClaim.Name
					core.PrependReactor("get", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
						if a.(ktesting.GetAction).GetName() == missing {
							if mode == "outside-timeout" {
								return true, nil, apierrors.NewTimeoutError("budget read delayed", 1)
							}
							return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, missing)
						}
						return false, nil, nil
					})
				}
				if mode == "other-host-unknown" {
					core.PrependReactor("get", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
						if a.(ktesting.GetAction).GetName() == name {
							return true, nil, resourceguard.ErrUnknown
						}
						return false, nil, nil
					})
				}
				runctx, cancel := context.WithTimeout(ctx, 45*time.Second)
				defer cancel()
				var tick Tick
				observed := false
				actionStart := len(core.Actions())
				err = worker.Run(runctx, PGLoopOptions{PollInterval: time.Millisecond, EnableInFlightGrow: true, Observe: func(next Tick) { tick = next; observed = true; cancel() }})
				require.True(t, observed, "must reach actual Run advance/audit observation")
				require.ErrorIs(t, err, context.Canceled)
				pg.mu.Lock()
				memoryReads, postsAfter := pg.budgetMemoryReads, pg.posts
				renewed := pg.leaseRenewals
				resizePosts := pg.resizePosts
				pg.mu.Unlock()
				if mode == "healthy" {
					if enforced {
						require.Greater(t, memoryReads, readsBefore, "完整来源复核后必须读回同一 PG")
						require.Zero(t, renewed)
					} else if renewed != 1 {
						t.Fatalf("Host publication waiting must renew exactly once: got %d", renewed)
					}
					require.NoError(t, tick.Err)
					require.Equal(t, pgexec.AwaitingFact, tick.Progress)
				} else {
					require.Zero(t, renewed, "bad source/other Unknown cannot renew")
					if enforced && mode == "outside-missing" {
						// 正式保护关闭本身可以成功；直接核验其写前记录、真实门禁及保留的原预算。
						require.NoError(t, tick.Err)
						require.Equal(t, pgexec.Recorded, tick.Progress)
					} else {
						require.Error(t, tick.Err)
					}
					if mode == "other-host-unknown" {
						require.True(t, errors.Is(tick.Err, resourceguard.ErrUnknown))
					}
				}
				after, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, request.Key, after.State().Active.Request.Key)
				if enforced && (mode == "healthy" || mode == "outside-timeout") {
					require.True(t, pg.shared().Observed.AdmissionOpen)
					require.Equal(t, postsBefore, postsAfter, "管理等待不会给 SQL 发关闭")
				}
				if enforced && mode == "outside-missing" {
					require.NotNil(t, after.State().ServiceClosure, "原来源确实消失时仍须隔离")
					require.Equal(t, journal.Settled, after.State().ServiceClosure.Phase)
					require.False(t, pg.shared().Observed.AdmissionOpen)
					require.Equal(t, postsBefore+1, postsAfter)
					require.Equal(t, before.State().InitialHeld, after.State().InitialHeld)
				}
				if mode == "healthy" || mode == "other-host-unknown" || mode == "outside-timeout" {
					require.Equal(t, before.State(), after.State(), "publication wait cannot write WAL")
				}
				for _, action := range []journal.Action{journal.ReserveHost, journal.ResizeVM, journal.GuestReady, journal.ResizePG, journal.FinalizeBudget, journal.ResumeAdmission} {
					require.Equal(t, journal.Planned, budgetStep(t, store, action).Phase)
				}
				require.Zero(t, resizePosts)
				for _, action := range core.Actions()[actionStart:] {
					if action.GetResource().Resource == "configmaps" || action.GetResource().Resource == "nodes" {
						require.NotContains(t, []string{"create", "update", "patch", "delete"}, action.GetVerb(), "Agent consumer cannot allocate/modify original Host source")
					}
				}
				_, err = core.CoreV1().ConfigMaps(session.binding.Namespace).Get(ctx, name, metav1.GetOptions{})
				require.Error(t, err)
				return true
			})
		})
	}
}
