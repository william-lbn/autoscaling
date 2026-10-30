package pgexec

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 真实执行器/SDK/Typed UID-RV CAS；PG与非PG资源仍为明确领域peer。
// 本组只闭合拒绝隔离证明的持久/重建，不把类别17资源在途写成已恢复服务。
func TestServicePGRejectionRecoveryPersistsBeforeSDKReconcile(t *testing.T) {
	for _, mode := range []string{"normal", "lost-proof-reply", "same-owner", "source-changed", "wrong-selector"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			intent := request(t, control.Grow)
			raw, err := os.ReadFile("../journal/testdata/initial-domain.json")
			require.NoError(t, err)
			var fixture struct {
				Initial budget.Initial                 `json:"initial"`
				Command budget.InitialAdmissionCommand `json:"command"`
				Receipt budget.InitialAdmissionReceipt `json:"receipt"`
			}
			require.NoError(t, json.Unmarshal(raw, &fixture))
			intent.Key.Instance = fixture.Initial.Instance
			api, store := prepareStore(t, intent, func(store *journal.Store, s *journal.Snapshot) *journal.Snapshot {
				actor := s.State().Owner
				s, err = store.BindResource(ctx, s, actor, fixture.Initial.PGAggregate.Identity)
				require.NoError(t, err)
				s, err = store.BindPG(ctx, s, actor, fixture.Initial.Instance)
				require.NoError(t, err)
				s, err = store.BindInitialBudget(ctx, s, actor, fixture.Initial)
				require.NoError(t, err)
				s, _, err = store.StartInitialAdmission(ctx, s, actor, fixture.Command)
				require.NoError(t, err)
				s, err = store.RecordInitialAdmission(ctx, s, actor, fixture.Receipt)
				require.NoError(t, err)
				return s
			})
			rejected := response(t, intent)
			rejected.Receipt.Disposition, rejected.Receipt.HasResult, rejected.Receipt.Result = control.ReceiptBusy, false, nil
			rejected.Observed.CapacityGeneration, rejected.Observed.Operation = intent.ExpectedCapacityGeneration, nil
			old, _ := computeClient(t, intent, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/dsg/v1/resize", r.URL.Path)
				send(w, rejected)
			})
			s, err := store.Load(ctx)
			require.NoError(t, err)
			executor, err := New(store, old, s.State().Owner)
			require.NoError(t, err)
			progress, err := executor.Advance(ctx)
			require.NoError(t, err)
			require.Equal(t, Recorded, progress)
			s, err = store.Load(ctx)
			require.NoError(t, err)
			closing := control.AdmissionRequest{ProtocolVersion: 1, Instance: intent.Key.Instance, IssuerEpoch: s.State().Owner.Epoch, Revision: 100, ExpectedCapacityGeneration: intent.ExpectedCapacityGeneration, Open: false, BudgetCommitmentID: intent.BudgetCommitmentID}
			s, _, err = store.StartActiveServiceClosure(ctx, s, s.State().Owner, closing)
			require.NoError(t, err)
			closed := rejected.Observed
			closed.AdmissionOpen, closed.AdmissionRevision = false, closing.Revision
			hash, err := closing.ContextSHA256()
			require.NoError(t, err)
			s, err = store.RecordServiceClosure(ctx, s, s.State().Owner, control.AdmissionResponse{ProtocolVersion: 1, VMShrinkPermit: false, Request: closing, ContextSHA256: hash, Disposition: control.AdmissionApplied, Observed: closed}, false)
			require.NoError(t, err)
			if mode != "same-owner" {
				s, err = store.Takeover(ctx, s, s.State().Owner, "recovery-owner")
				require.NoError(t, err)
			}
			original := s.State()
			actor := original.Owner
			closed.IssuerFloorEpoch = actor.Epoch
			var reads, mutations atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/dsg/v1/capabilities":
					history, provider, boot := uint32(16), true, intent.Key.Instance.PGBootID
					send(w, control.Capabilities{ProtocolVersion: 1, ProductionReady: false, VMShrinkPermit: false, Supported: true, BufferAPIVersion: 6, Enabled: true, InstanceBound: true, HistoryCapacity: &history, ProviderCanRelease: &provider, Instance: &control.DiscoveryIdentity{VMUID: intent.Key.Instance.VMUID, ComputeInstanceID: intent.Key.Instance.ComputeInstanceID, PGBootID: &boot}})
				case "/dsg/v1/issuer/fence":
					send(w, control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: intent.Key.Instance, RequestedEpoch: actor.Epoch, Disposition: control.Fenced, Observed: closed})
				case "/dsg/v1/resize/status":
					reads.Add(1)
					if mode == "source-changed" {
						api.mu.Lock()
						api.version++
						api.vm.ResourceVersion += "-changed"
						api.mu.Unlock()
					}
					fact := rejected
					fact.Receipt.Disposition, fact.Observed = control.ReceiptNotFound, closed
					send(w, fact)
				default:
					mutations.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			defer server.Close()
			config := compute.Config{BaseURL: server.URL, Instance: intent.Key.Instance, IssuerEpoch: actor.Epoch, Timeout: 5 * time.Second, Transport: server.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return "fixture-token", nil }, Pending: &intent, AdmissionPending: nil, AdmissionClose: nil, AdmissionContinuation: nil}
			client, err := compute.New(ctx, config)
			require.NoError(t, err)
			defer client.CloseIdleConnections()
			executor, err = New(store, client, actor)
			require.NoError(t, err)
			_, _, _, selector := original.ServiceClosure.Selection()
			if mode == "wrong-selector" {
				selector += "-wrong"
			}
			api.mu.Lock()
			api.loseNext = mode == "lost-proof-reply"
			api.mu.Unlock()
			confirmed, err := executor.RecoverServiceRejection(ctx, s, selector)
			require.Equal(t, &intent, client.Pending(), "保存屏障本身不得提前清SDK栅栏")
			if mode == "same-owner" || mode == "source-changed" || mode == "wrong-selector" {
				require.Error(t, err)
				require.Nil(t, confirmed)
				require.Zero(t, mutations.Load())
				return
			}
			if mode == "lost-proof-reply" {
				var lost *journal.UnconfirmedWrite
				require.ErrorAs(t, err, &lost)
				require.Nil(t, confirmed)
			} else {
				require.NoError(t, err)
			}
			s, err = store.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, original.Active, s.State().Active)
			require.NotNil(t, s.State().ServiceClosure.PGRejection)
			if mode == "lost-proof-reply" {
				client.CloseIdleConnections()
				client, err = compute.New(ctx, config)
				require.NoError(t, err)
				defer client.CloseIdleConnections()
				executor, err = New(store, client, actor)
				require.NoError(t, err)
			}
			require.NoError(t, executor.reconcileRejectedService(ctx, s.State()))
			require.Nil(t, client.Pending())
			require.Error(t, executor.ReconcileServiceRecovery(ctx), "17的完整资源恢复仍单独阻断")
			require.Positive(t, reads.Load())
			require.Zero(t, mutations.Load(), "拒绝恢复没有Resize/Cancel/Admission setter")
		})
	}
}
