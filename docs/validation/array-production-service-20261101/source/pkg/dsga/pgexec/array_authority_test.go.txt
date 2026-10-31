package pgexec

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
)

func arrayExecutorResourceView(t *testing.T, intent control.Request) resourceguard.View {
	t.Helper()
	id := resourceguard.Identity{VMUID: intent.Key.Instance.VMUID, ComputeInstanceID: intent.Key.Instance.ComputeInstanceID, GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3}
	limits := resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 0}}}
	view := resourceguard.View{ProtocolVersion: 1, ArrayAuthorityProtocolVersion: 1, Identity: id, IssuerFloorEpoch: 1, Bootstrap: resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: id, IssuerEpoch: 1, BudgetCommitmentID: intent.BudgetCommitmentID, Limits: limits}, BootstrapApplied: true, Birth: &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: id, PostmasterPID: 123, PostmasterStartTicks: 456}, Actual: resourceguard.Evidence{ProtocolVersion: 1, Identity: id, MemoryMaxBytes: limits.MemoryMaxBytes, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: 768 << 20, HugePools: limits.HugePools, HugeOvercommitDisabled: true, CgroupPopulated: true}}
	require.NoError(t, view.Validate())
	_, e := view.ConfirmedLimits()
	require.NoError(t, e)
	return view
}
func arrayExecutorQuiesced(t *testing.T, intent control.Request, view resourceguard.View) (budget.Prepared, budget.QuiesceCommand, budget.Quiesced) {
	t.Helper()
	aggregate, e := budget.RootAggregateFromView(view)
	require.NoError(t, e)
	snapshot := response(t, intent).Observed
	snapshot.Operation = nil
	snapshot.LowBuffers = 2048
	snapshot.HighBuffers = 2048
	snapshot.CapacityGeneration = intent.ExpectedCapacityGeneration
	snapshot.AdmissionRevision = 1
	snapshot.AdmissionOpen = false
	shared := control.SharedCostResponse{ProtocolVersion: 1, StartupMainUpperBytes: 16 << 20, StartupArraysUpperBytes: 16 << 20, CurrentMainAndArraysUpperBytes: 80 << 20, Observed: snapshot}
	bound := func(id string, n control.Uint64) budget.Bound {
		return budget.Bound{Kind: budget.CommittedUpper, SourceID: id, UpperBytes: n}
	}
	prepared := budget.Prepared{ProtocolVersion: 1, Request: intent, Identity: view.Identity, RunnerPodName: "explicit-resource-model", VMBeforeMemoryBytes: 2 << 30, GuestUsableMemoryBytes: 768 << 20, Shared: shared, TargetSharedUpperBytes: 80 << 20, Limits: view.Bootstrap.Limits, SystemOutsidePG: bound("explicit-system-model", 24<<20), Transition: bound("explicit-transition-model", 4<<20), Safety: bound("explicit-safety-model", 4<<20)}
	close := control.AdmissionRequest{ProtocolVersion: 1, Instance: intent.Key.Instance, IssuerEpoch: 1, Revision: 1, ExpectedCapacityGeneration: intent.ExpectedCapacityGeneration, BudgetCommitmentID: intent.BudgetCommitmentID}
	quiesced := budget.Quiesced{ProtocolVersion: 1, Request: intent, Close: close, PGAggregate: aggregate, DSM: control.DSMResponse{ProtocolVersion: 1, DSMAPIVersion: 1, CurrentBootOnly: true, AccountingValid: true, PageBytes: 4096, ControlUpperBytes: 4096, ExternalSegments: 1, UnknownSegments: 1, ReservedUpperBytes: 4096, Prepared: control.PreparedResponse{ProtocolVersion: 1, PreparedAPIVersion: 1, PreparedSlots: 1, MaximumPreparedSlots: 4, RuntimePreparesBlocked: true, Connections: control.ConnectionResponse{ProtocolVersion: 1, ConnectionAPIVersion: 1, Enabled: true, RequestedSerial: 2, AppliedSerial: 2, Acknowledged: true, PrivateManagementReady: true, PublicConnectionsBlocked: true, Work: control.WorkResponse{ProtocolVersion: 1, WorkAPIVersion: 2, ForegroundIdle: true, ObserverExcluded: true, BackgroundProcesses: 3, AuxiliaryProcesses: 2, Observed: snapshot}}}}}
	command := budget.QuiesceCommand{ProtocolVersion: 1, Request: intent, Close: close, PGAggregate: aggregate}
	require.NoError(t, prepared.ValidateFor(intent, view.Identity))
	require.NoError(t, quiesced.ValidateFor(prepared, 1))
	require.NoError(t, command.ValidateFor(prepared, 1))
	return prepared, command, quiesced
}

// PG17-U-ARRAY-EXECUTOR-001：真实typed HTTP UID/RV Start CAS；Root/PG/issuer
// 事实明确为模型。required=1时Shrink仍走原quiesce+DSM而不申请新增收费；Grow
// 只有Start已持久后才请求准确正文凭据，失败保Unconfirmed且不派发PG。
func TestArrayAuthorityExecutorEnabledGrowAndShrinkStartReplay(t *testing.T) {
	for _, kind := range []control.Kind{control.Grow, control.Shrink} {
		t.Run(string(kind), func(t *testing.T) {
			intent := request(t, kind)
			view := arrayExecutorResourceView(t, intent)
			_, store := prepareUnstartedStore(t, intent)
			var dsm control.DSMResponse
			if kind == control.Shrink {
				prepared, command, quiesced := arrayExecutorQuiesced(t, intent, view)
				dsm = quiesced.DSM
				snapshot, e := store.Load(context.Background())
				require.NoError(t, e)
				for _, step := range snapshot.State().Active.Steps {
					if step.Action == journal.ResizePG {
						break
					}
					cmdRaw := []byte(`{"explicit_non_pg_model":true}`)
					data := cmdRaw
					if step.Action == journal.PrepareBudget {
						data, e = json.Marshal(prepared)
						require.NoError(t, e)
					}
					if step.Action == journal.Quiesce {
						cmdRaw, e = json.Marshal(command)
						require.NoError(t, e)
						data, e = json.Marshal(quiesced)
						require.NoError(t, e)
					}
					var dispatch *journal.Dispatch
					snapshot, dispatch, e = store.Start(context.Background(), snapshot, snapshot.State().Owner, intent.Key, step.Action, intent.Key.Instance.ComputeInstanceID, cmdRaw)
					require.NoError(t, e)
					snapshot, e = store.Record(context.Background(), snapshot, snapshot.State().Owner, intent.Key, journal.Observation{Key: intent.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: intent.Key.Instance.ComputeInstanceID, Data: data})
					require.NoError(t, e)
				}
			} else { // Grow也只准备原计划PG之前的领域，不改变Start CAS入口。
				snapshot, e := store.Load(context.Background())
				require.NoError(t, e)
				for _, step := range snapshot.State().Active.Steps {
					if step.Action == journal.ResizePG {
						break
					}
					var dispatch *journal.Dispatch
					snapshot, dispatch, e = store.Start(context.Background(), snapshot, snapshot.State().Owner, intent.Key, step.Action, intent.Key.Instance.ComputeInstanceID, []byte(`{"explicit_non_pg_model":true}`))
					require.NoError(t, e)
					snapshot, e = store.Record(context.Background(), snapshot, snapshot.State().Owner, intent.Key, journal.Observation{Key: intent.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: intent.Key.Instance.ComputeInstanceID, Data: []byte(`{"explicit_non_pg_model":true}`)})
					require.NoError(t, e)
				}
			}
			var authority, resizes atomic.Int32
			resourceServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/dsg/v1/resource/state":
					send(w, view)
				case "/dsg/v1/resource/process":
					send(w, view.Birth)
				default:
					t.Errorf("unexpected Root action %s", r.URL.Path)
					http.Error(w, "unexpected", 500)
				}
			}))
			defer resourceServer.Close()
			resources, e := resourceguard.NewClient(resourceguard.ClientConfig{Context: context.Background(), BaseURL: resourceServer.URL, Identity: view.Identity, IssuerEpoch: 1, Timeout: 5 * time.Second, Transport: resourceServer.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return "explicit-test-token", nil }, ArrayAuthorityToken: func(_ context.Context, method, path, bodySHA string) (string, error) {
				authority.Add(1)
				step := stepFor(t, store, journal.ResizePG)
				require.Equal(t, journal.Unconfirmed, step.Phase)
				saved, e := control.DecodeRequest(step.Command)
				require.NoError(t, e)
				require.Equal(t, intent, saved)
				require.Equal(t, "POST", method)
				require.Contains(t, []string{"/dsg/v1/resource/array-authorize", "/dsg/v1/resource/array-replay-original"}, path)
				require.Len(t, bodySHA, 64)
				return "", fmt.Errorf("explicit issuer boundary: preserve original unknown")
			}})
			require.NoError(t, e)
			defer resources.CloseIdleConnections()
			pg, _ := computeClient(t, intent, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/dsg/v1/work/dsm" {
					send(w, dsm)
					return
				}
				if r.URL.Path == "/dsg/v1/resize" {
					n := resizes.Add(1)
					require.Equal(t, journal.Unconfirmed, stepFor(t, store, journal.ResizePG).Phase)
					if n == 1 {
						http.Error(w, "explicit lost reply", 503)
					} else {
						send(w, response(t, intent))
					}
					return
				}
				t.Errorf("unexpected PG action %s", r.URL.Path)
				http.Error(w, "unexpected", 500)
			})
			executor, e := New(store, pg, journal.Owner{HolderID: "owner-a", Epoch: 1}, resources)
			require.NoError(t, e)
			progress, e := executor.Advance(context.Background())
			require.Error(t, e)
			require.Equal(t, AwaitingFact, progress)
			require.True(t, resources.ArrayAuthorityEnabled())
			require.Equal(t, journal.Unconfirmed, stepFor(t, store, journal.ResizePG).Phase)
			if kind == control.Grow {
				require.EqualValues(t, 1, authority.Load())
				require.Zero(t, resizes.Load())
			} else {
				require.Zero(t, authority.Load())
				require.EqualValues(t, 1, resizes.Load())
			}
			progress, e = executor.ReplayOriginal(context.Background())
			if kind == control.Grow {
				require.Error(t, e)
				require.Equal(t, AwaitingFact, progress)
				require.EqualValues(t, 2, authority.Load())
				require.Zero(t, resizes.Load())
				require.Equal(t, journal.Unconfirmed, stepFor(t, store, journal.ResizePG).Phase)
			} else {
				require.NoError(t, e)
				require.Equal(t, Recorded, progress)
				require.Zero(t, authority.Load())
				require.EqualValues(t, 2, resizes.Load())
				require.Equal(t, journal.Settled, stepFor(t, store, journal.ResizePG).Phase)
			}
		})
	}
}
