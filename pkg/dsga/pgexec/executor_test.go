package pgexec

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

type vmAPI struct {
	mu       sync.Mutex
	vm       *vmv1.VirtualMachine
	version  int
	loseNext bool
}

func (api *vmAPI) serve(w http.ResponseWriter, r *http.Request) {
	api.mu.Lock()
	defer api.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPatch {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "body", http.StatusBadRequest)
			return
		}
		operations, err := jsonpatch.DecodePatch(body)
		if err != nil {
			http.Error(w, "patch", http.StatusBadRequest)
			return
		}
		original, _ := json.Marshal(api.vm)
		updated, err := operations.Apply(original)
		if err != nil {
			http.Error(w, "CAS failed", http.StatusUnprocessableEntity)
			return
		}
		var vm vmv1.VirtualMachine
		if json.Unmarshal(updated, &vm) != nil {
			http.Error(w, "fixture", http.StatusInternalServerError)
			return
		}
		api.version++
		vm.ResourceVersion = "rv-" + strconv.Itoa(api.version)
		api.vm = &vm
		if api.loseNext {
			api.loseNext = false
			http.Error(w, "reply lost after commit", http.StatusInternalServerError)
			return
		}
	} else if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	_ = json.NewEncoder(w).Encode(api.vm)
}

func response(t *testing.T, intent control.Request) control.ControlResponse {
	t.Helper()
	raw, err := os.ReadFile("../../api/dsga/testdata/response-v1.json")
	require.NoError(t, err)
	var fact control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &fact))
	hash, err := intent.ContextSHA256()
	require.NoError(t, err)
	fact.Receipt.Key = intent.Key
	for _, snapshot := range []*control.Snapshot{fact.Receipt.Result, &fact.Observed} {
		snapshot.Instance = intent.Key.Instance
		snapshot.IssuerFloorEpoch = intent.Key.IssuerEpoch
		snapshot.CapacityGeneration = intent.ExpectedCapacityGeneration + 1
		snapshot.LowBuffers = intent.TargetBuffers
		snapshot.HighBuffers = intent.TargetBuffers
		snapshot.Operation.Key = intent.Key
		snapshot.Operation.Kind = intent.Kind
		snapshot.Operation.TargetBuffers = intent.TargetBuffers
		snapshot.Operation.ExpectedCapacityGeneration = intent.ExpectedCapacityGeneration
		snapshot.Operation.ContextSHA256 = hash
	}
	require.NoError(t, fact.ValidateResize(intent, intent.Key.Instance))
	return fact
}

func request(t *testing.T, kind control.Kind) control.Request {
	t.Helper()
	raw, err := os.ReadFile("../../api/dsga/testdata/request-v1.json")
	require.NoError(t, err)
	var intent control.Request
	require.NoError(t, json.Unmarshal(raw, &intent))
	intent.Key.IssuerEpoch = 1
	intent.Kind = kind
	return intent
}

// 这里的HTTP/RFC6902仅模拟持久API的线性化点，不声称真实apiserver/etcd。
// 非PG前置领域明确为测试夹具；正式预算/Guest回执不能由这些body生成。
func prepareStore(t *testing.T, intent control.Request) (*vmAPI, *journal.Store) {
	t.Helper()
	var status vmv1.VirtualMachineStatus
	api := &vmAPI{mu: sync.Mutex{}, vm: &vmv1.VirtualMachine{
		TypeMeta:   metav1.TypeMeta{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "testing", Name: "vm", UID: types.UID(intent.Key.Instance.VMUID), ResourceVersion: "rv-1"},
		Spec:       vmv1.VirtualMachineSpec{}, Status: status,
	}, version: 1, loseNext: false}
	api.vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ComputeInstanceID: intent.Key.Instance.ComputeInstanceID, VMUID: "", ConfigSecretUID: "", Ready: false}
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	t.Cleanup(server.Close)
	config := new(rest.Config)
	config.Host = server.URL
	client, err := vmclient.NewForConfig(config)
	require.NoError(t, err)
	store, err := journal.NewStore(client.NeonvmV1().VirtualMachines("testing"), journal.Binding{Namespace: "testing", Name: "vm", VMUID: api.vm.UID, Incarnation: "1234567890abcdef1234567890abcdef"})
	require.NoError(t, err)
	initial, err := store.Initialize(context.Background(), "rv-1", "owner-a")
	require.NoError(t, err)
	_, err = store.ReleaseBootstrap(context.Background(), initial.ResourceVersion(), "config-uid")
	require.NoError(t, err)
	api.mu.Lock()
	api.vm.Status.Phase = vmv1.VmRunning
	api.vm.Status.PodName = "runner"
	api.vm.Status.PodIP = "127.0.0.1"
	api.mu.Unlock()
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	actor := snapshot.State().Owner
	snapshot, disposition, err := store.Begin(context.Background(), snapshot, actor, intent, 0)
	require.NoError(t, err)
	require.Equal(t, journal.Accepted, disposition)
	for _, step := range snapshot.State().Active.Steps {
		if step.Action == journal.ResizePG {
			break
		}
		var dispatch *journal.Dispatch
		snapshot, dispatch, err = store.Start(context.Background(), snapshot, actor, intent.Key, step.Action, "non-pg-fixture", []byte(`{"test_only_non_pg_domain":true}`))
		require.NoError(t, err)
		snapshot, err = store.Record(context.Background(), snapshot, actor, intent.Key, journal.Observation{Key: intent.Key, RequestSHA256: dispatch.RequestSHA256, CommandSHA256: dispatch.Step.CommandSHA256, Ordinal: dispatch.Step.Ordinal, Action: step.Action, Result: journal.Completed, SourceID: dispatch.Step.TargetID, Data: []byte(`{"test_only_non_pg_domain":true}`)})
		require.NoError(t, err)
	}
	return api, store
}

func send(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}

// TLS服务使用协议事实夹具；真实Compute/SQL链由独立native门禁证明。
func computeClient(t *testing.T, intent control.Request, handler func(http.ResponseWriter, *http.Request)) (*compute.Client, *atomic.Int32) {
	t.Helper()
	var rpcCount atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcCount.Add(1)
		switch r.URL.Path {
		case "/dsg/v1/capabilities":
			history := uint32(16)
			provider := true
			boot := intent.Key.Instance.PGBootID
			send(w, control.Capabilities{ProtocolVersion: 1, ProductionReady: false, VMShrinkPermit: false, Supported: true, BufferAPIVersion: 6, Enabled: true, InstanceBound: true, HistoryCapacity: &history, ProviderCanRelease: &provider, Instance: &control.DiscoveryIdentity{VMUID: intent.Key.Instance.VMUID, ComputeInstanceID: intent.Key.Instance.ComputeInstanceID, PGBootID: &boot}})
		case "/dsg/v1/issuer/fence":
			fact := response(t, intent)
			snapshot := fact.Observed
			snapshot.Operation = nil
			snapshot.CapacityGeneration = intent.ExpectedCapacityGeneration
			snapshot.HighBuffers = 256
			if intent.Kind == control.Shrink {
				snapshot.HighBuffers = 2048
			}
			snapshot.LowBuffers = snapshot.HighBuffers
			send(w, control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: intent.Key.Instance, RequestedEpoch: intent.Key.IssuerEpoch, Disposition: control.Fenced, Observed: snapshot})
		default:
			handler(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := compute.New(context.Background(), compute.Config{BaseURL: server.URL, Instance: intent.Key.Instance, IssuerEpoch: intent.Key.IssuerEpoch, Timeout: 5 * time.Second, Transport: server.Client().Transport.(*http.Transport), Token: func(context.Context) (string, error) { return "private-test-token", nil }, Pending: nil})
	require.NoError(t, err)
	t.Cleanup(client.CloseIdleConnections)
	return client, &rpcCount
}

func stepFor(t *testing.T, store *journal.Store, action journal.Action) journal.Step {
	t.Helper()
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	for _, step := range snapshot.State().Active.Steps {
		if step.Action == action {
			return step
		}
	}
	t.Fatal("missing step")
	var empty journal.Step
	return empty
}

func TestResizeIsWriteAheadAndLostReplyRecoversOnlyByFullIntentQuery(t *testing.T) {
	intent := request(t, control.Grow)
	_, store := prepareStore(t, intent)
	var resizes, queries atomic.Int32
	client, _ := computeClient(t, intent, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dsg/v1/resize":
			attempt := resizes.Add(1)
			step := stepFor(t, store, journal.ResizePG)
			if attempt == 1 {
				require.Equal(t, journal.Unconfirmed, step.Phase)
			} else {
				require.Equal(t, journal.Settled, step.Phase)
			}
			decoded, err := control.DecodeRequest(step.Command)
			require.NoError(t, err)
			require.Equal(t, intent, decoded)
			if attempt == 1 {
				http.Error(w, "lost after commit fixture", http.StatusServiceUnavailable)
			} else {
				send(w, response(t, intent))
			}
		case "/dsg/v1/resize/status":
			queries.Add(1)
			send(w, response(t, intent))
		default:
			http.Error(w, "unexpected", http.StatusInternalServerError)
		}
	})
	executor, err := New(store, client, journal.Owner{HolderID: "owner-a", Epoch: 1})
	require.NoError(t, err)
	progress, err := executor.Advance(context.Background())
	require.Error(t, err)
	require.Equal(t, AwaitingFact, progress)
	require.Equal(t, journal.Unconfirmed, stepFor(t, store, journal.ResizePG).Phase)
	progress, err = executor.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, Recorded, progress)
	require.EqualValues(t, 1, resizes.Load())
	require.EqualValues(t, 1, queries.Load())
	step := stepFor(t, store, journal.ResizePG)
	require.Equal(t, journal.Completed, step.Observation.Result)
	require.Contains(t, string(step.Observation.Data), `"issuer_fence"`)
	require.Contains(t, string(step.Observation.Data), `"response"`)
	require.Equal(t, &intent, client.Pending(), "PG query must not clear SDK unknown intent")
	before := append([]byte(nil), step.Observation.Data...)
	progress, err = executor.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, Reconciled, progress)
	require.Nil(t, client.Pending(), "完整原终态只读核对解除SDK同一Pending")
	require.EqualValues(t, 1, resizes.Load(), "只读核对不能重发Resize")
	require.Equal(t, before, stepFor(t, store, journal.ResizePG).Observation.Data)
	progress, err = executor.ReplayOriginal(context.Background())
	require.NoError(t, err)
	require.Equal(t, Replayed, progress)
	require.Nil(t, client.Pending())
	require.Equal(t, before, stepFor(t, store, journal.ResizePG).Observation.Data, "explicit replay cannot overwrite durable terminal facts")
	require.EqualValues(t, 2, resizes.Load(), "only explicit replay may send the exact second POST")
}

func TestUnconfirmedWriteGivesNoDispatchAndNotFoundDoesNotEraseIntent(t *testing.T) {
	intent := request(t, control.Grow)
	api, store := prepareStore(t, intent)
	var resizes atomic.Int32
	client, _ := computeClient(t, intent, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dsg/v1/resize" {
			resizes.Add(1)
			send(w, response(t, intent))
			return
		}
		fact := response(t, intent)
		fact.Receipt.Disposition = control.ReceiptNotFound
		fact.Receipt.HasResult = false
		fact.Receipt.Result = nil
		send(w, fact)
	})
	executor, err := New(store, client, journal.Owner{HolderID: "owner-a", Epoch: 1})
	require.NoError(t, err)
	api.mu.Lock()
	api.loseNext = true
	api.mu.Unlock()
	_, err = executor.Advance(context.Background())
	var unknown *journal.UnconfirmedWrite
	require.ErrorAs(t, err, &unknown)
	progress, err := executor.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, AwaitingFact, progress)
	require.Zero(t, resizes.Load())
	step := stepFor(t, store, journal.ResizePG)
	require.Equal(t, journal.Unconfirmed, step.Phase)
	require.Nil(t, step.Observation)
	progress, err = executor.ReplayOriginal(context.Background())
	require.NoError(t, err)
	require.Equal(t, Recorded, progress)
	require.EqualValues(t, 1, resizes.Load())
	require.Equal(t, journal.Completed, stepFor(t, store, journal.ResizePG).Observation.Result)
}

func TestCompleteIntentHashMismatchAndOwnerChangeCannotPersistSuccess(t *testing.T) {
	for _, mode := range []string{"changed-budget", "takeover", "lost-record-reply", "newer-floor"} {
		t.Run(mode, func(t *testing.T) {
			intent := request(t, control.Grow)
			api, store := prepareStore(t, intent)
			client, _ := computeClient(t, intent, func(w http.ResponseWriter, r *http.Request) {
				if mode == "changed-budget" && r.URL.Path == "/dsg/v1/resize" {
					http.Error(w, "lost reply fixture", http.StatusServiceUnavailable)
					return
				}
				fact := response(t, intent)
				switch mode {
				case "newer-floor":
					fact.Observed.IssuerFloorEpoch++
				case "changed-budget":
					changed := intent
					changed.BudgetCommitmentID += "-wrong"
					fact = response(t, changed)
				case "takeover":
					snapshot, err := store.Load(context.Background())
					require.NoError(t, err)
					_, err = store.Takeover(context.Background(), snapshot, snapshot.State().Owner, "owner-b")
					require.NoError(t, err)
				case "lost-record-reply":
					api.mu.Lock()
					api.loseNext = true
					api.mu.Unlock()
				}
				send(w, fact)
			})
			executor, err := New(store, client, journal.Owner{HolderID: "owner-a", Epoch: 1})
			require.NoError(t, err)
			progress, err := executor.Advance(context.Background())
			require.Error(t, err)
			require.Equal(t, AwaitingFact, progress)
			if mode == "changed-budget" {
				progress, err = executor.Advance(context.Background())
				require.Error(t, err, "Status key validation must not replace full intent hash validation")
				require.Equal(t, AwaitingFact, progress)
			}
			step := stepFor(t, store, journal.ResizePG)
			if mode == "lost-record-reply" {
				require.Equal(t, journal.Settled, step.Phase)
				progress, err = executor.Advance(context.Background())
				require.NoError(t, err)
				require.Equal(t, ExternalStep, progress)
			} else {
				require.Equal(t, journal.Unconfirmed, step.Phase)
				require.Nil(t, step.Observation)
			}
		})
	}
}

func verifiedArrays(fact *control.ControlResponse, intent control.Request) {
	for i := range fact.Observed.Arrays {
		a := &fact.Observed.Arrays[i]
		key := intent.Key
		a.AllocationUnknown = false
		a.ReleaseState = control.ReleaseVerified
		a.ReleaseKey = &key
		a.ReleaseCapacityGeneration = fact.Observed.CapacityGeneration
		a.Provider = control.ProviderFact{HasResult: true, Status: control.ProviderOK, SystemErrno: 0, RequestedOffset: 4096, RequestedBytes: 4096, AffectedOffset: 4096, AffectedBytes: 4096, PhysicalReleaseVerified: true, AllocationMayBePartial: false, ReleaseMayBePartial: false}
	}
}

func TestProviderStepCannotAdvanceOnLogicalCommitOrUnsupportedRelease(t *testing.T) {
	for _, mode := range []string{"verified", "unsupported", "regrown", "checkpoint-wait"} {
		t.Run(mode, func(t *testing.T) {
			intent := request(t, control.Shrink)
			_, store := prepareStore(t, intent)
			var resizes, queries atomic.Int32
			client, _ := computeClient(t, intent, func(w http.ResponseWriter, r *http.Request) {
				fact := response(t, intent)
				if r.URL.Path == "/dsg/v1/resize" {
					resizes.Add(1)
				} else {
					queries.Add(1)
					verifiedArrays(&fact, intent)
					switch mode {
					case "checkpoint-wait":
						fact.Observed.Arrays[3].ReleaseState = control.ReleaseWaitCheckpoint
						fact.Observed.Arrays[3].Provider = control.ProviderFact{HasResult: false, Status: control.ProviderUnknown, SystemErrno: 0, RequestedOffset: 0, RequestedBytes: 0, AffectedOffset: 0, AffectedBytes: 0, PhysicalReleaseVerified: false, AllocationMayBePartial: false, ReleaseMayBePartial: false}
					case "unsupported":
						fact.Observed.Arrays[2].ReleaseState = control.ReleaseUnsupported
						fact.Observed.Arrays[2].Provider.Status = control.ProviderUnsupported
						fact.Observed.Arrays[2].Provider.PhysicalReleaseVerified = false
					case "regrown":
						newIntent := intent
						newIntent.Kind = control.Grow
						newIntent.Key.Revision++
						newIntent.TargetBuffers = 2048
						newIntent.ExpectedCapacityGeneration++
						fact.Observed = response(t, newIntent).Observed
					}
				}
				send(w, fact)
			})
			executor, err := New(store, client, journal.Owner{HolderID: "owner-a", Epoch: 1})
			require.NoError(t, err)
			progress, err := executor.Advance(context.Background())
			require.NoError(t, err)
			require.Equal(t, Recorded, progress)
			require.Equal(t, journal.Planned, stepFor(t, store, journal.ReleasePG).Phase)
			progress, err = executor.Advance(context.Background())
			require.NoError(t, err)
			switch mode {
			case "verified":
				require.Equal(t, Recorded, progress)
				require.Equal(t, journal.Completed, stepFor(t, store, journal.ReleasePG).Observation.Result)
			case "regrown":
				require.Equal(t, AwaitingFact, progress)
				require.Equal(t, journal.Unconfirmed, stepFor(t, store, journal.ReleasePG).Phase)
			default:
				require.Equal(t, Recorded, progress)
				require.Equal(t, journal.Rejected, stepFor(t, store, journal.ReleasePG).Observation.Result)
				progress, err = executor.Advance(context.Background())
				require.NoError(t, err)
				require.Equal(t, ExternalStep, progress, "rejected release requires original-budget cleanup, never VM dispatch")
			}
			require.EqualValues(t, 1, resizes.Load())
			require.EqualValues(t, 1, queries.Load())
		})
	}
}

func TestOldOwnerBootAndMismatchedPersistedCommandAreFrozenBeforeRPC(t *testing.T) {
	for _, mode := range []string{"owner", "boot", "command", "target"} {
		t.Run(mode, func(t *testing.T) {
			intent := request(t, control.Grow)
			_, store := prepareStore(t, intent)
			clientIntent := intent
			actor := journal.Owner{HolderID: "owner-a", Epoch: 1}
			if mode == "owner" {
				actor.HolderID = "another-process"
			}
			if mode == "boot" {
				clientIntent.Key.Instance.PGBootID--
			}
			if mode == "command" || mode == "target" {
				snapshot, err := store.Load(context.Background())
				require.NoError(t, err)
				command := intent
				target := intent.Key.Instance.ComputeInstanceID
				if mode == "command" {
					command.TargetVMMemoryBytes++
				} else {
					target = "wrong-compute"
				}
				body, err := json.Marshal(command)
				require.NoError(t, err)
				_, _, err = store.Start(context.Background(), snapshot, actor, intent.Key, journal.ResizePG, target, body)
				require.NoError(t, err)
			}
			var calls atomic.Int32
			client, allCalls := computeClient(t, clientIntent, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				http.Error(w, "must not call", http.StatusInternalServerError)
			})
			before := allCalls.Load()
			executor, err := New(store, client, actor)
			require.NoError(t, err)
			_, err = executor.Advance(context.Background())
			require.Error(t, err)
			require.Zero(t, calls.Load())
			require.Equal(t, before, allCalls.Load(), "even fence must not be sent for a mismatched journal")
		})
	}
}

func TestProviderRequiresAllCurrentArrayFactsAndCannotBorrowHistory(t *testing.T) {
	intent := request(t, control.Shrink)
	fact := response(t, intent)
	for i := range fact.Observed.Arrays {
		a := &fact.Observed.Arrays[i]
		key := intent.Key
		a.AllocationUnknown = false
		a.ReleaseState = control.ReleaseVerified
		a.ReleaseKey = &key
		a.ReleaseCapacityGeneration = fact.Observed.CapacityGeneration
		a.Provider = control.ProviderFact{HasResult: true, Status: control.ProviderOK, SystemErrno: 0, RequestedOffset: 4096, RequestedBytes: 4096, AffectedOffset: 4096, AffectedBytes: 4096, PhysicalReleaseVerified: true, AllocationMayBePartial: false, ReleaseMayBePartial: false}
	}
	fence := control.FenceResponse{ProtocolVersion: 1, VMShrinkPermit: false, RequestedInstance: intent.Key.Instance, RequestedEpoch: 1, Disposition: control.Fenced, Observed: fact.Observed}
	require.True(t, releaseVerified(intent, fence, fact))
	for _, mode := range []string{"pending-executor", "unsupported", "partial", "allocation-unknown", "old-release", "new-generation", "different-budget", "incomplete-range", "checkpoint-wait", "unverified"} {
		t.Run(mode, func(t *testing.T) {
			raw, err := json.Marshal(fact)
			require.NoError(t, err)
			var changed control.ControlResponse
			require.NoError(t, json.Unmarshal(raw, &changed))
			f := fence
			switch mode {
			case "unverified":
				changed.Observed.Arrays[0].Provider.PhysicalReleaseVerified = false
			case "pending-executor":
				f.Disposition = control.FencePending
			case "unsupported":
				changed.Observed.Arrays[0].ReleaseState = control.ReleaseUnsupported
			case "partial":
				changed.Observed.Arrays[1].Provider.ReleaseMayBePartial = true
			case "allocation-unknown":
				changed.Observed.Arrays[2].AllocationUnknown = true
			case "old-release":
				changed.Observed.Arrays[3].ReleaseKey.Revision++
			case "new-generation":
				changed.Observed.CapacityGeneration++
			case "different-budget":
				changed.Observed.Operation.ContextSHA256 = fmt.Sprintf("%064d", 0)
			case "incomplete-range":
				changed.Observed.Arrays[1].Provider.AffectedBytes--
			case "checkpoint-wait":
				changed.Observed.Arrays[3].ReleaseState = control.ReleaseWaitCheckpoint
			}
			require.False(t, releaseVerified(intent, f, changed))
		})
	}
	fact.Observed.Arrays[0].Provider = control.ProviderFact{HasResult: true, Status: control.ProviderNoop, SystemErrno: 0, RequestedOffset: 0, RequestedBytes: 0, AffectedOffset: 0, AffectedBytes: 0, PhysicalReleaseVerified: false, AllocationMayBePartial: false, ReleaseMayBePartial: false}
	require.True(t, releaseVerified(intent, fence, fact), "kernel verified zero-tail needs no physical syscall")
}

func TestNewOwnerReconcilesOldUnconfirmedByCancelWithoutRedispatch(t *testing.T) {
	intent := request(t, control.Grow)
	_, store := prepareStore(t, intent)
	snapshot, err := store.Load(context.Background())
	require.NoError(t, err)
	command, err := json.Marshal(intent)
	require.NoError(t, err)
	snapshot, _, err = store.Start(context.Background(), snapshot, snapshot.State().Owner, intent.Key, journal.ResizePG, intent.Key.Instance.ComputeInstanceID, command)
	require.NoError(t, err)
	snapshot, err = store.Takeover(context.Background(), snapshot, snapshot.State().Owner, "owner-b")
	require.NoError(t, err)
	nextIntent := intent
	nextIntent.Key.IssuerEpoch = 2
	var cancels, resizes atomic.Int32
	client, _ := computeClient(t, nextIntent, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dsg/v1/resize/cancel" {
			resizes.Add(1)
			http.Error(w, "wrong method", http.StatusInternalServerError)
			return
		}
		cancels.Add(1)
		fact := response(t, intent)
		fact.Receipt.Disposition = control.ReceiptTooLate
		for _, observed := range []*control.Snapshot{fact.Receipt.Result, &fact.Observed} {
			observed.CapacityGeneration = intent.ExpectedCapacityGeneration
			observed.LowBuffers = 256
			observed.HighBuffers = 256
			observed.Operation.Phase = control.Denied
			observed.Operation.Reason = control.ReasonCancelled
			observed.Operation.CancelRequested = true
			observed.Operation.CancelClosed = false
		}
		fact.Observed.IssuerFloorEpoch = 2
		send(w, fact)
	})
	executor, err := New(store, client, snapshot.State().Owner)
	require.NoError(t, err)
	progress, err := executor.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, Recorded, progress)
	step := stepFor(t, store, journal.ResizePG)
	require.Equal(t, journal.Rejected, step.Observation.Result)
	require.EqualValues(t, 1, cancels.Load())
	require.Zero(t, resizes.Load())
	progress, err = executor.Advance(context.Background())
	require.NoError(t, err)
	require.Equal(t, ExternalStep, progress, "new owner still owes budget/admission cleanup")
	_, err = executor.ReplayOriginal(context.Background())
	require.Error(t, err, "new owner cannot replay old execution epoch")
	require.Zero(t, resizes.Load())
}

func TestExplicitReplayRejectsUndispatchedDeletingAbortedAndNonNativeRejection(t *testing.T) {
	for _, mode := range []string{"planned", "deleting", "aborted", "busy-without-native-result"} {
		t.Run(mode, func(t *testing.T) {
			intent := request(t, control.Grow)
			api, store := prepareStore(t, intent)
			client, calls := computeClient(t, intent, func(w http.ResponseWriter, _ *http.Request) {
				fact := response(t, intent)
				fact.Receipt.Result = nil
				fact.Receipt.HasResult = false
				fact.Receipt.Disposition = control.ReceiptBusy
				send(w, fact)
			})
			executor, err := New(store, client, journal.Owner{HolderID: "owner-a", Epoch: 1})
			require.NoError(t, err)
			switch mode {
			case "deleting":
				api.mu.Lock()
				now := metav1.Now()
				api.vm.DeletionTimestamp = &now
				api.version++
				api.vm.ResourceVersion = "rv-" + strconv.Itoa(api.version)
				api.mu.Unlock()
			case "aborted":
				snapshot, err := store.Load(context.Background())
				require.NoError(t, err)
				_, err = store.RequestAbort(context.Background(), snapshot, snapshot.State().Owner, intent.Key)
				require.NoError(t, err)
			case "busy-without-native-result":
				progress, err := executor.Advance(context.Background())
				require.NoError(t, err)
				require.Equal(t, Recorded, progress)
				require.Equal(t, journal.Rejected, stepFor(t, store, journal.ResizePG).Observation.Result)
			}
			before := calls.Load()
			_, err = executor.ReplayOriginal(context.Background())
			require.Error(t, err)
			require.Equal(t, before, calls.Load(), "rejected replay must not even send a fence")
		})
	}
}
