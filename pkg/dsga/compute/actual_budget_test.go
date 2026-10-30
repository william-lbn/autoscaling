package compute

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

func actualFactCopy(t *testing.T, fact control.ControlResponse) control.ControlResponse {
	t.Helper()
	raw, err := json.Marshal(fact)
	require.NoError(t, err)
	var owned control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &owned))
	return owned
}

// 预算/Root/Runner是显式领域输入，不认证真实平台来源。SDK只消费外层已
// 持久保存的完整合同；真实TLS peer在各测试中另核读写和原Key查询边界。
func actualBudgetFixture(t *testing.T, source string) (control.Request, budget.ReconciledCapacity, control.SharedCostResponse, control.ControlResponse) {
	t.Helper()
	intent, saved := fixtures(t)
	switch source {
	case "isolated":
		saved.Observed.IssuerFloorEpoch++
		saved.Observed.Operation = nil
		saved.Receipt.Disposition, saved.Receipt.HasResult, saved.Receipt.Result = control.ReceiptStale, false, nil
	case "unchanged":
		saved.Observed.CapacityGeneration = intent.ExpectedCapacityGeneration
		saved.Observed.LowBuffers, saved.Observed.HighBuffers = 512, 512
		saved.Observed.Operation = nil
		saved.Receipt.Disposition, saved.Receipt.HasResult, saved.Receipt.Result = control.ReceiptNotFound, false, nil
	}
	shared, ok := actualBudgetShared(16<<20, 16<<20, actualFactCopy(t, saved).Observed)
	require.True(t, ok)
	require.NoError(t, shared.ValidateFor(intent.Key.Instance))
	sources := budget.RunnerSources{BirthClaim: budget.ObjectRef{Name: "birth-actual", UID: "birth-actual-uid"}, StateClaim: budget.ObjectRef{Name: "state-actual", UID: "state-actual-uid"}, ReceiptClaim: budget.ObjectRef{Name: "receipt-actual", UID: "receipt-actual-uid"}, CommandSHA256: strings.Repeat("a", 64), JournalSHA256: strings.Repeat("b", 64)}
	host := budget.ObjectRef{Name: "host-actual", UID: "host-actual-uid"}
	capacity := budget.ReconciledCapacity{
		ProtocolVersion: 4, VMShrinkPermit: false, Request: intent, IssuerEpoch: saved.Observed.IssuerFloorEpoch, RunnerPodName: "runner", RunnerRestartCount: 0,
		VMBeforeMemoryBytes: 512 << 20, VMMemoryBytes: intent.TargetVMMemoryBytes, GuestUsableMemoryBytes: intent.TargetVMMemoryBytes - (256 << 20), CapacityGeneration: saved.Observed.CapacityGeneration, Buffers: saved.Observed.HighBuffers, MaximumBuffers: saved.Observed.MaximumBuffers,
		StartupMainUpperBytes: shared.StartupMainUpperBytes, StartupArraysUpperBytes: shared.StartupArraysUpperBytes, RetainedSharedUpperBytes: shared.CurrentMainAndArraysUpperBytes, PGOperation: nil,
		PGAggregate: budget.Bound{Kind: budget.HardLimit, UpperBytes: 256 << 20, SourceID: "original-root-actual"}, RootSHA256: strings.Repeat("c", 64), OutsideClaim: budget.ObjectRef{Name: "outside-actual", UID: "outside-actual-uid"}, OutsideRequest: intent, ResidualProfileID: "qualified-actual",
		SystemOutsidePG: budget.Bound{Kind: budget.HardLimit, UpperBytes: 24 << 20, SourceID: "system-actual"}, Transition: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "transition-actual"}, Safety: budget.Bound{Kind: budget.CommittedUpper, UpperBytes: 4 << 20, SourceID: "safety-actual"}, HostClaim: host, HostReservationID: "host-reservation-actual",
		VM: budget.NativeCapacity{Sources: sources, Request: intent, BeforeBytes: 512 << 20, Cancelled: false, PreparedSHA256: strings.Repeat("d", 64), GuestSHA256: "", HostClaim: host}, InitialOutside: nil, InitialVM: nil, RecoveryPG: nil, UnchangedPG: nil,
	}
	if source == "unchanged" {
		capacity.UnchangedPG = &shared
	} else {
		capacity.RecoveryPG = &budget.RecoveryPG{Fence: terminalFence(intent, capacity.IssuerEpoch, actualFactCopy(t, saved)), Status: actualFactCopy(t, saved)}
	}
	require.NoError(t, capacity.Validate())
	return intent, capacity, shared, saved
}

func TestActualBudgetReconcileKeepsExactSlotAndSavedProof(t *testing.T) {
	for _, mode := range []string{"terminal", "expired", "stale", "not-found-same-floor", "not-found-new-floor", "isolated", "isolated-result-appears", "conflict", "changed-context", "changed-phase", "shared-cost-growth", "status-cost-growth", "changed-status-head", "changed-floor", "changed-capacity", "different-pending", "inflight-before", "inflight-during-read", "slot-replaced", "same-slot-value-replaced", "empty-slot-audit", "bad-budget"} {
		t.Run(mode, func(t *testing.T) {
			source := "terminal"
			if strings.HasPrefix(mode, "isolated") {
				source = "isolated"
			}
			intent, capacity, shared, saved := actualBudgetFixture(t, source)
			epoch := capacity.IssuerEpoch
			if mode == "not-found-new-floor" {
				epoch++
				shared.Observed.IssuerFloorEpoch = epoch
			}
			var sharedReads, statusReads, writes atomic.Int32
			var restored *Client
			_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/dsg/v1/budget/shared":
					sharedReads.Add(1)
					current := shared
					current.Observed = actualFactCopy(t, control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: saved.Receipt, Observed: shared.Observed}).Observed
					if mode == "shared-cost-growth" {
						current.Observed.Arrays[0].CommittedUpperBytes++
						current.CurrentMainAndArraysUpperBytes++
					}
					if mode == "changed-capacity" {
						current.Observed.CapacityGeneration++
					}
					reply(w, current)
				case "/dsg/v1/resize/status":
					statusReads.Add(1)
					var query control.Query
					require.NoError(t, json.NewDecoder(r.Body).Decode(&query))
					require.Equal(t, intent.Key, query.Key)
					if mode == "inflight-during-read" || mode == "slot-replaced" || mode == "same-slot-value-replaced" {
						restored.mu.Lock()
						if mode == "inflight-during-read" {
							restored.inFlight = true
						} else {
							replacement := intent
							if mode == "slot-replaced" {
								replacement.BudgetCommitmentID += "-other"
							}
							restored.pending = &replacement
						}
						restored.mu.Unlock()
					}
					fact := actualFactCopy(t, saved)
					fact.Observed.IssuerFloorEpoch = epoch
					switch mode {
					case "expired", "stale", "not-found-same-floor", "not-found-new-floor":
						fact.Receipt.Disposition, fact.Receipt.HasResult, fact.Receipt.Result = control.ReceiptResultExpired, false, nil
						if mode == "stale" {
							fact.Receipt.Disposition = control.ReceiptStale
						} else if strings.HasPrefix(mode, "not-found") {
							fact.Receipt.Disposition = control.ReceiptNotFound
						}
					case "isolated-result-appears":
						_, fact = fixtures(t)
						fact.Observed.IssuerFloorEpoch = epoch
					case "conflict":
						fact.Receipt.Disposition, fact.Receipt.HasResult, fact.Receipt.Result = control.ReceiptConflict, false, nil
					case "changed-context":
						fact.Receipt.Result.Operation.ContextSHA256 = strings.Repeat("f", 64)
					case "changed-phase":
						fact.Receipt.Result.Operation.Phase, fact.Receipt.Result.Operation.Reason = control.Denied, control.ReasonTimeout
						fact.Receipt.Result.CapacityGeneration = intent.ExpectedCapacityGeneration
					case "status-cost-growth":
						fact.Observed.Arrays[0].CommittedUpperBytes++
					case "changed-status-head":
						fact.Observed.Operation.Key.Revision++
						fact.Observed.Operation.Key.OperationID.Low++
						fact.Observed.Operation.ExpectedCapacityGeneration = fact.Observed.CapacityGeneration
					case "changed-floor":
						fact.Observed.IssuerFloorEpoch++
					}
					reply(w, fact)
				default:
					writes.Add(1)
					w.WriteHeader(500)
				}
			})
			config.IssuerEpoch, config.Pending = epoch, &intent
			switch mode {
			case "different-pending":
				other := intent
				other.BudgetCommitmentID += "-different"
				config.Pending = &other
			case "empty-slot-audit":
				config.Pending = nil
			}
			var err error
			restored, err = New(context.Background(), config)
			require.NoError(t, err)
			defer restored.CloseIdleConnections()
			if mode == "inflight-before" {
				restored.mu.Lock()
				restored.inFlight = true
				restored.mu.Unlock()
			}
			fence := terminalFence(intent, epoch, control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: saved.Receipt, Observed: shared.Observed})
			if mode == "bad-budget" {
				capacity.ProtocolVersion = 1
			}
			_, err = restored.ReconcileActualBudget(context.Background(), intent, capacity, fence)
			if mode == "terminal" || mode == "expired" || mode == "stale" || mode == "not-found-new-floor" || mode == "isolated" || mode == "empty-slot-audit" {
				require.NoError(t, err)
				require.Nil(t, restored.Pending())
			} else {
				require.Error(t, err)
				require.NotNil(t, restored.Pending())
			}
			require.Zero(t, writes.Load())
			if mode == "different-pending" || mode == "inflight-before" || mode == "bad-budget" {
				require.Zero(t, sharedReads.Load())
				require.Zero(t, statusReads.Load())
			}
		})
	}
}

func TestActualBudgetUndispatchedPGRequiresEmptySDKSlot(t *testing.T) {
	for _, mode := range []string{"planned", "pending", "native-result"} {
		t.Run(mode, func(t *testing.T) {
			intent, capacity, shared, status := actualBudgetFixture(t, "unchanged")
			var writes atomic.Int32
			_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/dsg/v1/budget/shared":
					reply(w, shared)
				case "/dsg/v1/resize/status":
					if mode == "native-result" {
						_, terminal := fixtures(t)
						status.Receipt = terminal.Receipt
					}
					reply(w, status)
				default:
					writes.Add(1)
					w.WriteHeader(500)
				}
			})
			if mode == "pending" {
				config.Pending = &intent
			}
			restored, err := New(context.Background(), config)
			require.NoError(t, err)
			defer restored.CloseIdleConnections()
			_, err = restored.ReconcileActualBudget(context.Background(), intent, capacity, terminalFence(intent, config.IssuerEpoch, status))
			if mode == "planned" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if mode == "pending" {
				require.Equal(t, &intent, restored.Pending())
			} else {
				require.Nil(t, restored.Pending())
			}
			require.Zero(t, writes.Load())
		})
	}
}

func TestActualBudgetReconcileDetectsRealSameIntentExecutionSlotReplacement(t *testing.T) {
	intent, capacity, shared, status := actualBudgetFixture(t, "terminal")
	sharedStarted, sharedRelease := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(sharedRelease) })
	defer release()
	var writes atomic.Int32
	_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dsg/v1/budget/shared":
			close(sharedStarted)
			<-sharedRelease
			reply(w, shared)
		case "/dsg/v1/resize/status":
			reply(w, status)
		case "/dsg/v1/resize":
			// 独立执行池中的显式同意图请求丢失结果；执行结束后仍留下
			// 同数值的新Pending槽。仅检查前后inFlight=false不足以识别它。
			writes.Add(1)
			w.WriteHeader(500)
		default:
			w.WriteHeader(500)
		}
	})
	config.Pending = &intent
	restored, err := New(context.Background(), config)
	require.NoError(t, err)
	defer restored.CloseIdleConnections()
	done := make(chan error, 1)
	go func() {
		_, readErr := restored.ReconcileActualBudget(context.Background(), intent, capacity, terminalFence(intent, config.IssuerEpoch, status))
		done <- readErr
	}()
	select {
	case <-sharedStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("actual budget read did not reach independent observation connection")
	}
	_, err = restored.Resize(context.Background(), intent)
	require.Error(t, err)
	require.Equal(t, &intent, restored.Pending())
	release()
	select {
	case err = <-done:
		require.ErrorContains(t, err, "actual_budget_original_slot_changed_during_read")
	case <-time.After(2 * time.Second):
		t.Fatal("actual budget reconciliation did not finish after read release")
	}
	require.Equal(t, &intent, restored.Pending())
	require.Equal(t, int32(1), writes.Load(), "只有测试显式执行调用一次POST，对账不得再写")
}

// 此peer真的使用有界缓存：通过正式SDK发送后继无容量请求后逐项淘汰原
// Key，不手填ResultExpired。它仍是协议peer，不冒充真实PG内核验收。
func TestActualBudgetRebuildAfterRealPeerHistoryEviction(t *testing.T) {
	intent, capacity, shared, saved := actualBudgetFixture(t, "terminal")
	var mu sync.Mutex
	cache := map[control.Key]control.Snapshot{intent.Key: *actualFactCopy(t, saved).Receipt.Result}
	order := []control.Key{intent.Key}
	var writes, statusReads atomic.Int32
	_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/dsg/v1/budget/shared":
			reply(w, shared)
		case "/dsg/v1/resize/status":
			statusReads.Add(1)
			var query control.Query
			require.NoError(t, json.NewDecoder(r.Body).Decode(&query))
			fact := control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: query.Key, Disposition: control.ReceiptResultExpired, HasResult: false, Result: nil}, Observed: shared.Observed}
			if result, found := cache[query.Key]; found {
				fact.Receipt.Disposition, fact.Receipt.HasResult, fact.Receipt.Result = control.ReceiptReplayed, true, &result
			}
			reply(w, fact)
		case "/dsg/v1/resize":
			writes.Add(1)
			var next control.Request
			require.NoError(t, json.NewDecoder(r.Body).Decode(&next))
			require.Equal(t, shared.Observed.HighBuffers, next.TargetBuffers)
			require.Equal(t, shared.Observed.CapacityGeneration, next.ExpectedCapacityGeneration)
			hash, err := next.ContextSHA256()
			require.NoError(t, err)
			shared.Observed.Operation = &control.OperationFact{Key: next.Key, Kind: next.Kind, Phase: control.Committed, TargetBuffers: next.TargetBuffers, ExpectedCapacityGeneration: next.ExpectedCapacityGeneration, ContextSHA256: hash, Reason: control.ReasonNone, CancelRequested: false, CancelClosed: true, Replayed: false}
			result := shared.Observed
			cache[next.Key] = result
			order = append(order, next.Key)
			for len(order) > 2 {
				delete(cache, order[0])
				order = order[1:]
			}
			reply(w, control.ControlResponse{ProtocolVersion: 1, VMShrinkPermit: false, Receipt: control.Receipt{Key: next.Key, Disposition: control.ReceiptAccepted, HasResult: true, Result: &result}, Observed: shared.Observed})
		default:
			w.WriteHeader(500)
		}
	})
	writer, err := New(context.Background(), config)
	require.NoError(t, err)
	defer writer.CloseIdleConnections()
	for n := range 3 {
		next := intent
		next.Key.Revision += control.Uint64(n + 1)
		next.Key.OperationID.Low += control.Uint64(n + 1)
		next.ExpectedCapacityGeneration = capacity.CapacityGeneration
		_, err = writer.Resize(context.Background(), next)
		require.NoError(t, err)
	}
	mu.Lock()
	_, retained := cache[intent.Key]
	current := shared.Observed
	mu.Unlock()
	require.False(t, retained)
	config.Pending = &intent
	restored, err := New(context.Background(), config)
	require.NoError(t, err)
	defer restored.CloseIdleConnections()
	beforeWrites := writes.Load()
	fence := control.FenceResponse{ProtocolVersion: 1, RequestedInstance: intent.Key.Instance, RequestedEpoch: config.IssuerEpoch, Disposition: control.Fenced, Observed: current, VMShrinkPermit: false}
	_, err = restored.ReconcileRecovery(context.Background(), intent, capacity.RecoveryPG.Fence, capacity.RecoveryPG.Status, fence)
	require.Error(t, err, "旧单领域恢复仍不能把完整终态证明换成缺失结果")
	require.Equal(t, &intent, restored.Pending())
	fact, err := restored.ReconcileActualBudget(context.Background(), intent, capacity, fence)
	require.NoError(t, err)
	require.Equal(t, control.ReceiptResultExpired, fact.Receipt.Disposition)
	require.Nil(t, restored.Pending())
	require.Equal(t, beforeWrites, writes.Load(), "SDK重建只读预算/原Key，不重发原Resize")
	require.Equal(t, int32(2), statusReads.Load())
	require.NotNil(t, capacity.RecoveryPG.Status.Receipt.Result, "保存的完整终态不改成隔离证明")
}
