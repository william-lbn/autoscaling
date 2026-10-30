package compute

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 真实TLS/双连接SDK；PG为明确协议peer。保存的证明只来自可信调用方，
// 本测试验证“保存后再次查询”与并发本地Pending边界，不外推Linux资源完成。
func TestRecoveryReconcilePreservesOriginalUnknownUntilFreshExactRead(t *testing.T) {
	for _, mode := range []string{"committed", "denied", "isolated", "isolated-advanced", "later-owner", "same-owner", "missing-terminal", "appearing-terminal", "conflict", "changed-capacity", "changed-floor", "changed-boot", "pending-fence", "bad-retained", "different-pending", "inflight-during-read", "pending-changed-during-read"} {
		t.Run(mode, func(t *testing.T) {
			intent, saved := fixtures(t)
			epoch := intent.Key.IssuerEpoch + 1
			saved.Observed.IssuerFloorEpoch = epoch
			if mode == "denied" {
				for _, s := range []*control.Snapshot{&saved.Observed, saved.Receipt.Result} {
					s.CapacityGeneration = intent.ExpectedCapacityGeneration
					s.Operation.Phase, s.Operation.Reason = control.Denied, control.ReasonTimeout
				}
			} else if mode != "committed" && mode != "missing-terminal" {
				saved.Receipt.Disposition, saved.Receipt.HasResult, saved.Receipt.Result = control.ReceiptStale, false, nil
				saved.Observed.Operation = nil
				if mode != "isolated-advanced" {
					saved.Observed.CapacityGeneration = intent.ExpectedCapacityGeneration
				}
			}
			retainedFence := terminalFence(intent, epoch, saved)
			if mode == "later-owner" {
				epoch++
			}
			fact := saved
			fact.Observed.IssuerFloorEpoch = epoch
			var reads, writes atomic.Int32
			var restored *Client
			_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/dsg/v1/resize/status" {
					writes.Add(1)
					w.WriteHeader(500)
					return
				}
				reads.Add(1)
				if mode == "inflight-during-read" || mode == "pending-changed-during-read" {
					restored.mu.Lock()
					if mode == "inflight-during-read" {
						restored.inFlight = true
					} else {
						other := intent
						other.BudgetCommitmentID += "-other"
						restored.pending = &other
					}
					restored.mu.Unlock()
				}
				current := fact
				switch mode {
				case "missing-terminal":
					current.Receipt.Disposition, current.Receipt.HasResult, current.Receipt.Result = control.ReceiptStale, false, nil
					current.Observed.Operation = nil
				case "appearing-terminal":
					_, current = fixtures(t)
					for _, s := range []*control.Snapshot{&current.Observed, current.Receipt.Result} {
						s.CapacityGeneration = intent.ExpectedCapacityGeneration
						s.Operation.Phase, s.Operation.Reason = control.Denied, control.ReasonTimeout
					}
					current.Observed.IssuerFloorEpoch = epoch
				case "conflict":
					current.Receipt.Disposition = control.ReceiptConflict
				case "changed-capacity":
					current.Observed.CapacityGeneration++
				case "changed-floor":
					current.Observed.IssuerFloorEpoch++
				case "changed-boot":
					current.Observed.Instance.PGBootID++
				}
				reply(w, current)
			})
			config.IssuerEpoch, config.Pending = epoch, &intent
			if mode == "different-pending" {
				other := intent
				other.BudgetCommitmentID += "-different"
				config.Pending = &other
			}
			var err error
			restored, err = New(context.Background(), config)
			require.NoError(t, err)
			defer restored.CloseIdleConnections()
			currentFence := terminalFence(intent, epoch, fact)
			switch mode {
			case "same-owner":
				retainedFence.RequestedEpoch, retainedFence.Observed.IssuerFloorEpoch = intent.Key.IssuerEpoch, intent.Key.IssuerEpoch
				saved.Observed.IssuerFloorEpoch = intent.Key.IssuerEpoch
			case "pending-fence":
				currentFence.Disposition = control.FencePending
			case "bad-retained":
				saved.Observed.CapacityGeneration++
			}
			_, err = restored.ReconcileRecovery(context.Background(), intent, retainedFence, saved, currentFence)
			if mode == "committed" || mode == "denied" || mode == "isolated" || mode == "isolated-advanced" || mode == "later-owner" {
				require.NoError(t, err)
				require.Nil(t, restored.Pending())
			} else {
				require.Error(t, err)
				require.NotNil(t, restored.Pending())
			}
			require.Zero(t, writes.Load())
			if mode == "different-pending" || mode == "same-owner" || mode == "pending-fence" || mode == "bad-retained" {
				require.Zero(t, reads.Load(), "无原证明/不同Pending在RPC前拒绝")
			}
		})
	}
}
