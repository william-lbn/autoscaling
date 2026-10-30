package compute

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 完整TLS SDK；PG为明确协议peer。只验证原拒绝/高floor/查询组合及本地栅栏，
// 不声称资源完成或原生PG认证。保留原Rejected，不造一个Denied历史回执。
func TestRejectedReconcileRequiresRetainedBarrierAndFreshExactRead(t *testing.T) {
	for _, mode := range []string{"busy", "rejected", "stale-query", "later-owner", "same-owner", "pending-fence", "expired", "conflict", "preparing", "committed", "changed-boot", "changed-floor", "changed-generation", "original-has-result", "missing-rejection", "different-pending", "pending-changed-during-read", "inflight-during-read", "bad-retained-proof"} {
		t.Run(mode, func(t *testing.T) {
			intent, original := fixtures(t)
			original.Receipt.Disposition, original.Receipt.HasResult, original.Receipt.Result = control.ReceiptBusy, false, nil
			original.Observed.CapacityGeneration = intent.ExpectedCapacityGeneration
			original.Observed.Operation = nil
			if mode == "rejected" {
				original.Receipt.Disposition = control.ReceiptRejected
			}
			status := original
			status.Receipt.Disposition = control.ReceiptNotFound
			status.Observed.IssuerFloorEpoch++
			retained := terminalFence(intent, intent.Key.IssuerEpoch+1, status)
			saved := status
			var reads, writes atomic.Int32
			var restored *Client
			_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/dsg/v1/resize/status" {
					writes.Add(1)
					w.WriteHeader(500)
					return
				}
				reads.Add(1)
				if mode == "pending-changed-during-read" || mode == "inflight-during-read" {
					restored.mu.Lock()
					if mode == "inflight-during-read" {
						restored.inFlight = true
					} else {
						other := intent
						other.BudgetCommitmentID += "-changed"
						restored.pending = &other
					}
					restored.mu.Unlock()
				}
				fact := status
				switch mode {
				case "expired":
					fact.Receipt.Disposition = control.ReceiptResultExpired
				case "conflict":
					fact.Receipt.Disposition = control.ReceiptConflict
				case "stale-query":
					fact.Receipt.Disposition = control.ReceiptStale
				case "changed-boot":
					fact.Observed.Instance.PGBootID++
				case "changed-floor":
					fact.Observed.IssuerFloorEpoch++
				case "changed-generation":
					fact.Observed.CapacityGeneration++
				case "preparing", "committed":
					_, fact = fixtures(t)
					fact.Observed.IssuerFloorEpoch = intent.Key.IssuerEpoch + 1
					if mode == "preparing" {
						fact.Receipt.Result.Operation.Phase = control.Preparing
						fact.Receipt.Result.Operation.CancelClosed = false
						fact.Receipt.Result.CapacityGeneration = intent.ExpectedCapacityGeneration
					}
				}
				reply(w, fact)
			})
			config.Pending, config.IssuerEpoch = &intent, intent.Key.IssuerEpoch+1
			if mode == "later-owner" {
				config.IssuerEpoch++
				status.Observed.IssuerFloorEpoch = config.IssuerEpoch
			}
			if mode == "different-pending" {
				other := intent
				other.BudgetCommitmentID += "-other"
				config.Pending = &other
			}
			var err error
			restored, err = New(context.Background(), config)
			require.NoError(t, err)
			defer restored.CloseIdleConnections()
			fence := terminalFence(intent, config.IssuerEpoch, status)
			switch mode {
			case "same-owner":
				fence.RequestedEpoch, fence.Observed.IssuerFloorEpoch = intent.Key.IssuerEpoch, intent.Key.IssuerEpoch
			case "pending-fence":
				fence.Disposition = control.FencePending
			case "original-has-result":
				_, original = fixtures(t)
			case "missing-rejection":
				original.Receipt.Disposition = control.ReceiptNotFound
			case "bad-retained-proof":
				saved.Observed.CapacityGeneration++
			}
			_, err = restored.ReconcileRejected(context.Background(), intent, original, retained, saved, fence)
			if mode == "busy" || mode == "rejected" || mode == "stale-query" || mode == "later-owner" {
				require.NoError(t, err)
				require.Nil(t, restored.Pending())
			} else {
				require.Error(t, err)
				require.NotNil(t, restored.Pending())
			}
			require.Zero(t, writes.Load())
			if mode == "different-pending" || mode == "original-has-result" || mode == "missing-rejection" || mode == "bad-retained-proof" || mode == "same-owner" {
				require.Zero(t, reads.Load())
			}
		})
	}
}
