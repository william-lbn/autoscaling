package compute

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

func terminalFence(intent control.Request, epoch control.Uint64, fact control.ControlResponse) control.FenceResponse {
	s := fact.Observed
	s.IssuerFloorEpoch = epoch
	return control.FenceResponse{ProtocolVersion: 1, RequestedInstance: intent.Key.Instance, RequestedEpoch: epoch, Disposition: control.Fenced, Observed: s}
}

// 真实TLS/双连接SDK；服务端是完整PG协议夹具。逻辑终态确认不代表数组释放、
// 完整预算或VM完成。本测试覆盖新方法与普通Status之间必须保持的行为差异。
func TestTerminalReconcileRequiresCompleteOriginalAndCurrentFloor(t *testing.T) {
	for _, mode := range []string{"committed", "denied", "new-owner", "missing", "expired", "preparing", "changed-context", "changed-terminal", "superseded-floor", "older-than-fence", "different-pending"} {
		t.Run(mode, func(t *testing.T) {
			intent, saved := fixtures(t)
			if mode == "denied" {
				for _, s := range []*control.Snapshot{saved.Receipt.Result, &saved.Observed} {
					s.CapacityGeneration = intent.ExpectedCapacityGeneration
					s.Operation.Phase, s.Operation.Reason = control.Denied, control.ReasonTimeout
				}
			}
			var reads, writes atomic.Int32
			client, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/dsg/v1/resize/status" {
					writes.Add(1)
					t.Error("read-only reconcile sent mutation", r.URL.Path)
					w.WriteHeader(500)
					return
				}
				reads.Add(1)
				// 深拷贝避免修改保存的原终态来掩盖冲突。
				_, fact := fixtures(t)
				if mode == "denied" {
					fact = saved
				}
				if mode == "new-owner" {
					fact.Observed.IssuerFloorEpoch++
				}
				switch mode {
				case "missing", "expired":
					fact.Receipt.Disposition, fact.Receipt.HasResult, fact.Receipt.Result = control.ReceiptNotFound, false, nil
					if mode == "expired" {
						fact.Receipt.Disposition = control.ReceiptResultExpired
					}
				case "preparing":
					fact.Receipt.Result.CapacityGeneration = intent.ExpectedCapacityGeneration
					fact.Receipt.Result.Operation.Phase = control.Preparing
					fact.Receipt.Result.Operation.CancelClosed = false
				case "changed-context":
					other := intent
					other.BudgetCommitmentID = "another-budget-same-key"
					hash, err := other.ContextSHA256()
					require.NoError(t, err)
					fact.Receipt.Result.Operation.ContextSHA256 = hash
				case "changed-terminal":
					fact.Receipt.Result.Operation.Phase, fact.Receipt.Result.Operation.Reason = control.Denied, control.ReasonTimeout
					fact.Receipt.Result.CapacityGeneration = intent.ExpectedCapacityGeneration
				case "superseded-floor":
					fact.Observed.IssuerFloorEpoch++
				}
				reply(w, fact)
			})
			config.Pending = &intent
			if mode == "different-pending" {
				other := intent
				other.BudgetCommitmentID = "other-original"
				config.Pending = &other
			}
			if mode == "new-owner" {
				config.IssuerEpoch++
			}
			restored, err := New(context.Background(), config)
			require.NoError(t, err)
			defer restored.CloseIdleConnections()
			fence := terminalFence(intent, config.IssuerEpoch, saved)
			if mode == "older-than-fence" {
				fence.Observed.CapacityGeneration++
				fence.Observed.Operation = nil
			}
			// 只读普通Status依旧保留Pending，不能被此新方法隐式改变。
			if mode == "committed" {
				_, err = restored.Status(context.Background(), control.Query{ProtocolVersion: 1, Key: intent.Key})
				require.NoError(t, err)
				require.Equal(t, &intent, restored.Pending())
			}
			_, err = restored.ReconcileTerminal(context.Background(), intent, saved, fence)
			if mode == "committed" || mode == "denied" || mode == "new-owner" {
				require.NoError(t, err)
				require.Nil(t, restored.Pending())
			} else {
				require.Error(t, err)
				require.NotNil(t, restored.Pending())
			}
			require.Zero(t, writes.Load())
			if mode == "different-pending" {
				require.Zero(t, reads.Load(), "不同Pending在任何RPC前拒绝")
			} else {
				require.Positive(t, reads.Load())
			}
			require.Nil(t, client.Pending(), "不影响其他客户端的本地栅栏")
		})
	}
}
