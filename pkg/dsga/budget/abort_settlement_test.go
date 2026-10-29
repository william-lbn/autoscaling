package budget

import (
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 独立恢复保留原请求issuer，仅把实际结算owner记入SettlementEpoch。
// 后续取消必须能使用该预算，同时不能让旧派发借用尚未形成的未来预算。
func TestAbortCommandsRespectIndependentSettlementEpoch(t *testing.T) {
	for _, kind := range []string{"committed", "unchanged-readonly", "unchanged-reopen"} {
		t.Run(kind, func(t *testing.T) {
			f, committed, _ := committedAbortFixture(t)
			var unchanged UnchangedAbortCommand
			if kind != "committed" {
				f, unchanged, _ = unchangedAbortFixture(t, kind == "unchanged-reopen")
			}
			issuer := f.IssuerEpoch
			settlement := issuer + 2
			f.SettlementEpoch = settlement
			f.Shared.Observed.IssuerFloorEpoch = settlement
			committed.Admission.IssuerEpoch = settlement
			if unchanged.Admission != nil {
				unchanged.Admission.IssuerEpoch = settlement
			}
			validate := func(budget Finalized, epoch control.Uint64) error {
				if kind == "committed" {
					return committed.ValidateFor(budget, epoch)
				}
				return unchanged.ValidateFor(budget, epoch)
			}
			require.NoError(t, validate(f, settlement))
			// 只读取消或只查询原setter可在更高owner继续；重发旧setter仍拒绝。
			committed.MaySend = false
			if kind != "unchanged-reopen" {
				require.NoError(t, validate(f, settlement+1))
			}
			require.Equal(t, issuer, f.Request.Key.IssuerEpoch)
			for _, mode := range []string{"future-settlement", "before-issuer", "wrong-floor", "missing-settlement"} {
				t.Run(mode, func(t *testing.T) {
					bad := f
					switch mode {
					case "future-settlement":
						bad.SettlementEpoch++
						bad.Shared.Observed.IssuerFloorEpoch++
					case "before-issuer":
						bad.SettlementEpoch = issuer - 1
						bad.Shared.Observed.IssuerFloorEpoch = bad.SettlementEpoch
					case "wrong-floor":
						bad.Shared.Observed.IssuerFloorEpoch--
					case "missing-settlement":
						bad.SettlementEpoch = 0
					}
					require.Error(t, validate(bad, settlement))
				})
			}
		})
	}
}

func TestAbortReceiptsKeepOriginalSettlementAcrossReadbackTakeover(t *testing.T) {
	for _, kind := range []string{"committed", "unchanged-readonly", "unchanged-reopen"} {
		t.Run(kind, func(t *testing.T) {
			f, c, r := committedAbortFixture(t)
			var u UnchangedAbortCommand
			var receipt UnchangedAbortReceipt
			if kind != "committed" {
				f, u, receipt = unchangedAbortFixture(t, kind == "unchanged-reopen")
			}
			settlement := f.IssuerEpoch + 2
			f.SettlementEpoch, f.Shared.Observed.IssuerFloorEpoch = settlement, settlement
			for _, takeover := range []bool{false, true} {
				t.Run(map[bool]string{false: "original-response", true: "takeover-query"}[takeover], func(t *testing.T) {
					epoch := settlement
					if takeover {
						epoch++
					}
					shared := f.Shared
					shared.Observed.IssuerFloorEpoch, shared.Observed.AdmissionOpen = epoch, true
					shared.Observed.AdmissionRevision = c.Admission.Revision
					admission := c.Admission
					admission.IssuerEpoch = settlement
					hash, err := admission.ContextSHA256()
					require.NoError(t, err)
					disposition := control.AdmissionApplied
					if takeover {
						disposition = control.AdmissionReplayed
					}
					response := control.AdmissionResponse{ProtocolVersion: 1, Request: admission, ContextSHA256: hash, Disposition: disposition, Observed: shared.Observed, VMShrinkPermit: false}
					fence := control.FenceResponse{ProtocolVersion: 1, RequestedInstance: f.Request.Key.Instance, RequestedEpoch: epoch, Disposition: control.Fenced, Observed: shared.Observed, VMShrinkPermit: false}
					if kind == "committed" {
						command, observed := c, r
						command.Admission = admission
						observed.Command, observed.Query, observed.ResourceEpoch = command, takeover, epoch
						observed.Shared, observed.Fence, observed.Response = shared, fence, response
						require.NoError(t, observed.ValidateFor(command, f, settlement, epoch))
						require.Error(t, observed.ValidateFor(command, f, settlement-1, epoch))
						return
					}
					command, observed := u, receipt
					if kind == "unchanged-reopen" {
						command.Admission = &admission
						observed.Response, observed.Query = &response, takeover
					}
					observed.Command, observed.ResourceEpoch, observed.Shared, observed.Fence = command, epoch, shared, fence
					require.NoError(t, observed.ValidateFor(command, f, settlement, epoch))
					require.Error(t, observed.ValidateFor(command, f, settlement-1, epoch))
				})
			}
		})
	}
}
