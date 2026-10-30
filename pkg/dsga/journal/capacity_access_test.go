package journal

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 通过原Model写前/落账/关闭流程构造永久头。只验证统一访问器的格式、
// 来源摘要与深拷贝，不以领域回执替代PG/Root/Runner/Host网络来源认证。
func TestCurrentCapacityAccessKeepsLegacySourcesAndNestedOwnership(t *testing.T) {
	for _, mode := range []string{"native", "initial-cancelled", "initial-undispatched", "independent-cancellation"} {
		t.Run(mode, func(t *testing.T) {
			var m Model
			if mode == "independent-cancellation" {
				selected, current := reopenedCleanupRecovery(t, "actual", true, false, true)
				var err error
				m, err = selected.CompleteServiceReopen(selected.Owner, selected.ServiceClosure.Reopen.CommandSHA256, current)
				require.NoError(t, err)
				require.EqualValues(t, 2, m.CapacityHeld.ProtocolVersion)
			} else {
				var command budget.ReconciledAbortCommand
				var receipt budget.ReconciledAbortReceipt
				if mode == "native" {
					m, command, receipt = partialCapacityModel(t)
				} else {
					m, command, receipt = initialCapacityDomain(t, mode == "initial-undispatched")
				}
				m = recordPartialCapacity(t, m, command, receipt)
				var err error
				m, err = m.Finish(m.Owner, command.Capacity.Request.Key)
				require.NoError(t, err)
				require.EqualValues(t, 1, m.CapacityHeld.ProtocolVersion)
			}
			before, err := json.Marshal(m)
			require.NoError(t, err)
			var original budget.ReconciledAbortCommand
			require.NoError(t, json.Unmarshal(m.CapacityHeld.Cleanup.Command, &original))
			capacity, err := m.CurrentCapacity()
			require.NoError(t, err)
			require.Equal(t, original.Capacity, capacity)
			sourceSHA, err := m.CapacitySourceSHA256()
			require.NoError(t, err)
			require.Equal(t, m.CapacityHeld.Cleanup.CommandSHA256, sourceSHA, "旧版本继续绑定原命令字节，不能改成预算体摘要")
			private, digest, sequence, err := m.currentCapacityRecord()
			require.NoError(t, err)
			require.Equal(t, capacity, private)
			require.Equal(t, sourceSHA, digest)
			require.Equal(t, m.CapacityHeld.RecordedSequence, sequence)
			if capacity.PGOperation != nil {
				capacity.PGOperation.TargetBuffers++
			}
			if capacity.InitialOutside != nil {
				capacity.InitialOutside.PGAggregate.Bootstrap.Limits.HugePools[0].Pages++
				capacity.InitialOutside.Shared.Observed.Arrays[0].CommittedUpperBytes++
			}
			if capacity.InitialVM != nil {
				capacity.InitialVM.MemoryBytes++
			}
			again, err := m.CurrentCapacity()
			require.NoError(t, err)
			require.Equal(t, original.Capacity, again)
			after, err := json.Marshal(m)
			require.NoError(t, err)
			require.Equal(t, before, after, "读取/修改返回值不能改变任何原WAL或旧JSON字节形状")
			wrong := original.Capacity.Request
			wrong.BudgetCommitmentID += "-substituted"
			_, err = m.CapacityFor(wrong)
			require.Error(t, err, "同Key但不同完整Request不能收养当前预算")
			broken := m.Clone()
			broken.CapacityHeld.Cleanup.DispatchedBy = nil
			_, err = broken.CurrentCapacity()
			require.Error(t, err)
			_, err = broken.CapacitySourceSHA256()
			require.Error(t, err)
		})
	}
}
