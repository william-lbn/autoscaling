//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

// 只针对新预算交接授权和原真实WAL/fsync；内核/授权仍为已有明确模型夹具。
func TestNormalQuotaNewBudgetHandoffKeepsOldHeldUntilComplete(t *testing.T) {
	t.Run("legacy-authorization-stays-fixed", func(t *testing.T) {
		f := newHandoffFixture(t)
		a := f.q.auth
		a.original.Command.BudgetCommitmentID = "new-operation-budget"
		_, e := f.run(context.Background(), a, "apply", normalQuotaHandoffDiskIO())
		require.Error(t, e)
		require.Empty(t, f.k.quotaWrites)
	})
	t.Run("original-transition-survives-partial-cap", func(t *testing.T) {
		f := newHandoffFixture(t)
		a := f.q.auth
		old := a.original.Command.BudgetCommitmentID
		a.original.Command.BudgetCommitmentID = "new-operation-budget"
		f.k.expected = a.original.Command
		a.budgetHandoff = &normalQuotaBudgetHandoff{fromBudgetCommitmentID: old, toBudgetCommitmentID: a.original.Command.BudgetCommitmentID, requestSHA256: a.original.PGRequestSHA256}
		f.k.failAfter = "arrays"
		_, e := f.run(context.Background(), a, "apply", normalQuotaHandoffDiskIO())
		require.ErrorIs(t, e, ErrUnknown)
		r := f.state(t)
		require.False(t, r.Applied)
		require.Equal(t, "new-operation-budget", r.Original.BudgetCommitmentID)
		require.Nil(t, r.Held, "原Bootstrap仍持旧预算，不能在partial提前Held新预算")
		require.Equal(t, old, r.Bootstrap.Original.BudgetCommitmentID)
		f.k.failAfter = ""
		_, e = f.run(context.Background(), a, "replay", normalQuotaHandoffDiskIO())
		require.NoError(t, e)
		r = f.state(t)
		require.True(t, r.Applied)
		require.Equal(t, "new-operation-budget", r.Held.BudgetCommitmentID)
		writes := len(f.k.quotaWrites)
		_, e = f.run(context.Background(), a, "observe", normalQuotaHandoffDiskIO())
		require.NoError(t, e)
		require.Len(t, f.k.quotaWrites, writes)
		_, e = f.q.f.g.Fence(context.Background(), 2)
		require.NoError(t, e)
		_, e = f.run(context.Background(), a, "observe", normalQuotaHandoffDiskIO())
		require.NoError(t, e, "旧epoch只读观察必须可达")
		require.Len(t, f.k.quotaWrites, writes, "旧epochObserve不得重放cap")

	})
	t.Run("wrong-request-cannot-release-old-budget", func(t *testing.T) {
		f := newHandoffFixture(t)
		a := f.q.auth
		old := a.original.Command.BudgetCommitmentID
		a.original.Command.BudgetCommitmentID = "new-operation-budget"
		a.budgetHandoff = &normalQuotaBudgetHandoff{fromBudgetCommitmentID: old, toBudgetCommitmentID: a.original.Command.BudgetCommitmentID, requestSHA256: "wrong"}
		_, e := f.run(context.Background(), a, "apply", normalQuotaHandoffDiskIO())
		require.True(t, errors.Is(e, ErrUnknown))
		require.Empty(t, f.k.quotaWrites)
	})
}
