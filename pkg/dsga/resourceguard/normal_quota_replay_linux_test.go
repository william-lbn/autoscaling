//go:build linux

package resourceguard

import (
	"context"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

// 直接测试Root消费器的锁内动作选择与原handoff，不用HTTP模型backend代替Root。
// 原handoff夹具为单元内核模型；这些结果不代表真实PG/cgroup或端到端通过。
func TestNormalQuotaExplicitReplayRootSelection(t *testing.T) {
	cases := []struct {
		name   string
		exists bool
		count  int
		change func(*record, *Command)
		want   string
	}{
		{name: "absent-unstarted-same-owner", want: "apply"},
		{name: "entry-only", exists: true, want: "apply"},
		{name: "absent-orphan-first-prefix", count: 1},
		{name: "absent-orphan-complete-prefix", count: 4},
		{name: "old-owner", change: func(r *record, c *Command) { r.Floor++ }},
		{name: "future-owner", change: func(r *record, c *Command) { c.IssuerEpoch++ }},
		{name: "different-instance", change: func(r *record, c *Command) { c.Identity.ComputeInstanceID = "another-compute" }},
		{name: "different-birth-pid", change: func(r *record, c *Command) { c.PostmasterPID++ }},
		{name: "different-birth-start", change: func(r *record, c *Command) { c.PostmasterStartTicks++ }},
		{name: "nonconsecutive-revision", change: func(r *record, c *Command) { c.Revision++ }},
		{name: "unapplied-bootstrap", change: func(r *record, c *Command) { r.Bootstrap.Applied = false }},
		{name: "same-original-but-envelope-lost", change: func(r *record, c *Command) { x := cloneCommand(*c); r.Original = &x }},
		{name: "another-original-pending", change: func(r *record, c *Command) {
			x := cloneCommand(*c)
			x.BudgetCommitmentID = "another-budget"
			r.Original = &x
		}},
		{name: "entry-pending", exists: true, change: func(r *record, c *Command) { x := cloneCommand(*c); r.Original = &x }, want: "replay"},
		{name: "entry-partial", exists: true, count: 2, change: func(r *record, c *Command) { x := cloneCommand(*c); r.Original = &x }, want: "replay"},
		{name: "entry-complete", exists: true, count: 4, change: func(r *record, c *Command) { x := cloneCommand(*c); r.Original = &x; r.Held = &x; r.Applied = true }, want: "replay"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newHandoffFixture(t)
			r := f.state(t)
			c := cloneCommand(f.q.auth.original.Command)
			if tc.change != nil {
				tc.change(r, &c)
			}
			// Same original resource flock and loaded record as production consumer.
			s, err := lockState(context.Background(), f.q.f.dir)
			require.NoError(t, err)
			defer s.close()
			got, err := normalQuotaReplayModeLocked(r, c, tc.exists, tc.count)
			if tc.want == "" {
				require.ErrorIs(t, err, ErrUnknown)
				require.Empty(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
			require.Empty(t, f.k.quotaWrites, "selection is never authority or a cap effect")
		})
	}
}

func TestNormalQuotaExplicitReplayRetainsOriginalHandoff(t *testing.T) {
	t.Run("absent-before-first-effect", func(t *testing.T) {
		f := newHandoffFixture(t)
		mode, err := normalQuotaReplayModeLocked(f.state(t), f.q.auth.original.Command, false, 0)
		require.NoError(t, err)
		_, err = f.run(context.Background(), f.q.auth, mode, normalQuotaHandoffDiskIO())
		require.NoError(t, err)
		require.Equal(t, []string{"arrays", "normal"}, f.k.quotaWrites)
		r := f.state(t)
		require.True(t, r.Applied)
		require.True(t, reflect.DeepEqual(*r.Original, *r.Held))
	})
	t.Run("partial-replay-does-not-repeat-arrays", func(t *testing.T) {
		f := newHandoffFixture(t)
		f.k.failAfter = "arrays"
		_, err := f.run(context.Background(), f.q.auth, "apply", normalQuotaHandoffDiskIO())
		require.ErrorIs(t, err, ErrUnknown)
		s, err := lockState(context.Background(), f.q.f.dir)
		require.NoError(t, err)
		r, err := s.load()
		require.NoError(t, err)
		_, count, _, err := normalSplitQuotaRead(s, normalSplitQuotaCommandKey(f.q.auth.original.Command))
		require.NoError(t, err)
		mode, err := normalQuotaReplayModeLocked(r, f.q.auth.original.Command, true, count)
		s.close()
		require.NoError(t, err)
		require.Equal(t, "replay", mode)
		f.k.failAfter = ""
		_, err = f.run(context.Background(), f.q.auth, mode, normalQuotaHandoffDiskIO())
		require.NoError(t, err)
		require.Equal(t, []string{"arrays", "normal"}, f.k.quotaWrites)
	})
}
