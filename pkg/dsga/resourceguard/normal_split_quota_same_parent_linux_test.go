package resourceguard

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 这里只验证原事务夹具的移交与恢复合同，不代替真实 cgroup 或来源验证。
func TestNormalSplitQuotaSameParentArraysHandoff(t *testing.T) {
	for _, direction := range []string{"grow_arrays", "shrink_arrays", "same_arrays"} {
		t.Run(direction, func(t *testing.T) {
			f := newQuotaFixture(t)
			v := &f.auth.original
			v.Command.MemoryMaxBytes = v.Original.Parent.MemoryMaxBytes
			v.TargetArraysBytes = v.Original.Arrays.MemoryMaxBytes
			if direction == "grow_arrays" {
				v.TargetArraysBytes += 64 << 10
			}
			if direction == "shrink_arrays" {
				v.TargetArraysBytes -= 64 << 10
			}
			require.NoError(t, v.validate())
			require.False(t, v.growing())
			_, e := f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
			require.NoError(t, e)
			require.Equal(t, v.Command.MemoryMaxBytes, f.k.snapshot.Parent.MemoryMaxBytes)
			require.Equal(t, v.TargetArraysBytes, f.k.snapshot.Arrays.MemoryMaxBytes)
			require.NoError(t, f.gate(t))
		})
	}
}

func TestNormalSplitQuotaSameParentKeepsCoverage(t *testing.T) {
	for _, wrong := range []string{"missing_pg_request", "uncovered_continuous_budget", "wrong_parent_pids"} {
		t.Run(wrong, func(t *testing.T) {
			f := newQuotaFixture(t)
			v := &f.auth.original
			v.Command.MemoryMaxBytes = v.Original.Parent.MemoryMaxBytes
			v.TargetArraysBytes = v.Original.Arrays.MemoryMaxBytes
			switch wrong {
			case "missing_pg_request":
				v.PGRequestSHA256 = ""
			case "uncovered_continuous_budget":
				v.ForceUpperBytes = v.Command.MemoryMaxBytes
			case "wrong_parent_pids":
				v.Command.PidsMaxTasks++
			}
			require.Error(t, v.validate())
			_, e := f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
			require.ErrorIs(t, e, ErrUnknown)
			require.Empty(t, f.k.quotaWrites)
		})
	}
}

func TestNormalSplitQuotaSameParentPartialRequiresExactReplay(t *testing.T) {
	for _, direction := range []string{"grow_arrays", "shrink_arrays", "same_arrays"} {
		for _, at := range []string{"arrays", "normal"} {
			t.Run(direction+"/"+at, func(t *testing.T) {
				f := newQuotaFixture(t)
				v := &f.auth.original
				v.Command.MemoryMaxBytes = v.Original.Parent.MemoryMaxBytes
				v.TargetArraysBytes = v.Original.Arrays.MemoryMaxBytes
				if direction == "grow_arrays" {
					v.TargetArraysBytes += 64 << 10
				}
				if direction == "shrink_arrays" {
					v.TargetArraysBytes -= 64 << 10
				}
				f.k.failAfter = at
				_, e := f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
				require.ErrorIs(t, e, ErrUnknown)
				require.ErrorIs(t, f.gate(t), ErrUnknown)
				count := len(f.k.quotaWrites)
				_, e = f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
				require.ErrorIs(t, e, ErrUnknown)
				require.Len(t, f.k.quotaWrites, count)
				changed := f.auth
				changed.original.PGRequestSHA256 = strings.Repeat("34", 32)
				_, e = f.run(context.Background(), changed, "replay", normalSplitQuotaDiskIO())
				require.ErrorIs(t, e, ErrUnknown)
				require.Len(t, f.k.quotaWrites, count)
				f.k.failAfter = ""
				_, e = f.run(context.Background(), f.auth, "replay", normalSplitQuotaDiskIO())
				require.NoError(t, e)
				require.Equal(t, v.Command.MemoryMaxBytes, f.k.snapshot.Parent.MemoryMaxBytes)
				require.Equal(t, v.TargetArraysBytes, f.k.snapshot.Arrays.MemoryMaxBytes)
				require.NoError(t, f.gate(t))
			})
		}
	}
}
