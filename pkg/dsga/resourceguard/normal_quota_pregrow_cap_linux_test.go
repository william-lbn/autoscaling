//go:build linux

package resourceguard

import (
	"context"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// 这里只验证锁内计算与原 WAL/cgroup 模型合同；不冒充实际 PG/内核来源已合格。
func TestNormalQuotaPreGrowRetainsOriginalArrayCap(t *testing.T) {
	request := arrayRegistryWireFixture()
	request.StartupBuffers = 256
	v := NormalQuotaEnvelope{Phase: "pre-grow"}
	v.PGRequest.TargetBuffers = 8192
	// 与实际 PG17 注册布局相同：descriptor/block/IOCV/checkpoint 为 64/8192/16/20。
	committed := [4]control.Uint64{16384, 2097152, 4096, 8192}
	v.PGFence.Observed.Arrays = make([]control.ArrayFact, len(committed))
	for i := range committed {
		v.PGFence.Observed.Arrays[i].CommittedUpperBytes = committed[i]
	}
	const reserve control.Uint64 = 65 << 20
	t.Run("bootstrap-cap-covers-actual-grow", func(t *testing.T) {
		got, err := normalQuotaArrayTarget(v, request, reserve, 256<<20)
		require.NoError(t, err)
		require.EqualValues(t, 256<<20, got, "pre-grow 不得偷偷回收原保守 cap")
	})
	t.Run("larger-needed-cap-still-grows", func(t *testing.T) {
		got, err := normalQuotaArrayTarget(v, request, reserve, 64<<20)
		require.NoError(t, err)
		require.EqualValues(t, 136118272, got)
	})
	t.Run("current-committed-is-not-released", func(t *testing.T) {
		x := v
		x.PGFence.Observed.Arrays = append([]control.ArrayFact(nil), v.PGFence.Observed.Arrays...)
		x.PGRequest.TargetBuffers = 4096
		x.PGFence.Observed.Arrays[1].CommittedUpperBytes = 50 << 20
		got, err := normalQuotaArrayTarget(x, request, reserve, 64<<20)
		require.NoError(t, err)
		require.GreaterOrEqual(t, uint64(got), uint64(reserve)+(50<<20))
	})
	t.Run("missing-original-or-phase-is-unknown", func(t *testing.T) {
		_, err := normalQuotaArrayTarget(v, request, reserve, 0)
		require.ErrorIs(t, err, ErrUnknown)
		x := v
		x.PGFence.Observed.Arrays = append([]control.ArrayFact(nil), v.PGFence.Observed.Arrays...)
		x.Phase = "unconfirmed"
		_, err = normalQuotaArrayTarget(x, request, reserve, 256<<20)
		require.ErrorIs(t, err, ErrUnknown)
	})
}

func TestNormalSplitQuotaGrowRetainsBootstrapArrayCap(t *testing.T) {
	for _, test := range []struct {
		name                           string
		originalNormal, normal, arrays control.Uint64
		allowed                        bool
	}{
		{"grow-normal-same-array-cap", 1077411840, 1127743488, 256 << 20, true},
		{"grow-normal-lower-array-cap", 1077411840, 1127743488, (256 << 20) - (64 << 10), false},
		{"shrink-normal-same-array-cap", 1127743488, 1077411840, 256 << 20, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newQuotaFixture(t)
			f.k.snapshot.Parent.MemoryMaxBytes = test.originalNormal
			f.k.snapshot.Private.MemoryMaxBytes = 256 << 20
			f.k.snapshot.Arrays.MemoryMaxBytes = 256 << 20
			v := &f.auth.original
			v.Original = cloneSplit(f.k.snapshot)
			v.Command.MemoryMaxBytes = test.normal
			v.TargetArraysBytes = test.arrays
			v.ForceUpperBytes = 536313856
			v.CounterGapBytes = 4194304
			covered, err := normalInflightSum(uint64(v.Original.Private.MemoryMaxBytes), uint64(v.TargetArraysBytes), uint64(v.ForceUpperBytes), uint64(v.CounterGapBytes))
			require.NoError(t, err)
			require.LessOrEqual(t, covered, uint64(v.Command.MemoryMaxBytes), "先证明预算覆盖，方向负例不能靠 coverage 拒绝通过")
			if !test.allowed {
				require.Error(t, v.validate())
				if test.name == "shrink-normal-same-array-cap" {
					require.ErrorContains(t, v.validate(), "quota shrink must lower actual array domain")
				}
				_, err := f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
				require.ErrorIs(t, err, ErrUnknown)
				require.Empty(t, f.k.quotaWrites)
				return
			}
			require.NoError(t, v.validate())
			out, err := f.run(context.Background(), f.auth, "apply", normalSplitQuotaDiskIO())
			require.NoError(t, err)
			require.Equal(t, []string{"normal", "arrays"}, f.k.quotaWrites)
			require.Equal(t, v.Command.MemoryMaxBytes, out.Parent.MemoryMaxBytes)
			require.EqualValues(t, 256<<20, out.Arrays.MemoryMaxBytes)
			writes := len(f.k.quotaWrites)
			_, err = f.run(context.Background(), f.auth, "observe", normalSplitQuotaDiskIO())
			require.NoError(t, err)
			require.Len(t, f.k.quotaWrites, writes, "只读观察不重放任何写")
		})
	}
}

func TestNormalQuotaShrinkStillRequiresFourArrayRelease(t *testing.T) {
	_, _, command := fixtureGuard(t)
	valid := normalQuotaProtocolFixture(t, command)
	for i := 0; i < 4; i++ {
		t.Run(string(rune('0'+i)), func(t *testing.T) {
			x := cloneNormalQuota(valid)
			x.PGResponse.Observed.Arrays[i].ReleaseState = control.ReleaseFailed
			require.Error(t, x.Validate(), "任一数组无实际释放证明仍拒绝")
		})
	}
	missing := cloneNormalQuota(valid)
	missing.PGResponse = nil
	require.Error(t, missing.Validate())
}
