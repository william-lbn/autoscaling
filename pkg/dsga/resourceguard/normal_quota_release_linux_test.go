//go:build linux

package resourceguard

import (
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestNormalQuotaReleasedTailUsesActualPG17OriginalGeometry(t *testing.T) {
	raw, e := os.ReadFile("../../api/dsga/testdata/pg17-tail-release-f7b18b.json")
	require.NoError(t, e)
	var response control.ControlResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	checkpoint := response.Observed.Arrays[3]
	span := arrayRegistryWireRange{Page: 4096, Element: 20, Maximum: 8192 * 20, Mapped: 8192 * 20}
	upper, e := normalQuotaReleasedArrayUpper(checkpoint, span, 256)
	require.NoError(t, e)
	require.EqualValues(t, 8192, upper)
	for name, change := range map[string]func(*control.ArrayFact){
		"one-page-short": func(a *control.ArrayFact) { a.Provider.AffectedBytes -= 4096 },
		"inside-active-boundary-page": func(a *control.ArrayFact) {
			a.Provider.AffectedOffset = 6144
			a.Provider.AffectedBytes = control.Uint64(span.Mapped - 6144)
		},
		"past-mapped":              func(a *control.ArrayFact) { a.Provider.AffectedBytes += 4096 },
		"different-logical-target": func(a *control.ArrayFact) { a.Provider.RequestedOffset += 20; a.Provider.RequestedBytes -= 20 },
		"unknown-allocation":       func(a *control.ArrayFact) { a.AllocationUnknown = true },
	} {
		t.Run(name, func(t *testing.T) {
			a := checkpoint
			change(&a)
			_, e := normalQuotaReleasedArrayUpper(a, span, 256)
			require.Error(t, e)
		})
	}
	t.Run("owned-last-page-padding", func(t *testing.T) {
		s := span
		s.Maximum += 20
		s.Mapped += 4096
		a := checkpoint
		a.Provider.RequestedBytes += 20
		a.Provider.AffectedBytes += 4096
		_, e := normalQuotaReleasedArrayUpper(a, s, 256)
		require.NoError(t, e, "Tail独占padding与普通Release不同")
	})
	t.Run("known-zero-request-fast-path", func(t *testing.T) {
		a := checkpoint
		a.Provider = control.ProviderFact{HasResult: true, Status: control.ProviderNoop}
		_, e := normalQuotaReleasedArrayUpper(a, span, 256)
		require.NoError(t, e)
	})
	t.Run("logical-noop-only-when-whole-tail-pages-empty", func(t *testing.T) {
		a := checkpoint
		a.Provider = control.ProviderFact{HasResult: true, Status: control.ProviderNoop, RequestedOffset: 5120, RequestedBytes: 880}
		s := span
		s.Maximum = 6000
		s.Mapped = 8192
		_, e := normalQuotaReleasedArrayUpper(a, s, 256)
		require.NoError(t, e, "对应低层C整页区间为空，不是新PG完成事实")
		a.ReleaseState = control.ReleaseFailed
		_, e = normalQuotaReleasedArrayUpper(a, s, 256)
		require.Error(t, e, "不得把低层NOOP失败/未知升级为发布完成")
		a.ReleaseState = control.ReleaseVerified
		a.Provider.RequestedBytes = control.Uint64(span.Maximum - 5120)
		_, e = normalQuotaReleasedArrayUpper(a, span, 256)
		require.Error(t, e, "有完整页时非零logical NOOP不能冒完成")
	})
}
