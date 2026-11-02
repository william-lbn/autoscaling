package dsga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"testing"
)

// 来自原f7b实际PG17 SQL回执；这里只执行Go合同U，不冒新PG运行或Root许可。
func TestReleasedTailAcceptsActualPG17RoundedCheckpoint(t *testing.T) {
	raw, e := os.ReadFile("testdata/pg17-tail-release-f7b18b.json")
	require.NoError(t, e)
	var response ControlResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	require.True(t, response.Observed.AdmissionOpen, "保留原组件事实，不能冒关闭准入授权")
	for _, a := range response.Observed.Arrays {
		require.NoError(t, a.Provider.ValidateReleasedTail(), string(a.Array))
	}
	c := response.Observed.Arrays[3].Provider
	require.EqualValues(t, 5120, c.RequestedOffset)
	require.EqualValues(t, 8192, c.AffectedOffset)
	require.EqualValues(t, 158720, c.RequestedBytes)
	require.EqualValues(t, 155648, c.AffectedBytes)
	for name, change := range map[string]func(*ProviderFact){
		"missing-physical": func(p *ProviderFact) { p.PhysicalReleaseVerified = false },
		"partial":          func(p *ProviderFact) { p.ReleaseMayBePartial = true },
		"short-tail":       func(p *ProviderFact) { p.AffectedBytes -= 4096 },
		"overflow":         func(p *ProviderFact) { p.AffectedOffset = Uint64(math.MaxUint64); p.AffectedBytes = 2 },
	} {
		t.Run(name, func(t *testing.T) { p := c; change(&p); require.Error(t, p.ValidateReleasedTail()) })
	}
}
func TestReleasedTailNoopKeepsPhysicalEffectSeparate(t *testing.T) {
	p := ProviderFact{HasResult: true, Status: ProviderNoop}
	require.NoError(t, p.ValidateReleasedTail())
	p.RequestedOffset = 5120
	p.RequestedBytes = 880
	require.NoError(t, p.ValidateReleasedTail(), "只有结构成立；Root仍须原page证明完整页为空")
	p.AffectedBytes = 4096
	require.Error(t, p.ValidateReleasedTail())
}
