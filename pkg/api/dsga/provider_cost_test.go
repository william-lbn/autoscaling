package dsga

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

func providerCostFixture(t *testing.T) ProviderCostResponse {
	raw, e := os.ReadFile("testdata/response-v1.json")
	require.NoError(t, e)
	var base ControlResponse
	require.NoError(t, json.Unmarshal(raw, &base))
	c := ProviderCost{MinimumBuffers: 16, NormalMainAndArraysUpperBytes: 1 << 20}
	for _, a := range base.Observed.Arrays {
		c.NormalMainAndArraysUpperBytes += a.CommittedUpperBytes
		c.Arrays = append(c.Arrays, RegionProviderFact{Array: a.Array, Provider: "normal", GranuleBytes: 4096})
	}
	return ProviderCostResponse{ProtocolVersion: 1, Supported: true, ProviderCostAPIVersion: 1, Observed: &base.Observed, Cost: &c}
}
func TestProviderCostKeepsUnknownAndMinimum(t *testing.T) {
	f := providerCostFixture(t)
	bound := f.Observed.Instance
	require.NoError(t, f.ValidateFor(bound))
	raw, e := json.Marshal(f)
	require.NoError(t, e)
	require.Contains(t, string(raw), `"allocated_bytes":null`)
	for _, bad := range []string{strings.ReplaceAll(string(raw), `"allocated_bytes":null`, `"allocated_bytes":"0"`),
		strings.ReplaceAll(string(raw), `,"reserved_bytes":null`, ``), strings.Replace(string(raw), `"supported":true`, `"supported":true,"supported":false`, 1)} {
		var changed ProviderCostResponse
		e = json.Unmarshal([]byte(bad), &changed)
		if e == nil {
			require.Error(t, changed.ValidateFor(bound))
		}
	}
	f.Cost.MinimumBuffers = f.Observed.LowBuffers + 1
	require.Error(t, f.ValidateFor(bound))
	f = providerCostFixture(t)
	f.VMShrinkPermit = true
	require.Error(t, f.ValidateFor(bound))
}
func TestProviderCostSeparatesHugeAndUnsupported(t *testing.T) {
	f := providerCostFixture(t)
	bound := f.Observed.Instance
	size := f.Observed.Arrays[0].CommittedUpperBytes
	f.Cost.Arrays[0].Provider = "hugetlb"
	f.Cost.Arrays[0].GranuleBytes = 2 << 20
	f.Cost.NormalMainAndArraysUpperBytes -= size
	f.Cost.HugetlbMainAndArraysUpperBytes = size
	require.NoError(t, f.ValidateFor(bound))
	f.Cost.HugetlbMainAndArraysUpperBytes = 0
	require.Error(t, f.ValidateFor(bound))
	f.Supported = false
	f.ProviderCostAPIVersion = 0
	f.Cost = nil
	require.NoError(t, f.ValidateFor(bound))
	zero := ProviderCost{}
	f.Cost = &zero
	require.Error(t, f.ValidateFor(bound))
}
