//go:build linux

package resourceguard

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNormalImmutableBoundsDerivedOriginalTargets(t *testing.T) {
	_, raw, assets, _ := initialPrebootFixture(t)
	plan, e := DeriveRuntimeBootRequirements(raw, assets)
	require.NoError(t, e)
	require.Equal(t, NormalSplitV1PhysicalKernelProfileID, plan.ProfileID())
	var doc RuntimeDocument
	require.NoError(t, json.Unmarshal(raw, &doc))
	source, e := doc.Source()
	require.NoError(t, e)
	valid := source.Limits
	valid.MemoryMaxBytes += 64 << 10
	require.NoError(t, normalImmutableBoundsTarget(plan, source, valid), "mutable N is governed separately")
	for _, mode := range []string{"allocate-huge", "omit-hstate", "add-hstate", "size-hstate", "pids", "system", "as", "source-huge"} {
		t.Run(mode, func(t *testing.T) {
			target := valid
			target.HugePools = append([]HugePool(nil), valid.HugePools...)
			s := source
			s.Limits.HugePools = append([]HugePool(nil), source.Limits.HugePools...)
			switch mode {
			case "allocate-huge":
				target.HugePools[0].Pages = 1
			case "omit-hstate":
				target.HugePools = target.HugePools[:len(target.HugePools)-1]
			case "add-hstate":
				target.HugePools = append(target.HugePools, HugePool{PageSizeKB: 32768})
			case "size-hstate":
				target.HugePools[0].PageSizeKB++
			case "pids":
				target.PidsMaxTasks++
			case "system":
				target.SystemMemoryMaxBytes++
			case "as":
				target.AddressSpaceMaxBytes++
			case "source-huge":
				s.Limits.HugePools[0].Pages = 1
				target.HugePools[0].Pages = 1
			}
			require.Error(t, normalImmutableBoundsTarget(plan, s, target))
		})
	}
	legacy := RuntimeBootRequirements{}
	changed := valid
	changed.HugePools = append([]HugePool(nil), valid.HugePools...)
	changed.HugePools[0].Pages = 1
	require.NoError(t, normalImmutableBoundsTarget(legacy, source, changed), "unselected legacy profile behavior preserved; no normal authority issued")
}
