//go:build linux && dsga_native_filesystem_origin && dsga_native_system_lifecycle && dsga_native_normal_immutable_bounds

package resourceguard

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func init() {
	if normalFilesystemNativeMode {
		// 只影响DEV Go runner；内核/proc原argv仍为固定initial-boot。
		os.Args = []string{os.Args[0], "-test.run=^TestNativeNormalImmutableBounds$", "-test.v", "-test.timeout=100s"}
	}
}

// PG17-S-NORMAL-IMMUTABLE-BOUNDS-001。真实369/原ISO/单ext4 switch_root，
// 无PG：先原生产Bootstrap正控，再直接调用最终writer测试拒绝前后所有控制不变。
func TestNativeNormalImmutableBounds(t *testing.T) {
	require.True(t, normalFilesystemNativeMode)
	t.Run("actual-original-bootstrap-and-filesystem", TestNativeNormalFilesystemOriginalSource)
	source, _, e := runtimeSource()
	require.NoError(t, e)
	k := &linuxKernel{config: source.Config}
	require.NoError(t, k.normalImmutableBounds(source.Limits))
	controls := func() map[string]string {
		out := map[string]string{}
		for _, dir := range []string{pgCgroup, systemCgroup} {
			for _, name := range []string{"memory.max", "memory.swap.max", "pids.max"} {
				value, e := kernelText(filepath.Join(dir, name))
				require.NoError(t, e)
				out[filepath.Join(dir, name)] = value
			}
		}
		pools, e := allPools()
		require.NoError(t, e)
		for _, h := range pools {
			for _, name := range []string{"nr_hugepages", "nr_overcommit_hugepages", "surplus_hugepages"} {
				path := filepath.Join(poolDirectory(h.PageSizeKB), name)
				value, e := kernelText(path)
				require.NoError(t, e)
				out[path] = value
			}
		}
		return out
	}
	wal := func() map[string]string {
		out := map[string]string{}
		entries, e := os.ReadDir(source.Config.StateDir)
		require.NoError(t, e)
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".json") {
				b, e := os.ReadFile(filepath.Join(source.Config.StateDir, entry.Name()))
				require.NoError(t, e)
				out[entry.Name()] = physicalDigest(b)
			}
		}
		return out
	}
	before, originalWAL := controls(), wal()
	errorsByCase := map[string]string{}
	for _, mode := range []string{"huge-before-all-other-writes", "task", "system", "address-space"} {
		target := source.Limits
		target.HugePools = append([]HugePool(nil), source.Limits.HugePools...)
		target.MemoryMaxBytes += 64 << 10
		switch mode {
		case "huge-before-all-other-writes":
			target.HugePools[0].Pages = 1
			target.SystemMemoryMaxBytes += 64 << 10
			target.PidsMaxTasks = 1
		case "task":
			target.PidsMaxTasks = 1
		case "system":
			target.SystemMemoryMaxBytes += 64 << 10
		case "address-space":
			target.AddressSpaceMaxBytes++
		}
		e = k.ApplyBounds(target)
		require.ErrorIs(t, e, ErrUnknown)
		require.ErrorContains(t, e, "immutable normal bounds")
		if mode == "huge-before-all-other-writes" {
			require.ErrorContains(t, e, "HugeTLB target differs")
		}
		errorsByCase[mode] = e.Error()
		require.Equal(t, before, controls(), "rejection changed kernel controls")
		require.Equal(t, originalWAL, wal(), "rejection changed original journals")
	}
	report := map[string]any{"id": "PG17-S-NORMAL-IMMUTABLE-BOUNDS-001", "actual_original_Bootstrap_positive": true, "actual_final_writer_negative_cases": errorsByCase, "all_controls_unchanged": true, "all_original_JSON_unchanged": true, "PG_executed": false, "normal_authorized": false, "E": 0}
	b, e := json.Marshal(report)
	require.NoError(t, e)
	fmt.Println("NORMAL_IMMUTABLE_BOUNDS_REPORT=" + string(b))
}
