//go:build linux && amd64 && dsga_native_inflight_metadata_origin && dsga_native_stock_preparation && dsga_native_ordinary2 && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package vmguard

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 复用原真实RunQEMU/8WAL/6-8与唯一Wait，新增目标单独核B1标记，不重计旧stock S。
func TestNativeNormalInflightMetadataOriginHost(t *testing.T) {
	require.Equal(t, "qemu18c-transfer2", os.Getenv("DSGA_INITIAL_HOST_NATIVE"))
	TestNativeNormalStockUdevPreparationHost(t)
	raw, e := os.ReadFile("/proof/console.log")
	require.NoError(t, e)
	require.Equal(t, 1, bytes.Count(raw, []byte("INFLIGHT_METADATA_GUEST_EXIT=0")))
	require.NotContains(t, string(raw), "INFLIGHT_METADATA_ORIGIN_FAILURE")
	var value map[string]json.RawMessage
	count := 0
	for _, line := range strings.Split(string(raw), "\n") {
		const prefix = "INFLIGHT_METADATA_ORIGIN_JSON "
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		count++
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, prefix)), &value))
	}
	require.Equal(t, 1, count)
	for _, key := range []string{"original_memory2_r2_consumed", "original_R1_already_exited", "fresh_reference_checked", "new_guard_rejected", "canceled_rejected", "JSON_shape_rejected", "closed_source_rejected"} {
		require.Equal(t, json.RawMessage("true"), value[key], key)
	}
	for _, key := range []string{"Host_capacity_guaranteed", "normal_authorized"} {
		require.Equal(t, json.RawMessage("false"), value[key], key)
	}
	require.Equal(t, json.RawMessage(`"NEON-S-INFLIGHT-METADATA-ORIGIN-001"`), value["stable_id"])
	saveInitialNative(t, "inflight-metadata-origin-host-verdict", map[string]any{"stable_id": "NEON-S-INFLIGHT-METADATA-ORIGIN-001", "actual_original_QMP_8WAL": true, "real_R2_memory2_kernel2": true, "source_marker_count": count, "old_stock_target_is_dependency_not_additional_stable_id": true, "Host_Source_API_is_model": true, "Host_capacity_guaranteed": false, "normal_authorized": false, "E": 0})
}
