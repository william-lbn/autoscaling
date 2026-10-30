package resourceguard

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresExecEnvironmentRemovesPreMainInjectionAndCallerProfile(t *testing.T) {
	original := []string{
		"NEON_AUTH_TOKEN=storage-token-with=equals", "PGDATA=/var/db/postgres/compute",
		"NEON_CONTROL_PLANE_TOKEN=control-token", "LANG=en_US.UTF-8",
		"LD_PRELOAD=/tmp/early.so", "LD_LIBRARY_PATH=/tmp/libs", "LD_AUDIT=/tmp/audit.so",
		"LD_FUTURE_LOADER_OPTION=1", "GLIBC_TUNABLES=glibc.rtld.nns=16",
		"GCONV_PATH=/tmp/gconv", "LOCPATH=/tmp/locale", "NLSPATH=/tmp/nls",
		"GETCONF_DIR=/tmp/conf", "MALLOC_TRACE=/tmp/trace", "MALLOC_ARENA_MAX=1024",
		addressSpaceEnvironmentKey + "=1", "NEON_DSGA_EARLY_ARRAYS_PROFILE=test-v1:128",
		"NEON_DSGA_FUTURE_PROFILE=forged", "DATABRICKS_COMPUTE_ID=business-id",
	}
	before := append([]string(nil), original...)
	got, err := postgresExecEnvironment(original, finiteAS)
	require.NoError(t, err)
	require.Equal(t, []string{
		"NEON_AUTH_TOKEN=storage-token-with=equals", "PGDATA=/var/db/postgres/compute",
		"NEON_CONTROL_PLANE_TOKEN=control-token", "LANG=en_US.UTF-8",
		"DATABRICKS_COMPUTE_ID=business-id", addressSpaceEnvironmentKey + "=134217728",
	}, got)
	require.Equal(t, before, original, "不改调用者原环境，也不把 token 放进错误")
	got, err = postgresExecEnvironment(original, 0)
	require.NoError(t, err)
	for _, entry := range got {
		require.False(t, strings.HasPrefix(entry, "NEON_DSGA_"))
	}
}

func TestPostgresExecEnvironmentRejectsAmbiguityAndMalformedInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
	}{
		{"duplicate-token", []string{"NEON_AUTH_TOKEN=secret-one", "NEON_AUTH_TOKEN=secret-two"}},
		{"duplicate-loader", []string{"LD_PRELOAD=secret-one", "LD_PRELOAD=secret-two"}},
		{"missing-equals", []string{"SECRET"}},
		{"empty-key", []string{"=secret"}},
		{"nul", []string{"TOKEN=secret\x00other"}},
		{"invalid-key", []string{"BAD-NAME=secret"}},
		{"initial-digit", []string{"1TOKEN=secret"}},
		{"oversized", []string{"TOKEN=" + strings.Repeat("s", maxPostgresEnvironmentBytes)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := postgresExecEnvironment(tc.env, finiteAS)
			require.Error(t, err)
			require.Nil(t, got)
			require.NotContains(t, err.Error(), "secret")
		})
	}
	got, err := postgresExecEnvironment(nil, finiteAS+1)
	require.Error(t, err)
	require.Nil(t, got)
}

func TestPostgresExecEnvironmentFixedByteBoundary(t *testing.T) {
	// 环境总长包含每个 entry 的终止 NUL；恰好上限可用，再多一个字节拒绝。
	entry := "V=" + strings.Repeat("x", maxPostgresEnvironmentBytes-3)
	got, err := postgresExecEnvironment([]string{entry}, 0)
	require.NoError(t, err)
	require.Equal(t, []string{entry}, got)
	_, err = postgresExecEnvironment([]string{entry + "x"}, 0)
	require.Error(t, err)
	_, err = postgresExecEnvironment([]string{entry, "B="}, 0)
	require.Error(t, err)
}
