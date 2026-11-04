package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
)

func normalTrustedLauncherPlan(enabled bool) normalBuildPlan {
	env := []string{"PATH=/neonvm/bin", "RUST_LOG=info"}
	if enabled {
		env = append([]string{"OTEL_SDK_DISABLED=true"}, env...)
	}
	return normalBuildPlan{ProtocolVersion: 1, ExecutionModel: resourceguard.TrustedControllerExecutionModel, ComputeUID: 1000, ComputeGID: 1000, ComputeArgv: []string{"/usr/local/bin/compute_ctl", "--pgdata", "/var/db/postgres/compute"}, ComputeEnv: env, LoaderPaths: []string{}}
}
func normalTrustedLauncherFixture(t *testing.T) (string, []byte) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("original root-owned pre-base preparation requires root")
	}
	root := normalProductionConfigurationFixture(t)
	old := []byte(normalTrustedComputeScript(normalTrustedLauncherPlan(false)))
	require.NoError(t, os.WriteFile(filepath.Join(root, "neonvm/bin/vmstart"), old, 0755))
	for _, name := range []string{"neonvm/bin/dsga-resource-guard", "neonvm/bin/vminit", "usr/local/bin/compute_ctl", "neonvm/config/dsga-normal-system-source.json", "usr/share/neon-dsga/native-build.json"} {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte("protected-original"), 0644))
	}
	require.NoError(t, os.Chmod(filepath.Join(root, "neonvm/bin/dsga-resource-guard"), os.ModeSetuid|0755))
	return root, old
}
func TestTrustedComputeLauncherRefreshesPlanEnvironment(t *testing.T) {
	root, old := normalTrustedLauncherFixture(t)
	plan := normalTrustedLauncherPlan(true)
	require.NoError(t, normalPrepareTrustedComputeLauncher(root, normalSplitV3BuildMode, 2, plan, ""))
	got, e := os.ReadFile(filepath.Join(root, "neonvm/bin/vmstart"))
	require.NoError(t, e)
	require.False(t, bytes.Equal(got, old))
	require.True(t, bytes.Equal(got, []byte(normalTrustedComputeScript(plan))), "launcher must match original generator including fixed UID/argv/env/locks and guards")
	require.Contains(t, string(got), "OTEL_SDK_DISABLED=true")
	require.Contains(t, string(got), "check-guest")
	require.Contains(t, string(got), "wait-controller-ready")
	require.Contains(t, string(got), "flock -x")
	require.Contains(t, string(got), "env -i")
	require.NoError(t, normalPrepareTrustedComputeLauncher(root, normalSplitV3BuildMode, 2, plan, ""))
	again, e := os.ReadFile(filepath.Join(root, "neonvm/bin/vmstart"))
	require.NoError(t, e)
	require.True(t, bytes.Equal(got, again))
	st, e := os.Stat(filepath.Join(root, "neonvm/bin/vmstart"))
	require.NoError(t, e)
	require.Equal(t, os.FileMode(0755), st.Mode().Perm())
	owner := st.Sys().(*syscall.Stat_t)
	require.Equal(t, uint32(0), owner.Uid)
	require.Equal(t, uint32(0), owner.Gid)
	for _, name := range []string{"neonvm/bin/dsga-resource-guard", "neonvm/bin/vminit", "usr/local/bin/compute_ctl", "neonvm/config/dsga-normal-system-source.json", "usr/share/neon-dsga/native-build.json"} {
		b, e := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, e)
		require.Equal(t, "protected-original", string(b))
	}
	st, e = os.Stat(filepath.Join(root, "neonvm/bin/dsga-resource-guard"))
	require.NoError(t, e)
	require.Equal(t, os.ModeSetuid|0755, st.Mode()&(os.ModeSetuid|os.ModePerm))
}
func TestTrustedComputeLauncherKeepsOriginalConfigurationOnlyBoundary(t *testing.T) {
	root, old := normalTrustedLauncherFixture(t)
	require.NoError(t, normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, true, ""))
	got, e := os.ReadFile(filepath.Join(root, "neonvm/bin/vmstart"))
	require.NoError(t, e)
	require.True(t, bytes.Equal(got, old))
	require.NotContains(t, string(got), "OTEL_SDK_DISABLED")
	require.NoError(t, normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, false, ""))
	got, e = os.ReadFile(filepath.Join(root, "neonvm/bin/vmstart"))
	require.NoError(t, e)
	require.True(t, bytes.Equal(got, old))
}
func TestTrustedComputeLauncherRejectsWrongModeAndUnsafeTargets(t *testing.T) {
	for _, which := range []string{"ordinary", "legacy", "controls1", "post-base", "bad-env", "symlink", "hardlink", "fifo", "parent-alias"} {
		t.Run(which, func(t *testing.T) {
			root, old := normalTrustedLauncherFixture(t)
			plan := normalTrustedLauncherPlan(true)
			mode := normalSplitV3BuildMode
			controls := uint32(2)
			base := ""
			path := filepath.Join(root, "neonvm/bin/vmstart")
			switch which {
			case "ordinary":
				plan.ExecutionModel = ""
			case "legacy":
				mode = "legacy"
			case "controls1":
				controls = 1
			case "post-base":
				base = strings.Repeat("a", 64)
			case "bad-env":
				plan.ComputeEnv = []string{"LD_PRELOAD=forbidden"}
			case "symlink":
				require.NoError(t, os.Rename(path, path+".original"))
				require.NoError(t, os.Symlink("vmstart.original", path))
			case "hardlink":
				require.NoError(t, os.Link(path, path+".alias"))
			case "fifo":
				require.NoError(t, os.Remove(path))
				require.NoError(t, syscall.Mkfifo(path, 0755))
			case "parent-alias":
				require.NoError(t, os.Rename(filepath.Join(root, "neonvm/bin"), filepath.Join(root, "neonvm/real-bin")))
				require.NoError(t, os.Symlink("real-bin", filepath.Join(root, "neonvm/bin")))
			}
			require.Error(t, normalPrepareTrustedComputeLauncher(root, mode, controls, plan, base))
			if which != "fifo" {
				got, e := os.ReadFile(path)
				require.NoError(t, e)
				require.True(t, bytes.Equal(got, old))
			}
		})
	}
}
