//go:build linux

package resourceguard

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalSystemV3RolePlanUsesManifestVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m := normalSystemV3ModelManifest(t)
	// 这里只是原private plan逻辑的模型，不伪造真实System/Fork/exec来源。
	source := &normalSystemLifecycleSource{manifest: m, observed: normalSystemLifecycleObservation{Scope: normalSystemLifecycleScope}}
	plan, err := newNormalSystemRoleLaunchSpec(ctx, source, "udev-init")
	require.NoError(t, err)
	want, err := normalSystemFixedRoleArgvForVersion(3, "udev-init")
	require.NoError(t, err)
	require.Equal(t, want, plan.argv)
	require.Equal(t, []string{"PATH=/neonvm/bin"}, plan.env)
	require.Equal(t, normalSystemRoleBuildPlanPath, normalSystemV3BuildPlanPath)
	source.manifest.ProtocolVersion = 4
	_, err = newNormalSystemRoleLaunchSpec(ctx, source, "udev-init")
	require.Error(t, err, "unknown manifest version must not silently borrow default-v2 payload")
	require.ErrorIs(t, source.requireContinuousQualification(), ErrUnknown, "v3 static plan must not become continuous Source")
}
