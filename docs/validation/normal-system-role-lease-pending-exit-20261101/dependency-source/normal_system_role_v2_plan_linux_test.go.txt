//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestNormalSystemRoleV2PlanPreservesLegacyDigestAndBindsMode(t *testing.T) {
	old := normalSystemRoleLaunchSpec{role: "allow-start", argv: []string{"/neonvm/bin/busybox", "touch", "/neonvm/vmstart.allowed"}, env: []string{"PATH=/neonvm/bin"}, sourceSHA: strings.Repeat("ab", 32)}
	ah, e := normalSystemVectorSHA(old.argv)
	require.NoError(t, e)
	eh, e := normalSystemVectorSHA(old.env)
	require.NoError(t, e)
	raw, e := json.Marshal(struct {
		Role                       string
		UID, GID                   uint32
		ArgvSHA, EnvSHA, SourceSHA string
		Binding                    normalSystemLifecycleBinding
	}{old.role, old.uid, old.gid, ah, eh, old.sourceSHA, old.binding})
	require.NoError(t, e)
	before, e := old.referenceSHA256()
	require.NoError(t, e)
	require.Equal(t, physicalDigest(raw), before, "omitted zero mode preserves old pending plan bytes")
	next := old
	next.roleSourceProtocol = 2
	after, e := next.referenceSHA256()
	require.NoError(t, e)
	require.NotEqual(t, before, after, "new capability must be bound before durable pending")
}
func TestNormalSystemRoleV2MissingSourceCannotSelectCapability(t *testing.T) {
	for _, role := range []string{"network", "compute", "resource", normalSystemRoleBrokerKind} {
		_, selected, e := normalSystemRoleV2LaunchSpecIfSelected(context.Background(), nil, role)
		require.ErrorIs(t, e, ErrUnknown)
		require.False(t, selected)
	}
}
