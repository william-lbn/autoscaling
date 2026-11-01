//go:build linux

package resourceguard

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalSystemRolePlanExactAndNoEnvironmentDisclosure(t *testing.T) {
	raw := `{"protocol_version":1,"compute_uid":1000,"compute_gid":1000,"compute_argv":["/usr/local/bin/compute_ctl","--pgdata","/var/db/postgres/compute"],"compute_env":[],"loader_paths":[]}`
	p, e := decodeNormalSystemRoleBuildPlan([]byte(raw))
	require.NoError(t, e)
	require.NotNil(t, p.ComputeEnv)
	for name, s := range map[string]string{
		"case":        strings.Replace(raw, "protocol_version", "Protocol_Version", 1),
		"duplicate":   strings.Replace(raw, `"protocol_version":1`, `"protocol_version":1,"protocol_version":1`, 1),
		"alias":       strings.Replace(raw, `"protocol_version":1`, `"protocol_version":1,"Protocol_Version":1`, 1),
		"null":        strings.Replace(raw, `"compute_env":[]`, `"compute_env":null`, 1),
		"unknown":     strings.Replace(raw, `"protocol_version":1`, `"protocol_version":1,"path":"/bin/sh"`, 1),
		"root":        strings.Replace(raw, `"compute_uid":1000`, `"compute_uid":0`, 1),
		"program":     strings.Replace(raw, "/usr/local/bin/compute_ctl", "/bin/sh", 1),
		"environment": strings.Replace(raw, `"compute_env":[]`, `"compute_env":["LD_PRELOAD=NEVER_EMIT_THIS_VALUE"]`, 1),
		"double-env":  strings.Replace(raw, `"compute_env":[]`, `"compute_env":["PATH=/a","PATH=/b"]`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, e := decodeNormalSystemRoleBuildPlan([]byte(s))
			require.Error(t, e)
			require.NotContains(t, e.Error(), "NEVER_EMIT_THIS_VALUE")
		})
	}
}
func TestNormalSystemRoleCannotSpawnWithoutOriginalSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, e := spawnNormalSystemRoleOriginal(ctx, nil, nil, "resource", nil)
	require.ErrorIs(t, e, ErrUnknown)
	_, e = newNormalSystemRoleLaunchSpec(ctx, nil, "resource")
	require.ErrorIs(t, e, ErrUnknown)
}
