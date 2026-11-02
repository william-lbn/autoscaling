package agent

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestDSGAResourceReadSocketIsOptionalAndSeparatedFromManagement(t *testing.T) {
	config := dsgaAgentConfig(t)
	require.Empty(t, config.DSGA.ResourceReadSocketPath)
	config.DSGA.ResourceReadSocketPath = "/run/dsga-resource/read.sock"
	require.NoError(t, config.validate(), "资源读取socket可独立启用，不要求开放管理socket")
	config.DSGA.ControlSocketPath = "/run/dsga-admin/control.sock"
	require.NoError(t, config.validate())
	raw, err := json.Marshal(config)
	require.NoError(t, err)
	var decoded Config
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, config.DSGA.ResourceReadSocketPath, decoded.DSGA.ResourceReadSocketPath)
	for _, path := range []string{"relative.sock", "/run/dsga/../read.sock", "/" + strings.Repeat("x", 100)} {
		config.DSGA.ResourceReadSocketPath = path
		require.ErrorContains(t, config.validate(), "resource socket")
	}
	for _, path := range []string{"/run/dsga-admin/read.sock", "/run/dsga-admin/child/read.sock", "/run/read.sock"} {
		config.DSGA.ResourceReadSocketPath = path
		require.ErrorContains(t, config.validate(), "disjoint")
	}
}
