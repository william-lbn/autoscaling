package agent

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestDSGALocalControlConfigKeepsDefaultOffAndRejectsUnsafePaths(t *testing.T) {
	// 只读既有官方配置作为完整Agent基础，防止省略无关必填值让测试因
	// 其他字段失败。此测试不改manifest、不创建目录或准备部署。
	raw, err := os.ReadFile("../../autoscaler-agent/config_map.yaml")
	require.NoError(t, err)
	var configMap struct {
		Data map[string]string `json:"data"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &configMap))
	var config Config
	require.NoError(t, json.Unmarshal([]byte(configMap.Data["config.json"]), &config))
	require.NoError(t, config.validate())
	require.Nil(t, config.DSGA)
	config.DSGA = &DSGAConfig{GatewayURL: "https://management.invalid", ManagementCAFile: "/trusted/management-ca.pem", ProjectedTokenFile: "/trusted/agent-token", RequestTimeoutSeconds: 10, ComputeTimeoutSeconds: 90, PollIntervalSeconds: 1, ControlSocketPath: "", PrepareQualifications: nil}
	require.NoError(t, config.validate(), "existing trusted configuration does not enable a socket")
	config.DSGA.ControlSocketPath = "/run/dsga-agent/control.sock"
	require.NoError(t, config.validate(), "config validation does not create deployment directories")
	for _, path := range []string{"relative.sock", "/run/dsga/../control.sock", "/" + strings.Repeat("x", 100)} {
		config.DSGA.ControlSocketPath = path
		require.ErrorContains(t, config.validate(), "coordination socket")
	}
}
