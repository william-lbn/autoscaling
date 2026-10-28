package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
)

func dsgaAgentConfig(t *testing.T) *Config {
	t.Helper()
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
	config.DSGA = &DSGAConfig{GatewayURL: "https://management.invalid", ManagementCAFile: "/trusted/management-ca.pem", ProjectedTokenFile: "/trusted/agent-token", RequestTimeoutSeconds: 10, ComputeTimeoutSeconds: 90, PollIntervalSeconds: 1, ControlSocketPath: "", PrepareQualifications: nil, AutomaticMemory: nil}
	require.NoError(t, config.validate(), "existing trusted configuration does not enable a socket")
	return &config
}

func TestDSGALocalControlConfigKeepsDefaultOffAndRejectsUnsafePaths(t *testing.T) {
	config := dsgaAgentConfig(t)
	config.DSGA.ControlSocketPath = "/run/dsga-agent/control.sock"
	require.NoError(t, config.validate(), "config validation does not create deployment directories")
	for _, path := range []string{"relative.sock", "/run/dsga/../control.sock", "/" + strings.Repeat("x", 100)} {
		config.DSGA.ControlSocketPath = path
		require.ErrorContains(t, config.validate(), "coordination socket")
	}
}

// 自动策略只能由可信Agent配置按VM UID开启，且必须复用Prepare的原资格。
// 用完整既有基础配置排除无关必填值影响；仅解析和校验，不准备任何部署。
func TestDSGAAutomaticPolicyConfigRequiresExplicitTargetsAndOriginalQualification(t *testing.T) {
	config := dsgaAgentConfig(t)
	require.Nil(t, config.DSGA.AutomaticMemory)
	source := budget.QualificationSource{Claim: budget.ObjectRef{Name: "qualified-original", UID: "qualification-uid"}, SHA256: strings.Repeat("a", 64)}
	config.DSGA.PrepareQualifications = map[string]budget.QualificationSource{"vm-uid": source}
	c := policy.Config{Targets: []policy.Target{{VMMemoryBytes: 512 << 20, Buffers: 512}, {VMMemoryBytes: 1 << 30, Buffers: 1024}}, SampleIntervalSeconds: 1, MaximumSampleGapSeconds: 5, GrowWindowSeconds: 2, ShrinkWindowSeconds: 4, CooldownSeconds: 6, RequestDeadlineSeconds: 120, ShrinkThresholdPermille: 200, GrowThresholdPermille: 800, EmergencyThresholdPermille: 950}
	config.DSGA.AutomaticMemory = map[string]policy.Config{"vm-uid": c}
	require.NoError(t, config.validate())
	for _, mode := range []string{"missing-source", "bad-vm-uid", "bad-threshold", "bad-source"} {
		t.Run(mode, func(t *testing.T) {
			copyConfig := *config
			copyDSGA := *config.DSGA
			copyConfig.DSGA = &copyDSGA
			switch mode {
			case "missing-source":
				copyDSGA.PrepareQualifications = nil
			case "bad-vm-uid":
				copyDSGA.AutomaticMemory = map[string]policy.Config{"bad/uid": c}
			case "bad-threshold":
				bad := c
				bad.EmergencyThresholdPermille = 1001
				copyDSGA.AutomaticMemory = map[string]policy.Config{"vm-uid": bad}
			case "bad-source":
				bad := source
				bad.SHA256 = "short"
				copyDSGA.PrepareQualifications = map[string]budget.QualificationSource{"vm-uid": bad}
			}
			require.ErrorContains(t, copyConfig.validate(), "automatic DSGA memory policy")
		})
	}
	raw, err := json.Marshal(config)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	loaded, err := ReadConfig(path)
	require.NoError(t, err)
	require.Equal(t, c, loaded.DSGA.AutomaticMemory["vm-uid"])
	raw = []byte(strings.Replace(string(raw), "emergencyThresholdPermille", "emergencyThresholdTypo", 1))
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	_, err = ReadConfig(path)
	require.ErrorContains(t, err, "unknown field", "拼错自动策略字段不能被静默当作已配置")
}
