//go:build linux

package resourceguard

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// 这里只检固定部署合同，不能把纯文本测试当Source、normal或实际启动通过。
func TestNormalSystemSourceTrustedInittabDeployment(t *testing.T) {
	trusted := []byte(strings.Join(TrustedControllerInitEntries(), "\n") + "\n")
	old, e := NormalSystemSourceInitEntriesForVersion(3)
	require.NoError(t, e)
	legacy := []byte(strings.Join(old, "\n") + "\n")
	for _, c := range []struct {
		name, mode string
		version    uint32
		raw        []byte
		ok         bool
	}{
		{"exact-trusted", TrustedControllerExecutionModel, 3, trusted, true},
		{"unchanged-legacy", "", 3, legacy, true},
		{"no-model-cannot-adopt-direct", "", 3, trusted, false},
		{"trusted-rejects-legacy", TrustedControllerExecutionModel, 3, legacy, false},
		{"foreign-model", "foreign", 3, trusted, false},
		{"old-version-cannot-optin", TrustedControllerExecutionModel, 2, trusted, false},
		{"hidden-whitespace", TrustedControllerExecutionModel, 3, append(append([]byte{}, trusted...), ' '), false},
		{"extra-entry", TrustedControllerExecutionModel, 3, append(append([]byte{}, trusted...), []byte("::once:/bin/true\n")...), false},
		{"wrong-action", TrustedControllerExecutionModel, 3, []byte(strings.Replace(string(trusted), "::once:", "::wait:", 1)), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := normalSystemCheckDeploymentInittab(c.version, c.mode, c.raw)
			if c.ok {
				require.NoError(t, e)
			} else {
				require.Error(t, e)
			}
		})
	}
}
