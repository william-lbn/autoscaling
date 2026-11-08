package owner

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// Owner fixture 启动真实 Unix socket。t.TempDir 的长测试名路径可能超过 sockaddr_un
// 上限，因此沿用短 basename；Linux 不制造 Mac 专属 /private/tmp，也不改生产路径
// 或 canonical/owner 校验。MkdirTemp 提供原来的 0700 私有目录，生命周期仍由 t 负责。
func ownerSocketTempDir(t *testing.T, prefix string) string {
	t.Helper()
	base := "/tmp"
	if runtime.GOOS == "darwin" {
		base = "/private/tmp"
	}
	base, err := filepath.EvalSymlinks(base)
	require.NoError(t, err)
	dir, err := os.MkdirTemp(base, prefix)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
