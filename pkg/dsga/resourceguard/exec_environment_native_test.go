//go:build linux && dsga_native_exec_environment

package resourceguard

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// 真实 loader 对照：先证明同一 ELF 的 main 前确实执行了测试 DSO，再验证
// 生产净化函数的结果阻止该执行。它不是 PG/Root 出生迁组或 normal 降限验收。
func TestNativePostgresExecEnvironmentBlocksActualPreMainLoader(t *testing.T) {
	if os.Getenv("PG17_NATIVE_EXEC_ENVIRONMENT") != "isolated-loader-component-v1" {
		t.Fatal("explicit isolated loader component opt-in required")
	}
	dir := t.TempDir()
	marker := filepath.Join(dir, "constructor-ran")
	library := filepath.Join(dir, "pre-main.so")
	target := filepath.Join(dir, "target")
	constructor := `#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
__attribute__((constructor)) static void before_main(void) {
  const char *path = getenv("PG17_TEST_CONSTRUCTOR_MARKER");
  if (!path) _exit(91);
  int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
  if (fd < 0 || write(fd, "pre-main", 8) != 8) _exit(92);
  close(fd);
}
`
	program := `#include <stdlib.h>
#include <string.h>
int main(void) {
  const char *token = getenv("NEON_AUTH_TOKEN");
  if (!token || strcmp(token, "component-placeholder")) return 93;
  return 0;
}
`
	for name, contents := range map[string]string{"pre-main.c": constructor, "target.c": program} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(contents), 0600))
	}
	for _, args := range [][]string{
		{"-shared", "-fPIC", "-o", library, filepath.Join(dir, "pre-main.c")},
		{"-o", target, filepath.Join(dir, "target.c")},
	} {
		cmd := exec.Command("gcc", args...)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "component C compile: %s", out)
	}
	original := []string{"NEON_AUTH_TOKEN=component-placeholder", "LD_PRELOAD=" + library,
		"PG17_TEST_CONSTRUCTOR_MARKER=" + marker,
		"NEON_DSGA_EARLY_ARRAYS_PROFILE=test-v1:128"}
	run := func(environment []string) {
		t.Helper()
		cmd := exec.Command(target)
		cmd.Env = environment
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "isolated component target: %s", out)
	}
	run(original)
	contents, err := os.ReadFile(marker)
	require.NoError(t, err, "先证明该 DSO 真实在 main 前执行，排除 fixture 假绿")
	require.Equal(t, "pre-main", string(contents))
	require.NoError(t, os.Remove(marker))
	clean, err := postgresExecEnvironment(original, finiteAS)
	require.NoError(t, err)
	run(clean)
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "净化环境仍执行了 main 前 DSO：%v", err)
	fmt.Println("PG17_NATIVE_EXEC_ENVIRONMENT: baseline constructor=ran; sanitized constructor=absent; business token=preserved")
}
