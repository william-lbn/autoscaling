//go:build linux

package vmguard

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

// 真实Unix/Start/pidfd/WAL沿既有显式模型；不是KVM或现场7410故障原因证明。
// 延迟只注入成功的纯StateClaim GET，不伪造超时/Owner或重放任何写者。
func TestInitialQMPOperationBudgetExcludesAuthorityLocalWork(t *testing.T) {
	f, original, trace := initialLifecycleFixture(t, "success")
	lifecycle := f.runner.initial.(*linuxInitialLifecycle)
	require.Equal(t, 2*time.Second, lifecycle.q.timeout, "生产原I/O窗口必须保持2秒")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	originalDeadline, _ := ctx.Deadline()
	var delayed atomic.Bool
	f.core.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		get, ok := action.(ktesting.GetAction)
		if !ok || get.GetName() != f.runner.birth.StateClaim.Name {
			return false, nil, nil
		}
		// 该文件仅在第三个QMP连接已完成原peer/geometry检查、第二次授权前存在。
		_, prepared := os.Stat(filepath.Join(f.config.StateDir, initialBootFile(3)))
		_, mayHaveSent := os.Stat(filepath.Join(f.config.StateDir, initialBootFile(4)))
		if prepared != nil || !os.IsNotExist(mayHaveSent) || !delayed.CompareAndSwap(false, true) {
			return false, nil, nil
		}
		timer := time.NewTimer(lifecycle.q.timeout + 150*time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return true, nil, ctx.Err()
		case <-timer.C:
		}
		return false, nil, nil
	})
	cmd := exec.CommandContext(ctx, original.Path, original.Args[1:]...)
	cmd.Env = original.Env
	result := make(chan error, 1)
	go func() { result <- f.runner.RunQEMU(ctx, cmd); close(result) }()
	consumed := false
	t.Cleanup(func() {
		cancel()
		if !consumed {
			select {
			case <-result:
			case <-time.After(10 * time.Second):
				t.Error("原RunQEMU未join同一child")
			}
		}
	})
	for {
		select {
		case err := <-result:
			consumed = true
			require.True(t, delayed.Load(), "未到预定的成功授权延迟边界")
			traceBytes, _ := os.ReadFile(trace)
			t.Logf("REPRO initial drive returned after successful read; qom_set_count=%d, may_have_sent_file=%t, parent_alive=%t", bytes.Count(traceBytes, []byte("qom_set:")), fileExistsForInitialQMPTest(filepath.Join(f.config.StateDir, initialBootFile(4))), ctx.Err() == nil)
			require.NoError(t, err, "成功的本地/权限读取不应耗尽后继完整QMP操作的原2秒IO预算")
			return
		default:
		}
		traceBytes, _ := os.ReadFile(trace)
		lifecycle.mu.Lock()
		ready := lifecycle.ready != nil
		lifecycle.mu.Unlock()
		if ready && bytes.Contains(traceBytes, []byte("completion_received")) {
			require.True(t, delayed.Load())
			require.Equal(t, 1, bytes.Count(traceBytes, []byte("qom_set:")), "唯一mutation不可因等待重放")
			require.NotContains(t, string(traceBytes), "BAD_")
			deadline, _ := ctx.Deadline()
			require.Equal(t, originalDeadline, deadline)
			require.NoError(t, ctx.Err())
			break
		}
		require.NoError(t, initialBootPause(ctx))
	}
	cancel()
	require.Error(t, <-result)
	consumed = true
	require.NotNil(t, cmd.ProcessState, "唯一RunQEMU必须回收原child")
}

func fileExistsForInitialQMPTest(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
