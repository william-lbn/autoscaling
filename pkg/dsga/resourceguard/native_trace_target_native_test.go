//go:build linux && dsga_native_birth_tracer

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// 复用原真实PG PMbase停点，专测新增typed目标构造/原pidfd绑定与同停点回调。
// 没有数组helper、cgroup迁移或normal写者；原Source/WAL仍是明确fixture。
func TestNativeTraceOriginalStoppedKernelTarget(t *testing.T) {
	launcher, pg, hash := nativeTraceSetup(t)
	for _, mode := range []string{"valid", "wrong-start", "callback-cancel", "callback-panic"} {
		t.Run(mode, func(t *testing.T) {
			h := nativeTraceStartHarness(t, launcher, pg, hash)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			driver, err := newNativeBirthTraceKernel(h.conn, h.cfg, 0)
			require.NoError(t, err)
			defer driver.close()
			raw, err := driver.receive(ctx)
			require.NoError(t, err)
			request, err := decodeNativeBirthRequest(raw, h.cfg.Expected)
			require.NoError(t, err)
			bundle, err := driver.compile(ctx, request)
			require.NoError(t, err)
			dupFile := func(fd int) *os.File {
				n, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
				require.NoError(t, err)
				f := os.NewFile(uintptr(n), "native-stopped-original-source")
				t.Cleanup(func() { _ = f.Close() })
				return f
			}
			source := nativeTraceOriginalTargetSource{PID: h.cfg.Original.PostmasterPID, StartTicks: uint64(h.cfg.Original.PostmasterStartTicks), UID: h.cfg.PeerUID, GID: h.cfg.PeerGID, GuestBootID: h.cfg.Original.Identity.GuestBootID, PIDFD: dupFile(driver.pidfd), ProcDirectory: dupFile(driver.procfd), Executable: pg, ExecutableSHA256: hash}
			if mode == "wrong-start" {
				source.StartTicks++
			}
			target, err := newNativeTraceOriginalStoppedTarget(source)
			if mode == "wrong-start" {
				require.Error(t, err)
				require.Nil(t, target)
				require.NoError(t, h.conn.Close())
				report := h.finish(t)
				require.Equal(t, 1, report.Code)
				require.Zero(t, report.Signal)
				nativeTraceSave(t, "stopped-target-"+mode, map[string]any{"sourceRejected": true, "callbackReached": false, "child": report, "normalAuthorized": false})
				return
			}
			require.NoError(t, err)
			defer target.close()
			callbackReached := false
			tr, err := nativeTraceWithOriginalStoppedTarget(ctx, target, func(_ context.Context, original *nativeTraceOriginalStoppedTarget) error {
				callbackReached = true
				if original != target {
					return errors.New("original typed target changed")
				}
				if err := readAndCompareNativeKernelFilters(original.pid, unix.Gettid(), bundle, nativeFilterPMBase); err != nil {
					return err
				}
				if mode == "callback-cancel" {
					cancel()
				}
				if mode == "callback-panic" {
					panic("explicit stopped-target fixture panic")
				}
				return nil
			})
			require.True(t, callbackReached)
			if mode == "valid" {
				require.NoError(t, err)
				require.True(t, tr.OriginalConfirmed)
				require.True(t, tr.ExitKillInstalled)
				require.True(t, tr.CallbackCompleted)
				require.True(t, tr.Detached)
				require.NoError(t, h.conn.Close())
			} else {
				require.ErrorIs(t, err, ErrUnknown)
				require.False(t, tr.Detached)
			}
			report := h.finish(t)
			if mode == "valid" {
				require.Equal(t, 1, report.Code)
				require.Zero(t, report.Signal)
			} else {
				require.Equal(t, int(unix.SIGKILL), report.Signal)
			}
			if mode == "callback-panic" {
				require.True(t, tr.TracerThreadRetired)
			}
			detail := ""
			if err != nil {
				detail = err.Error()
			}
			nativeTraceSave(t, "stopped-target-"+mode, map[string]any{"transcript": tr, "callbackReached": callbackReached, "child": report, "error": detail, "runtime": runtime.Version(), "scope": fmt.Sprintf("REAL_NATIVE_TYPED_STOPPED_COMPONENT_%s_NOT_ARRAYS_OR_NORMAL", mode)})
		})
	}
}
