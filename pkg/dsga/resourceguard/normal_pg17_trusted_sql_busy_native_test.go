//go:build linux && dsga_native_trusted_control && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 仅启动真实 PG 前台工作；Host latch 只安排取消时点，不提供准入或降限许可。
func trustedControlNativeSQLBusy(t *testing.T, ctx context.Context, h *resourceHTTP, client *Client, g *Guard, grown trustedControlNativeObservation) func() {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://10.0.2.2:33381/pg17-fault-case", nil)
	require.NoError(t, err)
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	require.NoError(t, err)
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 65))
	_ = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.LessOrEqual(t, len(raw), 64)
	mode := string(raw)
	require.Contains(t, []string{"", "busy-cancel", "normal-reply-loss", "sql-busy-drain"}, mode)
	if mode != "sql-busy-drain" {
		return nil
	}

	workCtx, stopWork := context.WithTimeout(ctx, 90*time.Second)
	log, err := os.OpenFile("/var/log/trusted-sql-busy.private.log", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	cmd := exec.CommandContext(workCtx, "/usr/local/bin/psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=sqlstate", "-h", "/var/db/postgres/compute/dsg_socket", "-p", "55432", "-U", "cloud_admin", "-d", "postgres", "-c", "SELECT pg_sleep(90)")
	cmd.Env = trustedControlNativeManagementEnv()
	for i, env := range cmd.Env {
		if strings.HasPrefix(env, "PGAPPNAME=") {
			cmd.Env[i] = "PGAPPNAME=dsga:trusted_real_sql_busy"
		}
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000, Groups: []uint32{1000}}}
	cmd.Stdout, cmd.Stderr = log, log
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); _ = log.Sync(); _ = log.Close() }()
	joined := false
	t.Cleanup(func() {
		stopWork()
		if !joined {
			<-done
		}
		_ = log.Close()
	})
	var backendPID int
	trustedControlNativeWait(t, ctx, "actual SQL busy backend", func() bool {
		value := trustedControlNativeSQL(t, ctx, "SELECT pid FROM pg_catalog.pg_stat_activity WHERE application_name='dsga:trusted_real_sql_busy' AND state='active' AND wait_event='PgSleep'")
		n, e := strconv.Atoi(strings.TrimSpace(value))
		if e == nil && n > 1 {
			backendPID = n
			return true
		}
		return false
	})
	trustedControlNativeSave(t, "sql-busy-started", map[string]any{"actual_backend_pid": backendPID, "child_pid": cmd.Process.Pid, "work_kind": "real SQL pg_sleep; no buffer-pin claim", "vm_shrink_permit": false})
	fmt.Println("TRUSTED_PG17_OBSERVED_STAGE=sql-busy-ready")
	return func() {
		trustedControlNativeWait(t, ctx, "Host observed actual Work2 busy without lowering", func() bool {
			req, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://10.0.2.2:33381/pg17-sql-busy-release", nil)
			if e != nil {
				return false
			}
			r, e := (&http.Client{Timeout: time.Second}).Do(req)
			if e != nil {
				return false
			}
			_ = r.Body.Close()
			return r.StatusCode == http.StatusNoContent
		})
		held, observeErr := trustedControlNativeObserve(ctx, t, h, client, g, "sql-busy-held")
		require.NoError(t, observeErr)
		require.Equal(t, grown.Split.Identity, held.Split.Identity)
		require.Equal(t, grown.RootView.Birth, held.RootView.Birth)
		require.Equal(t, grown.Split.Parent.MemoryMaxBytes, held.Split.Parent.MemoryMaxBytes)
		require.Equal(t, grown.Split.Private.MemoryMaxBytes, held.Split.Private.MemoryMaxBytes)
		require.Equal(t, grown.Split.Arrays.MemoryMaxBytes, held.Split.Arrays.MemoryMaxBytes)
		require.Equal(t, grown.GuestMemTotalBytes, held.GuestMemTotalBytes)
		require.Equal(t, grown.PG.Observed.CapacityGeneration, held.PG.Observed.CapacityGeneration)
		require.Equal(t, grown.PG.Observed.LowBuffers, held.PG.Observed.LowBuffers)
		require.Equal(t, grown.PG.Observed.HighBuffers, held.PG.Observed.HighBuffers)
		trustedControlNativeSave(t, "sql-busy-held", held)
		// 原管理会话实际取消固定 SQL，不重启 PG 或改门。
		result := trustedControlNativeSQL(t, ctx, fmt.Sprintf("SELECT pg_catalog.pg_cancel_backend(%d)", backendPID))
		require.Equal(t, "t", strings.TrimSpace(result))
		err := <-done
		joined = true
		stopWork()
		var exit *exec.ExitError
		require.True(t, errors.As(err, &exit))
		require.Equal(t, 1, exit.ExitCode())
		data, read := os.ReadFile("/var/log/trusted-sql-busy.private.log")
		require.NoError(t, read)
		require.LessOrEqual(t, len(data), 65536)
		require.Contains(t, string(data), "57014")
		trustedControlNativeSave(t, "sql-busy-cancelled", map[string]any{"actual_backend_pid": backendPID, "sqlstate": "57014", "actual_unique_wait_exit": exit.ExitCode(), "new_admission_granted": false})
		syscall.Sync()
		fmt.Println("TRUSTED_PG17_SQL_BUSY_CANCELLED=1")
	}
}
