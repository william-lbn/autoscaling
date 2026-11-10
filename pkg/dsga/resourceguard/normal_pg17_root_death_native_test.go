//go:build linux && dsga_native_trusted_control && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// 此文件仅承载独立死亡负例的进程观察；不恢复PG所有权或提供normal写入权限。
type trustedRootDeathReplacement struct {
	command *exec.Cmd
	done    <-chan error
	pidfd   int
	oldPID  int
	newPID  int
}

// 只对原pidfd发送信号并读取原唯一Wait；不能由PID复用或连接拒绝推定死亡。
func trustedRootDeathKillOriginal(t *testing.T, ctx context.Context, root *exec.Cmd, done <-chan error, pidfd, computeFD int) {
	t.Helper()
	require.NoError(t, arrayRegistryPIDFDAlive(pidfd))
	require.NoError(t, arrayRegistryPIDFDAlive(computeFD))
	require.NoError(t, unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0))
	select {
	case err, ok := <-done:
		require.True(t, ok, "original Root Wait must be consumed once")
		var exit *exec.ExitError
		require.True(t, errors.As(err, &exit))
		status, valid := exit.Sys().(syscall.WaitStatus)
		require.True(t, valid)
		require.True(t, status.Signaled())
		require.Equal(t, syscall.SIGKILL, status.Signal())
	case <-ctx.Done():
		t.Fatal("original Root Wait was not confirmed")
	}
	require.Error(t, arrayRegistryPIDFDAlive(pidfd))
	require.NoError(t, arrayRegistryPIDFDAlive(computeFD))
	require.Greater(t, root.Process.Pid, 0)
}

// replacement使用原CLI及独立私有日志；不复用旧日志/旧Wait或旧Root身份。
func trustedRootDeathStartReplacement(t *testing.T, oldPID int) trustedRootDeathReplacement {
	t.Helper()
	log, err := os.OpenFile("/var/log/root-replacement-trusted.private.log", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	require.NoError(t, err)
	command := exec.Command("/neonvm/bin/dsga-resource-guard", "serve")
	command.Env = []string{"PATH=/neonvm/bin", "LC_ALL=C"}
	command.Stdout, command.Stderr = log, log
	fd := -1
	command.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd, Pdeathsig: syscall.SIGKILL}
	require.NoError(t, command.Start())
	require.NoError(t, log.Close())
	require.GreaterOrEqual(t, fd, 0)
	require.NotEqual(t, oldPID, command.Process.Pid)
	done := make(chan error, 1)
	go func() { done <- command.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = unix.PidfdSendSignal(fd, unix.SIGTERM, nil, 0)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
			<-done
		}
		_ = unix.Close(fd)
	})
	return trustedRootDeathReplacement{command: command, done: done, pidfd: fd, oldPID: oldPID, newPID: command.Process.Pid}
}

// 只读核实际新监听者的内核凭据；socket可连或旧ready文件不能替代新Root身份。
func trustedRootDeathFreshPeer(ctx context.Context, socketPath string, replacement trustedRootDeathReplacement) bool {
	if replacement.newPID <= 0 || replacement.oldPID == replacement.newPID || arrayRegistryPIDFDAlive(replacement.pidfd) != nil {
		return false
	}
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(bounded, "unix", socketPath)
	if err != nil {
		return false
	}
	defer conn.Close()
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return false
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return false
	}
	var peer *unix.Ucred
	var peerErr error
	err = raw.Control(func(fd uintptr) { peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
	return err == nil && peerErr == nil && peer != nil && int(peer.Pid) == replacement.newPID && peer.Uid == 0 && peer.Gid == 0 && arrayRegistryPIDFDAlive(replacement.pidfd) == nil
}

// 本负例复用正常PG17出生和首Grow前缀。Host固定闩锁仅安排故障时序，绝不授予资源权限。
func trustedRootDeathGuest(t *testing.T, ctx context.Context, root *exec.Cmd, rootDone <-chan error, rootFD, computeFD int, h *resourceHTTP, client *Client, g *Guard, checksumSQL, checksum string) bool {
	t.Helper()
	get := func(path string) (int, string) {
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://10.0.2.2:33381"+path, nil)
		require.NoError(t, e)
		response, e := (&http.Client{Timeout: 2 * time.Second}).Do(req)
		if e != nil {
			return 0, ""
		}
		defer response.Body.Close()
		raw, e := io.ReadAll(io.LimitReader(response.Body, 4097))
		require.NoError(t, e)
		require.LessOrEqual(t, len(raw), 4096)
		return response.StatusCode, string(raw)
	}
	var code int
	var mode string
	trustedControlNativeWait(t, ctx, "Host fixed fault-case listener", func() bool { code, mode = get("/pg17-fault-case"); return code == http.StatusOK })
	if mode != "root-death" {
		return false
	}
	trustedControlNativeWait(t, ctx, "original Root apply lost and old Run joined", func() bool { code, _ := get("/pg17-root-death-begin"); return code == http.StatusNoContent })
	before, e := client.InspectState(ctx)
	require.NoError(t, e)
	require.True(t, before.Applied)
	require.NotNil(t, before.Held)
	require.NotNil(t, before.Original)
	require.Equal(t, *before.Held, *before.Original)
	k, ok := g.kernel.(normalSplitKernel)
	require.True(t, ok)
	split, e := k.normalSplitRead()
	require.NoError(t, e)
	var pg control.MemoryResponse
	require.NoError(t, trustedControlNativeReadHTTP(ctx, t, h, "/dsg/v1/memory", &pg))
	// 只从原Root完整WAL读取实际原信封，不重造normal授权。
	var stored normalQuotaStoredEnvelope
	require.NoError(t, g.withRecord(ctx, func(r *record, state *lockedState) error {
		raw, found, e := state.readPhysicalFile(normalQuotaEnvelopeName(*before.Original))
		if e != nil || !found {
			return errors.Join(ErrUnknown, e)
		}
		if e = control.UnmarshalObject(raw, &stored); e != nil {
			return e
		}
		if stored.ProtocolVersion != 1 || stored.Envelope.Validate() != nil || !reflect.DeepEqual(stored.Envelope.Command, *before.Original) {
			return ErrUnknown
		}
		return nil
	}))
	trustedRootDeathKillOriginal(t, ctx, root, rootDone, rootFD, computeFD)
	replacement := trustedRootDeathStartReplacement(t, root.Process.Pid)
	trustedControlNativeWait(t, ctx, "new Root actual UDS peer", func() bool { return trustedRootDeathFreshPeer(ctx, FixedSocketPath, replacement) })
	// 身份正控与原恢复负例使用相同正式Compute路由和新签JWT；401/网络/超时不能通过。
	formal := func(path string, body any) (int, []byte) {
		method := http.MethodGet
		var raw []byte
		if body != nil {
			method = http.MethodPost
			var e error
			raw, e = json.Marshal(body)
			require.NoError(t, e)
		}
		req, e := http.NewRequestWithContext(ctx, method, "http://127.0.0.1:3080/dsg/v1/resource/"+path, bytes.NewReader(raw))
		require.NoError(t, e)
		req.Header.Set("Authorization", "Bearer "+resourceToken(t, h, normalAuthorityDEVKey(), func(c *resourceClaims) { c.IssuerEpoch = before.IssuerFloorEpoch }))
		req.Header.Set("Content-Type", "application/json")
		response, e := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		require.NoError(t, e)
		defer response.Body.Close()
		value, e := io.ReadAll(io.LimitReader(response.Body, 65537))
		require.NoError(t, e)
		require.LessOrEqual(t, len(value), 65536)
		return response.StatusCode, value
	}
	identityStatus, identityRaw := formal("identity", nil)
	require.Equal(t, http.StatusOK, identityStatus)
	var id Identity
	require.NoError(t, control.UnmarshalObject(identityRaw, &id))
	require.Equal(t, before.Identity, id)
	stateStatus, stateRaw := formal("state", nil)
	require.Equal(t, http.StatusConflict, stateStatus)
	var stateReject struct {
		Error  string `json:"error"`
		Permit bool   `json:"vm_shrink_permit"`
	}
	require.NoError(t, control.UnmarshalObject(stateRaw, &stateReject))
	require.Equal(t, "resource_unconfirmed", stateReject.Error)
	require.False(t, stateReject.Permit)
	recoveryStatus, recoveryRaw := formal("normal-recover", stored.Envelope)
	require.Equal(t, http.StatusConflict, recoveryStatus)
	var recoverReject struct {
		Error  string `json:"error"`
		Permit bool   `json:"vm_shrink_permit"`
	}
	require.NoError(t, control.UnmarshalObject(recoveryRaw, &recoverReject))
	require.Equal(t, "resource_unconfirmed", recoverReject.Error)
	require.False(t, recoverReject.Permit)
	// 新Root唯一日志的真实normal-recover阶段核对：3201只对应该Guard缺原数组注册，
	// 不能用独立DEV Guard的缓存缺失来冒充服务端实际Source错误。
	logFile, e := os.Open("/var/log/root-replacement-trusted.private.log")
	require.NoError(t, e)
	var logStat unix.Stat_t
	require.NoError(t, unix.Fstat(int(logFile.Fd()), &logStat))
	require.EqualValues(t, 0, logStat.Uid)
	require.EqualValues(t, 0600, logStat.Mode&0777)
	actualLog, e := io.ReadAll(io.LimitReader(logFile, 65537))
	require.NoError(t, e)
	require.NoError(t, logFile.Close())
	require.LessOrEqual(t, len(actualLog), 65536)
	stages := map[uint32]bool{}
	for _, line := range strings.Split(string(actualLog), "\n") {
		stage, errno, ok := trustedControlNativeNormalRPCDiagnosticLine(line)
		if ok && errno == 0 {
			stages[stage] = true
		}
	}
	for _, stage := range []uint32{3201, 2150, 1097, 1128} {
		require.True(t, stages[stage], "new Root original registry rejection missing: %d", stage)
	}
	// 独立DEV Guard确切拒绝可见Held充当其耐久确认；PG进程核验仍成功。

	_, stateErr := g.InspectState(ctx)
	require.ErrorIs(t, stateErr, ErrUnknown)
	require.True(t, strings.Contains(stateErr.Error(), "visible quota Held is not this Guard durable handoff"))
	livePG, e := g.InspectProcess(ctx)
	require.NoError(t, e)
	require.Equal(t, *before.Birth, livePG)
	trustedControlNativeSave(t, "root-death-replacement", map[string]any{"old_Root_PID": root.Process.Pid, "new_Root_PID": replacement.newPID, "old_pidfd_dead": true, "original_Wait_SIGKILL": true, "new_UDS_peer_PID_verified": true, "formal_identity_status": identityStatus, "formal_state_status": stateStatus, "formal_recovery_status": recoveryStatus, "state_error": stateReject.Error, "recovery_error": recoverReject.Error, "independent_DEV_Guard_durable_handoff_Unknown": true, "new_Root_normal_recover_missing_original_array_registry_stages": stages, "original_PG_process_unchanged": true, "before": before, "E": 0})
	fmt.Println("TRUSTED_PG17_OBSERVED_STAGE=root-death-new-peer")
	trustedControlNativeWait(t, ctx, "Host original recovery rejection complete", func() bool { code, _ := get("/pg17-driver-complete"); return code == http.StatusNoContent })
	final, e := k.normalSplitRead()
	require.NoError(t, e)
	require.Equal(t, split.Parent.MemoryMaxBytes, final.Parent.MemoryMaxBytes)
	require.NotNil(t, split.Private)
	require.NotNil(t, split.Arrays)
	require.NotNil(t, final.Private)
	require.NotNil(t, final.Arrays)
	require.Equal(t, split.Private.MemoryMaxBytes, final.Private.MemoryMaxBytes)
	require.Equal(t, split.Arrays.MemoryMaxBytes, final.Arrays.MemoryMaxBytes)
	var finalPG control.MemoryResponse
	require.NoError(t, trustedControlNativeReadHTTP(ctx, t, h, "/dsg/v1/memory", &finalPG))
	require.True(t, reflect.DeepEqual(pg.Observed, finalPG.Observed), "Root death cannot advance PG allocation or birth")
	require.NoError(t, arrayRegistryPIDFDAlive(computeFD))
	require.NoError(t, arrayRegistryPIDFDAlive(replacement.pidfd))
	require.Equal(t, checksum, trustedControlNativeSQL(t, ctx, checksumSQL))
	trustedControlNativeSave(t, "root-death-guest-complete", map[string]any{"before_split": split, "after_split": final, "PG_observed_unchanged": true, "checksum": checksum, "old_PG_still_live": true, "new_Root_has_no_live_PG_adoption": true, "Source": false, "E": 0})
	return true
}
