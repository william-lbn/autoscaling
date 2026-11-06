//go:build linux && dsga_native_trusted_control && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const trustedControlNativeOptIn = "isolated18c-real-compute-normal-v1"

func init() {
	target := trustedControlNativeEntrypoint(os.Getenv("DSGA_TRUSTED_CONTROL_NATIVE"))
	if target != "" && len(os.Args) == 2 && os.Args[0] == "/neonvm/bin/dsga-system-role-broker" && os.Args[1] == "initial-boot" {
		watchdog := "420s"
		if target == "TestNativePG17TrustedControl" {
			watchdog = "510s"
		}
		os.Args = []string{os.Args[0], "-test.run=^" + target + "$", "-test.v", "-test.timeout=" + watchdog}
	}
}

// 测试只编排真实生产入口/HTTP/SQL；没有Source构造器、normal私有授权调用或替身Compute。
func trustedControlNativeSave(t *testing.T, name string, v any) {
	t.Helper()
	raw, e := json.Marshal(v)
	require.NoError(t, e)
	dir := "/var/lib/neon-dsga/trusted-evidence"
	require.NoError(t, os.MkdirAll(dir, 0700))
	f, e := os.OpenFile(filepath.Join(dir, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, e)
	_, e = f.Write(raw)
	require.NoError(t, e)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}

// 只向串口反映Root已记录的固定数值失败，不输出error/raw/凭据，不参与任何许可。
func trustedControlNativeRPCDiagnosticLine(line string) (uint32, uint32, bool) {
	f := strings.Fields(line)
	if len(f) != 3 || f[0] != "DSGA_ARRAY_RPC_FAILURE" || !strings.HasPrefix(f[1], "stage=") || !strings.HasPrefix(f[2], "errno=") {
		return 0, 0, false
	}
	a, e1 := strconv.ParseUint(strings.TrimPrefix(f[1], "stage="), 10, 32)
	b, e2 := strconv.ParseUint(strings.TrimPrefix(f[2], "errno="), 10, 32)
	if e1 != nil || e2 != nil || a > 1000 || b > 4095 {
		return 0, 0, false
	}
	return uint32(a), uint32(b), true
}
func trustedControlNativeReadRPCDiagnostic() ([]byte, bool) {
	f, e := os.Open("/var/log/dsga-resource-guard-trusted.log")
	if e != nil {
		return nil, false
	}
	defer f.Close()
	var st unix.Stat_t
	if unix.Fstat(int(f.Fd()), &st) != nil || st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0777 != 0600 {
		return nil, false
	}
	raw, e := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if e != nil || len(raw) > 64<<10 {
		return nil, false
	}
	return raw, true
}
func trustedControlNativeRPCDiagnostic() bool {
	raw, ok := trustedControlNativeReadRPCDiagnostic()
	if !ok {
		return false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if stage, errno, ok := trustedControlNativeRPCDiagnosticLine(line); ok {
			fmt.Printf("TRUSTED_PG17_ROOT_RPC_FAILURE stage=%d errno=%d\n", stage, errno)
			return true
		}
	}
	return false
}

// 只识别Root固定normal前缀之后的两个数值；可忽略Root日志自带时间戳，
// 绝不输出前缀内容、原错误或请求。uint16阶段和Linux errno边界独立核验。
func trustedControlNativeNormalRPCDiagnosticLine(line string) (uint32, uint32, bool) {
	const marker = "DSGA_NORMAL_RPC_FAILURE "
	i := strings.Index(line, marker)
	if i < 0 {
		return 0, 0, false
	}
	f := strings.Fields(line[i:])
	if len(f) != 3 || f[0] != "DSGA_NORMAL_RPC_FAILURE" || !strings.HasPrefix(f[1], "stage=") || !strings.HasPrefix(f[2], "errno=") {
		return 0, 0, false
	}
	a, e1 := strconv.ParseUint(strings.TrimPrefix(f[1], "stage="), 10, 16)
	b, e2 := strconv.ParseUint(strings.TrimPrefix(f[2], "errno="), 10, 64)
	if e1 != nil || e2 != nil || b > 4095 {
		return 0, 0, false
	}
	return uint32(a), uint32(b), true
}

// normal与原ARRAY观察独立；按日志原顺序最多报告8个不同阶段，不能让后继
// Observe的“信封缺失”覆盖首Apply内层失败。此函数没有写API或许可副作用。
func trustedControlNativeNormalRPCDiagnostic(seen map[[2]uint32]bool) {
	if seen == nil || len(seen) >= 8 {
		return
	}
	raw, ok := trustedControlNativeReadRPCDiagnostic()
	if !ok {
		return
	}
	for _, line := range strings.Split(string(raw), "\n") {
		stage, errno, ok := trustedControlNativeNormalRPCDiagnosticLine(line)
		key := [2]uint32{stage, errno}
		if !ok || seen[key] {
			continue
		}
		if len(seen) >= 8 {
			return
		}
		seen[key] = true
		fmt.Printf("TRUSTED_PG17_NORMAL_RPC_FAILURE stage=%d errno=%d\n", stage, errno)
	}
}

func TestTrustedControlRootRPCDiagnosticFixed(t *testing.T) {
	for _, c := range []struct {
		line string
		ok   bool
	}{{"DSGA_ARRAY_RPC_FAILURE stage=2 errno=0", true}, {"DSGA_ARRAY_RPC_FAILURE stage=117 errno=4", true}, {"other stage=2 errno=0", false}, {"DSGA_ARRAY_RPC_FAILURE stage=2 errno=0 payload", false}, {"DSGA_ARRAY_RPC_FAILURE stage=-1 errno=0", false}, {"DSGA_ARRAY_RPC_FAILURE stage=1001 errno=0", false}, {"DSGA_ARRAY_RPC_FAILURE stage=2 errno=4096", false}} {
		t.Run(c.line, func(t *testing.T) {
			_, _, ok := trustedControlNativeRPCDiagnosticLine(c.line)
			if ok != c.ok {
				t.Fatal("fixed numeric diagnostic parser differs")
			}
		})
	}
}

func TestTrustedControlNormalRPCDiagnosticFixed(t *testing.T) {
	for n, c := range []struct {
		line string
		ok   bool
	}{
		{"DSGA_NORMAL_RPC_FAILURE stage=3211 errno=0", true},
		{"2026/11/02 13:00:00 DSGA_NORMAL_RPC_FAILURE stage=65535 errno=4095", true},
		{"DSGA_ARRAY_RPC_FAILURE stage=3211 errno=0", false},
		{"other stage=3211 errno=0", false},
		{"DSGA_NORMAL_RPC_FAILURE stage=65536 errno=0", false},
		{"DSGA_NORMAL_RPC_FAILURE stage=3211 errno=4096", false},
		{"DSGA_NORMAL_RPC_FAILURE stage=-1 errno=0", false},
		{"DSGA_NORMAL_RPC_FAILURE stage=3211 errno=0 payload", false},
		{"DSGA_NORMAL_RPC_FAILURE stage=1 errno=18446744073709551616", false},
		{"DSGA_NORMAL_RPC_FAILURE stage=1 errno=-1", false},
	} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			_, _, ok := trustedControlNativeNormalRPCDiagnosticLine(c.line)
			if ok != c.ok {
				t.Fatal("fixed bounded normal scalar parser differs")
			}
		})
	}
}

func trustedControlNativeWait(t *testing.T, ctx context.Context, stage string, fn func() bool) {
	t.Helper()
	nextDiagnostic := time.Now().Add(5 * time.Second)
	diagnosed := false
	normalSeen := make(map[[2]uint32]bool)
	for ctx.Err() == nil {
		if fn() {
			return
		}
		if (!diagnosed || len(normalSeen) < 8) && time.Now().After(nextDiagnostic) {
			unix.Sync()
			if !diagnosed {
				diagnosed = trustedControlNativeRPCDiagnostic()
			}
			trustedControlNativeNormalRPCDiagnostic(normalSeen)
			nextDiagnostic = time.Now().Add(5 * time.Second)
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("actual trusted stage deadline", stage, ctx.Err())
}

// 同Compute原私有管理端点/UID peer鉴权；不改公共连接门，全部阶段仍须真实AdmissionOpen。
// 与Compute private_config同一原管理startup合同；应用名不提供身份，仍由
// 原0700私有socket/OS peer/UID1000及PG会话权限决定，绝不打开普通admission。
func trustedControlNativeManagementEnv() []string {
	return []string{"PATH=/usr/local/bin:/neonvm/bin", "LC_ALL=C", "PGAPPNAME=compute_ctl:trusted_control_validation", "PGOPTIONS=-c role=cloud_admin -c default_transaction_read_only=off -c search_path='' -c statement_timeout=0 -c pgaudit.log=none"}
}

// 只观察固定聚合数值。短探针失败不改变原造数/原准入/原验收结论。
func trustedControlNativePrivateSQLDiagnostics(ctx context.Context) {
	probe, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	const sql = "SELECT json_build_object('backends',count(*),'lock_waiters',count(*) FILTER(WHERE wait_event_type='Lock'),'syncrep_waiters',count(*) FILTER(WHERE wait_event='SyncRep'),'client_read_waiters',count(*) FILTER(WHERE wait_event='ClientRead'),'role_ddl',count(*) FILTER(WHERE query ~* '^[[:space:]]*(CREATE|ALTER|GRANT)[[:space:]]+(ROLE|USER)'),'extension_ddl',count(*) FILTER(WHERE query ~* '^[[:space:]]*(CREATE|ALTER)[[:space:]]+EXTENSION'),'session_superuser',(current_setting('is_superuser')='on')::int) FROM pg_catalog.pg_stat_activity"
	cmd := exec.CommandContext(probe, "/usr/local/bin/psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-h", "/var/db/postgres/compute/dsg_socket", "-p", "55432", "-U", "cloud_admin", "-d", "postgres", "-c", sql)
	cmd.Env = trustedControlNativeManagementEnv()
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000, Groups: []uint32{1000}}}
	raw, e := cmd.Output()
	if e != nil {
		code := -1
		var exit *exec.ExitError
		if errors.As(e, &exit) {
			code = exit.ExitCode()
		}
		timed := 0
		if probe.Err() != nil {
			timed = 1
		}
		fmt.Printf("TRUSTED_PG17_PRIVATE_SQL_DIAG exit=%d timed_out=%d\n", code, timed)
		return
	}
	if len(raw) > 1024 {
		return
	}
	var values map[string]int64
	if json.Unmarshal(raw, &values) != nil || len(values) != 7 {
		return
	}
	for _, key := range []string{"backends", "lock_waiters", "syncrep_waiters", "client_read_waiters", "role_ddl", "extension_ddl", "session_superuser"} {
		n, ok := values[key]
		if !ok || n < 0 || n > 4096 {
			return
		}
	}
	out, e := json.Marshal(values)
	if e == nil {
		fmt.Printf("TRUSTED_PG17_PRIVATE_SQL_DIAG_JSON %s\n", out)
	}
}

type trustedControlNativeSQLWait struct {
	PID       int    `json:"pid"`
	State     string `json:"state"`
	WaitType  string `json:"wait_type"`
	WaitEvent string `json:"wait_event"`
}

func trustedControlNativeSQLWaitClean(rows []trustedControlNativeSQLWait) bool {
	if len(rows) > 16 {
		return false
	}
	states := map[string]bool{"active": true, "idle": true, "idle in transaction": true, "idle in transaction (aborted)": true, "fastpath function call": true, "disabled": true}
	types := map[string]bool{"Activity": true, "BufferPin": true, "Client": true, "Extension": true, "IPC": true, "IO": true, "Lock": true, "LWLock": true, "Timeout": true}
	// Neon名称逐项来自当前已编c75 source/neon.c866–875；其余是PG17固定wait名称。
	events := map[string]bool{"SyncRep": true, "WalSenderMain": true, "ReplicationSlotDrop": true, "Extension": true, "WALWrite": true, "WALSync": true, "DataFileRead": true, "DataFileWrite": true, "ClientRead": true, "ClientWrite": true, "BufferMapping": true, "BufferPin": true, "relation": true, "transactionid": true, "tuple": true, "virtualxid": true, "extend": true, "Neon/FileCache_Maintenance": true, "Neon/FileCache_Read": true, "Neon/FileCache_Truncate": true, "Neon/FileCache_Write": true, "Neon/FileCache_CvWait": true, "Neon/PS_Starting": true, "Neon/PS_Configuring": true, "Neon/PS_SendIO": true, "Neon/PS_ReadIO": true, "Neon/WAL_Download": true}
	for i := range rows {
		q := &rows[i]
		if q.PID < 1 || q.PID > 1<<22 {
			return false
		}
		if !states[q.State] {
			q.State = "OTHER"
		}
		if q.WaitType == "" {
			q.WaitType = "NONE"
		} else if !types[q.WaitType] {
			q.WaitType = "OTHER"
		}
		if q.WaitEvent == "" {
			q.WaitEvent = "NONE"
		} else if !events[q.WaitEvent] {
			q.WaitEvent = "OTHER"
		}
	}
	return true
}
func trustedControlNativePostSQLDiagnostics(ctx context.Context, stage uint32) {
	probe, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	const sql = "SELECT coalesce(json_agg(json_build_object('pid',pid,'state',state,'wait_type',wait_event_type,'wait_event',wait_event)), '[]'::json) FROM pg_catalog.pg_stat_activity WHERE application_name='compute_ctl:trusted_control_validation' AND pid<>pg_backend_pid()"
	cmd := exec.CommandContext(probe, "/usr/local/bin/psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-h", "/var/db/postgres/compute/dsg_socket", "-p", "55432", "-U", "cloud_admin", "-d", "postgres", "-c", sql)
	cmd.Env = trustedControlNativeManagementEnv()
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000, Groups: []uint32{1000}}}
	raw, e := cmd.Output()
	if e != nil {
		code := -1
		var exit *exec.ExitError
		if errors.As(e, &exit) {
			code = exit.ExitCode()
		}
		fmt.Printf("TRUSTED_PG17_SQL_WAIT_UNCONFIRMED stage=%d exit=%d\n", stage, code)
		return
	}
	if len(raw) > 8192 {
		return
	}
	var rows []trustedControlNativeSQLWait
	if json.Unmarshal(raw, &rows) != nil || !trustedControlNativeSQLWaitClean(rows) {
		return
	}
	out, e := json.Marshal(struct {
		Stage    uint32                        `json:"stage"`
		Sessions []trustedControlNativeSQLWait `json:"sessions"`
	}{stage, rows})
	if e == nil {
		fmt.Printf("TRUSTED_PG17_SQL_WAIT_JSON %s\n", out)
	}
}
func TestTrustedControlSQLWaitWhitelist(t *testing.T) {
	for _, name := range []string{"Neon", "core", "unknown", "badpid"} {
		t.Run(name, func(t *testing.T) {
			rows := []trustedControlNativeSQLWait{{PID: 123, State: "active", WaitType: "Extension", WaitEvent: "Neon/PS_ReadIO"}}
			switch name {
			case "core":
				rows[0].WaitType = "Activity"
				rows[0].WaitEvent = "WalSenderMain"
			case "unknown":
				rows[0].State = "untrusted-state"
				rows[0].WaitType = "untrusted-type"
				rows[0].WaitEvent = "untrusted-event"
			case "badpid":
				rows[0].PID = 0
			}
			ok := trustedControlNativeSQLWaitClean(rows)
			if name == "badpid" {
				if ok {
					t.Fatal("bad pid accepted")
				}
				return
			}
			if !ok {
				t.Fatal("valid fixed metadata refused")
			}
			if name == "unknown" && (rows[0].State != "OTHER" || rows[0].WaitType != "OTHER" || rows[0].WaitEvent != "OTHER") {
				t.Fatal("untrusted metadata leaked")
			}
		})
	}
}

func trustedControlNativeSQL(t *testing.T, ctx context.Context, sql string) string {
	return trustedControlNativeSQLStage(t, ctx, 0, sql)
}
func trustedControlNativeSQLStage(t *testing.T, ctx context.Context, stage uint32, sql string) string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "/usr/local/bin/psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-h", "/var/db/postgres/compute/dsg_socket", "-p", "55432", "-U", "cloud_admin", "-d", "postgres", "-c", sql)
	cmd.Env = trustedControlNativeManagementEnv()
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000, Groups: []uint32{1000}}}
	done := make(chan struct{})
	observed := make(chan struct{})
	go func() {
		defer close(observed)
		if stage == 0 {
			return
		}
		select {
		case <-done:
			return
		case <-time.After(5 * time.Second):
		}
		trustedControlNativePostSQLDiagnostics(ctx, stage)
	}()
	b, e := cmd.CombinedOutput()
	close(done)
	<-observed
	require.NoError(t, e, "formal management SQL failed: %s", b)
	return strings.TrimSpace(string(b))
}

// 观察者只发GET，所有Fence/Resize/normal/VM写入都由Host原productionSession执行。
func trustedControlNativeReadHTTP(ctx context.Context, t *testing.T, h *resourceHTTP, path string, out any) error {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:3080"+path, nil)
	if e != nil {
		return e
	}
	req.Header.Set("Authorization", "Bearer "+resourceToken(t, h, normalAuthorityDEVKey(), nil))
	resp, e := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 64<<10+1))
	if e != nil {
		return e
	}
	if len(raw) > 64<<10 || resp.StatusCode != http.StatusOK {
		return fmt.Errorf("formal Compute GET %s status=%d bytes=%d", path, resp.StatusCode, len(raw))
	}
	return json.Unmarshal(raw, out)
}
func trustedControlNativeStart(t *testing.T, path string, args, env []string, uid, gid uint32) (*exec.Cmd, <-chan error, int) {
	t.Helper()
	f, e := os.OpenFile("/var/log/"+filepath.Base(path)+"-trusted.log", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	require.NoError(t, e)
	cmd := exec.Command(path, args...)
	cmd.Env = env
	cmd.Stdout = f
	cmd.Stderr = f
	pf := -1
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &pf, Pdeathsig: syscall.SIGKILL}
	if uid != 0 {
		cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uid, Gid: gid, Groups: []uint32{gid}}
	}
	require.NoError(t, cmd.Start())
	require.NoError(t, f.Close())
	require.GreaterOrEqual(t, pf, 0)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = unix.PidfdSendSignal(pf, unix.SIGTERM, nil, 0)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = unix.PidfdSendSignal(pf, unix.SIGKILL, nil, 0)
			<-done
		}
		unix.Close(pf)
	})
	return cmd, done, pf
}

// MemTotal只观察Host已经执行的动作，不产生预算或VM许可。7.5GiB阈值仅
// 区分实际7G/8G；Host仍须原QMP peer、journal、claim与发布CAS独立确认。
func trustedControlNativeMemTotal() (uint64, error) {
	raw, e := os.ReadFile("/proc/meminfo")
	if e != nil {
		return 0, e
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "MemTotal:" && fields[2] == "kB" {
			kb, e := strconv.ParseUint(fields[1], 10, 64)
			if e != nil {
				return 0, e
			}
			return kb * 1024, nil
		}
	}
	return 0, fmt.Errorf("actual MemTotal missing")
}

type trustedControlNativeObservation struct {
	ProtocolVersion uint32                 `json:"protocol_version"`
	Scope           string                 `json:"scope"`
	Phase           string                 `json:"phase"`
	VMShrinkPermit  bool                   `json:"vm_shrink_permit"`
	PG              control.MemoryResponse `json:"pg_memory"`
	RootView        View                   `json:"root_view"`
	// 仅同内核实际只读快照，不构造或消费normal资格；RootView的N读回也必须相同。
	Split              NormalSplitTopologySnapshot `json:"split_actual"`
	GuestMemTotalBytes uint64                      `json:"guest_mem_total_bytes"`
	Checksum           string                      `json:"checksum"`
}

type trustedControlNativeObservationError struct {
	stage uint32
	err   error
}

func (e *trustedControlNativeObservationError) Error() string { return e.err.Error() }
func (e *trustedControlNativeObservationError) Unwrap() error { return e.err }
func trustedControlNativeClientErrorCode(e error) uint32 {
	if e == nil {
		return 0
	}
	var ce *ClientError
	if errors.As(e, &ce) {
		switch ce.Code {
		case "response_unconfirmed":
			return 1
		case "resource_state_unconfirmed":
			return 2
		case "transport_unconfirmed":
			return 3
		case "credential_unavailable":
			return 4
		case "array_original_profile_downgraded":
			return 5
		case "normal_original_profile_downgraded":
			return 6
		default:
			return 7
		}
	}
	if errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled) {
		return 8
	}
	return 9
}

func trustedControlNativeObserve(ctx context.Context, t *testing.T, h *resourceHTTP, client *Client, g *Guard, phase string) (o trustedControlNativeObservation, err error) {
	stage := uint32(1)
	defer func() {
		if err != nil {
			err = &trustedControlNativeObservationError{stage: stage, err: err}
		}
	}()
	o = trustedControlNativeObservation{ProtocolVersion: 1, Scope: "trusted-pg17-live-control-observation-v1", Phase: phase}
	if e := trustedControlNativeReadHTTP(ctx, t, h, "/dsg/v1/memory", &o.PG); e != nil {
		return o, e
	}
	var e error
	stage = 2
	o.RootView, e = client.InspectState(ctx)
	if e != nil {
		return o, e
	}
	stage = 3
	k, ok := g.kernel.(normalSplitKernel)
	if !ok {
		return o, ErrUnknown
	}
	o.Split, e = k.normalSplitRead()
	if e != nil {
		return o, e
	}
	stage = 4
	o.GuestMemTotalBytes, e = trustedControlNativeMemTotal()
	if e != nil {
		return o, e
	}
	stage = 5
	if o.Split.Private == nil || o.Split.Arrays == nil || o.RootView.Actual.MemoryMaxBytes != o.Split.Parent.MemoryMaxBytes {
		return o, ErrUnknown
	}
	stage = 6
	if _, e = o.RootView.ConfirmedLimits(); e != nil {
		return o, e
	}
	return o, nil
}
func trustedControlNativePublish(t *testing.T, o trustedControlNativeObservation) {
	t.Helper()
	raw, e := json.Marshal(o)
	require.NoError(t, e)
	require.LessOrEqual(t, len(raw), 64<<10)
	trustedControlNativeSave(t, "observation-"+o.Phase, o)
	// 此marker仅让Host等待Guest完成断言；它不是任何预算、claim或VM许可。
	fmt.Printf("TRUSTED_PG17_OBSERVATION_V1 %s\n", raw)
	fmt.Printf("TRUSTED_PG17_OBSERVED_STAGE=%s\n", o.Phase)
}
func TestNativePG17TrustedControl(t *testing.T) {
	require.Equal(t, trustedControlNativeOptIn, os.Getenv("DSGA_TRUSTED_CONTROL_NATIVE"))
	trustedControlNativeRun(t, 17)
}

// 两个DEV包装只选择实际版本断言；共同生产HTTP/PG/Root/VM流程不按major分叉。
func trustedControlNativeRun(t *testing.T, expectedMajor int) {
	require.Contains(t, []int{17, 18}, expectedMajor)
	require.Equal(t, 1, os.Getppid())
	defer func() {
		status := 0
		if t.Failed() {
			status = 1
		}
		// Host会在终态marker后清理原QEMU；先完成既有Sync，避免错误日志
		// 尚在Guest缓存就被终止。只改变DEV证据发布顺序，不改变退出判据。
		unix.Sync()
		fmt.Printf("TRUSTED_PG17_GUEST_EXIT=%d\n", status)
		_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
	}()
	observationTimeout := 390 * time.Second
	if expectedMajor == 17 {
		observationTimeout = 480 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), observationTimeout)
	defer cancel()
	// 同正式CLI完成原KernelPrep2/memory2/Host6/7/8、split和Bootstrap。显式模式
	// 在CheckGuest后返回，让PID1可启动Root/Compute；默认broker分支仍原样保留。
	require.NoError(t, RunInitialBootGuest(ctx))
	source, e := sealedRuntimeSource()
	require.NoError(t, e)
	require.Equal(t, TrustedControllerExecutionModel, source.Config.ExecutionModel)
	for _, pid := range []int{1, os.Getpid()} {
		group, e := bootProcGroup(pid)
		require.NoError(t, e)
		require.Equal(t, "/neon-system", group)
	}
	// PID1仍等待本显式测试，故仅提前执行原固定Guest网络启动所需动作。
	// 不改宿主网络；所有这些真实普通进程已继承同有限System域。
	for _, args := range [][]string{{"ip", "link", "set", "lo", "up"}, {"ip", "link", "set", "eth0", "up"}, {"udhcpc", "-n", "-q", "-t", "3", "-T", "1", "-i", "eth0", "-s", "/neonvm/bin/udhcpc.script"}} {
		b, e := exec.CommandContext(ctx, "/neonvm/bin/busybox", args...).CombinedOutput()
		require.NoError(t, e, "actual Guest network startup: %s", b)
	}
	root, rootDone, rootFD := trustedControlNativeStart(t, "/neonvm/bin/dsga-resource-guard", []string{"serve"}, []string{"PATH=/neonvm/bin", "LC_ALL=C"}, 0, 0)
	require.NoError(t, WaitTrustedControllerReady(ctx))
	require.NoError(t, arrayRegistryPIDFDAlive(rootFD))
	raw, e := os.ReadFile(normalSystemRoleBuildPlanPath)
	require.NoError(t, e)
	var plan struct {
		ProtocolVersion uint32   `json:"protocol_version"`
		ExecutionModel  string   `json:"execution_model,omitempty"`
		ComputeUID      uint32   `json:"compute_uid"`
		ComputeGID      uint32   `json:"compute_gid"`
		ComputeArgv     []string `json:"compute_argv"`
		ComputeEnv      []string `json:"compute_env"`
		LoaderPaths     []string `json:"loader_paths"`
	}
	require.NoError(t, control.UnmarshalObject(raw, &plan))
	require.Equal(t, TrustedControllerExecutionModel, plan.ExecutionModel)
	require.Equal(t, uint32(1000), plan.ComputeUID)
	require.Equal(t, uint32(1000), plan.ComputeGID)
	require.Greater(t, len(plan.ComputeArgv), 1)
	require.Equal(t, trustedControllerComputePath, plan.ComputeArgv[0])
	compute, computeDone, computeFD := trustedControlNativeStart(t, plan.ComputeArgv[0], plan.ComputeArgv[1:], plan.ComputeEnv, plan.ComputeUID, plan.ComputeGID)
	g, e := Open(source.Config)
	require.NoError(t, e)
	h, e := newResourceHandler(source, g)
	require.NoError(t, e)
	transport := &http.Transport{DialTLSContext: func(c context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(c, "tcp", "127.0.0.1:3080")
	}}
	id, e := g.InspectIdentity()
	require.NoError(t, e)
	client, e := NewClient(ClientConfig{Context: ctx, BaseURL: "https://trusted-compute.invalid", Identity: id, IssuerEpoch: 1, Timeout: 5 * time.Second, Transport: transport, Token: func(context.Context) (string, error) { return resourceToken(t, h, normalAuthorityDEVKey(), nil), nil }})
	require.NoError(t, e)
	defer client.CloseIdleConnections()
	nextWaitReport := time.Now()
	report := func(stage uint32, detail int) {
		if time.Now().Before(nextWaitReport) {
			return
		}
		fmt.Printf("TRUSTED_PG17_WAIT_REASON stage=%d detail=%d\n", stage, detail)
		nextWaitReport = time.Now().Add(5 * time.Second)
	}
	trustedControlNativeWait(t, ctx, "true Compute/PG running", func() bool {
		select {
		case e := <-rootDone:
			t.Fatal("actual Root service exited", e)
		case e := <-computeDone:
			t.Fatal("actual Compute exited", e)
		default:
		}
		if arrayRegistryPIDFDAlive(computeFD) != nil {
			return false
		}
		v, e := client.InspectState(ctx)
		if e != nil {
			report(1, int(trustedControlNativeClientErrorCode(e)))
			return false
		}
		if v.Birth == nil {
			report(2, 0)
			return false
		}
		// Root只读接口可在PG出生前就绪；必须等待真实PG监听可用，不能把
		// HTTP可达冒作主PG启动完成，也不以辅助sync-safekeepers的出生抢跑SQL。
		ready := exec.CommandContext(ctx, "/usr/local/bin/pg_isready", "-q", "-h", "/var/db/postgres/compute/dsg_socket", "-p", "55432", "-U", "cloud_admin", "-d", "postgres")
		ready.Env = []string{"PATH=/usr/local/bin:/neonvm/bin", "LC_ALL=C"}
		ready.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000, Groups: []uint32{1000}}}
		err := ready.Run()
		if err != nil {
			code := -1
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				code = exit.ExitCode()
			}
			report(3, code)
		}
		return err == nil
	})
	fmt.Println("TRUSTED_PG17_PRIVATE_READY")
	// 从同一真实管理连接读取server版本；旧同步前缀不是版本来源。
	actualMajor, majorErr := strconv.Atoi(strings.TrimSpace(trustedControlNativeSQL(t, ctx, "SELECT current_setting('server_version_num')::int / 10000")))
	require.NoError(t, majorErr)
	require.Equal(t, expectedMajor, actualMajor)
	trustedControlNativeSave(t, "actual-pg-major", map[string]any{"actual_pg_major": actualMajor, "expected_pg_major": expectedMajor, "vm_shrink_permit": false})
	fmt.Printf("TRUSTED_PG_ACTUAL_MAJOR=%d\n", actualMajor)
	require.True(t, client.NormalQuotaEnabled())
	require.True(t, client.ArrayAuthorityEnabled())
	view, e := client.InspectState(ctx)
	require.NoError(t, e)
	require.NotNil(t, view.Birth)
	trustedControlNativeSave(t, "startup", map[string]any{"RootPID": root.Process.Pid, "ComputePID": compute.Process.Pid, "state": view, "real_production_CLI": true, "real_Compute": true, "PG_server_started": true})
	trustedControlNativePrivateSQLDiagnostics(ctx)
	// 每次真实运行使用独立表名；保留此前已提交的数据，不删除或覆盖未知对象。
	var tableNonce [16]byte
	_, e = rand.Read(tableNonce[:])
	require.NoError(t, e)
	table := "public.trusted_control_data_" + hex.EncodeToString(tableNonce[:])
	trustedControlNativeSave(t, "table-index", map[string]any{"table": table, "rows": 512})
	checksumSQL := "SELECT count(*),md5(string_agg(pad,',' ORDER BY i)) FROM " + table
	fmt.Println("TRUSTED_PG17_SQL_CREATE_BEGIN")
	for i, sql := range []string{"CREATE EXTENSION IF NOT EXISTS neon", "CREATE TABLE " + table + "(i int,pad text)", "INSERT INTO " + table + " SELECT i,repeat(md5(i::text),64) FROM generate_series(1,512)i"} {
		fmt.Printf("TRUSTED_PG17_SETUP_SQL_BEGIN=%d\n", i+1)
		trustedControlNativeSQLStage(t, ctx, uint32(i+1), sql)
		fmt.Printf("TRUSTED_PG17_SETUP_SQL_DONE=%d\n", i+1)
	}
	fmt.Println("TRUSTED_PG17_SQL_CREATE_DONE")
	checksum := trustedControlNativeSQL(t, ctx, checksumSQL)
	fmt.Println("TRUSTED_PG17_SQL_CHECKSUM_DONE")
	// External实际监听[::]:3080；本fixture必须真实支持IPv4映射，供Host端口转发。
	dual, e := os.ReadFile("/proc/sys/net/ipv6/bindv6only")
	require.NoError(t, e)
	require.Equal(t, "0", strings.TrimSpace(string(dual)))
	var baseline trustedControlNativeObservation
	trustedControlNativeWait(t, ctx, "baseline live Root/PG", func() bool {
		var e error
		baseline, e = trustedControlNativeObserve(ctx, t, h, client, g, "baseline")
		if e != nil {
			var observed *trustedControlNativeObservationError
			if errors.As(e, &observed) {
				report(20+observed.stage, int(trustedControlNativeClientErrorCode(observed.err)))
			} else {
				report(20, 9)
			}
		}
		return e == nil
	})
	require.EqualValues(t, 256, baseline.PG.Observed.HighBuffers)
	require.EqualValues(t, 1, baseline.PG.Observed.CapacityGeneration)
	require.Less(t, baseline.GuestMemTotalBytes, uint64(15)<<29)
	baseline.Checksum = checksum
	trustedControlNativePublish(t, baseline)
	previous := baseline
	for step, target := range []uint32{1024, 256, 2048} {
		phase := []string{"grow-8", "shrink-7", "regrow-8"}[step]
		var observed trustedControlNativeObservation
		trustedControlNativeWait(t, ctx, "Host productionSession "+phase, func() bool {
			var e error
			observed, e = trustedControlNativeObserve(ctx, t, h, client, g, phase)
			if e != nil {
				return false
			}
			p := observed.PG.Observed
			if p.Instance != baseline.PG.Observed.Instance || p.HighBuffers != target || p.LowBuffers != target || p.CapacityGeneration != baseline.PG.Observed.CapacityGeneration+control.Uint64(step+1) || !p.AdmissionOpen {
				return false
			}
			if (observed.GuestMemTotalBytes >= uint64(15)<<29) != (step != 1) {
				return false
			}
			if observed.Split.Private.MemoryMaxBytes != baseline.Split.Private.MemoryMaxBytes {
				return false
			}
			if step == 1 && !(observed.Split.Parent.MemoryMaxBytes < previous.Split.Parent.MemoryMaxBytes && observed.Split.Arrays.MemoryMaxBytes < previous.Split.Arrays.MemoryMaxBytes) {
				return false
			}
			if step == 2 && !(observed.Split.Parent.MemoryMaxBytes > previous.Split.Parent.MemoryMaxBytes && observed.Split.Arrays.MemoryMaxBytes > previous.Split.Arrays.MemoryMaxBytes) {
				return false
			}
			if step == 1 {
				for _, a := range p.Arrays {
					if a.ReleaseState != control.ReleaseVerified || a.AllocationUnknown {
						return false
					}
				}
			}
			return true
		})
		require.Equal(t, baseline.RootView.Birth, observed.RootView.Birth)
		require.Equal(t, baseline.Split.Identity, observed.Split.Identity)
		require.NoError(t, arrayRegistryPIDFDAlive(computeFD))
		require.NoError(t, arrayRegistryPIDFDAlive(rootFD))
		require.Equal(t, checksum, trustedControlNativeSQL(t, ctx, checksumSQL))
		observed.Checksum = checksum
		var finishSQLBusy func()
		if step == 0 {
			finishSQLBusy = trustedControlNativeSQLBusy(t, ctx, h, client, g, observed)
		}
		trustedControlNativePublish(t, observed)
		if finishSQLBusy != nil {
			finishSQLBusy()
		}
		previous = observed
	}
	// Host必须完成真正Finalize/ResumeAdmission、claim结清和发布读回后才置此只读
	// 结束闩锁；它只控制测试关机，不能签任何预算或VM动作。
	trustedControlNativeWait(t, ctx, "Host production driver final publication", func() bool {
		req, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://10.0.2.2:33381/pg17-driver-complete", nil)
		if e != nil {
			return false
		}
		resp, e := (&http.Client{Timeout: time.Second}).Do(req)
		if e != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusNoContent
	})
	require.NoError(t, arrayRegistryPIDFDAlive(computeFD))
	require.NoError(t, arrayRegistryPIDFDAlive(rootFD))
	// Guest完成全阶段读回；Host的原生产journal/claims/发布链另行归档。
	trustedControlNativeSave(t, "functional-complete", map[string]any{"PG17": actualMajor == 17, "PG18": actualMajor == 18, "actual_pg_major": actualMajor, "expected_pg_major": expectedMajor, "real_Compute": true, "formal_HTTP": true, "normal_lowering_executed": true, "checksum": checksum, "VM_Guest_MemTotal_7_8_7_8_observed": true, "E": 0})
}
