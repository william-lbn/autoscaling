//go:build linux && dsga_native_birth_tracer

package resourceguard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type nativeTraceChildReport struct {
	PID                   int
	Start                 uint64
	Done                  bool
	Code, Signal          int
	Stdout, Stderr, Error string
}
type nativeTraceHarness struct {
	conn   *net.UnixConn
	cfg    nativeBirthTraceConfig
	parent *exec.Cmd
	report *json.Decoder
	reader *os.File
	output bytes.Buffer
	waited bool
}

func nativeTraceHelperParent(t *testing.T) {
	// 与tracer不同TGID的实际parent负责严格Wait，避免同进程wait4与os/exec
	// 争抢同一child退出状态。这里不是生产Root Source；身份由实际内核再核。
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	file := os.NewFile(3, "native-test-parent-report")
	require.NotNil(t, file)
	_, closeExecErr := unix.FcntlInt(file.Fd(), unix.F_SETFD, unix.FD_CLOEXEC)
	require.NoError(t, closeExecErr, "report FD must never reach unprivileged native PG")
	defer file.Close()
	var environment []string
	require.NoError(t, json.Unmarshal([]byte(os.Getenv("PG17_TRACE_NATIVE_ENV")), &environment))
	cmd := exec.Command(os.Getenv("PG17_TRACE_LAUNCHER"), os.Getenv("PG17_TRACE_PG"))
	cmd.Env = environment
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000}, Pdeathsig: syscall.SIGKILL}
	var out, errout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errout
	require.NoError(t, cmd.Start())
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			require.Error(t, cmd.Wait())
		}
	}()
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", cmd.Process.Pid))
	require.NoError(t, err)
	start, err := postmasterTicks(string(raw), cmd.Process.Pid)
	require.NoError(t, err)
	enc := json.NewEncoder(file)
	require.NoError(t, enc.Encode(nativeTraceChildReport{PID: cmd.Process.Pid, Start: start}))
	waitErr := cmd.Wait()
	if waitErr != nil {
		var exit *exec.ExitError
		require.ErrorAs(t, waitErr, &exit, "unexpected actual child wait failure")
	}
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	report := nativeTraceChildReport{PID: cmd.Process.Pid, Start: start, Done: true, Code: cmd.ProcessState.ExitCode(), Stdout: out.String(), Stderr: errout.String()}
	if status.Signaled() {
		report.Signal = int(status.Signal())
	}
	require.NoError(t, enc.Encode(report))
}

func nativeTraceSetup(t *testing.T) (string, *os.File, [32]byte) {
	t.Helper()
	require.Equal(t, "isolated-pg17-root-tracer-v1", os.Getenv("PG17_NATIVE_BIRTH_TRACER"))
	require.Equal(t, "go1.24.3", runtime.Version())
	require.Equal(t, 0, os.Getuid())
	require.NoError(t, os.Mkdir("/run/neon-dsga", 0755), "fresh isolated /run required; never remove an existing production directory")
	t.Cleanup(func() { require.NoError(t, os.RemoveAll("/run/neon-dsga")) })
	path := os.Getenv("PG17_TRACE_PG")
	require.NotEmpty(t, path)
	pg, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Close() })
	raw, err := hex.DecodeString(os.Getenv("PG17_TRACE_PG_SHA256"))
	require.NoError(t, err)
	require.Len(t, raw, 32)
	var hash [32]byte
	copy(hash[:], raw)
	sum := sha256.New()
	_, err = io.Copy(sum, pg)
	require.NoError(t, err)
	require.Equal(t, raw, sum.Sum(nil))
	const c = "#include <sys/prctl.h>\n#include <unistd.h>\nint main(int n,char **v){if(n!=2)return 9;if(prctl(PR_SET_NO_NEW_PRIVS,1,0,0,0))return 10;char *a[]={v[1],\"--version\",0};execv(v[1],a);_exit(11);}\n"
	sourceSHA := sha256.Sum256([]byte(c))
	if prebuilt := os.Getenv("PG17_TRACE_PREBUILT_LAUNCHER"); prebuilt != "" {
		// 精确Guest复用同静态NNP helper，不引入Guest gcc；源与产物均由外部
		// 原测试manifest给定。它只是fixture，不冒充生产Root launcher。
		require.True(t, filepath.IsAbs(prebuilt))
		require.Equal(t, hex.EncodeToString(sourceSHA[:]), os.Getenv("PG17_TRACE_LAUNCHER_SOURCE_SHA256"))
		binary, err := os.ReadFile(prebuilt)
		require.NoError(t, err)
		binarySHA := sha256.Sum256(binary)
		require.Equal(t, hex.EncodeToString(binarySHA[:]), os.Getenv("PG17_TRACE_LAUNCHER_SHA256"))
		t.Logf("prebuilt NNP sourceSHA256=%x binarySHA256=%x", sourceSHA, binarySHA)
		return prebuilt, pg, hash
	}
	// t.TempDir的0700祖父目录不可由真实UID1000遍历；只创建本组件独立
	// /tmp直接子目录并0755，不修改Go全局临时目录/宿主系统或其他任务目录。
	dir, err := os.MkdirTemp("/tmp", "native-tracer-launcher-")
	require.NoError(t, err)
	require.NoError(t, os.Chmod(dir, 0755))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	source := filepath.Join(dir, "nnp.c")
	launcher := filepath.Join(dir, "nnp")
	require.NoError(t, os.WriteFile(source, []byte(c), 0644))
	build := exec.Command("gcc", "-static", "-O2", "-Wall", "-Wextra", "-o", launcher, source)
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))
	binary, err := os.ReadFile(launcher)
	require.NoError(t, err)
	binarySHA := sha256.Sum256(binary)
	evidence := os.Getenv("PG17_TRACE_EVIDENCE")
	require.NotEmpty(t, evidence)
	require.NoError(t, os.WriteFile(filepath.Join(evidence, "nnp-launcher"), binary, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(evidence, "nnp-launcher.c"), []byte(c), 0644))
	nativeTraceSave(t, "nnp-launcher-source", map[string]any{"sourceSHA256": hex.EncodeToString(sourceSHA[:]), "binarySHA256": hex.EncodeToString(binarySHA[:]), "scope": "TEST_ONLY_STATIC_NNP_HELPER_NOT_PRODUCTION_ROOT_LAUNCHER"})
	t.Logf("actual NNP launcher C SHA256=%x binarySHA256=%x", sourceSHA, binarySHA)
	return launcher, pg, hash
}
func nativeTraceExpectedFixture() nativeBirthExpected {
	// ABI来自C14准确PG17 c990...的实际compiled header/原KVM前Host收据；
	// 不是复用wire纯模型的任意24B checkpoint ABI，也不从对端自报补齐。
	e := nativeBirthExpected{AddressSpaceBytes: 2 << 30, MaxBuffers: 8192, PageBytes: 4096, ElementBytes: [4]uint64{64, 8192, 16, 20}}
	for i := range e.Nonce {
		e.Nonce[i] = 0x12
		e.SourceSHA[i] = 0x34
		e.ProfileSHA[i] = 0x56
	}
	return e
}
func nativeTraceStartHarness(t *testing.T, launcher string, pg *os.File, hash [32]byte) *nativeTraceHarness {
	t.Helper()
	e := nativeTraceExpectedFixture()
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: "/run/neon-dsga/birth.sock", Net: "unixpacket"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	require.NoError(t, os.Chmod("/run/neon-dsga/birth.sock", 0666))
	require.NoError(t, listener.SetDeadline(time.Now().Add(8*time.Second)))
	nativeEnv := []string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C",
		"NEON_DSGA_ADDRESS_SPACE_MAX_BYTES=" + strconv.FormatUint(e.AddressSpaceBytes, 10),
		"NEON_DSGA_EARLY_ARRAYS_PROFILE=root-v1:" + strconv.Itoa(int(e.MaxBuffers)),
		"NEON_DSGA_EARLY_ARRAYS_FILTER=root-v1",
		"NEON_DSGA_NATIVE_BIRTH_NONCE=" + hex.EncodeToString(e.Nonce[:]),
		"NEON_DSGA_NATIVE_BIRTH_SOURCE_SHA256=" + hex.EncodeToString(e.SourceSHA[:]),
		"NEON_DSGA_NATIVE_BIRTH_PROFILE_SHA256=" + hex.EncodeToString(e.ProfileSHA[:]),
		"NEON_DSGA_NATIVE_BIRTH_STARTUP_BUFFERS=256"}
	encoded, err := json.Marshal(nativeEnv)
	require.NoError(t, err)
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	require.NoError(t, reader.SetReadDeadline(time.Now().Add(12*time.Second)))
	self, err := os.Executable()
	require.NoError(t, err)
	h := &nativeTraceHarness{reader: reader, report: json.NewDecoder(reader)}
	h.parent = exec.Command(self, "-test.run=^TestNativeBirthTracerKernelSession$", "-test.count=1", "-test.v")
	h.parent.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C", "PG17_TRACE_PARENT_HELPER=1",
		"PG17_TRACE_NATIVE_ENV=" + string(encoded), "PG17_TRACE_LAUNCHER=" + launcher, "PG17_TRACE_PG=" + os.Getenv("PG17_TRACE_PG")}
	h.parent.ExtraFiles = []*os.File{writer}
	h.parent.Stdout, h.parent.Stderr = &h.output, &h.output
	require.NoError(t, h.parent.Start())
	require.NoError(t, writer.Close())
	t.Cleanup(func() {
		if h.conn != nil {
			_ = h.conn.Close()
		}
		if !h.waited {
			_ = h.parent.Process.Kill()
			_ = h.parent.Wait()
		}
		_ = reader.Close()
	})
	var initial nativeTraceChildReport
	initialErr := h.report.Decode(&initial)
	if initialErr != nil {
		_ = h.parent.Process.Kill()
		waitErr := h.parent.Wait()
		h.waited = true
		waitText := ""
		if waitErr != nil {
			waitText = waitErr.Error()
		}
		nativeTraceSave(t, "parent-start-error-"+strings.ReplaceAll(t.Name(), "/", "_"), map[string]any{"readError": initialErr.Error(), "waitError": waitText, "actualParentOutput": h.output.String()})
	}
	require.NoError(t, initialErr, h.output.String())
	require.Greater(t, initial.PID, 1)
	require.NotZero(t, initial.Start)
	h.conn, err = listener.AcceptUnix()
	require.NoError(t, err)
	// listener close removes only this fixture's path; accepted conn remains valid。
	require.NoError(t, listener.Close())
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	var st unix.Stat_t
	require.NoError(t, unix.Stat("/sys/fs/cgroup", &st))
	id := Identity{VMUID: "native-tracer-test-v1", ComputeInstanceID: "native-tracer-fixture-v1", GuestBootID: strings.TrimSpace(string(boot)), CgroupDevice: control.Uint64(st.Dev), CgroupInode: control.Uint64(st.Ino)}
	require.NoError(t, id.Validate())
	h.cfg = nativeBirthTraceConfig{Original: ProcessIdentity{ProtocolVersion: 1, Identity: id, PostmasterPID: initial.PID, PostmasterStartTicks: control.Uint64(initial.Start), AddressSpaceMaxBytes: control.Uint64(e.AddressSpaceBytes)}, PeerUID: 1000, PeerGID: 1000, Expected: e, AuditBinary: pg, BinarySHA256: hash}
	return h
}
func (h *nativeTraceHarness) finish(t *testing.T) nativeTraceChildReport {
	t.Helper()
	require.NoError(t, h.reader.SetReadDeadline(time.Now().Add(10*time.Second)))
	var report nativeTraceChildReport
	require.NoError(t, h.report.Decode(&report))
	require.True(t, report.Done)
	err := h.parent.Wait()
	h.waited = true
	require.NoError(t, err, h.output.String())
	return report
}
func nativeTraceSave(t *testing.T, name string, data any) {
	t.Helper()
	p := os.Getenv("PG17_TRACE_EVIDENCE")
	require.NotEmpty(t, p)
	b, err := json.MarshalIndent(data, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(p, name+".json"), append(b, '\n'), 0644))
}

func TestNativeBirthTracerKernelSession(t *testing.T) {
	if os.Getenv("PG17_TRACE_PARENT_HELPER") == "1" {
		nativeTraceHelperParent(t)
		return
	}
	launcher, pg, hash := nativeTraceSetup(t)
	for _, mode := range []string{"valid", "wrong-nonce", "wrong-start", "wrong-peer", "wrong-binary-sha", "base-persist-fail", "final-persist-fail", "base-cancel", "final-cancel", "base-panic", "base-goexit"} {
		t.Run(mode, func(t *testing.T) {
			h := nativeTraceStartHarness(t, launcher, pg, hash)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var phases []nativeFilterPhase
			walPath := filepath.Join(t.TempDir(), "fixture-wal")
			h.cfg.Persist = func(ctx context.Context, phase nativeFilterPhase, tr nativeBirthTraceTranscript) error {
				phases = append(phases, phase)
				require.Equal(t, h.cfg.Original, tr.Original)
				require.NotEqual(t, os.Getpid(), tr.TracerTID)
				require.True(t, tr.BaseVerified)
				require.False(t, tr.DetachedNonAuthorizing)
				if phase == nativeFilterPMFinal {
					require.True(t, tr.FinalVerified)
				}
				if mode == "base-panic" && phase == nativeFilterPMBase {
					panic("explicit native fixture panic")
				}
				if mode == "base-goexit" && phase == nativeFilterPMBase {
					runtime.Goexit()
				}
				if (mode == "base-persist-fail" && phase == nativeFilterPMBase) || (mode == "final-persist-fail" && phase == nativeFilterPMFinal) {
					return errNativeTraceModel
				}
				f, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					return err
				}
				_, err = f.WriteString(strconv.Itoa(int(phase)) + "\n")
				if err == nil {
					err = f.Sync()
				}
				closeErr := f.Close()
				if err != nil {
					return err
				}
				if closeErr != nil {
					return closeErr
				}
				if (mode == "base-cancel" && phase == nativeFilterPMBase) || (mode == "final-cancel" && phase == nativeFilterPMFinal) {
					cancel()
				}
				return nil
			}
			if mode == "wrong-nonce" {
				h.cfg.Expected.Nonce[0] ^= 1
			}
			if mode == "wrong-start" {
				h.cfg.Original.PostmasterStartTicks++
			}
			if mode == "wrong-peer" {
				h.cfg.Original.PostmasterPID++
			}
			if mode == "wrong-binary-sha" {
				h.cfg.BinarySHA256[0] ^= 1
			}
			tr, err := runNativeBirthTraceSession(ctx, h.conn, h.cfg)
			report := h.finish(t)

			detail := ""
			if err != nil {
				detail = err.Error()
			}
			nativeTraceSave(t, "session-"+mode, map[string]any{"transcript": tr, "error": detail, "child": report, "callbackPhases": phases, "runtime": runtime.Version(), "scope": "REAL_HOST_GO_PG_COMPONENT_ONLY_SYNTHETIC_SOURCE_AND_WAL_NO_NORMAL"})
			if mode == "valid" {
				require.NoError(t, err)
				require.True(t, tr.DetachedNonAuthorizing)
				require.Equal(t, []nativeFilterPhase{nativeFilterPMBase, nativeFilterPMFinal}, phases)
				require.Equal(t, 0, report.Code)
				require.Zero(t, report.Signal)
				require.Contains(t, report.Stdout, "postgres (PostgreSQL)")
				wal, err := os.ReadFile(walPath)
				require.NoError(t, err)
				require.Equal(t, "1\n2\n", string(wal))
			} else {
				require.Error(t, err)
				require.False(t, tr.DetachedNonAuthorizing)
				require.NotEqual(t, 0, report.Code)
				if mode == "base-panic" || mode == "base-goexit" {
					require.ErrorIs(t, err, ErrUnknown)
					require.True(t, tr.TracerThreadRetired)
					require.Equal(t, int(unix.SIGKILL), report.Signal)
				} else if strings.Contains(mode, "persist") || strings.Contains(mode, "cancel") {
					require.Equal(t, int(unix.SIGKILL), report.Signal)
					require.Equal(t, "original-pidfd-killed-and-exit-observed", tr.Cleanup)
					require.ErrorIs(t, err, ErrUnknown)
				} else {
					require.Empty(t, phases)
				}
			}
		})
	}
}

func TestNativeBirthTracerKernelThreadRetirement(t *testing.T) {
	launcher, pg, hash := nativeTraceSetup(t)
	for _, confirmed := range []bool{false, true} {
		t.Run(fmt.Sprintf("confirmed-%t", confirmed), func(t *testing.T) {
			h := nativeTraceStartHarness(t, launcher, pg, hash)
			var driver *nativeBirthTraceKernel
			tr, err := nativeTraceRunLockedWorker(func(tid int) (nativeBirthTraceTranscript, error, bool) {
				out := nativeBirthTraceTranscript{Original: h.cfg.Original, TracerTID: tid}
				var err error
				driver, err = newNativeBirthTraceKernel(h.conn, h.cfg, tid)
				if err != nil {
					return out, err, false
				}
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				if err = driver.verify(nil, 0); err != nil {
					return out, err, false
				}
				raw, err := driver.receive(ctx)
				if err != nil {
					return out, err, false
				}
				request, err := decodeNativeBirthRequest(raw, h.cfg.Expected)
				if err != nil {
					return out, err, false
				}
				bundle, err := driver.compile(ctx, request)
				if err != nil {
					return out, err, false
				}
				if err = driver.seize(); err != nil {
					return out, err, false
				}
				if err = driver.interrupt(); err != nil {
					return out, err, true
				}
				if err = driver.waitStop(ctx, false); err != nil {
					return out, err, true
				}
				if !confirmed {
					// 明确机械组件：stand-in不作原对象确认，绝不SETOPTIONS/ACK。
					out.Cleanup = "test-only-tentative-auto-detach"
					return out, errNativeTraceModel, true
				}
				if err = driver.verify(&request, tid); err != nil {
					return out, err, true
				}
				if err = driver.exitKill(); err != nil {
					return out, err, true
				}
				if err = driver.filters(bundle, nativeFilterPMBase); err != nil {
					return out, err, true
				}
				if err = driver.cont(); err != nil {
					return out, err, true
				}
				if err = driver.ack(ctx, h.cfg.Expected); err != nil {
					return out, err, true
				}
				if err = driver.waitStop(ctx, true); err != nil {
					return out, err, true
				}
				if err = driver.verify(&request, tid); err != nil {
					return out, err, true
				}
				if err = driver.filters(bundle, nativeFilterPMFinal); err != nil {
					return out, err, true
				}
				if err = driver.closed198(); err != nil {
					return out, err, true
				}
				out.Cleanup = "test-only-confirmed-exitkill"
				return out, errNativeTraceModel, true
			})
			require.ErrorIs(t, err, errNativeTraceModel)
			require.True(t, tr.TracerThreadRetired)
			require.NotEqual(t, os.Getpid(), tr.TracerTID)
			require.False(t, tr.DetachedNonAuthorizing)
			require.NotNil(t, driver)
			defer driver.close()
			if !confirmed {
				require.NoError(t, driver.alive(), "tentative stand-in must not be killed by tracer retirement")
				status, err := nativeTraceProcText(driver.procfd, "status")
				require.NoError(t, err)
				pid, err := nativeTraceStatusValues(status, "TracerPid", 1)
				require.NoError(t, err)
				require.Equal(t, []uint64{0}, pid)
				require.NoError(t, h.conn.Close()) // 无ACK，native只在此之后EOF关门。
			}
			report := h.finish(t)
			if confirmed {
				require.Equal(t, int(unix.SIGKILL), report.Signal)
			} else {
				require.Equal(t, 1, report.Code)
				require.Zero(t, report.Signal)
			}
			nativeTraceSave(t, fmt.Sprintf("retirement-confirmed-%t", confirmed), map[string]any{"transcript": tr, "child": report, "scope": "MECHANICAL_THREAD_RETIREMENT_NOT_REAL_PID_REUSE_OR_PRODUCTION_ROOT_WAL"})
		})
	}
}

// P_PIDFD机制实证：不同Go线程收走原child后，旧pidfd只能ECHILD，不能收新child。
// Host只证机制；强制numeric复用仅准确369隔离Guest且显式opt-in，绝不改Host sysctl。
func TestNativeBirthTracerKernelPidfdReaper(t *testing.T) {
	require.Equal(t, "isolated-pg17-root-tracer-v1", os.Getenv("PG17_NATIVE_BIRTH_TRACER"))
	require.Equal(t, 0, os.Getuid())
	force := os.Getenv("PG17_TRACE_FORCE_PID_REUSE") == "isolated369-initial-pidns-v1"
	if force {
		var u unix.Utsname
		require.NoError(t, unix.Uname(&u))
		release := string(u.Release[:])
		release = strings.TrimRight(release, "\x00")
		require.Equal(t, "6.12.26-william-normal-split-v1-e210f5e3ee94", release)
		cap, err := os.ReadFile("/proc/sys/kernel/neonvm_pid_max_limit")
		require.NoError(t, err)
		require.Equal(t, "512", strings.TrimSpace(string(cap)))
		cmdline, err := os.ReadFile("/proc/cmdline")
		require.NoError(t, err)
		require.Contains(t, string(cmdline), "pg17_native_tracer_component=1")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	parentTID := unix.Gettid()
	attempts := 1
	if force {
		attempts = 8
	}
	var facts []map[string]any
	reused := false
	for i := 0; i < attempts; i++ {
		original := exec.Command("/bin/true")
		require.NoError(t, original.Start())
		oldPID := original.Process.Pid
		fd, err := unix.PidfdOpen(oldPID, 0)
		require.NoError(t, err)
		type outcome struct {
			tid int
			err error
		}
		reaped := make(chan outcome, 1)
		go func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			reaped <- outcome{unix.Gettid(), original.Wait()}
		}()
		result := <-reaped
		require.NotEqual(t, parentTID, result.tid)
		require.NoError(t, result.err)
		if force {
			require.NoError(t, os.WriteFile("/proc/sys/kernel/ns_last_pid", []byte(strconv.Itoa(oldPID-1)), 0600))
		}
		fresh := exec.Command("/bin/true")
		require.NoError(t, fresh.Start())
		reused = fresh.Process.Pid == oldPID
		k := &nativeBirthTraceKernel{pidfd: fd, cfg: nativeBirthTraceConfig{Original: ProcessIdentity{PostmasterPID: oldPID}, PeerUID: uint32(os.Getuid())}}
		got, _, waitErr := k.waitOriginal()
		freshWait := fresh.Wait()
		require.NoError(t, unix.Close(fd))
		require.ErrorIs(t, waitErr, unix.ECHILD, "old pidfd must not retarget fresh child")
		require.Zero(t, got)
		require.NoError(t, freshWait, "fresh child exit must remain available for its real waiter")
		facts = append(facts, map[string]any{"oldPID": oldPID, "freshPID": fresh.Process.Pid, "numericPIDReused": reused, "parentTID": parentTID, "actualOtherReaperTID": result.tid, "originalPidfdWait": "ECHILD", "freshOwnWaitSucceeded": true})
		if !force || reused {
			break
		}
	}
	if force {
		require.True(t, reused, "controlled Guest must actually reuse numeric PID; no guessed green")
	}
	nativeTraceSave(t, "pidfd-reaper", map[string]any{"facts": facts, "forcedReuseGuestOnly": force, "actualNumericReuse": reused, "scope": "REAL_PIDFD_WAIT_REAPER_COMPONENT_NOT_PRODUCTION_ROOT_WAL_OR_NORMAL"})
}
