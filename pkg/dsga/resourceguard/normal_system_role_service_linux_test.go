//go:build linux

package resourceguard

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func normalRoleServiceTransferFixture() normalSystemRoleServiceTransfer {
	hash := strings.Repeat("ab", 32)
	boot := "11111111-2222-4333-8444-555555555555"
	parent := normalSystemLifecycleBinding{RootConsumer: nativeBirthConsumer{PID: 101, StartTicks: 123, InstanceNonce: strings.Repeat("cd", 32)}, GuestBootID: boot, VMUID: "vm-1", ComputeInstanceID: "compute-1", RuntimeDocumentSHA256: hash, NativeBuildSHA256: hash, SystemManifestSHA256: hash, SystemDevice: 50, SystemInode: 51, SystemMemoryMaxBytes: 64 << 20}
	ref := normalFilesystemOriginReference{LayoutVersion: 2, ProtocolVersion: 1, OriginalRoot: parent.RootConsumer, GuestBootID: boot, RuntimeDocumentSHA256: hash, NativeBuildSHA256: hash, SystemManifestSHA256: hash, VminitSHA256: hash, IntentSHA256: hash, CompletionSHA256: hash, Namespace: normalFilesystemObject{Device: 4, Inode: 5, MountID: 6}, Root: normalFilesystemObject{Device: 7, Inode: 8, MountID: 9, Readonly: true}}
	ns := []normalSystemRoleNamespaceIdentity{}
	for i, name := range []string{"mnt", "pid", "user", "cgroup", "ipc", "uts", "net", "time"} {
		n := normalSystemRoleNamespaceIdentity{Name: name, Device: 4, Inode: uint64(100 + i)}
		if name == "mnt" {
			n.Inode = 5
		}
		ns = append(ns, n)
	}
	o := normalSystemRoleServiceOrigin{ProtocolVersion: 1, Scope: normalSystemRoleServiceStartScope, Nonce: strings.Repeat("ef", 32), Parent: parent, Filesystem: ref, DeclarationSHA256: hash, Namespace: ns, ResourceIdentity: Identity{GuestBootID: boot, VMUID: "vm-1", ComputeInstanceID: "compute-1"}, ResourceFloor: 1, StateDevice: 11, StateInode: 12, ResourceStageSHA256: hash}
	task := normalSystemRoleObservation{Role: normalSystemRoleBrokerKind, PID: 222, StartTicks: 333, Binding: parent, ExecutableSHA256: hash, ArgvSHA256: hash, EnvironmentSHA256: hash, PIDFDDevice: 13, PIDFDInode: 14, OriginalTaskObserved: true, FixedArgvEnvironmentObserved: true}
	return normalSystemRoleServiceTransfer{ProtocolVersion: 1, Origin: o, IntentSHA256: hash, GateSHA256: [3]string{hash, hash, hash}, Task: task}
}
func TestNormalSystemRoleServiceStrictFrame(t *testing.T) {
	request := normalSystemRoleServiceFrame{Operation: normalSystemRoleServiceExecute, Role: 1, BrokerNonce: [32]byte{1}, RequestNonce: [32]byte{2}}
	raw := encodeNormalSystemRoleServiceFrame(request)
	got, e := decodeNormalSystemRoleServiceFrame(raw)
	require.NoError(t, e)
	require.Equal(t, request, got)
	for _, mode := range []string{"short", "magic", "version", "operation", "role", "reserved", "broker nonce", "request nonce"} {
		t.Run(mode, func(t *testing.T) {
			b := append([]byte{}, raw...)
			switch mode {
			case "short":
				b = b[:95]
			case "magic":
				b[0] = 'x'
			case "version":
				binary.BigEndian.PutUint32(b[8:12], 2)
			case "operation":
				binary.BigEndian.PutUint32(b[12:16], 3)
			case "role":
				binary.BigEndian.PutUint32(b[16:20], 12)
			case "reserved":
				b[31] = 1
			case "broker nonce":
				clear(b[32:64])
			case "request nonce":
				clear(b[64:96])
			}
			_, e := decodeNormalSystemRoleServiceFrame(b)
			require.Error(t, e)
		})
	}
	probe := request
	probe.Operation = normalSystemRoleServiceProbeReady
	_, e = decodeNormalSystemRoleServiceFrame(encodeNormalSystemRoleServiceFrame(probe))
	require.Error(t, e)
	probe.Role = 0
	_, e = decodeNormalSystemRoleServiceFrame(encodeNormalSystemRoleServiceFrame(probe))
	require.NoError(t, e)
	for _, role := range normalSystemRoleServiceRoles {
		code, e := normalSystemRoleServiceRoleCode(role)
		require.NoError(t, e)
		name, e := normalSystemRoleServiceRoleName(code)
		require.NoError(t, e)
		require.Equal(t, role, name)
	}
	for _, role := range []string{"", normalSystemRoleBrokerKind, "/bin/sh", "resource ", "Resource"} {
		_, e = normalSystemRoleServiceRoleCode(role)
		require.Error(t, e)
	}
	for _, status := range []uint32{normalSystemRoleServiceAccepted, normalSystemRoleServiceComplete, normalSystemRoleServiceUnknown} {
		reply := normalSystemRoleServiceReply(request, status, 0)
		s, x, e := decodeNormalSystemRoleServiceReply(reply, request)
		require.NoError(t, e)
		require.Equal(t, status, s)
		require.Zero(t, x)
		reply[64] ^= 1
		_, _, e = decodeNormalSystemRoleServiceReply(reply, request)
		require.Error(t, e)
	}
}
func TestNormalSystemRoleServiceOriginalTransferBinding(t *testing.T) {
	valid := normalRoleServiceTransferFixture()
	require.NoError(t, valid.validate())
	for name, edit := range map[string]func(*normalSystemRoleServiceTransfer){"old layout": func(v *normalSystemRoleServiceTransfer) { v.Origin.Filesystem.LayoutVersion = 0 }, "wrong original consumer": func(v *normalSystemRoleServiceTransfer) {
		v.Origin.Filesystem.OriginalRoot.InstanceNonce = strings.Repeat("01", 32)
	}, "wrong boot": func(v *normalSystemRoleServiceTransfer) {
		v.Origin.Filesystem.GuestBootID = "22222222-2222-4333-8444-555555555555"
	}, "wrong mount namespace": func(v *normalSystemRoleServiceTransfer) {
		v.Origin.Namespace = append([]normalSystemRoleNamespaceIdentity{}, v.Origin.Namespace...)
		v.Origin.Namespace[0].Inode++
	}, "no full resource hash": func(v *normalSystemRoleServiceTransfer) { v.Origin.ResourceStageSHA256 = "" }, "no gate source": func(v *normalSystemRoleServiceTransfer) { v.GateSHA256[1] = "" }, "borrow resource role": func(v *normalSystemRoleServiceTransfer) { v.Task.Role = "resource" }, "root rewritten": func(v *normalSystemRoleServiceTransfer) {
		v.Task.Binding.RootConsumer.InstanceNonce = strings.Repeat("01", 32)
	}, "fake qualification": func(v *normalSystemRoleServiceTransfer) { v.Task.ContinuousWritersClosed = true }} {
		t.Run(name, func(t *testing.T) { next := valid; edit(&next); require.Error(t, next.validate()) })
	}
}
func TestNormalSystemRoleServiceRequiresOriginalRootCapabilities(t *testing.T) {
	valid := "NoNewPrivs:\t0\nSeccomp:\t0\nCapEff:\t0000000000280020\n"
	require.NoError(t, normalSystemRoleServicePrivilegeStatus(valid))
	for _, mode := range []string{"nnp", "seccomp", "kill only", "no cap", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			s := valid
			switch mode {
			case "nnp":
				s = strings.Replace(s, "NoNewPrivs:\t0", "NoNewPrivs:\t1", 1)
			case "seccomp":
				s = strings.Replace(s, "Seccomp:\t0", "Seccomp:\t2", 1)
			case "kill only":
				s = strings.Replace(s, "0000000000280020", "0000000000000020", 1)
			case "no cap":
				s = strings.Replace(s, "0000000000280020", "0000000000000000", 1)
			case "malformed":
				s = strings.Replace(s, "CapEff:\t0000000000280020", "CapEff:\tprivate-sensitive-value", 1)
			}
			e := normalSystemRoleServicePrivilegeStatus(s)
			require.Error(t, e)
			require.NotContains(t, e.Error(), "private-sensitive-value")
		})
	}
}
func roleServiceTestPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, e)
	connections := make([]*net.UnixConn, 2)
	for i, fd := range fds {
		f := os.NewFile(uintptr(fd), "test original startup")
		c, e := net.FileConn(f)
		f.Close()
		require.NoError(t, e)
		connections[i] = c.(*net.UnixConn)
		t.Cleanup(func() { c.Close() })
	}
	return connections[0], connections[1]
}
func TestNormalSystemRoleServiceFixedFourDescriptorTransfer(t *testing.T) {
	t.Run("actual four FD and atomic CLOEXEC", func(t *testing.T) {
		sender, receiver := roleServiceTestPair(t)
		files := make([]*os.File, 4)
		for i := range files {
			f, e := os.CreateTemp(t.TempDir(), "original")
			require.NoError(t, e)
			files[i] = f
			defer f.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		transfer := normalRoleServiceTransferFixture()
		require.NoError(t, normalSystemRoleServiceWriteTransfer(sender, transfer, files, ctx))
		actual, received, e := normalSystemRoleServiceReadTransfer(receiver, ctx)
		require.NoError(t, e)
		require.Equal(t, transfer, actual)
		require.Len(t, received, 4)
		for i, f := range received {
			defer f.Close()
			var a, b unix.Stat_t
			require.NoError(t, unix.Fstat(int(files[i].Fd()), &a))
			require.NoError(t, unix.Fstat(int(f.Fd()), &b))
			require.Equal(t, a.Ino, b.Ino)
			flags, e := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
			require.NoError(t, e)
			require.NotZero(t, flags&unix.FD_CLOEXEC)
		}
	})
	for _, count := range []int{0, 3, 5} {
		t.Run(fmt.Sprintf("wrong rights %d", count), func(t *testing.T) {
			sender, receiver := roleServiceTestPair(t)
			transfer := normalRoleServiceTransferFixture()
			payload, e := json.Marshal(transfer)
			require.NoError(t, e)
			raw := make([]byte, normalSystemRoleServiceStartupBytes)
			copy(raw, "NSYSBF01")
			binary.BigEndian.PutUint32(raw[8:12], uint32(len(payload)))
			copy(raw[12:], payload)
			fds := []int{}
			for i := 0; i < count; i++ {
				f, e := os.CreateTemp(t.TempDir(), "extra")
				require.NoError(t, e)
				defer f.Close()
				fds = append(fds, int(f.Fd()))
			}
			var oob []byte
			if count > 0 {
				oob = unix.UnixRights(fds...)
			}
			_, _, e = sender.WriteMsgUnix(raw, oob, nil)
			require.NoError(t, e)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, received, e := normalSystemRoleServiceReadTransfer(receiver, ctx)
			require.ErrorIs(t, e, ErrUnknown)
			require.Nil(t, received)
		})
	}
}
func TestNormalSystemRoleServiceMissingOriginalSourcesHaveNoEffect(t *testing.T) {
	_, e := startNormalSystemRoleBroker(context.Background(), nil, nil, nil)
	require.ErrorIs(t, e, ErrUnknown)
	require.Error(t, RunNormalSystemRoleBroker(nil))
	_, e = RunNormalSystemRoleClient(context.Background(), "/bin/sh")
	require.Error(t, e)
}

func TestNormalSystemRoleServiceGateJournalAndOriginalChain(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires actual root-owned private state fixture")
	}
	for _, mode := range []string{"original", "changed task", "missing pending", "wrong chain hash", "wrong boot", "wrong resource snapshot"} {
		t.Run(mode, func(t *testing.T) {
			dir, e := os.Open(t.TempDir())
			require.NoError(t, e)
			defer dir.Close()
			state := &lockedState{dir: dir}
			transfer := normalRoleServiceTransferFixture()
			var st unix.Stat_t
			require.NoError(t, unix.Fstat(int(dir.Fd()), &st))
			transfer.Origin.StateDevice, transfer.Origin.StateInode = st.Dev, st.Ino
			intent := normalSystemRoleServiceStage{ProtocolVersion: 1, Origin: transfer.Origin, Stage: "intent"}
			transfer.IntentSHA256, e = normalSystemRoleServiceAppendStage(state, intent)
			require.NoError(t, e)
			pending := normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: transfer.Origin.Nonce, Stage: "pending", Role: normalSystemRoleBrokerKind, PlanSHA256: strings.Repeat("ab", 32), Binding: transfer.Origin.Parent, Namespace: transfer.Origin.Namespace, ResourceIdentity: transfer.Origin.ResourceIdentity, ResourceFloor: transfer.Origin.ResourceFloor, StateDevice: st.Dev, StateInode: st.Ino}
			j := &normalSystemRoleServiceGateJournal{state: state, pending: pending}
			sha, e := j.append(context.Background(), "pending", nil)
			require.NoError(t, e)
			j.hashes[0] = sha
			require.NoError(t, j.persistOriginalSystemRoleStopped(context.Background(), transfer.Task, pending.Namespace, pending.PlanSHA256))
			transfer.GateSHA256 = j.hashes
			// 独立family：新的普通broker history scanner不会误认原initialRoot为本runs。
			require.NoFileExists(t, filepath.Join(dir.Name(), normalSystemRoleStageName(pending.Nonce, "pending")))
			switch mode {
			case "changed task":
				transfer.Task.StartTicks++
			case "missing pending":
				require.NoError(t, os.Remove(filepath.Join(dir.Name(), normalSystemRoleServiceGateName(pending.Nonce, normalSystemRoleBrokerGatePending))))
			case "wrong chain hash":
				transfer.GateSHA256[1] = strings.Repeat("01", 32)
			case "wrong boot":
				transfer.Origin.Parent.GuestBootID = "22222222-2222-4333-8444-555555555555"
			case "wrong resource snapshot":
				transfer.Origin.ResourceStageSHA256 = strings.Repeat("01", 32)
			}
			e = normalSystemRoleServiceValidateTransferLocked(context.Background(), state, transfer)
			if mode == "original" {
				require.NoError(t, e)
			} else {
				require.Error(t, e)
			}
		})
	}
}

func TestNormalSystemRoleServiceComputeLockFDInfo(t *testing.T) {
	st := unix.Stat_t{Dev: unix.Mkdev(0, 0x28), Ino: 301}
	valid := "flags:\t0100001\nlock:\t1: FLOCK ADVISORY WRITE 99 00:28:301 0 EOF\n"
	flags, e := normalSystemRoleServiceFDFlags(valid)
	require.NoError(t, e)
	require.Equal(t, uint64(0100001), flags)
	require.NoError(t, normalSystemRoleServiceFlockWrite(valid, st))
	for _, mode := range []string{"no lock", "POSIX instead of OFD flock", "read lock", "wrong inode", "wrong device", "partial range", "duplicate", "bad flags"} {
		t.Run(mode, func(t *testing.T) {
			s := valid
			switch mode {
			case "no lock":
				s = "flags:\t0100001\n"
			case "POSIX instead of OFD flock":
				s = strings.Replace(s, "FLOCK", "POSIX", 1)
			case "read lock":
				s = strings.Replace(s, "WRITE", "READ", 1)
			case "wrong inode":
				s = strings.Replace(s, ":301", ":302", 1)
			case "wrong device":
				s = strings.Replace(s, "00:28:", "00:29:", 1)
			case "partial range":
				s = strings.Replace(s, " 0 EOF", " 1 EOF", 1)
			case "duplicate":
				s += strings.Split(valid, "\n")[1] + "\n"
			case "bad flags":
				s = strings.Replace(s, "0100001", "private-value", 1)
			}
			if mode == "bad flags" {
				_, e := normalSystemRoleServiceFDFlags(s)
				require.Error(t, e)
				require.NotContains(t, e.Error(), "private-value")
			} else {
				require.Error(t, normalSystemRoleServiceFlockWrite(s, st))
			}
		})
	}
}

func TestNormalSystemRoleServiceEmptyClientEnvironment(t *testing.T) {
	if len(os.Args) > 1 && os.Args[len(os.Args)-1] == "role-service-proc-env-child" {
		fmt.Println("ROLE_ENV_CHILD_USER_READY")
		var ack [1]byte
		_, e := io.ReadFull(os.Stdin, ack[:])
		require.NoError(t, e)
		return
	}

	expected := normalSystemRoleServiceExpectedEnvironment([]string{normalSystemRoleBrokerExecutable, "exec-system-role", "network"})
	require.NotNil(t, expected)
	require.Empty(t, expected)
	require.NoError(t, normalSystemRoleServiceClientEnvironment([]string{}))
	for _, argv := range [][]string{normalSystemRoleBrokerArgv(), {normalSystemRoleBrokerExecutable, "initial-boot"}, {normalSystemRoleBrokerExecutable, "recover-initial-boot"}} {
		require.Equal(t, []string{"PATH=/neonvm/bin"}, normalSystemRoleServiceExpectedEnvironment(argv))
	}
	for _, v := range [][]string{nil, {""}, {"PATH=/neonvm/bin"}, {"PRIVATE_KEY=NEVER_EMIT"}} {
		e := normalSystemRoleServiceClientEnvironment(v)
		require.Error(t, e)
		require.NotContains(t, e.Error(), "NEVER_EMIT")
	}
	for _, environment := range [][]string{{}, {"PATH=/neonvm/bin"}} {
		exe, e := os.Executable()
		require.NoError(t, e)
		cmd := exec.Command(exe, "-test.run=^TestNormalSystemRoleServiceEmptyClientEnvironment$", "--", "role-service-proc-env-child")
		cmd.Env = environment
		stdin, e := cmd.StdinPipe()
		require.NoError(t, e)
		stdout, e := cmd.StdoutPipe()
		require.NoError(t, e)
		require.NoError(t, cmd.Start())
		line, e := bufio.NewReader(stdout).ReadString('\n')
		require.NoError(t, e)
		require.Equal(t, "ROLE_ENV_CHILD_USER_READY\n", line)
		proc, e := os.Open("/proc/" + strconv.Itoa(cmd.Process.Pid))
		require.NoError(t, e)
		actual, e := normalSystemReadVector(int(proc.Fd()), "environ")
		require.NoError(t, e)
		proc.Close()
		require.NotNil(t, actual)
		require.Equal(t, environment, actual)
		e = normalSystemRoleServiceClientEnvironment(actual)
		if len(environment) == 0 {
			require.NoError(t, e)
		} else {
			require.Error(t, e)
		}
		_, e = stdin.Write([]byte{1})
		require.NoError(t, e)
		stdin.Close()
		_, e = io.Copy(io.Discard, stdout)
		require.NoError(t, e)
		require.NoError(t, cmd.Wait())
	}

}

func TestNormalSystemRoleServiceLockDoesNotOccupyRoleNonceHistory(t *testing.T) {
	path := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(path, normalSystemRoleServiceLockName), nil, 0600))
	dir, e := os.Open(path)
	require.NoError(t, e)
	defer dir.Close()
	state := &lockedState{dir: dir}
	broker := &normalSystemRoleBrokerComponent{source: &normalSystemLifecycleSource{}, runs: map[string]*normalSystemRoleOriginalRun{}}
	require.NoError(t, broker.rejectUnknownHistoryLocked(state))
	require.NoError(t, os.WriteFile(filepath.Join(path, "system-role-broker.lock"), nil, 0600))
	require.ErrorIs(t, broker.rejectUnknownHistoryLocked(state), ErrUnknown)
}

// 此U只模型化child的done/Wait结果；resource flock、append、fsync与原见证实做。
// 真正不同live角色并发spawn由独立369 BusyBox13rows服务S覆盖，不冒充此模型。
func TestNormalSystemRoleServiceRetiresOnlyConfirmedOriginalExitedRuns(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires actual root-owned persistent fixture")
	}
	for _, mode := range []string{"long role stays known", "short exit before next role", "unknown stays unknown", "unconfirmed Wait stays unknown", "missing witness stays unknown"} {
		t.Run(mode, func(t *testing.T) {
			g, _, command := fixtureGuard(t)
			binding := normalSystemLifecycleBinding{SystemManifestSHA256: strings.Repeat("ab", 32)}
			b := &normalSystemRoleBrokerComponent{guard: g, source: &normalSystemLifecycleSource{binding: binding}, runs: map[string]*normalSystemRoleOriginalRun{}, byRole: map[string]*normalSystemRoleOriginalRun{}}
			makeRun := func(nonce, role string) *normalSystemRoleOriginalRun {
				pidfd, e := unix.PidfdOpen(os.Getpid(), 0)
				require.NoError(t, e)
				pf := os.NewFile(uintptr(pidfd), "test original task pidfd")
				t.Cleanup(func() { pf.Close() })
				child := &normalSystemRoleOriginalChild{pidfd: pf, done: make(chan struct{})}
				child.waitOnce.Do(func() {})
				r := &normalSystemRoleOriginalRun{broker: b, child: child}
				require.NoError(t, g.withRecord(context.Background(), func(rec *record, state *lockedState) error {
					var st unix.Stat_t
					if e := unix.Fstat(int(state.dir.Fd()), &st); e != nil {
						return e
					}
					pending := normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: nonce, Stage: "pending", Role: role, PlanSHA256: strings.Repeat("ef", 32), Binding: binding, ResourceIdentity: command.Identity, StateDevice: st.Dev, StateInode: st.Ino}
					j := &normalSystemRoleJournal{state: state, pending: pending, seen: map[string]normalSystemRoleFileWitness{}}
					if e := j.append(context.Background(), "pending", nil, false); e != nil {
						return e
					}
					obs := normalSystemRoleObservation{Role: role, Binding: binding, OriginalTaskObserved: true, FixedArgvEnvironmentObserved: true}
					if e := j.persistOriginalSystemRoleStopped(context.Background(), obs, nil, pending.PlanSHA256); e != nil {
						return e
					}
					r.pending = pending
					r.previous = j.previous
					r.seen = j.seen
					child.observed.Task = obs
					return nil
				}))
				b.runs[nonce] = r
				b.byRole[role] = r
				return r
			}
			first := makeRun(strings.Repeat("cd", 32), "logs")
			service := &normalSystemRoleService{broker: b}
			switch mode {
			case "unknown stays unknown":
				first.unknown = true
			case "short exit before next role":
				first.child.waited = true
				close(first.child.done)
			case "unconfirmed Wait stays unknown":
				close(first.child.done)
			case "missing witness stays unknown":
				first.child.waited = true
				close(first.child.done)
				first.seen = map[string]normalSystemRoleFileWitness{}
			}
			if mode == "long role stays known" {
				require.ErrorIs(t, service.finishExitedRole(first), ErrUnknown)
				require.False(t, first.unknown)
			}
			// 下一个角色的持久前缀不会把仍活着的原服务标Unknown。
			if mode == "long role stays known" {
				_ = makeRun(strings.Repeat("01", 32), "network")
			}
			service.execMu.Lock()
			progress, e := service.retireExitedRolesLocked()
			service.execMu.Unlock()
			if mode == "unknown stays unknown" || mode == "unconfirmed Wait stays unknown" || mode == "missing witness stays unknown" {
				require.ErrorIs(t, e, ErrUnknown)
				require.True(t, first.unknown)
				return
			}
			require.NoError(t, e)
			require.Equal(t, mode == "short exit before next role", progress)
			require.False(t, first.unknown)
			require.NoError(t, g.withRecord(context.Background(), func(_ *record, state *lockedState) error { return b.rejectUnknownHistoryLocked(state) }))
			if mode == "short exit before next role" {
				require.True(t, first.terminal)
				require.NoError(t, service.finishExitedRole(first))
				require.FileExists(t, filepath.Join(g.config.StateDir, normalSystemRoleStageName(first.pending.Nonce, "exited")))
				_ = makeRun(strings.Repeat("01", 32), "network")
				require.NoError(t, g.withRecord(context.Background(), func(_ *record, state *lockedState) error { return b.rejectUnknownHistoryLocked(state) }))
			}
		})
	}
}

// 真正exec/wait子进程退出；这里只模型化既有journal的terminal标志，不把U当原
// cgroup/Gate持久源。完整broker WAL/client真实退出码由同369 S另行覆盖。
func TestNormalSystemRoleServiceExactOriginalExitCode(t *testing.T) {
	for _, item := range []struct {
		name, script string
		want         uint32
	}{{"zero", "exit 0", 0}, {"exit2", "exit 2", 2}, {"exit7", "exit 7", 7}, {"exit255", "exit 255", 255}, {"signalTERM", "kill -TERM $$", 143}} {
		t.Run(item.name, func(t *testing.T) {
			cmd := exec.Command("/bin/sh", "-c", item.script)
			waitErr := cmd.Run()
			closed := func() chan struct{} { ch := make(chan struct{}); close(ch); return ch }
			child := &normalSystemRoleOriginalChild{cmd: cmd, done: closed(), creatorLeaseDone: closed(), waited: true, waitErr: waitErr}
			run := &normalSystemRoleOriginalRun{broker: &normalSystemRoleBrokerComponent{}, child: child, terminal: true}
			joined := errors.Join(errors.Join(waitErr, nil), nil)
			status, code := normalSystemRoleServiceOriginalExitResult(run, joined)
			require.Equal(t, uint32(normalSystemRoleServiceComplete), status)
			require.Equal(t, item.want, code)
			if waitErr != nil {
				var target *exec.ExitError
				require.ErrorAs(t, waitErr, &target)
				require.Same(t, cmd.ProcessState, target.ProcessState)
				actual := cmd.ProcessState.Sys().(syscall.WaitStatus)
				require.True(t, actual.Exited() || actual.Signaled())
			}
			for _, bad := range []string{"not durable", "unknown", "creator failure", "Wait missing", "process missing", "Wait unconfirmed", "foreign wait error", "exit plus unknown", "creator lease pending", "done pending"} {
				t.Run(bad, func(t *testing.T) {
					copyChild := &normalSystemRoleOriginalChild{cmd: cmd, done: closed(), creatorLeaseDone: closed(), waited: true, waitErr: waitErr}
					copyRun := &normalSystemRoleOriginalRun{broker: &normalSystemRoleBrokerComponent{}, child: copyChild, terminal: true}
					combined := joined
					switch bad {
					case "not durable":
						copyRun.terminal = false
					case "unknown":
						copyRun.unknown = true
					case "creator failure":
						copyChild.creatorLeaseErr = ErrUnknown
					case "Wait missing":
						copyChild.waited = false
					case "process missing":
						copyChild.cmd = &exec.Cmd{}
					case "Wait unconfirmed":
						copyChild.waitErr = ErrUnknown
					case "foreign wait error":
						copyChild.waitErr = errors.New("other wait")
					case "exit plus unknown":
						combined = errors.Join(joined, ErrUnknown)
					case "creator lease pending":
						copyChild.creatorLeaseDone = make(chan struct{})
					case "done pending":
						copyChild.done = make(chan struct{})
					}
					status, code := normalSystemRoleServiceOriginalExitResult(copyRun, combined)
					require.Equal(t, uint32(normalSystemRoleServiceUnknown), status)
					require.Zero(t, code)
				})
			}
		})
	}
}
