//go:build linux && dsga_native_role_gate && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// DEV TestMain仅替代尚未接的CLI分支；同一个真实static Go ELF分别执行受信
// Gate和站位resource。不是正式neonvmd/chronyd/vector/PG或normal资格。
func TestMain(m *testing.M) {
	if len(os.Args) == 2 {
		switch os.Args[1] {
		case normalSystemRoleGateAction:
			if e := RunNormalSystemRoleExecGate(context.Background()); e != nil {
				fmt.Fprintln(os.Stderr, "DEV trusted Gate refused:", e)
				os.Exit(42)
			}
			os.Exit(0)
		case "initial-boot":
			pattern := "^TestNativeNormalSystemRoleGateBrokerLifetime$"
			if _, e := os.Stat("/neonvm/config/dev-broker-death"); e == nil {
				pattern = "^TestNativeNormalSystemRoleGateBrokerDeathBeforeRelease$"
			}
			_ = flag.Set("test.run", pattern)
			_ = flag.Set("test.v", "true")
			os.Exit(m.Run())
		case "serve":
			f, e := os.OpenFile("/tmp/normal-role-target-entry", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if e != nil {
				os.Exit(43)
			}
			_, _ = f.WriteString("APPROVED_STATIC_TARGET_ENTRY\n")
			_ = f.Sync()
			_ = f.Close()
			for {
				time.Sleep(time.Second)
			}
		case "system-role-broker":
			if typ, e := unix.GetsockoptInt(5, unix.SOL_SOCKET, unix.SO_TYPE); e == nil && typ == unix.SOCK_SEQPACKET {
				if e := runDEVNormalSystemRoleBrokerLifetime(); e != nil {
					fmt.Fprintln(os.Stderr, "DEV broker lifetime refused:", e)
					os.Exit(47)
				}
				os.Exit(0)
			}
			if typ, e := unix.GetsockoptInt(3, unix.SOL_SOCKET, unix.SO_TYPE); e == nil && typ == unix.SOCK_SEQPACKET {
				conn, e := normalSystemRoleGateConnection(3)
				if e != nil {
					os.Exit(44)
				}
				raw, e := arrayServicePacket(conn, time.Now().Add(5*time.Second), 32, false)
				if e != nil || (string(raw[:8]) != "DEVKILL1" && string(raw[:8]) != "DEVTHRD1") {
					os.Exit(45)
				}
				index := binary.BigEndian.Uint32(raw[8:12])
				if index >= uint32(len(devRoleGateDeathStages)) {
					os.Exit(46)
				}
				devRoleGateDeathStage = devRoleGateDeathStages[index]
				devRoleGateDeathControl = conn
				testName := "^TestNativeNormalSystemRoleGateDeathHelper$"
				if string(raw[:8]) == "DEVTHRD1" {
					testName = "^TestNativeNormalSystemRoleGateThreadHelper$"
				}
				_ = flag.Set("test.run", testName)
				_ = flag.Set("test.v", "true")
				os.Exit(m.Run())
			}
			pattern := "^TestNativeNormalSystemRoleGate(Lifecycle|FaultDeaths|ThreadDeaths)$"
			if _, e := os.Stat(normalSystemRoleBuildPlanPath); e == nil {
				pattern = "^TestNativeNormalSystemRoleGateDynamic$"
			}
			_ = flag.Set("test.run", pattern)
			_ = flag.Set("test.v", "true")
			os.Exit(m.Run())
		}
	}
	os.Exit(m.Run())
}
func TestExportNormalSystemRoleGateFixture(t *testing.T) {
	TestExportNormalSystemLifecycleFixture(t)
	exportNormalSystemRoleDynamicFixture(t)
	exportNormalSystemRoleBrokerFixture(t)
}

type devNormalSystemRoleGateWriter struct {
	j     *normalSystemRoleJournal
	calls int
	fail  bool
}

func (w *devNormalSystemRoleGateWriter) originalSystemRoleGateReference() (normalSystemRoleGateReference, error) {
	return normalSystemRoleGateReference{w.j.pending.Nonce, w.j.previous}, nil
}
func (w *devNormalSystemRoleGateWriter) persistOriginalSystemRoleStopped(ctx context.Context, o normalSystemRoleObservation, ns []normalSystemRoleNamespaceIdentity, plan string) error {
	w.calls++
	if w.fail {
		return fmt.Errorf("DEV deliberate stopped-stage refusal")
	}
	return w.j.persistOriginalSystemRoleStopped(ctx, o, ns, plan)
}
func TestNativeNormalSystemRoleGateLifecycle(t *testing.T) {
	require.Equal(t, []string{"/neonvm/bin/dsga-resource-guard", "system-role-broker"}, os.Args)
	source, e := inspectNormalSystemLifecycleOriginal(context.Background())
	require.NoError(t, e)
	defer source.close()
	ns, e := captureNormalSystemRoleNamespaces()
	require.NoError(t, e)
	defer ns.close()
	runtime, e := sealedRuntimeSource()
	require.NoError(t, e)
	state, e := lockState(context.Background(), runtime.Config.StateDir)
	require.NoError(t, e)
	defer state.close()
	identities, e := ns.snapshot()
	require.NoError(t, e)
	makeWriter := func(fail bool) *devNormalSystemRoleGateWriter {
		nonce, e := newNativeBirthNonce()
		require.NoError(t, e)
		plan, e := newNormalSystemRoleLaunchSpec(context.Background(), source, "resource")
		require.NoError(t, e)
		sha, e := plan.referenceSHA256()
		require.NoError(t, e)
		pending := normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: nonce, Stage: "pending", Role: "resource", PlanSHA256: sha, Binding: source.binding, Namespace: identities}
		j := &normalSystemRoleJournal{state: state, pending: pending, seen: map[string]normalSystemRoleFileWitness{}}
		require.NoError(t, j.append(context.Background(), "pending", nil, false))
		return &devNormalSystemRoleGateWriter{j: j, fail: fail}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	writer := makeWriter(false)
	child, e := spawnNormalSystemRoleOriginal(ctx, source, ns, "resource", writer)
	require.NoError(t, e)
	require.Equal(t, 1, writer.calls)
	require.True(t, child.observed.Released)
	require.NotEqual(t, os.Getpid(), child.observed.TracerTID)
	require.Eventually(t, func() bool { _, e := os.Stat("/tmp/normal-role-target-entry"); return e == nil }, 3*time.Second, 10*time.Millisecond)
	// spawn函数已经返回，但原creator M必须仍持活；不能只加Pdeathsig后把M还池。
	require.NoError(t, normalSystemRoleGateThreadAlive(int(child.creatorPIDFD.Fd())))
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, arrayRegistryPIDFDAlive(int(child.pidfd.Fd())))
	require.NoError(t, normalSystemRoleGateThreadAlive(int(child.creatorPIDFD.Fd())))
	require.NoError(t, child.cancelAndWait())
	require.True(t, child.waited)
	select {
	case <-child.creatorLeaseDone:
	case <-time.After(2 * time.Second):
		t.Fatal("original creator lease not joined")
	}
	fmt.Println("NEON-S-ROLE-GATE-001 PASS true Go Gate to target second exec-stop; actual journal before target user entry")
	fmt.Println("NEON-S-ROLE-GATE-002 PASS returned nonleader creator remains live for long role, then original Wait/lease join without late kill")
	require.NoError(t, os.Remove("/tmp/normal-role-target-entry"))
	refuse := makeWriter(true)
	_, e = spawnNormalSystemRoleOriginal(ctx, source, ns, "resource", refuse)
	require.ErrorIs(t, e, ErrUnknown)
	require.Equal(t, 1, refuse.calls)
	require.NoFileExists(t, "/tmp/normal-role-target-entry")
	fmt.Println("NEON-S-ROLE-GATE-003 PASS target exec-stop stage refusal never executes target user instruction")
	payload, _ := json.Marshal(map[string]any{"scope": "DEV real static Go Gate/standin target, original namespace/clone/pidfd/two exec stops/journal facts", "normal_permit": false, "E": 0})
	fmt.Println("ROLE_GATE_RESULT=" + string(payload))
}

var devRoleGateDeathStages = []string{"before-options", "after-options-before-go", "after-go-before-cont", "target-exec-stop"}
var devRoleGateDeathStage string
var devRoleGateDeathControl *net.UnixConn

func TestNativeNormalSystemRoleGateDeathHelper(t *testing.T) {
	require.NotNil(t, devRoleGateDeathControl)
	source, e := inspectNormalSystemLifecycleOriginal(context.Background())
	require.NoError(t, e)
	defer source.close()
	ns, e := captureNormalSystemRoleNamespaces()
	require.NoError(t, e)
	defer ns.close()
	config, e := sealedRuntimeSource()
	require.NoError(t, e)
	state, e := lockState(context.Background(), config.Config.StateDir)
	require.NoError(t, e)
	defer state.close()
	ids, e := ns.snapshot()
	require.NoError(t, e)
	nonce, e := newNativeBirthNonce()
	require.NoError(t, e)
	plan, e := newNormalSystemRoleLaunchSpec(context.Background(), source, "resource")
	require.NoError(t, e)
	ph, e := plan.referenceSHA256()
	require.NoError(t, e)
	pending := normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: nonce, Stage: "pending", Role: "resource", PlanSHA256: ph, Binding: source.binding, Namespace: ids}
	j := &normalSystemRoleJournal{state: state, pending: pending, seen: map[string]normalSystemRoleFileWitness{}}
	require.NoError(t, j.append(context.Background(), "pending", nil, false))
	writer := &devNormalSystemRoleGateWriter{j: j}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, normalSystemRoleGateCheckpointKey{}, normalSystemRoleGateCheckpoint(func(stage string, pidfd, tid int) {
		if stage != devRoleGateDeathStage {
			return
		}
		raw := make([]byte, 32)
		copy(raw, "DEVSTOP1")
		binary.BigEndian.PutUint32(raw[8:12], uint32(tid))
		n, on, e := devRoleGateDeathControl.WriteMsgUnix(raw, unix.UnixRights(pidfd), nil)
		if e != nil || n != 32 || on == 0 {
			panic("DEV checkpoint descriptor write failed")
		}
		select {}
	}))
	_, e = spawnNormalSystemRoleOriginal(ctx, source, ns, "resource", writer)
	t.Fatalf("DEV helper unexpectedly completed: %v", e)
}
func TestNativeNormalSystemRoleGateFaultDeaths(t *testing.T) {
	_ = os.Remove("/tmp/normal-role-target-entry")
	group, e := os.Open(systemCgroup)
	require.NoError(t, e)
	defer group.Close()
	for index, stage := range devRoleGateDeathStages {
		t.Run(stage, func(t *testing.T) {
			pair, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, e)
			parent := os.NewFile(uintptr(pair[0]), "DEV tracer-death-controller")
			child := os.NewFile(uintptr(pair[1]), "DEV tracer-death-helper")
			connection, e := net.FileConn(parent)
			parent.Close()
			require.NoError(t, e)
			conn := connection.(*net.UnixConn)
			defer conn.Close()
			defer child.Close()
			pfd := -1
			cmd := exec.Command("/neonvm/bin/dsga-resource-guard", "system-role-broker")
			cmd.Env = []string{"PATH=/neonvm/bin"}
			cmd.ExtraFiles = []*os.File{child}
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(group.Fd()), PidFD: &pfd}
			require.NoError(t, cmd.Start())
			original := os.NewFile(uintptr(pfd), "DEV original creator-process pidfd")
			defer original.Close()
			raw := make([]byte, 32)
			copy(raw, "DEVKILL1")
			binary.BigEndian.PutUint32(raw[8:12], uint32(index))
			_, e = conn.Write(raw)
			require.NoError(t, e)
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(20*time.Second)))
			data := make([]byte, 32)
			oob := make([]byte, unix.CmsgSpace(4))
			n, on, flags, _, e := conn.ReadMsgUnix(data, oob)
			require.NoError(t, e)
			require.Equal(t, 32, n)
			require.Zero(t, flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC))
			require.Equal(t, "DEVSTOP1", string(data[:8]))
			tid := binary.BigEndian.Uint32(data[8:12])
			require.NotEqual(t, uint32(cmd.Process.Pid), tid)
			messages, e := unix.ParseSocketControlMessage(oob[:on])
			require.NoError(t, e)
			require.Len(t, messages, 1)
			fds, e := unix.ParseUnixRights(&messages[0])
			require.NoError(t, e)
			require.Len(t, fds, 1)
			tracee := os.NewFile(uintptr(fds[0]), "DEV original tracee pidfd transferred at exact checkpoint")
			defer tracee.Close()
			require.NoError(t, arrayRegistryPIDFDAlive(int(tracee.Fd())))
			require.NoError(t, unix.PidfdSendSignal(pfd, unix.SIGKILL, nil, 0))
			e = cmd.Wait()
			require.Error(t, e)
			require.Eventually(t, func() bool { done, e := normalSystemRoleGateExitObserved(int(tracee.Fd())); return e == nil && done }, 8*time.Second, 10*time.Millisecond)
			require.NoFileExists(t, "/tmp/normal-role-target-entry")
			fmt.Println("NEON-S-ROLE-GATE-DEATH PASS original creator killed at " + stage + "; target role/loader user code never released")
		})
	}
	fmt.Println("NEON-S-ROLE-GATE-004 PASS real Go creator death before/after options and GO, all original tracees exited without target marker")
}

func TestNativeNormalSystemRoleGateThreadHelper(t *testing.T) {
	require.NotNil(t, devRoleGateDeathControl)
	source, e := inspectNormalSystemLifecycleOriginal(context.Background())
	require.NoError(t, e)
	defer source.close()
	ns, e := captureNormalSystemRoleNamespaces()
	require.NoError(t, e)
	defer ns.close()
	config, e := sealedRuntimeSource()
	require.NoError(t, e)
	state, e := lockState(context.Background(), config.Config.StateDir)
	require.NoError(t, e)
	defer state.close()
	ids, e := ns.snapshot()
	require.NoError(t, e)
	nonce, e := newNativeBirthNonce()
	require.NoError(t, e)
	plan, e := newNormalSystemRoleLaunchSpec(context.Background(), source, "resource")
	require.NoError(t, e)
	ph, e := plan.referenceSHA256()
	require.NoError(t, e)
	pending := normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: nonce, Stage: "pending", Role: "resource", PlanSHA256: ph, Binding: source.binding, Namespace: ids}
	j := &normalSystemRoleJournal{state: state, pending: pending, seen: map[string]normalSystemRoleFileWitness{}}
	require.NoError(t, j.append(context.Background(), "pending", nil, false))
	writer := &devNormalSystemRoleGateWriter{j: j}
	type originals struct {
		creator, tracee int
		tid             int
	}
	checkpoint := make(chan originals, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, normalSystemRoleGateCheckpointKey{}, normalSystemRoleGateCheckpoint(func(stage string, pidfd, tid int) {
		if stage != devRoleGateDeathStage {
			return
		}
		thread, e := unix.PidfdOpen(tid, normalSystemRoleCreatorThreadFDFlag)
		if e != nil {
			panic("DEV original creator thread pin failed")
		}
		tracee, e := unix.FcntlInt(uintptr(pidfd), unix.F_DUPFD_CLOEXEC, 0)
		if e != nil {
			panic("DEV tracee pin failed")
		}
		checkpoint <- originals{thread, tracee, tid}
		// 只SYS_exit退出当前locked M，不exit_group，不Goexit，不执行任何defer。
		// 用Syscall wrapper先归还P，保同TGID其它Go M能继续持证/输出，避免停世界假象。
		_, _, _ = unix.Syscall(unix.SYS_EXIT, 0, 0, 0)
		panic("DEV SYS_exit returned")
	}))
	go func() { _, _ = spawnNormalSystemRoleOriginal(ctx, source, ns, "resource", writer) }()
	var old originals
	select {
	case old = <-checkpoint:
	case <-time.After(20 * time.Second):
		t.Fatal("DEV thread death checkpoint missing")
	}
	defer unix.Close(old.creator)
	defer unix.Close(old.tracee)
	require.Eventually(t, func() bool { v, e := normalSystemRoleGateExitObserved(old.creator); return e == nil && v }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { v, e := normalSystemRoleGateExitObserved(old.tracee); return e == nil && v }, 8*time.Second, 10*time.Millisecond)
	self, e := unix.PidfdOpen(os.Getpid(), 0)
	require.NoError(t, e)
	defer unix.Close(self)
	require.NoError(t, arrayRegistryPIDFDAlive(self))
	require.NotEqual(t, os.Getpid(), old.tid)
	require.NoFileExists(t, "/tmp/normal-role-target-entry")
	raw := make([]byte, 32)
	copy(raw, "DEVTRES1")
	binary.BigEndian.PutUint32(raw[8:12], uint32(old.tid))
	binary.BigEndian.PutUint32(raw[12:16], uint32(os.Getpid()))
	n, on, e := devRoleGateDeathControl.WriteMsgUnix(raw, unix.UnixRights(old.creator, old.tracee), nil)
	require.NoError(t, e)
	require.Equal(t, 32, n)
	require.Greater(t, on, 0)
	ack, e := arrayServicePacket(devRoleGateDeathControl, time.Now().Add(10*time.Second), 8, false)
	require.NoError(t, e)
	require.Equal(t, "DEVACK01", string(ack))
}
func TestNativeNormalSystemRoleGateThreadDeaths(t *testing.T) {
	_ = os.Remove("/tmp/normal-role-target-entry")
	group, e := os.Open(systemCgroup)
	require.NoError(t, e)
	defer group.Close()
	for index, stage := range devRoleGateDeathStages {
		t.Run(stage, func(t *testing.T) {
			pair, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, e)
			parent := os.NewFile(uintptr(pair[0]), "DEV thread-only-death-controller")
			child := os.NewFile(uintptr(pair[1]), "DEV thread-only-death-helper")
			connection, e := net.FileConn(parent)
			parent.Close()
			require.NoError(t, e)
			conn := connection.(*net.UnixConn)
			defer conn.Close()
			defer child.Close()
			pfd := -1
			cmd := exec.Command("/neonvm/bin/dsga-resource-guard", "system-role-broker")
			cmd.Env = []string{"PATH=/neonvm/bin"}
			cmd.ExtraFiles = []*os.File{child}
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(group.Fd()), PidFD: &pfd}
			require.NoError(t, cmd.Start())
			original := os.NewFile(uintptr(pfd), "DEV original live TGID pidfd")
			defer original.Close()
			raw := make([]byte, 32)
			copy(raw, "DEVTHRD1")
			binary.BigEndian.PutUint32(raw[8:12], uint32(index))
			_, e = conn.Write(raw)
			require.NoError(t, e)
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(25*time.Second)))
			data := make([]byte, 32)
			oob := make([]byte, unix.CmsgSpace(8))
			n, on, flags, _, e := conn.ReadMsgUnix(data, oob)
			require.NoError(t, e)
			require.Equal(t, 32, n)
			require.Zero(t, flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC))
			require.Equal(t, "DEVTRES1", string(data[:8]))
			tid := binary.BigEndian.Uint32(data[8:12])
			require.Equal(t, uint32(cmd.Process.Pid), binary.BigEndian.Uint32(data[12:16]))
			require.NotEqual(t, uint32(cmd.Process.Pid), tid)
			messages, e := unix.ParseSocketControlMessage(oob[:on])
			require.NoError(t, e)
			require.Len(t, messages, 1)
			fds, e := unix.ParseUnixRights(&messages[0])
			require.NoError(t, e)
			require.Len(t, fds, 2)
			defer unix.Close(fds[0])
			defer unix.Close(fds[1])
			threadFlags, e := unix.FcntlInt(uintptr(fds[0]), unix.F_GETFL, 0)
			require.NoError(t, e)
			require.NotZero(t, threadFlags&normalSystemRoleCreatorThreadFDFlag)
			exited, e := normalSystemRoleGateExitObserved(fds[0])
			require.NoError(t, e)
			require.True(t, exited)
			exited, e = normalSystemRoleGateExitObserved(fds[1])
			require.NoError(t, e)
			require.True(t, exited)
			require.NoError(t, arrayRegistryPIDFDAlive(pfd))
			require.NoFileExists(t, "/tmp/normal-role-target-entry")
			_, e = conn.Write([]byte("DEVACK01"))
			require.NoError(t, e)
			require.NoError(t, cmd.Wait())
			fmt.Println("NEON-S-ROLE-GATE-THREAD PASS original locked M SYS_exit at " + stage + "; TGID independently alive, original thread/tracee exited, no target")
		})
	}
	fmt.Println("NEON-S-ROLE-GATE-005 PASS true Go creator-thread-only death at four checkpoints; no defer/exit_group substitution")
}
