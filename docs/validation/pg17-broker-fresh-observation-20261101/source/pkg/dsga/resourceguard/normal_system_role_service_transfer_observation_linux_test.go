//go:build linux

package resourceguard

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// 此夹具仅持真实kernel Task用于拒绝测试，不构造System/FS/Source资格。
func brokerObservationDEVTask(t *testing.T, pid int) *normalSystemOriginalTask {
	t.Helper()
	pf, e := unix.PidfdOpen(pid, 0)
	require.NoError(t, e)
	p := os.NewFile(uintptr(pf), "DEV real pidfd")
	defer p.Close()
	fd, e := unix.Open("/proc/"+strconv.Itoa(pid), unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	require.NoError(t, e)
	f := os.NewFile(uintptr(fd), "DEV real procfd")
	defer f.Close()
	raw, e := nativeTraceProcText(fd, "stat")
	require.NoError(t, e)
	ticks, e := postmasterTicks(raw, pid)
	require.NoError(t, e)
	boot, e := kernelText("/proc/sys/kernel/random/boot_id")
	require.NoError(t, e)
	task, e := newNormalSystemOriginalTask(normalSystemOriginalTaskSource{PID: pid, StartTicks: ticks, UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), GuestBootID: boot, PIDFD: p, ProcDirectory: f})
	require.NoError(t, e)
	t.Cleanup(task.close)
	return task
}
func TestNormalBrokerFreshObservationRejectsOriginalProofAndTaskDrift(t *testing.T) {
	require.Zero(t, os.Getuid(), "this targeted Linux validation uses real root-owned Task fixture")
	for _, kind := range []string{"closed-proof", "foreign-System", "closed-task", "wrong-start", "wrong-boot", "different-live-task"} {
		t.Run(kind, func(t *testing.T) {
			current, e := nativeBirthCurrentConsumerIdentity()
			require.NoError(t, e)
			task := brokerObservationDEVTask(t, os.Getpid())
			if kind == "different-live-task" {
				path, e := exec.LookPath("sleep")
				require.NoError(t, e)
				child := exec.Command(path, "30")
				require.NoError(t, child.Start())
				defer func() { child.Process.Kill(); child.Wait() }()
				task = brokerObservationDEVTask(t, child.Process.Pid)
			}
			system := &normalSystemLifecycleSource{binding: normalSystemLifecycleBinding{RootConsumer: current}}
			dir, e := os.Open(t.TempDir())
			require.NoError(t, e)
			defer dir.Close()
			state := &lockedState{dir: dir}
			p := &normalSystemRoleBrokerExecProof{system: system, binding: system.binding, task: task, declaration: &normalSystemRoleDeclarationSource{closed: true}, witnesses: map[string]normalSystemRoleFileWitness{"a": {}, "b": {}, "c": {}, "d": {}}}
			want := ""
			switch kind {
			case "closed-proof":
				p.closed = true
				want = "original broker proof closed"
			case "foreign-System":
				system = &normalSystemLifecycleSource{binding: system.binding}
				want = "proof System differs"
			case "closed-task":
				task.close()
				want = "original System task closed"
			case "wrong-start":
				task.source.StartTicks++
				want = "original System start differs"
			case "wrong-boot":
				task.source.GuestBootID = "11111111-2222-4333-8444-555555555555"
				want = "original System Guest differs"
			case "different-live-task":
				want = "original broker task is not this process"
			}
			trace := &normalBrokerInspectionDEVTrace{}
			ctx := context.WithValue(context.Background(), normalBrokerInspectionDEVTraceKey{}, trace)
			obs, e := p.verifyOriginalObservationLocked(ctx, state, system)
			require.ErrorContains(t, e, want)
			require.Equal(t, normalSystemRoleObservation{}, obs)
			require.ErrorContains(t, p.verifyLocked(ctx, state, system), want)
			got, e := p.originalTaskForFilesystemLocked(ctx, state, system)
			require.ErrorContains(t, e, want)
			require.Nil(t, got)
			require.Equal(t, normalBrokerInspectionDEVSummary{}, trace.snapshot(), "negative must reach its stated gate before the final inspection")
		})
	}
}

// 这里仅实做原四WAL字节/身份链；引用沿用既有语法fixture，不冒完整proof来源。
func TestNormalBrokerFreshObservationRetainsOriginalTransferWALRejection(t *testing.T) {
	for _, kind := range []string{"original", "missing-intent", "missing-pending", "changed-released-bytes", "changed-task", "changed-namespace-binding"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			require.NoError(t, os.Chmod(directory, 0700))
			dir, e := os.Open(directory)
			require.NoError(t, e)
			defer dir.Close()
			state := &lockedState{dir: dir}
			transfer := normalRoleServiceTransferFixture()
			var st unix.Stat_t
			require.NoError(t, unix.Fstat(int(dir.Fd()), &st))
			transfer.Origin.StateDevice, transfer.Origin.StateInode = st.Dev, st.Ino
			transfer.IntentSHA256, e = normalSystemRoleServiceAppendStage(state, normalSystemRoleServiceStage{ProtocolVersion: 1, Origin: transfer.Origin, Stage: "intent"})
			require.NoError(t, e)
			pending := normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: transfer.Origin.Nonce, Stage: "pending", Role: normalSystemRoleBrokerKind, PlanSHA256: strings.Repeat("ab", 32), Binding: transfer.Origin.Parent, Namespace: transfer.Origin.Namespace, ResourceIdentity: transfer.Origin.ResourceIdentity, ResourceFloor: transfer.Origin.ResourceFloor, StateDevice: st.Dev, StateInode: st.Ino}
			journal := &normalSystemRoleServiceGateJournal{state: state, pending: pending}
			sha, e := journal.append(context.Background(), "pending", nil)
			require.NoError(t, e)
			journal.hashes[0] = sha
			require.NoError(t, journal.persistOriginalSystemRoleStopped(context.Background(), transfer.Task, pending.Namespace, pending.PlanSHA256))
			transfer.GateSHA256 = journal.hashes
			require.NoError(t, normalSystemRoleServiceValidateTransferLocked(context.Background(), state, transfer), "baseline must reach actual valid WAL checks first")
			switch kind {
			case "missing-intent":
				require.NoError(t, os.Remove(filepath.Join(directory, normalSystemRoleServiceStageName(transfer.Origin.Nonce, "intent"))))
			case "missing-pending":
				require.NoError(t, os.Remove(filepath.Join(directory, normalSystemRoleServiceGateName(transfer.Origin.Nonce, normalSystemRoleBrokerGatePending))))
			case "changed-released-bytes":
				require.NoError(t, os.WriteFile(filepath.Join(directory, normalSystemRoleServiceGateName(transfer.Origin.Nonce, normalSystemRoleBrokerGateReleased)), []byte(`{"changed":true}`), 0600))
			case "changed-task":
				transfer.Task.StartTicks++
			case "changed-namespace-binding":
				transfer.Origin.Namespace = append([]normalSystemRoleNamespaceIdentity{}, transfer.Origin.Namespace...)
				for i := range transfer.Origin.Namespace {
					if transfer.Origin.Namespace[i].Name == "mnt" {
						transfer.Origin.Namespace[i].Inode++
						transfer.Origin.Filesystem.Namespace.Inode = transfer.Origin.Namespace[i].Inode
					}
				}
				require.NoError(t, transfer.validate(), "still syntactically valid; refusal must come from original persisted binding")
			}
			e = normalSystemRoleServiceValidateTransferLocked(context.Background(), state, transfer)
			if kind == "original" {
				require.NoError(t, e)
			} else {
				require.Error(t, e)
			}
		})
	}
}
func TestNormalBrokerFreshObservationDEVTraceIsDiagnosticOnly(t *testing.T) {
	background := beginNormalBrokerInspectionDEV(context.Background())
	require.Nil(t, background.trace)
	background.finish()
	trace := &normalBrokerInspectionDEVTrace{}
	ctx := context.WithValue(context.Background(), normalBrokerInspectionDEVTraceKey{}, trace)
	path := filepath.Join(t.TempDir(), "DEV read bytes")
	raw := make([]byte, 32<<10)
	require.NoError(t, os.WriteFile(path, raw, 0600))
	meter := beginNormalBrokerInspectionDEV(ctx)
	got, e := os.ReadFile(path)
	require.NoError(t, e)
	require.Len(t, got, len(raw))
	meter.finish()
	snapshot := trace.snapshot()
	require.Equal(t, uint64(1), snapshot.Calls)
	require.Greater(t, snapshot.DurationNS, uint64(0))
	require.Zero(t, snapshot.MissingIOSamples)
	require.GreaterOrEqual(t, snapshot.WholeProcessRcharDelta, uint64(len(raw)))
	// rchar包含采样与其它goroutine：不要求等于ELF字节，也不由本计数构造Source。
}
