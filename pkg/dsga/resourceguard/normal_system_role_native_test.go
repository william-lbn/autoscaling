//go:build linux && dsga_native_system_role && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// DEV仅真实exec与kernel/task/WAL机制；生产固定broker/原Host Source和角色持续
// writer调用图不由此测试签发。所有程序沿上一fixture仍是同一个静态C站位ELF。
func TestExportNormalSystemRoleFixture(t *testing.T) { TestExportNormalSystemLifecycleFixture(t) }

type devNormalSystemRoleStoppedWriter struct {
	path          string
	fail          bool
	called        bool
	originalPIDFD *os.File
	atEntry       bool
}

func (w *devNormalSystemRoleStoppedWriter) persistOriginalSystemRoleStopped(ctx context.Context, o normalSystemRoleObservation, ns []normalSystemRoleNamespaceIdentity, plan string) error {
	w.called = true
	var regs unix.PtraceRegs
	if e := unix.PtraceGetRegs(o.PID, &regs); e != nil {
		return e
	}
	exe, e := os.Open(fmt.Sprintf("/proc/%d/exe", o.PID))
	if e != nil {
		return e
	}
	var h [32]byte
	_, e = exe.ReadAt(h[:], 0)
	exe.Close()
	if e != nil {
		return e
	}
	if string(h[:4]) != "\x7fELF" || h[4] != 2 || h[5] != 1 {
		return fmt.Errorf("DEV expected exact ELF64LE")
	}
	entry := binary.LittleEndian.Uint64(h[24:32])
	if regs.Rip != entry {
		return fmt.Errorf("DEV first user instruction already passed")
	}
	w.atEntry = true
	// DEV观察端只在本线程已确认原exec-stop时取副本并核原PIDFS身份；不向
	// numeric目标发任何信号，也不是生产broker的原spawn pidfd替代。
	fd, e := unix.PidfdOpen(o.PID, 0)
	if e != nil {
		return e
	}
	w.originalPIDFD = os.NewFile(uintptr(fd), "DEV exact stopped-original pidfd")
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Dev != o.PIDFDDevice || st.Ino != o.PIDFDInode {
		return fmt.Errorf("DEV original pidfd kernelobject differs")
	}
	raw, e := json.Marshal(struct {
		Task      normalSystemRoleObservation
		Namespace []normalSystemRoleNamespaceIdentity
		PlanSHA   string
	}{o, ns, plan})
	if e != nil {
		return e
	}
	f, e := os.OpenFile(w.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(raw)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	dir, e := os.Open(filepath.Dir(w.path))
	if e != nil {
		return e
	}
	e = dir.Sync()
	dir.Close()
	if e != nil {
		return e
	}
	if w.fail {
		return fmt.Errorf("DEV visible stage deliberately unconfirmed")
	}
	return ctx.Err()
}
func TestNativeNormalSystemRoleOriginalExecStopped(t *testing.T) {
	require.Equal(t, "DEV-369-SYSTEM-ROLE-NO-QUALIFICATION", os.Getenv("NEON_NATIVE_SYSTEM_ROLE"))
	source, e := inspectNormalSystemLifecycleOriginal(context.Background())
	require.NoError(t, e)
	defer source.close()
	ns, e := captureNormalSystemRoleNamespaces()
	require.NoError(t, e)
	defer ns.close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w := &devNormalSystemRoleStoppedWriter{path: "/var/lib/neon-dsga/state/DEV-role-stopped.json"}
	child, e := spawnNormalSystemRoleOriginal(ctx, source, ns, "resource", w)
	require.NoError(t, e)
	require.True(t, w.called)
	require.True(t, w.atEntry)
	require.True(t, child.observed.ExecStopObserved)
	require.True(t, child.observed.Released)
	require.Equal(t, uint32(0), child.observed.Task.UID)
	require.Equal(t, uint32(0), child.observed.Task.GID)
	require.False(t, child.observed.Task.ContinuousWritersClosed)
	require.Greater(t, child.observed.TracerTID, 0)
	require.NotEqual(t, os.Getpid(), child.observed.TracerTID)
	require.NoError(t, child.cancelAndWait())
	require.True(t, child.waited)
	require.Error(t, arrayRegistryPIDFDAlive(int(w.originalPIDFD.Fd())))
	w.originalPIDFD.Close()
	fmt.Println("NEON-S-SYSTEM-ROLE-001 PASS atomic System clone+pidfd; exact original namespaces/vectors/ELF and first-user-entry stop before durable release")
	fail := &devNormalSystemRoleStoppedWriter{path: "/var/lib/neon-dsga/state/DEV-role-unconfirmed.json", fail: true}
	_, e = spawnNormalSystemRoleOriginal(ctx, source, ns, "resource", fail)
	require.ErrorIs(t, e, ErrUnknown)
	require.True(t, fail.called)
	require.True(t, fail.atEntry)
	require.FileExists(t, fail.path)
	require.Error(t, arrayRegistryPIDFDAlive(int(fail.originalPIDFD.Fd())))
	fail.originalPIDFD.Close()
	fmt.Println("NEON-S-SYSTEM-ROLE-002 PASS visible-but-unconfirmed stage refuses release; original pidfd kill+actual exit wait")
	ns.mu.Lock()
	ns.identities[0].Inode++
	ns.mu.Unlock()
	blocked := &devNormalSystemRoleStoppedWriter{path: "/var/lib/neon-dsga/state/DEV-role-never.json"}
	_, e = spawnNormalSystemRoleOriginal(ctx, source, ns, "resource", blocked)
	require.Error(t, e)
	require.False(t, blocked.called)
	require.NoFileExists(t, blocked.path)
	fmt.Println("NEON-S-SYSTEM-ROLE-003 PASS wrong original namespace refuses before spawn; no role callback/WAL/user entry")
	fmt.Println("SYSTEM_ROLE_RESULT={\"scope\":\"DEV fixed static C stand-ins plus actual clone/pidfd/ptrace/namespace/WAL facts\",\"normal_permit\":false,\"production_broker_listener\":false,\"E\":0}")
}
