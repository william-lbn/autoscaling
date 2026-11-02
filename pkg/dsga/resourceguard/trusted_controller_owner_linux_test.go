//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestTrustedControllerExecutionModelStrictContract(t *testing.T) {
	for _, mode := range []string{"", TrustedControllerExecutionModel} {
		var c Config
		raw, _ := json.Marshal(map[string]string{"vm_uid": "vm-original", "compute_instance_id": "compute-original", "state_dir": FixedStateDir, "execution_model": mode})
		require.NoError(t, json.Unmarshal(raw, &c))
		require.Equal(t, mode, c.ExecutionModel)
	}
	for _, mode := range []string{"trusted-controller", "trusted-controller-v2", " trusted-controller-v1", "TRUSTED-CONTROLLER-V1"} {
		var c Config
		raw, _ := json.Marshal(map[string]string{"execution_model": mode})
		require.Error(t, json.Unmarshal(raw, &c))
	}
	_, _, command := fixtureGuard(t)
	source := sourceFixture(command)
	source.Config.ExecutionModel = TrustedControllerExecutionModel
	require.Error(t, source.Validate(), "mode cannot omit actual System domain")
	source.Config.SystemCgroup = true
	source.Limits.SystemMemoryMaxBytes = 64 << 20
	source.Limits.OutsideUpperBytes = 64 << 20
	require.NoError(t, source.Validate())
}
func TestTrustedControllerRawModeCannotConstructOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, c := range []context.Context{nil, ctx, context.Background()} {
		owner, e := newTrustedControllerOwner(c, &Guard{config: Config{ExecutionModel: TrustedControllerExecutionModel, SystemCgroup: true}}, SourceConfig{}, nil)
		require.Error(t, e)
		require.Nil(t, owner)
	}
	var owner *trustedControllerOwner
	require.Error(t, owner.verifyLocked(context.Background(), nil, nil))
	var pin *trustedControllerComputePin
	require.Error(t, pin.verify(context.Background(), nil, nil))
	require.Error(t, pin.verifyLaunchParent(nil))
}

// 仅核原procfd与PPid/原pidfd的实际关系，不把本测试进程当Compute或Source。
func TestTrustedControllerLaunchParentUsesOriginalProcAndTask(t *testing.T) {
	cmd := exec.Command("/bin/sleep", "30")
	require.NoError(t, cmd.Start())
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	pid := cmd.Process.Pid
	pf, e := unix.PidfdOpen(os.Getpid(), 0)
	require.NoError(t, e)
	ownProc, e := unix.Open("/proc/self", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	require.NoError(t, e)
	stat, e := nativeTraceProcText(ownProc, "stat")
	require.NoError(t, e)
	ticks, e := postmasterTicks(stat, os.Getpid())
	require.NoError(t, e)
	boot, e := kernelText("/proc/sys/kernel/random/boot_id")
	require.NoError(t, e)
	pfile := os.NewFile(uintptr(pf), "DEV-original-parent-pidfd")
	procfile := os.NewFile(uintptr(ownProc), "DEV-original-parent-proc")
	task, e := newNormalSystemOriginalTask(normalSystemOriginalTaskSource{PID: os.Getpid(), StartTicks: ticks, UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), GuestBootID: boot, PIDFD: pfile, ProcDirectory: procfile})
	require.NoError(t, e)
	defer task.close()
	// Wrap input descriptors only while caller owns them; prevent os.File finalizers
	// from outliving the corresponding raw descriptor by using explicit Close below.
	defer pfile.Close()
	defer procfile.Close()
	childProc, e := unix.Open("/proc/"+strconv.Itoa(pid), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	require.NoError(t, e)
	defer unix.Close(childProc)
	pin := &trustedControllerComputePin{task: task}
	require.NoError(t, pin.verifyLaunchParent(&arrayLaunchPin{procfd: childProc}))
	require.Error(t, pin.verifyLaunchParent(&arrayLaunchPin{procfd: ownProc}), "own PPid is not self")
	task.close()
	require.Error(t, pin.verifyLaunchParent(&arrayLaunchPin{procfd: childProc}))
}
