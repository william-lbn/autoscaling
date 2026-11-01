//go:build linux

package resourceguard

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestNormalPGMemoryControlStrictBPFInstructions(t *testing.T) {
	valid := []nativeFilterInstruction{{Code: 0x20, K: 0}, {Code: 0x15, K: 59, JF: 1}, {Code: 0x06, K: 0x50001}, {Code: 0x06, K: 0x7fff0000}}
	require.NoError(t, normalPGMemoryControlValidateProgram(valid))
	for name, change := range map[string]func([]nativeFilterInstruction){
		"unknown-opcode":                func(p []nativeFilterInstruction) { p[0].Code = 0xffff },
		"unaligned-absolute":            func(p []nativeFilterInstruction) { p[0].K = 1 },
		"absolute-outside-seccomp-data": func(p []nativeFilterInstruction) { p[0].K = 64 },
		"backward-overflow-jump":        func(p []nativeFilterInstruction) { p[0] = nativeFilterInstruction{Code: 5, K: ^uint32(0)} },
		"branch-outside":                func(p []nativeFilterInstruction) { p[1].JT = 255 },
		"unknown-return-action":         func(p []nativeFilterInstruction) { p[2].K = 0x7ffc0000 },
		"nonbranch-offset":              func(p []nativeFilterInstruction) { p[2].JF = 1 },
		"scratch-outside":               func(p []nativeFilterInstruction) { p[0] = nativeFilterInstruction{Code: 0x60, K: 16} },
	} {
		t.Run(name, func(t *testing.T) {
			p := append([]nativeFilterInstruction(nil), valid...)
			change(p)
			require.Error(t, normalPGMemoryControlValidateProgram(p))
		})
	}
}
func TestNormalPGMemoryControlDenialIsNotSampledArgument(t *testing.T) {
	noArgs := []nativeFilterInstruction{{Code: 0x20, K: 0}, {Code: 0x15, K: 59, JF: 1}, {Code: 0x06, K: 0x50001}, {Code: 0x06, K: 0x7fff0000}}
	for _, args := range [][6]uint64{{}, {^uint64(0), 1, 2, 3, 4, 5}} {
		action, read, err := normalPGMemoryControlEvaluate(noArgs, 59, args)
		require.NoError(t, err)
		require.Equal(t, uint32(0x50001), action)
		require.Zero(t, read)
	}
	// 相同args=0结果但读取arg0，不能被误称与任意参数无关。
	sampled := []nativeFilterInstruction{{Code: 0x20, K: 16}, {Code: 0x15, K: 0, JF: 1}, {Code: 0x06, K: 0x50001}, {Code: 0x06, K: 0x7fff0000}}
	action, reads, err := normalPGMemoryControlEvaluate(sampled, 59, [6]uint64{})
	require.NoError(t, err)
	require.Equal(t, uint32(0x50001), action)
	require.Equal(t, uint8(1), reads)
	action, _, err = normalPGMemoryControlEvaluate(sampled, 59, [6]uint64{1})
	require.NoError(t, err)
	require.Equal(t, uint32(0x7fff0000), action)
}
func TestNormalPGMemoryControlStdioRejectsAuthorityObjects(t *testing.T) {
	for name, path := range map[string]string{"proc": "/proc/self/status", "sysfs": "/sys/devices/system/cpu/online", "cgroup": "/sys/fs/cgroup/memory.max"} {
		t.Run(name, func(t *testing.T) {
			f, err := os.Open(path)
			require.NoError(t, err)
			defer f.Close()
			_, err = normalPGMemoryControlInspectStdio(int(f.Fd()))
			require.Error(t, err)
		})
	}
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	defer unix.Close(pair[0])
	defer unix.Close(pair[1])
	_, err = normalPGMemoryControlInspectStdio(pair[0])
	require.Error(t, err)
	_, err = normalPGMemoryControlInspectStdio(-1)
	require.Error(t, err)
	null, err := os.Open(os.DevNull)
	require.NoError(t, err)
	defer null.Close()
	got, err := normalPGMemoryControlInspectStdio(int(null.Fd()))
	require.NoError(t, err)
	require.Equal(t, "dev-null", got.Kind)
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer reader.Close()
	defer writer.Close()
	got, err = normalPGMemoryControlInspectStdio(int(writer.Fd()))
	require.NoError(t, err)
	require.Equal(t, "pipe-unbound-original", got.Kind)
}
func TestNormalPGMemoryControlDescriptorCLOEXECPrivateTable(t *testing.T) {
	if os.Getenv("DSGA_PG_FD_TEST_HELPER") == "1" {
		runtime.LockOSThread()
		if _, err := normalPGMemoryControlPrepareExecDescriptors(); err != nil {
			os.Exit(91)
		}
		for _, fd := range []int{3, 4} {
			flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
			if err != nil || flags&unix.FD_CLOEXEC == 0 {
				os.Exit(92)
			}
		}
		// 原Go管理表未被关闭；这里只做机制标记检查，真实exec/SUID由369用例另验。
		os.Exit(0)
	}
	a, err := os.Open(os.DevNull)
	require.NoError(t, err)
	defer a.Close()
	b, err := os.Open(os.DevNull)
	require.NoError(t, err)
	defer b.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNormalPGMemoryControlDescriptorCLOEXECPrivateTable$")
	cmd.Env = append(os.Environ(), "DSGA_PG_FD_TEST_HELPER=1")
	cmd.ExtraFiles = []*os.File{a, b}
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	_, err = a.Stat()
	require.NoError(t, err, "child seal must not mutate parent descriptors")
}
func TestNormalPGMemoryControlNoJSONOrReconstructedGuardSource(t *testing.T) {
	var g *Guard
	_, err := g.captureNormalPGMemoryControlSource(context.Background())
	require.ErrorIs(t, err, ErrUnknown)
	g = &Guard{}
	_, err = g.captureNormalPGMemoryControlSourceLocked(context.Background(), nil, nil)
	require.ErrorIs(t, err, ErrUnknown)
	source := &normalPGMemoryControlSource{}
	err = source.requireContinuousQualification()
	require.ErrorIs(t, err, ErrUnknown)
	require.True(t, strings.Contains(err.Error(), "typed stdio"))
}

// 原校验器会对IP=0给EPERM且reads=0；这不代表其它IP同样拒绝。
// 本profile完整原程序不需要IP，未知IP依赖必须先被拒绝，不能借采样签资格。
func TestNormalPGMemoryControlDenialCannotDependOnInstructionPointer(t *testing.T) {
	for _, offset := range []uint32{8, 12} {
		conditional := []nativeFilterInstruction{{Code: 0x20, K: offset}, {Code: 0x15, K: 0, JF: 1}, {Code: 0x06, K: 0x50001}, {Code: 0x06, K: 0x7fff0000}}
		require.Error(t, normalPGMemoryControlValidateProgram(conditional))
		_, _, err := normalPGMemoryControlEvaluate(conditional, 59, [6]uint64{})
		require.Error(t, err, "IP-dependent denial cannot inherit a constant-IP argument proof")
	}
}
