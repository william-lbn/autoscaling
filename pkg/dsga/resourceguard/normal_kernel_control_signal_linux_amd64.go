//go:build linux && amd64

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

const normalKernelSignalScope = uint64(1 << 1) // Linux6.12 Landlock ABI6。

type normalKernelSignalRulesetAttr struct {
	HandledFS  uint64
	HandledNet uint64
	Scoped     uint64
}

// 前三个字段是ASM只读布局；其余typed字段保持全部C向量/底层字节存活。
// 传入ASM的是Go指针，不以uintptr隐藏liveness，也没有scope后的KeepAlive调用。
type normalKernelSignalExecImage struct {
	Path                *byte
	Argv                **byte
	Environ             **byte
	argumentPointers    []*byte
	environmentPointers []*byte
	argumentBytes       [][]byte
	environmentBytes    [][]byte
}

func newNormalKernelSignalExecImage(argv, env []string) (*normalKernelSignalExecImage, error) {
	if len(argv) == 0 || len(argv) > 64 || env == nil || len(env) > 128 || !filepath.IsAbs(argv[0]) || filepath.Clean(argv[0]) != argv[0] {
		return nil, fmt.Errorf("fixed signal exec vectors missing or outside bound")
	}
	out := &normalKernelSignalExecImage{argumentPointers: make([]*byte, len(argv)+1), environmentPointers: make([]*byte, len(env)+1)}
	total := 0
	for which, values := range [][]string{argv, env} {
		for i, value := range values {
			total += len(value) + 1
			if len(value) > 8192 || total > 64<<10 || strings.ContainsAny(value, "\x00\r\n") {
				return nil, fmt.Errorf("fixed signal exec vector malformed or outside bound")
			}
			data, e := unix.ByteSliceFromString(value)
			if e != nil {
				return nil, e
			}
			if which == 0 {
				out.argumentBytes = append(out.argumentBytes, data)
				out.argumentPointers[i] = &data[0]
			} else {
				out.environmentBytes = append(out.environmentBytes, data)
				out.environmentPointers[i] = &data[0]
			}
		}
	}
	out.Path = out.argumentPointers[0]
	out.Argv = &out.argumentPointers[0]
	out.Environ = &out.environmentPointers[0]
	return out, nil
}

func newNormalKernelSignalScopeRuleset() (*os.File, error) {
	if e := normalKernelControlExactBuild(); e != nil {
		return nil, e
	}
	attr := normalKernelSignalRulesetAttr{Scoped: normalKernelSignalScope}
	fd, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, uintptr(unsafe.Pointer(&attr)), unsafe.Sizeof(attr), 0)
	runtime.KeepAlive(&attr)
	if errno != 0 {
		return nil, errors.Join(ErrUnknown, errno)
	}
	f := os.NewFile(fd, "ordinary-final-signal-scope")
	flags, e := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
	if e != nil || flags&unix.FD_CLOEXEC == 0 {
		f.Close()
		return nil, errors.Join(ErrUnknown, fmt.Errorf("final signal scope must be original CLOEXEC ruleset"), e)
	}
	return f, nil
}

// 不把amd64-only plan类型泄露到通用Linux Gate；其它架构有明确Unknown stub。
// 唯一调用处应是已经核原pending/parent/namespace/role向量的System3普通九角色Gate。
func execNormalSystemOrdinaryRoleWithKernelControls(ctx context.Context, s *normalSystemLifecycleSource, role string) error {
	if ctx == nil || ctx.Err() != nil || s == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original ordinary signal source absent"))
	}
	p, e := newNormalKernelOrdinaryControlPlan(ctx, s, role)
	if e != nil {
		return e
	}
	return execNormalKernelOrdinaryRoleSignalScoped(ctx, p)
}

// 所有可分配/检查/AllThreads动作都在scoped前完成。返回error只可能在scoped前；
// 原leader最终ASM门成功exec，或raw exit_group，绝不回Go调度/Unlock/错误分支。
func execNormalKernelOrdinaryRoleSignalScoped(ctx context.Context, p *normalKernelOrdinaryControlPlan) error {
	if ctx == nil || ctx.Err() != nil || p == nil || p.source == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original ordinary signal plan absent"))
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread() // 仅scoped前错误路径；ASM没有RET。
	if !normalSystemRoleGateMainPinned || unix.Gettid() != os.Getpid() || os.Getuid() != 0 || os.Geteuid() != 0 || os.Getgid() != 0 || os.Getegid() != 0 || runtime.Version() != "go1.24.3" {
		return errors.Join(ErrUnknown, fmt.Errorf("ordinary signal gate requires exact original Go leader"))
	}
	p.source.mu.Lock()
	version := p.source.manifest.ProtocolVersion
	p.source.mu.Unlock()
	if version != 3 {
		return errors.Join(ErrUnknown, fmt.Errorf("ordinary signal gate needs explicit System3"))
	}
	if e := p.recheck(ctx); e != nil {
		return e
	}
	argv, e := normalSystemFixedRoleArgv(p.rule.Role)
	if e != nil {
		return e
	}
	env := []string{"PATH=/neonvm/bin"}
	ah, e := normalSystemVectorSHA(argv)
	if e != nil {
		return e
	}
	eh, e := normalSystemVectorSHA(env)
	if e != nil {
		return e
	}
	if ah != p.rule.ArgvSHA256 || eh != p.rule.EnvironmentSHA256 {
		return errors.Join(ErrUnknown, fmt.Errorf("ordinary final signal vectors differ from original plan"))
	}
	image, e := newNormalKernelSignalExecImage(argv, env)
	if e != nil {
		return e
	}
	scope, e := newNormalKernelSignalScopeRuleset()
	if e != nil {
		return e
	}
	defer scope.Close()
	fd := scope.Fd() // finalizer不能提前关原FD；defer保持其所有者存活直到不返回ASM。
	if e = ctx.Err(); e != nil {
		return e
	}
	if e = installNormalKernelOrdinaryRoleControls(ctx, p); e != nil {
		return e
	}
	// 旧installer的FS约束/NNP/cap/TSYNC已全部checked完成；从此不再Go跨M检查。
	normalKernelSignalScopeExecRaw(fd, image)
	panic("unreachable ordinary signal exec return")
}

//go:noescape
func normalKernelSignalScopeExecRaw(fd uintptr, image *normalKernelSignalExecImage)
