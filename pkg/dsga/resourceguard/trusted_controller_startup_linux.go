//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// 仅固定CLI启动观察：真实Root peer/pidfd、原Consumer、执行文件和监听器均同源。
// 就绪允许Compute开始生产握手；不表示budget已恢复或normal已获许可。
func WaitTrustedControllerReady(ctx context.Context) error {
	if ctx == nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return ErrUnknown
	}
	source, e := sealedRuntimeSource()
	if e != nil || source.Config.ExecutionModel != TrustedControllerExecutionModel {
		return errors.Join(ErrUnknown, e)
	}
	call, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var last error
	for call.Err() == nil {
		if last = trustedControllerReadyOnce(call, source); last == nil {
			return nil
		}
		select {
		case <-call.Done():
			return errors.Join(ErrUnknown, call.Err(), last)
		case <-time.After(100 * time.Millisecond):
		}
	}
	return errors.Join(ErrUnknown, call.Err(), last)
}
func trustedControllerReadyOnce(ctx context.Context, sealed SourceConfig) error {
	source, pg, e := readNativeBirthOriginalSource()
	if e != nil {
		return e
	}
	pg.Close()
	if source.resource.Config != sealed.Config || source.resource.ControlPublicKey != sealed.ControlPublicKey {
		return ErrUnknown
	}
	metadata, e := readArrayBirthConsumer(source)
	if e != nil {
		return e
	}
	// 每个socket独立SO_PEERPIDFD，随后匹配同一原进程，不以路径存在作ready。
	original := -1
	defer func() {
		if original >= 0 {
			unix.Close(original)
		}
	}()
	for _, endpoint := range []struct{ network, path string }{{"unixpacket", fixedArrayBirthSocketPath}, {"unixpacket", fixedArrayPopulateSocketPath}, {"unixpacket", normalPgStderrSocket}, {"unix", FixedSocketPath}} {
		conn, e := (&net.Dialer{}).DialContext(ctx, endpoint.network, endpoint.path)
		if e != nil {
			return e
		}
		u, ok := conn.(*net.UnixConn)
		if !ok {
			conn.Close()
			return ErrUnknown
		}
		raw, e := u.SyscallConn()
		if e != nil {
			u.Close()
			return e
		}
		var cred *unix.Ucred
		fd := -1
		var inner error
		e = raw.Control(func(s uintptr) {
			cred, inner = unix.GetsockoptUcred(int(s), unix.SOL_SOCKET, unix.SO_PEERCRED)
			if inner == nil {
				fd, inner = unix.GetsockoptInt(int(s), unix.SOL_SOCKET, nativeTracePeerPIDFD)
			}
		})
		u.Close()
		if e != nil || inner != nil || fd < 0 || cred == nil || cred.Uid != 0 || int(cred.Pid) != metadata.Consumer.PID {
			if fd >= 0 {
				unix.Close(fd)
			}
			return errors.Join(ErrUnknown, e, inner)
		}
		unix.CloseOnExec(fd)
		if original < 0 {
			original = fd
		} else {
			e = arrayMatchPIDFD(original, fd)
			unix.Close(fd)
			if e != nil {
				return e
			}
		}
	}
	stat, e := bootProcText(metadata.Consumer.PID, "stat")
	if e != nil {
		return e
	}
	ticks, e := postmasterTicks(stat, metadata.Consumer.PID)
	if e != nil || ticks != uint64(metadata.Consumer.StartTicks) {
		return ErrUnknown
	}
	group, e := bootProcGroup(metadata.Consumer.PID)
	if e != nil || group != "/neon-system" {
		return ErrUnknown
	}
	installed, e := rootFile("/neonvm/bin/dsga-resource-guard", false)
	if e != nil {
		return e
	}
	defer installed.Close()
	actual, e := os.Open("/proc/" + strconv.Itoa(metadata.Consumer.PID) + "/exe")
	if e != nil {
		return e
	}
	defer actual.Close()
	var a, b unix.Stat_t
	if unix.Fstat(int(actual.Fd()), &a) != nil || unix.Fstat(int(installed.Fd()), &b) != nil || a.Dev != b.Dev || a.Ino != b.Ino {
		return fmt.Errorf("trusted Root executable changed")
	}
	return arrayRegistryPIDFDAlive(original)
}

// 镜像固定启动表与只读计划必须选择同一个部署合同；不借runtime字符串让旧
// 九角色镜像静默按另一张表运行。仅校部署接口，不做全树/全部ELF资格推导。
func trustedControllerImageContract() error {
	f, e := rootFile(normalSystemRoleBuildPlanPath, false)
	if e != nil {
		return e
	}
	raw, e := io.ReadAll(io.LimitReader(f, 64<<10+1))
	f.Close()
	if e != nil || len(raw) > 64<<10 {
		return ErrUnknown
	}
	var plan struct {
		ProtocolVersion uint32   `json:"protocol_version"`
		ExecutionModel  string   `json:"execution_model,omitempty"`
		ComputeUID      uint32   `json:"compute_uid"`
		ComputeGID      uint32   `json:"compute_gid"`
		ComputeArgv     []string `json:"compute_argv"`
		ComputeEnv      []string `json:"compute_env"`
		LoaderPaths     []string `json:"loader_paths"`
	}
	if e = control.UnmarshalObject(raw, &plan); e != nil || plan.ProtocolVersion != 1 || plan.ExecutionModel != TrustedControllerExecutionModel || plan.ComputeUID == 0 || plan.ComputeGID == 0 || len(plan.ComputeArgv) == 0 || plan.ComputeArgv[0] != trustedControllerComputePath {
		return ErrUnknown
	}
	f, e = rootFile("/etc/inittab", false)
	if e != nil {
		return e
	}
	raw, e = io.ReadAll(io.LimitReader(f, 64<<10+1))
	f.Close()
	if e != nil || !bytes.Equal(raw, []byte(strings.Join(TrustedControllerInitEntries(), "\n")+"\n")) {
		return errors.Join(ErrUnknown, fmt.Errorf("trusted-controller fixed init contract differs"), e)
	}
	return nil
}
