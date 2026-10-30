//go:build linux && dsga_native_birth_wire

package resourceguard

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// 单根真实 Unix seqpacket 测试。子进程实际降为 nobody 后 connect，父进程
// 以真实 SO_PEERCRED 核原 PID/UID/GID；不是模拟 peer，也不连接生产 socket。
func TestNativeBirthWireKernelPeerAndMalformedFrames(t *testing.T) {
	if child := os.Getenv("PG17_NATIVE_BIRTH_WIRE_CHILD"); child != "" {
		nativeBirthWireChild(t, child)
		return
	}
	if os.Getenv("PG17_NATIVE_BIRTH_WIRE") != "isolated-seqpacket-component-v1" || os.Geteuid() != 0 {
		t.Fatal("isolated native component requires explicit opt-in and root for real child setuid")
	}
	dir, err := os.MkdirTemp("/tmp", "pg17-native-wire-")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	require.NoError(t, os.Chmod(dir, 0755))
	// Go 测试的原临时编译目录未必可由 nobody 遍历；只复制本测试 ELF 到
	// 本组件独立 root-owned 目录，不更改系统用户、生产目录或任意宿主权限。
	executable, err := os.Executable()
	require.NoError(t, err)
	src, err := os.Open(executable)
	require.NoError(t, err)
	dst, err := os.OpenFile(filepath.Join(dir, "wire.test"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
	require.NoError(t, err)
	_, copyErr := io.Copy(dst, src)
	require.NoError(t, src.Close())
	require.NoError(t, dst.Close())
	require.NoError(t, copyErr)
	for _, mode := range []string{"valid", "wrong-pid", "wrong-uid", "wrong-gid", "oversized", "short", "rights", "truncated-rights", "stream", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(dir, "birth.sock")
			kind := "unixpacket"
			if mode == "stream" {
				kind = "unix"
			}
			listener, err := net.ListenUnix(kind, &net.UnixAddr{Name: path, Net: kind})
			require.NoError(t, err)
			defer listener.Close()
			require.NoError(t, os.Chmod(path, 0666))
			require.NoError(t, listener.SetDeadline(time.Now().Add(10*time.Second)))
			cmd := exec.Command(filepath.Join(dir, "wire.test"), "-test.run=^TestNativeBirthWireKernelPeerAndMalformedFrames$", "-test.count=1", "-test.v")
			cmd.Env = []string{"PG17_NATIVE_BIRTH_WIRE_CHILD=" + mode, "PG17_NATIVE_BIRTH_WIRE_SOCKET=" + path}
			cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}
			var output strings.Builder
			cmd.Stdout = &output
			cmd.Stderr = &output
			require.NoError(t, cmd.Start())
			defer func() {
				if cmd.ProcessState == nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			conn, err := listener.AcceptUnix()
			require.NoError(t, err)
			defer conn.Close()
			peer := nativeBirthPeer{PID: int32(cmd.Process.Pid), UID: 65534, GID: 65534}
			if mode == "wrong-pid" {
				peer.PID++
			}
			if mode == "wrong-uid" {
				peer.UID++
			}
			if mode == "wrong-gid" {
				peer.GID++
			}
			before := nativeBirthWireFDCount(t)
			deadline := time.Now().Add(5 * time.Second)
			if mode == "deadline" {
				deadline = time.Now().Add(50 * time.Millisecond)
			}
			frame, receiveErr := readNativeBirthFrame(conn, peer, deadline)
			if mode == "valid" {
				require.NoError(t, receiveErr)
				e, expectedFrame := nativeBirthWireFixture()
				require.Equal(t, expectedFrame, frame)
				_, err = decodeNativeBirthRequest(frame, e)
				require.NoError(t, err)
			} else {
				require.Error(t, receiveErr)
				require.Nil(t, frame)
			}
			if mode == "rights" || mode == "truncated-rights" {
				// 子进程仍停在明确测试屏障中，stdio复制管道和pidfd尚未退出关闭；
				// 否则无关FD减少既可能误报，也可能抵消一个真实泄漏。
				require.Equal(t, before, nativeBirthWireFDCount(t), "拒绝后不得遗留传入的FD")
				_, err = conn.Write([]byte{0x7a})
				require.NoError(t, err)
			}
			require.NoError(t, conn.Close())
			require.NoError(t, cmd.Wait(), output.String())
		})
	}
	fmt.Println("PG17_NATIVE_BIRTH_WIRE: real unprivileged peer; exact seqpacket; wrong peer/length/ancillary/type/deadline rejected; received rights closed")
}

func nativeBirthWireFDCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

func nativeBirthWireChild(t *testing.T, mode string) {
	require.Equal(t, 65534, os.Getuid())
	require.Equal(t, 65534, os.Getgid())
	_, frame := nativeBirthWireFixture()
	kind := "unixpacket"
	if mode == "stream" {
		kind = "unix"
	}
	conn, err := net.DialUnix(kind, nil, &net.UnixAddr{Name: os.Getenv("PG17_NATIVE_BIRTH_WIRE_SOCKET"), Net: kind})
	require.NoError(t, err)
	defer conn.Close()
	if mode == "deadline" {
		time.Sleep(150 * time.Millisecond)
		return
	}
	if mode == "short" {
		frame = frame[:len(frame)-1]
	}
	if mode == "oversized" {
		frame = append(frame, make([]byte, 400)...)
	}
	if mode == "rights" || mode == "truncated-rights" {
		file, err := os.Open("/dev/null")
		require.NoError(t, err)
		defer file.Close()
		count := 1
		if mode == "truncated-rights" {
			count = 64
		}
		fds := make([]int, count)
		for i := range fds {
			fds[i] = int(file.Fd())
		}
		n, oobn, err := conn.WriteMsgUnix(frame, unix.UnixRights(fds...), nil)
		require.NoError(t, err)
		require.Equal(t, len(frame), n)
		require.Positive(t, oobn)
		// 这只是测试fixture的完成屏障，不是112B生产出生ACK。父进程核完FD
		// 集合后才准退出，消除os/exec后台stdio关闭与计数之间的真实竞态。
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		ack := make([]byte, 1)
		n, err = conn.Read(ack)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Equal(t, byte(0x7a), ack[0])
	} else {
		_, err = conn.Write(frame)
		// 错误peer/type会在接收端不消费即关闭；EPIPE/ECONNRESET在该负路径
		// 是允许的拒绝结果，不把真正 valid 发送失败归为成功。
		if mode != "wrong-pid" && mode != "wrong-uid" && mode != "wrong-gid" && mode != "stream" {
			require.NoError(t, err)
		}
	}
}
