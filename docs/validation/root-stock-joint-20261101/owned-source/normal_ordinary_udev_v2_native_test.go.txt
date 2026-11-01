//go:build linux && dsga_native_ordinary_udev_v2

package resourceguard

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// 无PG新Guest机制S：只触发本VM专用log测试设备，不构造/补签FS/Kernel/Memory Source。
func TestNativeOrdinaryUdevV2KernelUUIDAndPublicCodec(t *testing.T) {
	require.Equal(t, "isolated18c-uuid-codec-v1", os.Getenv("DSGA_ORDINARY_UDEV_NATIVE"))
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, e)
	release, e := kernelText("/proc/sys/kernel/osrelease")
	require.NoError(t, e)
	require.Equal(t, ref.KernelRelease, release)
	notes, e := physicalKernelNotes()
	require.NoError(t, e)
	require.Equal(t, ref.KernelNotesSHA256, physicalDigest(notes))
	require.NoError(t, normalKernelPrepV2R2Caller())
	paths, e := filepath.Glob("/sys/class/virtio-ports/*")
	require.NoError(t, e)
	var original *os.File
	var devpath string
	for _, p := range paths {
		canonical, e := filepath.EvalSymlinks(p)
		require.NoError(t, e)
		name, e := kernelText(filepath.Join(canonical, "name"))
		require.NoError(t, e)
		if name != "tech.neon.log.0" {
			continue
		}
		require.Nil(t, original, "dedicated test device must be unique")
		fd, e := unix.Open(filepath.Join(canonical, "uevent"), unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		require.NoError(t, e)
		original = os.NewFile(uintptr(fd), "own isolated Guest log-device trigger")
		devpath = canonical[len("/sys"):]
	}
	require.NotNil(t, original)
	defer original.Close()
	monitor, e := openNormalOrdinaryUdevV2KernelMonitor()
	require.NoError(t, e)
	var mu sync.Mutex
	var ledger normalOrdinaryUdevV2Ledger
	packets := make(chan normalOrdinaryUdevV2KernelPacket, 8)
	failures := make(chan error, 1)
	monitor.start(func(p normalOrdinaryUdevV2KernelPacket) error {
		mu.Lock()
		defer mu.Unlock()
		e := ledger.add(p.raw, p.properties, p.sequence, time.Now())
		if e == nil && p.properties["DEVPATH"] == devpath {
			select {
			case packets <- p:
			default:
				return fmt.Errorf("mechanism receipt queue exhausted")
			}
		}
		return e
	}, func(e error) {
		select {
		case failures <- e:
		default:
		}
	}, func() error { mu.Lock(); defer mu.Unlock(); return ledger.expire(time.Now()) })
	defer monitor.close()
	consumer, e := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.NETLINK_KOBJECT_UEVENT)
	require.NoError(t, e)
	defer unix.Close(consumer)
	require.NoError(t, unix.SetsockoptInt(consumer, unix.SOL_SOCKET, unix.SO_PASSCRED, 1))
	require.NoError(t, unix.Bind(consumer, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 2}))
	emitter, e := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.NETLINK_KOBJECT_UEVENT)
	require.NoError(t, e)
	defer unix.Close(emitter)
	require.NoError(t, unix.Bind(emitter, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}))
	address, e := unix.Getsockname(emitter)
	require.NoError(t, e)
	emitterPort := address.(*unix.SockaddrNetlink).Pid
	for round := 0; round < 2; round++ {
		uuid, e := normalOrdinaryUdevV2NewTriggerUUID()
		require.NoError(t, e)
		literal := []byte("add " + uuid + "\n")
		n, e := unix.Pwrite(int(original.Fd()), literal, 0)
		require.NoError(t, e)
		require.Equal(t, len(literal), n)
		var p normalOrdinaryUdevV2KernelPacket
		select {
		case p = <-packets:
		case e = <-failures:
			require.NoError(t, e)
		case <-time.After(3 * time.Second):
			t.Fatal("real kernel UUID event absent")
		}
		require.Equal(t, uuid, p.properties["SYNTH_UUID"])
		require.Equal(t, "add", p.properties["ACTION"])
		require.Equal(t, "virtio-ports", p.properties["SUBSYSTEM"])
		require.NoError(t, monitor.verify())
		public, e := normalOrdinaryUdevV2PublicPacket(p.properties)
		require.NoError(t, e)
		require.NoError(t, normalOrdinaryUdevV2Broadcast(emitter, public))
		ready := []unix.PollFd{{Fd: int32(consumer), Events: unix.POLLIN}}
		n, e = unix.Poll(ready, 3000)
		require.NoError(t, e)
		require.Equal(t, 1, n)
		raw := make([]byte, 16384)
		oob := make([]byte, 256)
		n, on, flags, from, e := unix.Recvmsg(consumer, raw, oob, unix.MSG_CMSG_CLOEXEC)
		require.NoError(t, e)
		require.Zero(t, flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC))
		require.Equal(t, emitterPort, from.(*unix.SockaddrNetlink).Pid)
		require.True(t, bytes.Equal(public, raw[:n]))
		messages, e := unix.ParseSocketControlMessage(oob[:on])
		require.NoError(t, e)
		require.Len(t, messages, 1)
		credential, e := unix.ParseUnixCredentials(&messages[0])
		require.NoError(t, e)
		require.Zero(t, credential.Uid)
		require.Zero(t, credential.Gid)
		require.EqualValues(t, os.Getpid(), credential.Pid)
		mu.Lock()
		ticket, e := ledger.take(p.sequence, time.Now())
		require.NoError(t, e)
		require.NoError(t, ledger.commit(ticket))
		mu.Unlock()
		t.Logf("REAL_18C_KERNEL_UUID_ROUND=%d seq=%d uuid=%s ROOT_PUBLIC_HEADER_BYTES=%d kernel_pid=0 root_uid=0 normal=false fullSource=false", round, p.sequence, uuid, len(public))
	}
	t.Log("MECHANISM_ONLY no actual typed FS/KernelR2/MemoryR2/lease/prep source constructed; static catalog/rules/kind2/kind3 production S remains pending")
}
