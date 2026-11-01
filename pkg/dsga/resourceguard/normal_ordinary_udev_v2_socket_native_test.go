//go:build linux && dsga_native_ordinary_socket

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"testing"
)

// 仅独立18c Guest新建netlink socket事实；不发事件，不触及既有服务或控制对象。
func TestNativeOrdinaryNetlinkSocketFacts(t *testing.T) {
	if os.Getenv("DSGA_ORDINARY_SOCKET_NATIVE") != "isolated18c-socket-v1" {
		t.Skip("dedicated isolated Guest only")
	}
	var u unix.Utsname
	require.NoError(t, unix.Uname(&u))
	release := string(unix.ByteSliceToString(u.Release[:]))
	require.Equal(t, "6.12.26-william-normal-split-metadata-v1-c67181104ec1", release)
	for _, groups := range []uint32{1, 3} {
		fd, e := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, unix.NETLINK_KOBJECT_UEVENT)
		require.NoError(t, e)
		require.NoError(t, unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: groups}))
		var st unix.Stat_t
		var fs unix.Statfs_t
		var sx unix.Statx_t
		require.NoError(t, unix.Fstat(fd, &st))
		require.NoError(t, unix.Fstatfs(fd, &fs))
		require.NoError(t, unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW, unix.STATX_BASIC_STATS|unix.STATX_MNT_ID, &sx))
		cookie, e := unix.GetsockoptUint64(fd, unix.SOL_SOCKET, unix.SO_COOKIE)
		require.NoError(t, e)
		require.NotZero(t, cookie)
		netcookie, e := unix.GetsockoptUint64(fd, unix.SOL_SOCKET, unix.SO_NETNS_COOKIE)
		require.NoError(t, e)
		require.NotZero(t, netcookie)
		addr, e := unix.Getsockname(fd)
		require.NoError(t, e)
		a, ok := addr.(*unix.SockaddrNetlink)
		require.True(t, ok)
		require.Equal(t, groups, a.Groups)
		require.NotZero(t, a.Pid)
		object, oldErr := normalFilesystemFDObject(fd)
		netns, e := os.Readlink("/proc/self/ns/net")
		require.NoError(t, e)
		result := map[string]any{"release": release, "groups": groups, "device": st.Dev, "inode": st.Ino, "mode": st.Mode, "filesystem": fs.Type, "statx_mask": sx.Mask, "mount_id": sx.Mnt_id, "SO_COOKIE": cookie, "SO_NETNS_COOKIE": netcookie, "port": a.Pid, "network_namespace": netns, "old_filesystem_object": object, "generic_error": fmt.Sprint(oldErr), "normal": false, "Source": false}
		raw, e := json.Marshal(result)
		require.NoError(t, e)
		fmt.Printf("NORMAL_UDEV_SOCKET_FACT_JSON %s\n", raw)
		require.NoError(t, unix.Close(fd))
	}
	stock, se := openNormalOrdinaryStockUdevMonitor()
	if se == nil {
		stock.close()
	}
	runtime, re := openNormalOrdinaryUdevV2KernelMonitor()
	if re == nil {
		runtime.close()
	}
	raw, e := json.Marshal(map[string]any{"stock_open_error": fmt.Sprint(se), "runtime_open_error": fmt.Sprint(re), "normal": false, "Source": false})
	require.NoError(t, e)
	fmt.Printf("NORMAL_UDEV_SOCKET_OPEN_JSON %s\n", raw)
}

// 仅新建独立tmpfs及普通规则文件。模型System/reader不是原生产Source；这里验证FD交接机制。
func TestNativeOrdinaryOriginalRuleFD(t *testing.T) {
	if os.Getenv("DSGA_ORDINARY_SOCKET_NATIVE") != "isolated18c-socket-v1" {
		t.Skip("dedicated isolated Guest only")
	}
	dir := t.TempDir()
	require.NoError(t, unix.Mount("tmpfs", dir, "tmpfs", unix.MS_NOSUID|unix.MS_NODEV, "size=1m"))
	t.Cleanup(func() { require.NoError(t, unix.Unmount(dir, 0)) })
	paths := []string{filepath.Join(dir, "50-own.rules"), filepath.Join(dir, "99-own.rules")}
	raws := [][]byte{[]byte("# exact first ordinary file\n"), []byte("# exact second ordinary file\n")}
	system := &normalSystemLifecycleSource{}
	for i, path := range paths {
		require.NoError(t, os.WriteFile(path, raws[i], 0444))
		system.manifest.Files = append(system.manifest.Files, NormalSystemSourceFile{Path: path, CanonicalPath: path, Kind: "config", Bytes: uint64(len(raws[i])), SHA256: physicalDigest(raws[i])})
	}
	require.NoError(t, unix.Mount("", dir, "", unix.MS_REMOUNT|unix.MS_RDONLY|unix.MS_NOSUID|unix.MS_NODEV, ""))
	makeReader := func() *normalOrdinaryRuleDEVReader {
		r := &normalOrdinaryRuleDEVReader{}
		for i, path := range paths {
			fd, e := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
			require.NoError(t, e)
			r.rules = append(r.rules, normalOrdinaryUdevV2RuleSource{path: path, sha: physicalDigest(raws[i]), raw: bytes.Clone(raws[i]), original: os.NewFile(uintptr(fd), "own isolated RO rule")})
		}
		return r
	}
	reader := makeReader()
	out, e := takeNormalOrdinaryUdevV2OriginalRules(context.Background(), system, reader)
	require.NoError(t, e)
	require.Len(t, out, 2)
	for i, rule := range out {
		object, e := normalFilesystemFDObject(int(rule.original.Fd()))
		require.NoError(t, e)
		require.True(t, object.Readonly)
		require.Equal(t, raws[i], rule.raw)
		reader.rules[i].raw[0] = 'X'
		require.Equal(t, byte('#'), rule.raw[0])
		require.NoError(t, rule.original.Close())
	}
	for _, mode := range []string{"duplicate", "wrong actual bytes", "missing"} {
		t.Run(mode, func(t *testing.T) {
			r := makeReader()
			fds := []int{int(r.rules[0].original.Fd()), int(r.rules[1].original.Fd())}
			switch mode {
			case "duplicate":
				r.rules[1].path = r.rules[0].path
				r.rules[1].sha = r.rules[0].sha
				r.rules[1].raw = bytes.Clone(r.rules[0].raw)
			case "wrong actual bytes":
				r.rules[0].raw[0] = 'X'
			case "missing":
				r.rules[1].original.Close()
				r.rules = r.rules[:1]
			}
			_, e := takeNormalOrdinaryUdevV2OriginalRules(context.Background(), system, r)
			require.ErrorIs(t, e, ErrUnknown)
			for _, fd := range fds {
				var st unix.Stat_t
				require.ErrorIs(t, unix.Fstat(fd, &st), unix.EBADF)
			}
		})
	}
	fmt.Println("NORMAL_UDEV_RULE_FD_POSITIVE two exact original readonly regular CLOEXEC pins transferred; deep copy; three negative cases fail closed; no Source")
}
