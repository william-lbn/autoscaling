//go:build linux

package resourceguard

import (
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
	"io"
	"math"
	"os"
	"strconv"
)

// 固定路径+原inode目录FD下打开memory.max；不改P/pids/swap，不接任何请求路径。
// 调用者必须持原resource flock。同步write期间不提前返回或放锁。
func (k *linuxKernel) normalSplitQuotaWrite(name string, expected normalSplitNode, target control.Uint64) error {
	if target == 0 || uint64(target) >= math.MaxInt64 || uint64(target)%(64<<10) != 0 {
		return fmt.Errorf("quota target outside finite aligned profile")
	}
	path := pgCgroup
	if name == "arrays" {
		path += "/arrays"
	} else if name != "normal" {
		return fmt.Errorf("quota cannot write private or arbitrary cgroup")
	}
	if e := secureParents(path); e != nil {
		return e
	}
	fd, e := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(fd, &st) != nil || unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.CGROUP2_SUPER_MAGIC || uint64(expected.Device) != st.Dev || uint64(expected.Inode) != st.Ino || st.Uid != 0 || st.Mode&0022 != 0 || !expected.RootControlled {
		return fmt.Errorf("quota original root cgroup inode differs")
	}
	out, e := unix.Openat(fd, "memory.max", unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(out), "original-normal-quota-memory.max")
	defer f.Close()
	if unix.Fstat(out, &st) != nil || unix.Fstatfs(out, &fs) != nil || fs.Type != unix.CGROUP2_SUPER_MAGIC || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != 0 || st.Mode&0022 != 0 {
		return fmt.Errorf("quota control not original root cgroup2")
	}
	raw := []byte(strconv.FormatUint(uint64(target), 10) + "\n")
	n, e := f.Write(raw)
	if e != nil {
		return e
	}
	if n != len(raw) {
		return io.ErrShortWrite
	}
	return f.Close()
}
