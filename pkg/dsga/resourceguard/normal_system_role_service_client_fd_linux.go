//go:build linux

package resourceguard

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const normalSystemRoleComputeClientLockPath = "/run/neon-dsga/vmstart.lock"

// 客户端只核可跨exec继承的FD；Go自己的epoll/pidfd等CLOEXEC不被误判为
// caller提供的能力。此事实不替代后继原stdio opaque，也不靠一次census签未来资格。
func normalSystemRoleServiceClientDescriptors(procfd int, role string) error {
	fd, e := unix.Openat(procfd, "fd", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	directory := os.NewFile(uintptr(fd), "fixed role client descriptors")
	defer directory.Close()
	names, e := directory.Readdirnames(4097)
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	if len(names) > 4096 {
		return fmt.Errorf("fixed role client descriptor inventory exceeds bound")
	}
	lockSeen := false
	for _, name := range names {
		n, e := strconv.ParseUint(name, 10, 31)
		if e != nil {
			return fmt.Errorf("fixed role client descriptor name differs")
		}
		if n <= 2 {
			continue
		}
		text, e := nativeTraceProcText(procfd, "fdinfo/"+name)
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return e
		}
		flags, e := normalSystemRoleServiceFDFlags(text)
		if e != nil {
			return e
		}
		if flags&unix.O_CLOEXEC != 0 {
			continue
		}
		if role != "compute" || n != 9 {
			return fmt.Errorf("fixed role client has an unexpected inheritable descriptor")
		}
		if flags&unix.O_PATH != 0 || flags&unix.O_ACCMODE == unix.O_RDONLY || flags&(unix.O_DIRECT|unix.O_ASYNC|unix.O_APPEND) != 0 {
			return fmt.Errorf("fixed compute lock descriptor access differs")
		}
		lockSeen = true
	}
	if role == "compute" && !lockSeen {
		return fmt.Errorf("fixed compute client lock descriptor missing")
	}
	return nil
}
func normalSystemRoleServiceFDFlags(text string) (uint64, error) {
	found := false
	var flags uint64
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "flags:") {
			continue
		}
		if found {
			return 0, fmt.Errorf("fixed descriptor flags duplicate")
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return 0, fmt.Errorf("fixed descriptor flags format differs")
		}
		value, e := strconv.ParseUint(fields[1], 8, 64)
		if e != nil {
			return 0, fmt.Errorf("fixed descriptor flags invalid")
		}
		flags = value
		found = true
	}
	if !found {
		return 0, fmt.Errorf("fixed descriptor flags missing")
	}
	return flags, nil
}
func normalSystemRoleServiceFlockWrite(text string, st unix.Stat_t) error {
	found := false
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "lock:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 9 || fields[2] != "FLOCK" || fields[3] != "ADVISORY" || fields[4] != "WRITE" || fields[7] != "0" || fields[8] != "EOF" {
			return fmt.Errorf("fixed compute lock is not whole-file FLOCK WRITE")
		}
		parts := strings.Split(fields[6], ":")
		if len(parts) != 3 {
			return fmt.Errorf("fixed compute lock object format differs")
		}
		major, e1 := strconv.ParseUint(parts[0], 16, 32)
		minor, e2 := strconv.ParseUint(parts[1], 16, 32)
		inode, e3 := strconv.ParseUint(parts[2], 10, 64)
		if e1 != nil || e2 != nil || e3 != nil || major != uint64(unix.Major(st.Dev)) || minor != uint64(unix.Minor(st.Dev)) || inode != st.Ino || found {
			return fmt.Errorf("fixed compute lock object differs")
		}
		found = true
	}
	if !found {
		return fmt.Errorf("fixed compute client does not hold original FLOCK WRITE")
	}
	return nil
}

// 调用者传来的fd必须是F_DUPFD_CLOEXEC或pidfd_getfd的原OFD duplicate，
// 不允许通过/proc/fd路径重开来假称same OFD。它不转移给任何目标role。
func normalSystemRoleServiceValidateComputeLock(fd int) (*os.File, error) {
	var st unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(fd, &st) != nil || unix.Fstatfs(fd, &fs) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != 0 || st.Gid != 0 || st.Mode&0777 != 0600 || st.Nlink != 1 || fs.Type != unix.TMPFS_MAGIC {
		return nil, fmt.Errorf("fixed compute client lock file differs")
	}
	target, e := unix.Open(normalSystemRoleComputeClientLockPath, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	defer unix.Close(target)
	var expected unix.Stat_t
	if unix.Fstat(target, &expected) != nil || st.Dev != expected.Dev || st.Ino != expected.Ino {
		return nil, fmt.Errorf("fixed compute client lock path changed")
	}
	run, e := unix.Open("/run", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	defer unix.Close(run)
	a, e := normalFilesystemFDObject(fd)
	if e != nil {
		return nil, e
	}
	b, e := normalFilesystemFDObject(run)
	if e != nil || a.Device != b.Device || a.MountID != b.MountID || a.Readonly {
		return nil, fmt.Errorf("fixed compute client lock is outside original run tmpfs")
	}
	raw, e := os.ReadFile("/proc/self/fdinfo/" + strconv.Itoa(fd))
	if e != nil || len(raw) > 4096 {
		return nil, fmt.Errorf("fixed compute client lock fdinfo unavailable")
	}
	if e = normalSystemRoleServiceFlockWrite(string(raw), st); e != nil {
		return nil, e
	}
	dup, e := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	return os.NewFile(uintptr(dup), "original compute client lock OFD pin"), nil
}
func normalSystemRoleServiceCaptureComputeLockPeer(task *normalSystemOriginalTask) (*os.File, error) {
	if task == nil {
		return nil, ErrUnknown
	}
	if e := task.verify(); e != nil {
		return nil, e
	}
	fd, e := unix.PidfdGetfd(task.pidfd, 9, 0)
	if e != nil {
		return nil, e
	}
	defer unix.Close(fd)
	return normalSystemRoleServiceValidateComputeLock(fd)
}
func normalSystemRoleServiceCaptureComputeLockSelf() (*os.File, error) {
	fd, e := unix.FcntlInt(9, unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	defer unix.Close(fd)
	return normalSystemRoleServiceValidateComputeLock(fd)
}
