//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// 仅低层固定文件系统原语。调用者必须是原prepared/未COMMIT的同R2，先持久写入
// stock准备intent，并核原FS2/三actor；本函数不从FD、UID或目录存在签发Source。
// 新17mount图的/run是原tmpfs，旧官方AtomicWriter供给仍在同一固定路径发生。
// 不收养已有目录或证书，不删除失败前缀，不重放。任何部分失败由外层原WAL留Unknown。
func prepareNormalOrdinaryManagementTLSAtRun(ctx context.Context, run *os.File) error {
	if ctx == nil || ctx.Err() != nil || run == nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return ErrUnknown
	}
	original, e := normalManagementTLSOriginalRun(run)
	if e != nil {
		return e
	}
	var st unix.Stat_t
	if e = unix.Fstatat(int(run.Fd()), "neon", &st, unix.AT_SYMLINK_NOFOLLOW); e != unix.ENOENT {
		return errors.Join(ErrUnknown, fmt.Errorf("fixed management parent exists or is unreadable"), e)
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	if e = unix.Mkdirat(int(run.Fd()), "neon", 0700); e != nil {
		return e
	}
	// 部分mkdir失败也持久留痕，由同stock intent阻止重试。
	if e = unix.Fsync(int(run.Fd())); e != nil {
		return e
	}
	parent, e := normalManagementTLSOpenChild(run, "neon")
	if e != nil {
		return e
	}
	defer parent.Close()
	if e = unix.Fchmod(int(parent.Fd()), 0755); e != nil {
		return e
	}
	if e = unix.Fsync(int(parent.Fd())); e != nil {
		return e
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	if e = unix.Mkdirat(int(parent.Fd()), "management", 0700); e != nil {
		return e
	}
	if e = unix.Fsync(int(parent.Fd())); e != nil {
		return e
	}
	management, e := normalManagementTLSOpenChild(parent, "management")
	if e != nil {
		return e
	}
	defer management.Close()
	if e = unix.Fchmod(int(management.Fd()), 0755); e != nil {
		return e
	}
	if e = unix.Fsync(int(management.Fd())); e != nil {
		return e
	}
	if e = unix.Fsync(int(parent.Fd())); e != nil {
		return e
	}
	if e = unix.Fsync(int(run.Fd())); e != nil {
		return e
	}
	now, e := normalManagementTLSOriginalRun(run)
	if e != nil || now != original {
		return errors.Join(ErrUnknown, e)
	}
	for _, f := range []*os.File{parent, management} {
		if unix.Fstat(int(f.Fd()), &st) != nil || st.Uid != 0 || st.Gid != 0 || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Mode&07777 != 0755 {
			return ErrUnknown
		}
		o, e := normalFilesystemFDObject(int(f.Fd()))
		if e != nil || o.Readonly || o.MountID != original.MountID || o.Filesystem != unix.TMPFS_MAGIC {
			return ErrUnknown
		}
	}
	names, e := normalFilesystemReadDirAt(int(management.Fd()), ".", 1)
	if e != nil || len(names) != 0 {
		return errors.Join(ErrUnknown, e)
	}
	return ctx.Err()
}
func normalManagementTLSOriginalRun(run *os.File) (normalFilesystemObject, error) {
	var z normalFilesystemObject
	var st unix.Stat_t
	if run == nil || unix.Fstat(int(run.Fd()), &st) != nil || st.Uid != 0 || st.Gid != 0 || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Mode&07777 != 0755 {
		return z, ErrUnknown
	}
	flags, e := unix.FcntlInt(run.Fd(), unix.F_GETFD, 0)
	if e != nil || flags&unix.FD_CLOEXEC == 0 {
		return z, ErrUnknown
	}
	o, e := normalFilesystemFDObject(int(run.Fd()))
	if e != nil || o.Readonly || o.Filesystem != unix.TMPFS_MAGIC {
		return z, ErrUnknown
	}
	return o, nil
}
func normalManagementTLSOpenChild(parent *os.File, name string) (*os.File, error) {
	// 所有调用者名称均为本文件两个literal；不给请求提供自由relative path入口。
	if parent == nil || name != "neon" && name != "management" {
		return nil, ErrUnknown
	}
	fd, e := unix.Openat2(int(parent.Fd()), name, &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC, Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV})
	if e != nil {
		return nil, e
	}
	return os.NewFile(uintptr(fd), "original-fixed-management-directory"), nil
}
