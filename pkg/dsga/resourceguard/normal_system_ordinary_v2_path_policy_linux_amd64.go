//go:build linux && amd64

package resourceguard

import (
	"golang.org/x/sys/unix"
	"reflect"
)

// 只解释已固定的ordinary2角色/目录合同；此处不签发Source，也不替代原FS、
// layout和stock持续见证。调用者仍用原O_PATH FD装Landlock并钉住实际inode。
func normalSystemOrdinaryV2PrivatePathRights(rule NormalSystemOrdinaryV2Role, path string, st unix.Stat_t) (uint64, error) {
	expected, e := NormalSystemOrdinaryV2RolePlan(rule.Role)
	if e != nil || !reflect.DeepEqual(expected, rule) {
		return 0, ErrUnknown
	}
	member := false
	for _, p := range expected.PrivatePaths {
		if p == path {
			member = true
			break
		}
	}
	if !member {
		return 0, ErrUnknown
	}
	// stock的Root sticky父目录不能泛chown成60004。只给准确udev角色/固定路径，
	// 规则目录仍靠Root所有权、sticky DAC与原stock库存持续拒绝普通worker替换。
	if rule.Role == "udev" && path == "/run/udev" {
		if st.Uid != 0 || st.Gid != 60004 || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Mode&07777 != 01771 {
			return 0, ErrUnknown
		}
		return nkWriteTree, nil
	}
	for _, p := range normalOrdinaryV2Paths() {
		if p.path != path {
			continue
		}
		kind := uint32(unix.S_IFDIR)
		if p.file {
			kind = unix.S_IFREG
		}
		if p.uid != rule.UID || p.gid != rule.GID || st.Uid != p.uid || st.Gid != p.gid || st.Mode&unix.S_IFMT != kind || st.Mode&07777 != p.mode {
			return 0, ErrUnknown
		}
		if p.file {
			// DNS必须仍由PG/其它服务读取；只开放原regular inode的读写/截断，
			// 不给它的父目录替换/创建权限，硬链接也不作为原文件接纳。
			if st.Nlink != 1 {
				return 0, ErrUnknown
			}
			return nkRead | nkWrite | nkTruncate, nil
		}
		return nkWriteTree, nil
	}
	return 0, ErrUnknown
}

func normalSystemOrdinaryV2FixedWorkingDirectory() error {
	if e := unix.Chdir("/"); e != nil {
		return e
	}
	actual, e := unix.Getwd()
	if e != nil {
		return e
	}
	if actual != "/" {
		return ErrUnknown
	}
	return nil
}
