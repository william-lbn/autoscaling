//go:build !linux

package resourceguard

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"strings"
)

// 非Linux不能创建本profile；旧无quota历史路径保持兼容，发现跨平台/残留历史
// 则Unknown，不能因没有Linux执行器把pending当作已完成。仅扫描，不写盘。
func (g *Guard) rejectNormalSplitQuotaPendingLocked(s *lockedState) error {
	if s == nil || s.dir == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("quota original state missing"))
	}
	fd, e := unix.Openat(int(s.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), "unsupported-quota-history")
	defer f.Close()
	names, e := f.Readdirnames(4097)
	if e != nil && e != io.EOF {
		return e
	}
	if len(names) > 4096 {
		return errors.Join(ErrUnknown, fmt.Errorf("quota state inventory outside bound"))
	}
	for _, name := range names {
		if strings.HasPrefix(name, "normal-split-quota.") {
			return errors.Join(ErrUnknown, fmt.Errorf("quota history cannot be recovered by unsupported platform"))
		}
	}
	return nil
}
