//go:build linux

package resourceguard

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"strings"
)

// 每个最终cap写者都在同一原resource flock内调用。仅观察，不fsync、不清
// pending、不重新构造原pidfd。已完成helper收费仍由实际A/parent计数继续承担。
func (g *Guard) rejectArrayPendingLocked(s *lockedState, r *record) error {
	if e := rejectOutstandingArrayPopulateLocked(s); e != nil {
		return e
	}
	value, exists := arrayGuardBindings.Load(g)
	if !exists {
		fd, e := unix.Openat(int(s.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if e != nil {
			return e
		}
		scan := os.NewFile(uintptr(fd), "original-array-pending-gate")
		names, e := scan.Readdirnames(-1)
		scan.Close()
		if e != nil {
			return e
		}
		for _, name := range names {
			if strings.HasPrefix(name, "array-") || strings.HasPrefix(name, "native-birth.") {
				return errors.Join(ErrUnknown, fmt.Errorf("array/native history lacks original live registry pin"))
			}
		}
		return nil
	}
	b := value.(*arrayGuardBinding)
	o := b.original
	o.mu.Lock()
	defer o.mu.Unlock()
	if e := o.verifyOriginal(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if r == nil || r.Birth == nil || *r.Birth != o.pending.Original || o.current == nil || o.current.retiring || o.failed {
		return errors.Join(ErrUnknown, fmt.Errorf("array original birth/generation is not active"))
	}
	for _, done := range o.current.startup {
		if !done {
			return errors.Join(ErrUnknown, fmt.Errorf("original N0 populate incomplete"))
		}
	}
	return b.rejectRPCUnknown(s)
}
