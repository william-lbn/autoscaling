//go:build linux || darwin

package resourceguard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

type lockedState struct{ dir, lock *os.File }

func privateRegular(fd int, mode uint32) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return err
	}
	if stat.Uid != uint32(os.Geteuid()) || uint32(stat.Mode)&unix.S_IFMT != mode || stat.Mode&0077 != 0 {
		return fmt.Errorf("resource journal must be private and owned by guard")
	}
	return nil
}

func lockState(ctx context.Context, path string) (*lockedState, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("resource state directory must be absolute and clean")
	}
	// 目录由可信安装者提前创建；不自动创建/清空目录，避免把持久盘未挂载误当首次
	// 启动。最终目录和文件均O_NOFOLLOW；目录FD固定rename/openat的作用域。
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	dir := os.NewFile(uintptr(fd), path)
	if err := privateRegular(fd, unix.S_IFDIR); err != nil {
		dir.Close()
		return nil, err
	}
	lockfd, err := unix.Openat(fd, "lock", unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		dir.Close()
		return nil, err
	}
	s := &lockedState{dir: dir, lock: os.NewFile(uintptr(lockfd), "resource-lock")}
	if err := privateRegular(lockfd, unix.S_IFREG); err != nil {
		s.close()
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			s.close()
			return nil, err
		}
		err = unix.Flock(lockfd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			s.close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			s.close()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *lockedState) close() {
	if s.lock != nil {
		s.lock.Close()
	}
	if s.dir != nil {
		s.dir.Close()
	}
}

func (s *lockedState) load() (*record, error) {
	fd, err := unix.Openat(int(s.dir.Fd()), "state.json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "resource-state")
	defer f.Close()
	if err := privateRegular(fd, unix.S_IFREG); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 64*1024 {
		return nil, fmt.Errorf("resource state exceeds bound")
	}
	var r record
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *lockedState) save(r *record) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(data) > 64*1024 {
		return fmt.Errorf("resource state exceeds bound")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	name := "state-" + hex.EncodeToString(nonce[:]) + ".tmp"
	dirfd := int(s.dir.Fd())
	fd, err := unix.Openat(dirfd, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), name)
	defer unix.Unlinkat(dirfd, name, 0)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = unix.Renameat(dirfd, name, dirfd, "state.json"); err != nil {
		return err
	}
	// rename原子性不等于掉电持久性；必须将父目录也fsync。fsync失败保留未知，
	// 不返回“未执行”，调用方也不能因此另造首次epoch或清空原命令。
	return s.dir.Sync()
}
