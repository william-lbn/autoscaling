//go:build linux || darwin

package vmguard

import (
	"bytes"
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

const processFileLimit = 64 * 1024

func randomProcessNonce() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(nonce[:]), nil
}

// readProcessFile只在原目录flock内读取固定启动/关闭文件，不能把调用方路径变成
// 任意文件访问。ENOENT保留为“尚无事实”，符号链接/权限/超长或读失败均拒绝。
func (s *lockedState) readProcessFile(name string) ([]byte, error) {
	if name != "process-launch.json" && name != "process-exit.json" && name != "birth-intent.json" && name != "birth-abort.json" && name != "birth-close-state.json" && name != "birth-close-sources.json" {
		return nil, fmt.Errorf("unknown original process file")
	}
	fd, err := unix.Openat(int(s.dir.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	if err := privateFile(fd, unix.S_IFREG); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(f, processFileLimit+1))
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > processFileLimit {
		return nil, fmt.Errorf("original process file is empty or exceeds bound")
	}
	return raw, nil
}

// writeOnce先同步临时完整文件，再以linkat的“不替换”语义安装并同步目录。
// 同一原文可恢复丢回复，另一nonce/退出结果永不覆盖。锁序始终先目录flock，
// 此处不做网络调用；失败留下原事实/临时文件，不删除或重置出生WAL。
func (s *lockedState) writeOnce(name string, raw []byte) error {
	old, err := s.readProcessFile(name)
	if err != nil {
		return err
	}
	if old != nil {
		if !bytes.Equal(old, raw) {
			return fmt.Errorf("original process fact already exists with another body")
		}
		return s.dir.Sync()
	}
	if len(raw) == 0 || len(raw) > processFileLimit {
		return fmt.Errorf("original process fact exceeds bound")
	}
	nonce, err := randomProcessNonce()
	if err != nil {
		return err
	}
	tmp := "process-" + nonce + ".tmp"
	dirfd := int(s.dir.Fd())
	fd, err := unix.Openat(dirfd, tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), tmp)
	defer func() { _ = unix.Unlinkat(dirfd, tmp, 0) }()
	if n, err := f.Write(raw); err != nil || n != len(raw) {
		f.Close()
		return errOrShortWrite(err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := unix.Linkat(dirfd, tmp, dirfd, name, 0); err != nil {
		return err
	}
	return s.dir.Sync()
}

// 本VM独占锁覆盖floor/完整WAL持久化和一次有期限的私有QMP调用。锁在所有
// Runner进程间生效，不依赖goroutine或PG boot；不持有Agent/Controller全局锁。
// 目录由原Runner出生流程预建，不能在Load失败后自动Mkdir/重置成首次启动。
type lockedState struct{ dir, lock *os.File }

func privateFile(fd int, kind uint32) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return err
	}
	if st.Uid != uint32(os.Geteuid()) || uint32(st.Mode)&unix.S_IFMT != kind || st.Mode&0o077 != 0 {
		return fmt.Errorf("Runner journal requires private original owner and file type")
	}
	return nil
}

func lockState(ctx context.Context, path string) (*lockedState, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("Runner journal directory must be absolute and clean")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	s := &lockedState{dir: os.NewFile(uintptr(fd), path), lock: nil}
	if err := privateFile(fd, unix.S_IFDIR); err != nil {
		s.close()
		return nil, err
	}
	lockfd, err := unix.Openat(fd, "lock", unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		s.close()
		return nil, err
	}
	s.lock = os.NewFile(uintptr(lockfd), "runner-memory-lock")
	if err := privateFile(lockfd, unix.S_IFREG); err != nil {
		s.close()
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			s.close()
			return nil, err
		}
		err := unix.Flock(lockfd, unix.LOCK_EX|unix.LOCK_NB)
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
	fd, err := unix.Openat(int(s.dir.Fd()), "state.json", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "runner-memory-state")
	defer f.Close()
	if err := privateFile(fd, unix.S_IFREG); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 64*1024 {
		return nil, fmt.Errorf("Runner journal exceeds 64KiB")
	}
	var r record
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *lockedState) save(r *record) error {
	if err := r.validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(raw) > 64*1024 {
		return fmt.Errorf("Runner journal exceeds 64KiB")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	name := "state-" + hex.EncodeToString(nonce[:]) + ".tmp"
	dirfd := int(s.dir.Fd())
	fd, err := unix.Openat(dirfd, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), name)
	// rename成功后临时名已经不存在；此清理只尽力移除失败路径临时文件，
	// 不覆盖真实写入/fsync错误，也不删除正式状态。
	defer func() { _ = unix.Unlinkat(dirfd, name, 0) }()
	if n, err := f.Write(raw); err != nil || n != len(raw) {
		f.Close()
		if err != nil {
			return err
		}
		return io.ErrShortWrite
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := unix.Renameat(dirfd, name, dirfd, "state.json"); err != nil {
		return err
	}
	// rename后目录fsync失败属于未知，禁止后续发QMP；即使下一次读到完整文件
	// 也只能按保留的prepared/may_have_sent恢复，绝不回退floor或删原命令。
	return s.dir.Sync()
}

// 出生nonce在第一次Kube CREATE之前固化。存在此文件而state缺失意味着出生
// 已经开始但结论未知，Initialize不得删掉它再生成新身份。只有新VM/受控恢复
// 的专门流程能处理该状态，本正常启动路径不提供重置/覆盖入口。
func (s *lockedState) birthIntent(id Identity) error {
	raw, err := json.Marshal(id)
	if err != nil {
		return err
	}
	fd, err := unix.Openat(int(s.dir.Fd()), "birth-intent.json", unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("Runner birth is already started or directory unavailable: %w", err)
	}
	f := os.NewFile(uintptr(fd), "runner-birth-intent")
	if n, err := f.Write(raw); err != nil || n != len(raw) {
		f.Close()
		return errOrShortWrite(err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return s.dir.Sync()
}
