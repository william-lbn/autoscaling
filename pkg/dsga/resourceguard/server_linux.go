//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/net/netutil"
	"golang.org/x/sys/unix"
)

// ServeGuest只监听root控制目录中的固定UDS；普通Compute不能指定配置、执行
// 程序或状态路径。它不复用旧25183端口，也不需要对root预算写开放TCP。
// 服务锁先于socket清理获取，存活服务/第二进程不会互相unlink。每次资源动作
// 仍独立获取原资源文件锁，HTTP断连绝不提前释放正在同步内核写入的锁。
func ServeGuest(ctx context.Context) error {
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return fmt.Errorf("resource service requires real root")
	}
	source, err := sealedRuntimeSource()
	if err != nil {
		return err
	}
	g, err := Open(source.Config)
	if err != nil {
		return err
	}
	if err := g.withRecord(ctx, func(r *record, _ *lockedState) error {
		if r.Bootstrap == nil {
			return fmt.Errorf("original resource bootstrap missing")
		}
		return sameBootstrapSource(source, r.Bootstrap.Original)
	}); err != nil {
		return err
	}
	handler, err := newResourceHandler(source, g)
	if err != nil {
		return err
	}
	// 此锁位于既有root私有持久目录，和资源日志不同文件，避免长持日志锁造成
	// 自身死锁。进程退出内核释放flock；崩溃留下socket可在唯一服务锁内安全清理。
	lockPath := filepath.Join(source.Config.StateDir, "server.lock")
	fd, err := unix.Open(lockPath, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 {
		return fmt.Errorf("invalid root service lock")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("resource service already active")
	}
	defer unix.Flock(fd, unix.LOCK_UN)
	if err := rootDirectory(filepath.Dir(FixedSocketPath), 0755); err != nil {
		return err
	}
	if err := unix.Lstat(FixedSocketPath, &st); err == nil {
		if st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFSOCK {
			return fmt.Errorf("foreign resource socket path")
		}
		if err := os.Remove(FixedSocketPath); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: FixedSocketPath, Net: "unix"})
	if err != nil {
		return err
	}
	defer listener.Close()
	// 可连接不代表有执行权：每请求必须通过原公钥验签。目录始终root独占写，
	// postgres无权替换socket冒充root；不依赖可轮转的postgres数值GID作授权来源。
	if err := os.Chmod(FixedSocketPath, 0666); err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 12 * 1024}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Close()
		case <-done:
		}
	}()
	// 限制固定控制面服务的并发连接数，避免未认证连接无限占用Guest外部预算。
	err = server.Serve(netutil.LimitListener(listener, 16))
	if errors.Is(err, http.ErrServerClosed) {
		return ctx.Err()
	}
	return err
}
