//go:build linux

package vmguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"golang.org/x/sys/unix"
)

const initialBootSocketName = "initial-boot.sock"
const initialBootPortName = "tech.neon.dsga.boot.0"
const initialBootChannelLifetime = 3 * time.Minute

// 单次原QEMU会话。端点不接受HTTP/env/path输入，不unlink旧socket，不重发语义帧。
// process由唯一RunQEMU在Start后/Wait前固定，channel关闭不消费Wait、不杀进程或退预算。
type initialBootChannel struct {
	mu       sync.Mutex
	conn     *net.UnixConn
	process  *initialBootProcess
	source   initialBootSource
	deadline time.Time
	cancel   context.CancelFunc
	stop     func() bool
	phase    uint8
	prepared bootprotocol.InitialPrebootReceipt
	observed bootprotocol.InitialMovableReceipt
	closed   bool
}

func openInitialBootChannel(ctx context.Context, dir string, p *initialBootProcess, source initialBootSource) (*initialBootChannel, error) {
	if ctx == nil || p == nil || source.validate() != nil || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || strings.ContainsAny(dir, ",\x00\r\n") {
		return nil, fmt.Errorf("fixed original boot channel inputs required")
	}
	fd, e := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	parent := os.NewFile(uintptr(fd), "original-boot-state-directory")
	defer parent.Close()
	if e = privateFile(fd, unix.S_IFDIR); e != nil {
		return nil, e
	}
	call, cancel := context.WithTimeout(ctx, initialBootChannelLifetime)
	deadline, _ := call.Deadline()
	path := filepath.Join(dir, initialBootSocketName)
	// 仅重试尚无语义发送的只读connect；任何peer不符立刻关闭，不收养现有别的实例。
	for {
		if e = p.alive(); e != nil {
			cancel()
			return nil, e
		}
		conn, err := (&net.Dialer{}).DialContext(call, "unix", path)
		if err == nil {
			u, ok := conn.(*net.UnixConn)
			if !ok {
				conn.Close()
				cancel()
				return nil, fmt.Errorf("fixed boot Unix connection required")
			}
			if e = p.checkPeer(&qmpConnection{conn: u}, source); e != nil {
				u.Close()
				cancel()
				return nil, e
			}
			c := &initialBootChannel{conn: u, process: p, source: source, deadline: deadline, cancel: cancel}
			c.stop = context.AfterFunc(call, func() { u.Close() })
			return c, nil
		}
		if !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ECONNREFUSED) {
			cancel()
			return nil, err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-call.Done():
			timer.Stop()
			cancel()
			return nil, call.Err()
		case <-timer.C:
		}
	}
}

func (c *initialBootChannel) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeLocked()
}
func (c *initialBootChannel) closeLocked() {
	if c.closed {
		return
	}
	c.closed = true
	if c.stop != nil {
		c.stop()
	}
	if c.cancel != nil {
		c.cancel()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
func (c *initialBootChannel) failLocked(err error) error {
	c.closeLocked()
	return fmt.Errorf("original boot channel unknown; no automatic resend: %w", err)
}
func (c *initialBootChannel) beginLocked(ctx context.Context, phase uint8) (func(), error) {
	if ctx == nil || c.closed || c.conn == nil || c.phase != phase {
		return nil, c.failLocked(fmt.Errorf("original boot channel phase differs or closed"))
	}
	if e := ctx.Err(); e != nil {
		return nil, c.failLocked(e)
	}
	if e := c.process.checkPeer(&qmpConnection{conn: c.conn}, c.source); e != nil {
		return nil, c.failLocked(e)
	}
	deadline := c.deadline
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if e := c.conn.SetDeadline(deadline); e != nil {
		return nil, c.failLocked(e)
	}
	stop := context.AfterFunc(ctx, func() { c.conn.Close() })
	return func() { stop() }, nil
}
func (c *initialBootChannel) challengeMatchesSource(v bootprotocol.InitialPrebootChallenge) bool {
	return v.Validate() == nil && v.SourceSHA256 == initialBootSHA(initialBootCanonical(c.source)) && v.Identity == bootprotocol.RunnerIdentity(c.source.Identity) && v.RuntimeSHA256 == c.source.RuntimeSHA256 && v.ResourceSHA256 == c.source.ResourceSHA256 && v.ConfigSecretUID == c.source.ConfigSecretUID && v.KernelBuildSHA256 == c.source.KernelBuildSHA256 && v.OriginalBase == c.source.OriginalBase
}
func (c *initialBootChannel) prepare(ctx context.Context, want bootprotocol.InitialPrebootChallenge) (bootprotocol.InitialPrebootReceipt, error) {
	var zero bootprotocol.InitialPrebootReceipt
	if c == nil {
		return zero, fmt.Errorf("original boot channel missing")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.challengeMatchesSource(want) {
		return zero, c.failLocked(fmt.Errorf("original Host challenge source differs"))
	}
	stop, e := c.beginLocked(ctx, 0)
	if e != nil {
		return zero, e
	}
	defer stop()
	if e = bootprotocol.WriteInitialPrebootChallenge(c.conn, want); e != nil {
		return zero, c.failLocked(e)
	}
	got, e := bootprotocol.ReadInitialPrebootReceipt(c.conn)
	if e != nil {
		return zero, c.failLocked(e)
	}
	if e = got.MatchChallenge(want); e != nil {
		return zero, c.failLocked(e)
	}
	if e = c.process.checkPeer(&qmpConnection{conn: c.conn}, c.source); e != nil {
		return zero, c.failLocked(e)
	}
	c.prepared = got
	c.phase = 1
	return got, nil
}
func (c *initialBootChannel) movable(ctx context.Context, want bootprotocol.InitialMovableChallenge) (bootprotocol.InitialMovableReceipt, error) {
	var zero bootprotocol.InitialMovableReceipt
	if c == nil {
		return zero, fmt.Errorf("original boot channel missing")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if want.Validate() != nil || want.Preboot != c.prepared {
		return zero, c.failLocked(fmt.Errorf("original prepared receipt differs"))
	}
	stop, e := c.beginLocked(ctx, 1)
	if e != nil {
		return zero, e
	}
	defer stop()
	if e = bootprotocol.WriteInitialMovableChallenge(c.conn, want); e != nil {
		return zero, c.failLocked(e)
	}
	got, e := bootprotocol.ReadInitialMovableReceipt(c.conn)
	if e != nil {
		return zero, c.failLocked(e)
	}
	if e = got.MatchChallenge(want); e != nil {
		return zero, c.failLocked(e)
	}
	if e = c.process.checkPeer(&qmpConnection{conn: c.conn}, c.source); e != nil {
		return zero, c.failLocked(e)
	}
	c.observed = got
	c.phase = 2
	return got, nil
}
func (c *initialBootChannel) complete(ctx context.Context, want bootprotocol.InitialCompletion) error {
	if c == nil {
		return fmt.Errorf("original boot channel missing")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := want.MatchReceipt(c.observed); e != nil {
		return c.failLocked(e)
	}
	stop, e := c.beginLocked(ctx, 2)
	if e != nil {
		return e
	}
	defer stop()
	if e = bootprotocol.WriteInitialCompletion(c.conn, want); e != nil {
		return c.failLocked(e)
	}
	if e = c.process.checkPeer(&qmpConnection{conn: c.conn}, c.source); e != nil {
		return c.failLocked(e)
	}
	c.phase = 3
	c.closeLocked()
	return nil
}
