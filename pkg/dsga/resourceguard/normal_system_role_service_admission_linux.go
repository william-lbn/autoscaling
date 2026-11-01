//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

const normalAuthorityProbePath = "/internal/normal-authority/ready"
const normalAuthorityProbeHeader = "X-Neon-Original-Authority"
const normalAuthorityProbeMagic = "NAUTHR01"
const normalAuthorityProbeACK = "NAUTHA01"

// 门仅由原R2对象创建；候选只probe，COMMIT才能打开，同一实例失败后不可再开。
// owner来自原4FD exec proof，不是按当前PID重开/self pidfd或请求bool。
type normalAuthorityAdmission struct {
	guard   *Guard
	owner   *normalSystemOriginalTask
	nonce   [32]byte
	enabled atomic.Bool
	failed  atomic.Bool
	done    chan struct{}
	once    sync.Once
}

func newNormalAuthorityAdmission(g *Guard, owner *normalSystemOriginalTask) (*normalAuthorityAdmission, error) {
	if g == nil || owner == nil {
		return nil, ErrUnknown
	}
	owner.mu.Lock()
	e := owner.verify()
	owner.mu.Unlock()
	if e != nil {
		return nil, e
	}
	n, e := normalSystemRoleServiceNonce()
	if e != nil {
		return nil, e
	}
	return &normalAuthorityAdmission{guard: g, owner: owner, nonce: n, done: make(chan struct{})}, nil
}
func (a *normalAuthorityAdmission) fail() {
	if a == nil {
		return
	}
	a.once.Do(func() { a.failed.Store(true); a.enabled.Store(false); close(a.done) })
}
func (a *normalAuthorityAdmission) activate() error {
	if a == nil || a.failed.Load() || a.enabled.Load() {
		return ErrUnknown
	}
	a.enabled.Store(true)
	if a.failed.Load() {
		a.enabled.Store(false)
		return ErrUnknown
	}
	return nil
}
func (a *normalAuthorityAdmission) businessAllowed() bool {
	return a != nil && a.enabled.Load() && !a.failed.Load()
}
func (a *normalAuthorityAdmission) matchOriginalPeer(cred *unix.Ucred, pidfd int) error {
	if a == nil || a.failed.Load() || cred == nil || cred.Uid != 0 || cred.Gid != 0 {
		return ErrUnknown
	}
	a.owner.mu.Lock()
	defer a.owner.mu.Unlock()
	if e := a.owner.verify(); e != nil {
		return e
	}
	return arrayMatchPIDFD(pidfd, a.owner.pidfd)
}
func (a *normalAuthorityAdmission) native(ctx context.Context, g *Guard, conn *net.UnixConn, kind uint32) (bool, error) {
	if a == nil {
		return false, nil
	}
	if a.guard != g || a.failed.Load() {
		return true, ErrUnknown
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return true, ErrUnknown
	}
	magic, e := arrayServicePeekMagic(conn, deadline)
	if e != nil {
		return true, e
	}
	if magic != normalAuthorityProbeMagic {
		if !a.businessAllowed() {
			return true, ErrUnknown
		}
		return false, nil
	}
	raw, e := arrayServicePacket(conn, deadline, 64, false)
	if e != nil {
		return true, e
	}
	if binary.BigEndian.Uint32(raw[8:12]) != 1 || binary.BigEndian.Uint32(raw[12:16]) != kind {
		return true, ErrUnknown
	}
	for _, b := range raw[48:] {
		if b != 0 {
			return true, ErrUnknown
		}
	}
	if string(raw[16:48]) != string(a.nonce[:]) {
		return true, ErrUnknown
	}
	cred, pidfd, e := arraySocketPeer(conn)
	if e != nil {
		return true, e
	}
	defer unix.Close(pidfd)
	if e = a.matchOriginalPeer(cred, pidfd); e != nil {
		return true, e
	}
	reply := append([]byte{}, raw...)
	copy(reply, normalAuthorityProbeACK)
	if e = conn.SetWriteDeadline(deadline); e != nil {
		return true, e
	}
	n, e := conn.Write(reply)
	if e != nil || n != len(reply) {
		return true, errors.Join(ErrUnknown, e)
	}
	return true, nil
}

type normalAuthorityHTTPPeerKey struct{}

// 每个HTTP连接在原Accept时钉住kernel peer；不按地址/字符串PID重开。
// 限流也是本wrapper持有，避免netutil外层包装丢失原UnixConn对象。
type normalAuthorityHTTPConnection struct {
	*net.UnixConn
	peer    *unix.Ucred
	pidfd   int
	release func()
	once    sync.Once
}

func (c *normalAuthorityHTTPConnection) Close() error {
	var e error
	c.once.Do(func() { e = c.UnixConn.Close(); unix.Close(c.pidfd); c.release() })
	return e
}

type normalAuthorityHTTPListener struct {
	*net.UnixListener
	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}

func (l *normalAuthorityHTTPListener) Close() error {
	var e error
	l.once.Do(func() { close(l.done); e = l.UnixListener.Close() })
	return e
}
func (l *normalAuthorityHTTPListener) Accept() (net.Conn, error) {
	for {
		select {
		case l.slots <- struct{}{}:
		case <-l.done:
			return nil, net.ErrClosed
		}
		conn, e := l.UnixListener.AcceptUnix()
		if e != nil {
			<-l.slots
			return nil, e
		}
		cred, pidfd, e := normalAuthorityHTTPPeer(conn)
		if e != nil {
			conn.Close()
			<-l.slots
			continue
		}
		return &normalAuthorityHTTPConnection{UnixConn: conn, peer: cred, pidfd: pidfd, release: func() { <-l.slots }}, nil
	}
}
func (a *normalAuthorityAdmission) httpHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == normalAuthorityProbePath {
			w.Header().Set("Cache-Control", "no-store")
			peer, ok := r.Context().Value(normalAuthorityHTTPPeerKey{}).(*normalAuthorityHTTPConnection)
			supplied := r.Header.Values(normalAuthorityProbeHeader)
			if !ok || a.failed.Load() || r.Method != http.MethodGet || r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" || r.ContentLength != 0 || len(supplied) != 1 || supplied[0] != hex.EncodeToString(a.nonce[:]) || a.matchOriginalPeer(peer.peer, peer.pidfd) != nil {
				http.Error(w, "authority_not_ready", http.StatusForbidden)
				return
			}
			// 真正调用原g的只读内核身份入口，不触WAL/Birth/配额私有状态。
			if _, e := a.guard.InspectIdentity(); e != nil {
				http.Error(w, "authority_not_ready", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if !a.businessAllowed() {
			w.Header().Set("Cache-Control", "no-store")
			http.Error(w, "authority_not_committed", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (a *normalAuthorityAdmission) probe(ctx context.Context, httpPath string, nativePaths []string) error {
	if a == nil || a.failed.Load() || a.enabled.Load() {
		return ErrUnknown
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return ErrUnknown
	}
	transport := &http.Transport{DialContext: func(c context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(c, "unix", httpPath)
	}, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	request, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://original-authority"+normalAuthorityProbePath, nil)
	if e != nil {
		return e
	}
	request.Header.Set(normalAuthorityProbeHeader, hex.EncodeToString(a.nonce[:]))
	reply, e := (&http.Client{Transport: transport}).Do(request)
	if e != nil {
		return e
	}
	io.Copy(io.Discard, io.LimitReader(reply.Body, 1024))
	reply.Body.Close()
	if reply.StatusCode != http.StatusNoContent {
		return fmt.Errorf("original resource HTTP probe refused")
	}
	if len(nativePaths) != 2 {
		return ErrUnknown
	}
	for i, path := range nativePaths {
		c, e := (&net.Dialer{}).DialContext(ctx, "unixpacket", path)
		if e != nil {
			return e
		}
		conn, ok := c.(*net.UnixConn)
		if !ok {
			c.Close()
			return ErrUnknown
		}
		raw := make([]byte, 64)
		copy(raw, normalAuthorityProbeMagic)
		binary.BigEndian.PutUint32(raw[8:12], 1)
		binary.BigEndian.PutUint32(raw[12:16], uint32(i+1))
		copy(raw[16:48], a.nonce[:])
		e = conn.SetWriteDeadline(deadline)
		if e == nil {
			n, writeErr := conn.Write(raw)
			e = writeErr
			if n != len(raw) {
				e = errors.Join(ErrUnknown, e)
			}
		}
		if e == nil {
			got, readErr := arrayServicePacket(conn, deadline, len(raw), false)
			e = readErr
			if e == nil {
				copy(raw, normalAuthorityProbeACK)
				if string(got) != string(raw) {
					e = ErrUnknown
				}
			}
		}
		conn.Close()
		if e != nil {
			return e
		}
	}
	return nil
}

// 成员类型只供原server构造；无外部路径或budget输入。
func normalAuthorityConnContext(ctx context.Context, conn net.Conn) context.Context {
	if peer, ok := conn.(*normalAuthorityHTTPConnection); ok {
		return context.WithValue(ctx, normalAuthorityHTTPPeerKey{}, peer)
	}
	return ctx
}

var _ net.Listener = (*normalAuthorityHTTPListener)(nil)

// HTTP是Unix STREAM，不能复用严格SEQPACKET的native peer函数。两条路径
// 分别固定socket类型，均由真实SO_PEERPIDFD钉对象，不接受PID/uid请求字段。
func normalAuthorityHTTPPeer(conn *net.UnixConn) (*unix.Ucred, int, error) {
	if conn == nil {
		return nil, -1, ErrUnknown
	}
	raw, e := conn.SyscallConn()
	if e != nil {
		return nil, -1, e
	}
	var peer *unix.Ucred
	fd := -1
	var inner error
	e = raw.Control(func(socket uintptr) {
		var kind int
		kind, inner = unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, unix.SO_TYPE)
		if inner != nil {
			return
		}
		if kind != unix.SOCK_STREAM {
			inner = fmt.Errorf("authority HTTP peer is not stream")
			return
		}
		peer, inner = unix.GetsockoptUcred(int(socket), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if inner == nil {
			fd, inner = unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, nativeTracePeerPIDFD)
		}
	})
	if e != nil || inner != nil {
		if fd >= 0 {
			unix.Close(fd)
		}
		return nil, -1, errors.Join(e, inner)
	}
	flags, e := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if e != nil || flags&unix.FD_CLOEXEC == 0 {
		unix.Close(fd)
		return nil, -1, errors.Join(ErrUnknown, e)
	}
	return peer, fd, nil
}
