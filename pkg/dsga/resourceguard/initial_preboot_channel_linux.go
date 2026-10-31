//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"golang.org/x/sys/unix"
)

const initialGuestPortName = "tech.neon.dsga.boot.0"
const initialGuestChannelLifetime = 3 * time.Minute

var errInitialGuestPortPending = errors.New("fixed initial virtio port is not present yet")
var errInitialPrebootMovablePending = errors.New("initial MOVABLE online is still below the original target")

// 仅原sysfs命名的唯一字符设备，不接受路径/FD/verified参数；每次打开是独立原OFD。
type initialGuestStream struct {
	fd      int
	ctx     context.Context
	started bool
}

func (s *initialGuestStream) close() {
	if s != nil && s.ctx != nil && s.fd >= 0 {
		unix.Close(s.fd)
		s.fd = -1
	}
}
func initialGuestWait(ctx context.Context) error {
	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (s *initialGuestStream) poll(event int16) error {
	if s == nil || s.ctx == nil || s.fd < 0 {
		return fmt.Errorf("original Guest stream missing")
	}
	for {
		if e := s.ctx.Err(); e != nil {
			return e
		}
		ms := 100
		if end, ok := s.ctx.Deadline(); ok {
			left := time.Until(end)
			if left <= 0 {
				return context.DeadlineExceeded
			}
			if left < 100*time.Millisecond {
				ms = int((left + time.Millisecond - 1) / time.Millisecond)
			}
		}
		fds := []unix.PollFd{{Fd: int32(s.fd), Events: event}}
		n, e := unix.Poll(fds, ms)
		if e == unix.EINTR {
			continue
		}
		if e != nil {
			return e
		}
		if n == 0 {
			continue
		}
		if fds[0].Revents&unix.POLLNVAL != 0 {
			return unix.EBADF
		}
		if fds[0].Revents&unix.POLLERR != 0 {
			return unix.EIO
		}
		if event == unix.POLLOUT && fds[0].Revents&unix.POLLHUP != 0 {
			return unix.EPIPE
		}
		return nil
	}
}
func (s *initialGuestStream) Read(p []byte) (int, error) {
	if s == nil || s.ctx == nil || s.fd < 0 {
		return 0, fmt.Errorf("original Guest stream missing")
	}
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if e := s.ctx.Err(); e != nil {
			return 0, e
		}
		n, e := unix.Read(s.fd, p)
		if n > 0 {
			s.started = true
			return n, e
		}
		if e == unix.EINTR {
			continue
		}
		if e == unix.EAGAIN {
			if e = s.poll(unix.POLLIN); e != nil {
				return 0, e
			}
			continue
		}
		if e != nil {
			return 0, e
		}
		// 准确virtio_console.c在Host未连接时read0。仅第一字节前有界等待；
		// 已经开始的帧/会话EOF不能伪装可重放的新连接。
		if s.started {
			return 0, io.EOF
		}
		if e = initialGuestWait(s.ctx); e != nil {
			return 0, e
		}
	}
}
func (s *initialGuestStream) Write(p []byte) (int, error) {
	if s == nil || s.ctx == nil || s.fd < 0 {
		return 0, fmt.Errorf("original Guest stream missing")
	}
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if e := s.ctx.Err(); e != nil {
			return 0, e
		}
		n, e := unix.Write(s.fd, p)
		if n > 0 {
			return n, e
		}
		if e == unix.EINTR {
			continue
		}
		if e == unix.EAGAIN {
			if e = s.poll(unix.POLLOUT); e != nil {
				return 0, e
			}
			continue
		}
		if e != nil {
			return 0, e
		}
		return 0, io.ErrShortWrite
	}
}
func initialGuestDeviceName(name string) bool {
	if !strings.HasPrefix(name, "vport") {
		return false
	}
	a, b, ok := strings.Cut(strings.TrimPrefix(name, "vport"), "p")
	if !ok {
		return false
	}
	for _, v := range []string{a, b} {
		n, e := strconv.ParseUint(v, 10, 32)
		if e != nil || strconv.FormatUint(n, 10) != v {
			return false
		}
	}
	return true
}
func initialGuestDeviceNumber(text string) (uint64, error) {
	a, b, ok := strings.Cut(text, ":")
	if !ok {
		return 0, fmt.Errorf("invalid original virtio device number")
	}
	major, e := strconv.ParseUint(a, 10, 32)
	minor, f := strconv.ParseUint(b, 10, 32)
	if e != nil || f != nil || major == 0 || fmt.Sprintf("%d:%d", major, minor) != text {
		return 0, fmt.Errorf("noncanonical original virtio device number")
	}
	return unix.Mkdev(uint32(major), uint32(minor)), nil
}
func initialGuestReadSys(fd int, name string, limit int) (string, error) {
	child, e := unix.Openat(fd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return "", e
	}
	f := os.NewFile(uintptr(child), name)
	defer f.Close()
	var st unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(child, &st) != nil || unix.Fstatfs(child, &fs) != nil || fs.Type != unix.SYSFS_MAGIC || st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0022 != 0 {
		return "", fmt.Errorf("original virtio sysfs leaf differs")
	}
	raw, e := io.ReadAll(io.LimitReader(f, int64(limit+1)))
	if e != nil || len(raw) > limit {
		return "", fmt.Errorf("original virtio sysfs leaf exceeds bound: %w", e)
	}
	return strings.TrimSuffix(string(raw), "\n"), nil
}
func openInitialGuestPort(ctx context.Context) (*initialGuestStream, error) {
	fd, e := unix.Open("/sys/class/virtio-ports", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e == unix.ENOENT {
		return nil, errInitialGuestPortPending
	}
	if e != nil {
		return nil, e
	}
	root := os.NewFile(uintptr(fd), "fixed-virtio-class")
	defer root.Close()
	var fs unix.Statfs_t
	if e = unix.Fstatfs(fd, &fs); e != nil || fs.Type != unix.SYSFS_MAGIC {
		return nil, fmt.Errorf("fixed virtio class is not sysfs")
	}
	entries, e := root.ReadDir(65)
	if e != nil && e != io.EOF {
		return nil, e
	}
	if len(entries) > 64 {
		return nil, fmt.Errorf("virtio port inventory exceeds bound")
	}
	selected := -1
	selectedName := ""
	defer func() {
		if selected >= 0 {
			unix.Close(selected)
		}
	}()
	for _, entry := range entries {
		if !initialGuestDeviceName(entry.Name()) {
			continue
		}
		// class条目是内核建立的sysfs符号链接；仅此步允许跟随，目标仍须原sysfs。
		port, err := unix.Openat(fd, entry.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, err
		}
		name, err := initialGuestReadSys(port, "name", 128)
		if err != nil {
			unix.Close(port)
			return nil, err
		}
		if name != initialGuestPortName {
			unix.Close(port)
			continue
		}
		if selected >= 0 {
			unix.Close(port)
			return nil, fmt.Errorf("duplicate fixed initial virtio ports")
		}
		selected, selectedName = port, entry.Name()
	}
	if selected < 0 {
		return nil, errInitialGuestPortPending
	}
	number, e := initialGuestReadSys(selected, "dev", 32)
	if e != nil {
		return nil, e
	}
	rdev, e := initialGuestDeviceNumber(number)
	if e != nil {
		return nil, e
	}
	device, e := unix.Open("/dev/"+selectedName, unix.O_RDWR|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	var st unix.Stat_t
	if e = unix.Fstat(device, &st); e != nil || st.Mode&unix.S_IFMT != unix.S_IFCHR || st.Uid != 0 || st.Mode&0077 != 0 || st.Rdev != rdev {
		unix.Close(device)
		return nil, fmt.Errorf("fixed original virtio character device identity/mode differs")
	}
	again, err := initialGuestReadSys(selected, "dev", 32)
	name, ne := initialGuestReadSys(selected, "name", 128)
	if err != nil || ne != nil || again != number || name != initialGuestPortName {
		unix.Close(device)
		return nil, fmt.Errorf("original virtio sysfs identity changed during open")
	}
	return &initialGuestStream{fd: device, ctx: ctx}, nil
}

func initialGuestChannelInputs() (SourceConfig, []byte, map[string][]byte, error) {
	var zero SourceConfig
	s, raw, e := runtimeSource()
	if e != nil {
		return zero, nil, nil, e
	}
	if s.Config.StateDir != FixedStateDir || !s.Config.SystemCgroup {
		return zero, nil, nil, fmt.Errorf("fixed original System resource config required")
	}
	expected, e := RenderRuntimePolicyAssets(raw)
	if e != nil {
		return zero, nil, nil, e
	}
	actual := map[string][]byte{RuntimeResourceAssetName: raw}
	for name, want := range expected {
		physical, e := RuntimeISOAssetName(name)
		if e != nil {
			return zero, nil, nil, e
		}
		got, e := readKernelBootstrapISO("/neonvm/runtime/" + physical)
		if e != nil || !bytes.Equal(got, want) {
			return zero, nil, nil, fmt.Errorf("original fixed initial ISO asset differs: %s", name)
		}
		actual[name] = got
	}
	plan, e := DeriveRuntimeBootRequirements(raw, actual)
	if e != nil || !plan.Enabled() {
		return zero, nil, nil, fmt.Errorf("complete original normal-split source required")
	}
	line, e := kernelText("/proc/cmdline")
	if e != nil {
		return zero, nil, nil, e
	}
	if e = plan.ValidateKernelCommandLine(line); e != nil {
		return zero, nil, nil, e
	}
	if _, e = kernelBootstrapNamespaces(); e != nil {
		return zero, nil, nil, e
	}
	return s, raw, actual, nil
}
func initialGuestProvisionInputs(s SourceConfig, raw []byte) (*Guard, error) {
	for _, dir := range []struct {
		p    string
		mode uint32
	}{{"/etc/neon-dsga", 0755}, {"/var/lib/neon-dsga", 0755}, {FixedStateDir, 0700}} {
		if e := rootDirectory(dir.p, dir.mode); e != nil {
			return nil, e
		}
	}
	if e := sealFile(FixedSealedRuntimePath, raw); e != nil {
		return nil, e
	}
	config, e := json.Marshal(s.Config)
	if e != nil {
		return nil, e
	}
	if e = sealFile(FixedConfigPath, config); e != nil {
		return nil, e
	}
	if e = provisionCgroup(s.Limits); e != nil {
		return nil, e
	}
	return Open(s.Config)
}
func initialGuestExchange(ctx context.Context, port io.ReadWriter, g *Guard, c bootprotocol.InitialPrebootChallenge, recoverOriginal bool) error {
	var prepared bootprotocol.InitialPrebootReceipt
	var e error
	if recoverOriginal {
		prepared, e = g.RecoverInitialPreboot(ctx, c)
	} else {
		prepared, e = g.PrepareInitialPreboot(ctx, c)
	}
	if e != nil {
		return e
	}
	if e = bootprotocol.WriteInitialPrebootReceipt(port, prepared); e != nil {
		return e
	}
	next, e := bootprotocol.ReadInitialMovableChallenge(port)
	if e != nil {
		return e
	}
	if next.Preboot != prepared {
		return fmt.Errorf("Host movable phase changed original prepared receipt")
	}
	var observed InitialPrebootMovableObservation
	for {
		observed, e = g.InspectInitialPrebootMovable(ctx, prepared)
		if e == nil {
			break
		}
		if !errors.Is(e, errInitialPrebootMovablePending) {
			return e
		}
		if e = initialGuestWait(ctx); e != nil {
			return e
		}
	}
	receipt := bootprotocol.InitialMovableReceipt{Challenge: next, GuestBootID: observed.GuestBootID, RootInstanceNonce: observed.RootInstanceNonce, PreparedReceiptSHA256: observed.PreparedReceiptSHA256, PhysicalOriginSHA256: observed.PhysicalOriginSHA256, PreSystemSHA256: observed.PreSystemSHA256, CurrentPhysicalSHA256: observed.CurrentPhysicalSHA256, ExpectedIncrementBytes: observed.ExpectedIncrementBytes, MovablePresentPages: observed.MovablePresentPages}
	if e = receipt.MatchChallenge(next); e != nil {
		return e
	}
	if e = bootprotocol.WriteInitialMovableReceipt(port, receipt); e != nil {
		return e
	}
	done, e := bootprotocol.ReadInitialCompletion(port)
	if e != nil {
		return e
	}
	return done.MatchReceipt(receipt)
}
func runInitialBootGuest(ctx context.Context, recoverOriginal bool) error {
	if ctx == nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fmt.Errorf("fixed initial Guest channel requires live real Root")
	}
	call, cancel := context.WithTimeout(ctx, initialGuestChannelLifetime)
	defer cancel()
	source, raw, assets, e := initialGuestChannelInputs()
	if e != nil {
		return e
	}
	var port *initialGuestStream
	for {
		port, e = openInitialGuestPort(call)
		if e == nil {
			break
		}
		if !errors.Is(e, errInitialGuestPortPending) {
			return e
		}
		if e = initialGuestWait(call); e != nil {
			return e
		}
	}
	defer port.close()
	challenge, e := bootprotocol.ReadInitialPrebootChallenge(port)
	if e != nil {
		return e
	}
	// 原challenge/ISO/完整预算先核后才建立固定配置/cgroup或任何WAL。
	if _, e = initialPrebootBindInputs(challenge, raw, assets, source.Config); e != nil {
		return e
	}
	g, e := initialGuestProvisionInputs(source, raw)
	if e != nil {
		return e
	}
	if e = initialGuestExchange(call, port, g, challenge, recoverOriginal); e != nil {
		return e
	}
	if _, e = ProvisionGuest(call); e != nil {
		return e
	}
	if _, e = g.PrepareNormalSplitTopology(call); e != nil {
		return e
	}
	// 初次内存握手不是服务/PG就绪；仍须原全量生产检查，不能以ACK替代。
	return CheckGuest(call)
}
func RunInitialBootGuest(ctx context.Context) error     { return runInitialBootGuest(ctx, false) }
func RecoverInitialBootGuest(ctx context.Context) error { return runInitialBootGuest(ctx, true) }
