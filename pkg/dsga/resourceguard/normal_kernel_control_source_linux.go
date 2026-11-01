//go:build linux && amd64

package resourceguard

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// plan只从原RO System Source中列名的能力manifest构造；不是请求/JSON中的trusted。
type normalKernelOrdinaryControlPlan struct {
	source                 *normalSystemLifecycleSource
	binding                normalSystemLifecycleBinding
	sourceSHA, manifestSHA string
	rule                   normalKernelControlRoleRule
}
type normalKernelControlOpenRule struct {
	file     *os.File
	logical  string
	rights   uint64
	identity [3]uint64
}

func normalKernelControlPlatformSupported() bool { return runtime.GOARCH == "amd64" }

func newNormalKernelOrdinaryControlPlan(ctx context.Context, s *normalSystemLifecycleSource, role string) (*normalKernelOrdinaryControlPlan, error) {
	if ctx == nil || ctx.Err() != nil || s == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original ordinary control source missing"))
	}
	original, e := s.snapshot()
	if e != nil {
		return nil, e
	}
	sha, e := s.referenceSHA256()
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	canonical := s.logical[normalKernelControlManifestPath]
	f := s.files[canonical]
	if s.closed || canonical == "" || f == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original System manifest has no explicit kernel-control capability"))
	}
	raw, e := nativeBirthReadBounded(f, 64<<10)
	if e != nil {
		return nil, e
	}
	var expected string
	for _, entry := range s.manifest.Files {
		if entry.Path == normalKernelControlManifestPath {
			expected = entry.SHA256
		}
	}
	if expected == "" || physicalDigest(raw) != expected {
		return nil, fmt.Errorf("original kernel controls manifest bytes differ")
	}
	m, e := decodeNormalKernelControlManifest(raw)
	if e != nil {
		return nil, e
	}
	rule, e := m.role(role)
	if e != nil {
		return nil, e
	}
	return &normalKernelOrdinaryControlPlan{source: s, binding: original.Binding, sourceSHA: sha, manifestSHA: expected, rule: rule}, nil
}
func (p *normalKernelOrdinaryControlPlan) recheck(ctx context.Context) error {
	if p == nil || p.source == nil || ctx == nil || ctx.Err() != nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original ordinary control plan absent"))
	}
	now, e := newNormalKernelOrdinaryControlPlan(ctx, p.source, p.rule.Role)
	if e != nil {
		return e
	}
	a, _ := json.Marshal(p.rule)
	b, _ := json.Marshal(now.rule)
	if p.binding != now.binding || p.sourceSHA != now.sourceSHA || p.manifestSHA != now.manifestSHA || !bytes.Equal(a, b) {
		return errors.Join(ErrUnknown, fmt.Errorf("original ordinary control plan changed"))
	}
	return nil
}

// 只能由固定Gate的target exec前调用；失败必须终止该原Gate，不得继续旧exec分支。
// Root/resource、Compute、PID1与shutdown都没有此plan，不能被悄悄过滤。
func installNormalKernelOrdinaryRoleControls(ctx context.Context, p *normalKernelOrdinaryControlPlan) error {
	if e := p.recheck(ctx); e != nil {
		return e
	}
	return applyNormalKernelControlConstraints(p.rule)
}
func normalKernelControlExactBuild() error {
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if e != nil {
		return e
	}
	release, e := kernelText("/proc/sys/kernel/osrelease")
	if e != nil {
		return e
	}
	notes, e := physicalKernelNotes()
	if e != nil {
		return e
	}
	if release != ref.KernelRelease || physicalDigest(notes) != ref.KernelNotesSHA256 {
		return fmt.Errorf("ordinary controls require exact reviewed369 kernel")
	}
	abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if errno != 0 || abi != 6 {
		return fmt.Errorf("ordinary controls require actual Landlock ABI6")
	}
	return nil
}
func normalKernelControlOpenPaths(rule normalKernelControlRoleRule) ([]normalKernelControlOpenRule, error) {
	var out []normalKernelControlOpenRule
	fail := func(e error) ([]normalKernelControlOpenRule, error) {
		for _, v := range out {
			v.file.Close()
		}
		return nil, e
	}
	for _, v := range rule.Paths {
		canonical, e := filepath.EvalSymlinks(v.Path)
		if os.IsNotExist(e) && v.Optional {
			continue
		}
		if e != nil {
			return fail(e)
		}
		if v.Rights&(nkWrite|nkMakeReg|nkMakeChar|nkMakeBlock|nkMakeDir|nkRemoveFile|nkRemoveDir|nkTruncate) != 0 && canonical != v.Path && v.Kind != "virtio-log" {
			return fail(fmt.Errorf("writable ordinary path alias is unsupported"))
		}
		fd, e := unix.Open(canonical, unix.O_PATH|unix.O_CLOEXEC, 0)
		if e != nil {
			return fail(e)
		}
		f := os.NewFile(uintptr(fd), "fixed-ordinary-control-path")
		var st, after unix.Stat_t
		if e = unix.Fstat(fd, &st); e != nil {
			f.Close()
			return fail(e)
		}
		typ := st.Mode & unix.S_IFMT
		want := uint32(unix.S_IFDIR)
		switch v.Kind {
		case "file":
			want = unix.S_IFREG
		case "char", "virtio-log", "ptp":
			want = unix.S_IFCHR
		case "dir":
		default:
			f.Close()
			return fail(fmt.Errorf("ordinary rule kind differs"))
		}
		if typ != want || st.Uid != 0 || st.Ino == 0 {
			f.Close()
			return fail(fmt.Errorf("ordinary fixed path identity differs"))
		}
		if v.Kind == "char" {
			major, minor := unix.Major(uint64(st.Rdev)), unix.Minor(uint64(st.Rdev))
			valid := false
			switch v.Path {
			case "/dev/null":
				valid = major == 1 && minor == 3
			case "/dev/zero":
				valid = major == 1 && minor == 5
			case "/dev/random":
				valid = major == 1 && minor == 8
			case "/dev/urandom":
				valid = major == 1 && minor == 9
			case "/dev/console":
				valid = major == 5 && minor == 1
			}
			if !valid {
				f.Close()
				return fail(fmt.Errorf("ordinary permitted device identity differs"))
			}
		}
		if v.Kind == "virtio-log" {
			if e = normalKernelControlVerifyLogDevice(uint64(st.Rdev)); e != nil {
				f.Close()
				return fail(e)
			}
		}
		if v.Kind == "ptp" {
			d, e := kernelText("/sys/class/ptp/ptp0/dev")
			wanted := fmt.Sprintf("%d:%d", unix.Major(uint64(st.Rdev)), unix.Minor(uint64(st.Rdev)))
			if e != nil || d != wanted {
				f.Close()
				return fail(fmt.Errorf("fixed PTP device differs"))
			}
		}
		again, e := filepath.EvalSymlinks(v.Path)
		if e != nil || again != canonical || unix.Stat(canonical, &after) != nil || after.Dev != st.Dev || after.Ino != st.Ino || after.Rdev != st.Rdev {
			f.Close()
			return fail(fmt.Errorf("ordinary path changed across open"))
		}
		out = append(out, normalKernelControlOpenRule{file: f, logical: v.Path, rights: v.Rights, identity: [3]uint64{st.Dev, st.Ino, uint64(st.Rdev)}})
	}
	return out, nil
}
func normalKernelControlVerifyLogDevice(rdev uint64) error {
	paths, e := filepath.Glob("/sys/class/virtio-ports/*")
	if e != nil {
		return e
	}
	matches := 0
	for _, p := range paths {
		name, e := kernelText(filepath.Join(p, "name"))
		if e != nil {
			return e
		}
		if name != "tech.neon.log.0" {
			continue
		}
		d, e := kernelText(filepath.Join(p, "dev"))
		if e != nil {
			return e
		}
		a, b, ok := strings.Cut(d, ":")
		if !ok {
			return fmt.Errorf("fixed virtio log device malformed")
		}
		major, e := strconv.ParseUint(a, 10, 32)
		if e != nil {
			return e
		}
		minor, e := strconv.ParseUint(b, 10, 32)
		if e != nil {
			return e
		}
		if unix.Mkdev(uint32(major), uint32(minor)) != rdev {
			return fmt.Errorf("fixed virtio log device differs")
		}
		matches++
	}
	if matches != 1 {
		return fmt.Errorf("fixed original virtio log device absent or duplicate")
	}
	return nil
}
func normalKernelControlAll(trap, a1, a2, a3 uintptr) error {
	_, _, e := syscall.AllThreadsSyscall(trap, a1, a2, a3)
	if e != 0 {
		return e
	}
	return nil
}
func normalKernelControlAllPrctl(op, a1, a2 uintptr) error {
	_, _, e := syscall.AllThreadsSyscall6(unix.SYS_PRCTL, op, a1, a2, 0, 0, 0)
	if e != 0 {
		return e
	}
	return nil
}
func normalKernelControlCapPreflight(keep uint64) error {
	if os.Getuid() != 0 || os.Geteuid() != 0 || os.Getgid() != 0 || os.Getegid() != 0 {
		return fmt.Errorf("ordinary install requires fixed real root Gate")
	}
	// CGO禁止AllThreads；先用无副作用同TGID调用确认，不能部分写后才发现不支持。
	if e := normalKernelControlAll(unix.SYS_GETPID, 0, 0, 0); e != nil {
		return e
	}
	last, e := kernelText("/proc/sys/kernel/cap_last_cap")
	if e != nil || last != "40" {
		return fmt.Errorf("ordinary capability ABI differs")
	}
	var h = unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	var d [2]unix.CapUserData
	if e = unix.Capget(&h, &d[0]); e != nil {
		return e
	}
	effective := uint64(d[0].Effective) | (uint64(d[1].Effective) << 32)
	permitted := uint64(d[0].Permitted) | (uint64(d[1].Permitted) << 32)
	if effective&(1<<unix.CAP_SETPCAP) == 0 || effective&keep != keep || permitted&keep != keep {
		return fmt.Errorf("ordinary Gate lacks original capability prerequisites")
	}
	for cap := 0; cap <= 40; cap++ {
		if keep&(uint64(1)<<cap) == 0 {
			continue
		}
		v, e := unix.PrctlRetInt(unix.PR_CAPBSET_READ, uintptr(cap), 0, 0, 0)
		if e != nil || v != 1 {
			return fmt.Errorf("ordinary required bounding capability absent")
		}
	}
	return nil
}
func applyNormalKernelControlConstraints(rule normalKernelControlRoleRule) error {
	expected, e := normalKernelControlExpectedManifest().role(rule.Role)
	if e != nil {
		return e
	}
	a, _ := json.Marshal(rule)
	b, _ := json.Marshal(expected)
	if !bytes.Equal(a, b) {
		return fmt.Errorf("ordinary install rule differs from compiled fixed policy")
	}
	if e = normalKernelControlExactBuild(); e != nil {
		return e
	}
	if e = normalKernelControlCapPreflight(rule.KeepCapabilities); e != nil {
		return e
	}
	paths, e := normalKernelControlOpenPaths(rule)
	if e != nil {
		return e
	}
	defer func() {
		for _, v := range paths {
			v.file.Close()
		}
	}()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	// Landlock只约束调用线程，必须同Go1.24.3 AllThreads应用到所有原线程。
	// 先NNP，再Landlock成功，再不可逆降cap，最后装专用BPF作为固定代码顺序见证。
	if e = normalKernelControlAllPrctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	var attr uint64 = nkHandled
	fd, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, uintptr(unsafe.Pointer(&attr)), 8, 0)
	runtime.KeepAlive(&attr)
	if errno != 0 {
		return errors.Join(ErrUnknown, errno)
	}
	defer unix.Close(int(fd))
	for _, v := range paths {
		var wire [12]byte
		binary.LittleEndian.PutUint64(wire[:8], v.rights)
		binary.LittleEndian.PutUint32(wire[8:], uint32(v.file.Fd()))
		_, _, errno = unix.Syscall6(unix.SYS_LANDLOCK_ADD_RULE, fd, unix.LANDLOCK_RULE_PATH_BENEATH, uintptr(unsafe.Pointer(&wire[0])), 0, 0, 0)
		runtime.KeepAlive(&wire)
		if errno != 0 {
			return errors.Join(ErrUnknown, errno)
		}
	}
	if e = normalKernelControlAll(unix.SYS_LANDLOCK_RESTRICT_SELF, fd, 0, 0); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	// CAP_SETPCAP保留到全部bounding与securebits处理完；没有中途“尽力”成功分支。
	for cap := 0; cap <= 40; cap++ {
		if rule.KeepCapabilities&(uint64(1)<<cap) == 0 {
			if e = normalKernelControlAllPrctl(unix.PR_CAPBSET_DROP, uintptr(cap), 0); e != nil {
				return errors.Join(ErrUnknown, e)
			}
		}
	}
	if e = normalKernelControlAllPrctl(unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_CLEAR_ALL, 0); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if e = normalKernelControlAllPrctl(unix.PR_SET_SECUREBITS, 1|2|4|8, 0); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	h := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	var data [2]unix.CapUserData
	for i := 0; i < 2; i++ {
		v := uint32(rule.KeepCapabilities >> uint(i*32))
		data[i] = unix.CapUserData{Effective: v, Permitted: v, Inheritable: v}
	}
	_, _, ce := syscall.AllThreadsSyscall(unix.SYS_CAPSET, uintptr(unsafe.Pointer(&h)), uintptr(unsafe.Pointer(&data[0])), 0)
	runtime.KeepAlive(&h)
	runtime.KeepAlive(&data)
	if ce != 0 {
		return errors.Join(ErrUnknown, ce)
	}
	for cap := 0; cap <= 40; cap++ {
		if rule.KeepCapabilities&(uint64(1)<<cap) != 0 {
			if e = normalKernelControlAllPrctl(unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_RAISE, uintptr(cap)); e != nil {
				return errors.Join(ErrUnknown, e)
			}
		}
	}
	program := normalKernelControlFilter()
	descriptor := unix.SockFprog{Len: uint16(len(program)), Filter: &program[0]}
	result, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER, unix.SECCOMP_FILTER_FLAG_TSYNC, uintptr(unsafe.Pointer(&descriptor)))
	runtime.KeepAlive(program)
	runtime.KeepAlive(&descriptor)
	if errno != 0 || result != 0 {
		return errors.Join(ErrUnknown, fmt.Errorf("ordinary TSYNC failed: result=%d errno=%v", result, errno))
	}
	return nil
}
