//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// 只钉住kmod31-r2实际输入，不产模块禁用/原actor/stock完成或normal许可。
type normalOrdinaryStockUdevKmodInputs struct {
	mu     sync.Mutex
	closed bool
	inputs *normalOrdinaryStockUdevInputs
	dirs   *normalOrdinaryStockUdevRuntimeDirs
	run    *os.File
	pins   map[string]*os.File
	ref    normalOrdinaryStockUdevKmodInputReference
}
type normalOrdinaryStockUdevKmodRoot struct {
	Path          string                 `json:"path"`
	State         string                 `json:"state"`
	AnchorPath    string                 `json:"anchor_path"`
	CanonicalPath string                 `json:"canonical_path"`
	Object        normalFilesystemObject `json:"object"`
}
type normalOrdinaryStockUdevKmodPseudoInput struct {
	Path    string                 `json:"path"`
	Present bool                   `json:"present"`
	Bytes   uint64                 `json:"bytes"`
	SHA256  string                 `json:"sha256"`
	Object  normalFilesystemObject `json:"object"`
	Mode    uint32                 `json:"mode"`
	UID     uint32                 `json:"uid"`
	GID     uint32                 `json:"gid"`
}
type normalOrdinaryStockUdevKmodInputReference struct {
	ProtocolVersion       uint32                                 `json:"protocol_version"`
	Scope                 string                                 `json:"scope"`
	Binding               normalSystemLifecycleBinding           `json:"original_system"`
	InputsReferenceSHA256 string                                 `json:"original_inputs_sha256"`
	Kernel                PhysicalKernelBuildReference           `json:"kernel"`
	EnvironmentSHA256     string                                 `json:"environment_sha256"`
	Roots                 []normalOrdinaryStockUdevKmodRoot      `json:"static_roots"`
	Run                   normalFilesystemObject                 `json:"original_run"`
	Cmdline               normalOrdinaryStockUdevKmodPseudoInput `json:"cmdline"`
	Compression           normalOrdinaryStockUdevKmodPseudoInput `json:"compression"`
	MissingSources        []string                               `json:"missing_sources"`
}

func validateNormalOrdinaryStockUdevKmodEnvironment(env []string) error {
	expected := []string{"PATH=/neonvm/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C", "LC_ALL=C"}
	if len(env) != len(expected) {
		return ErrUnknown
	}
	for n, v := range env {
		if v != expected[n] || strings.HasPrefix(v, "KMOD_LOG=") {
			return ErrUnknown
		}
	}
	return nil
}
func normalOrdinaryStockUdevKmodReleaseRoot(release string) (string, error) {
	if release == "" || len(release) > 128 {
		return "", ErrUnknown
	}
	for _, c := range []byte(release) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '+') {
			return "", ErrUnknown
		}
	}
	if release == "." || release == ".." {
		return "", ErrUnknown
	}
	return "/lib/modules/" + release, nil
}
func normalOrdinaryStockUdevKmodCurrentRelease(expected string) error {
	var uts unix.Utsname
	if e := unix.Uname(&uts); e != nil {
		return e
	}
	if unix.ByteSliceToString(uts.Release[:]) != expected {
		return fmt.Errorf("kmod original System kernel release differs from actual kernel")
	}
	return nil
}
func pinNormalOrdinaryStockUdevKmodInputs(ctx context.Context, inputs *normalOrdinaryStockUdevInputs, dirs *normalOrdinaryStockUdevRuntimeDirs) (*normalOrdinaryStockUdevKmodInputs, error) {
	fail := func(e error) (*normalOrdinaryStockUdevKmodInputs, error) { return nil, errors.Join(ErrUnknown, e) }
	if ctx == nil || ctx.Err() != nil || inputs == nil || dirs == nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fail(fmt.Errorf("kmod inputs require original root inputs and runtime directory objects"))
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if e := inputs.recheck(ctx); e != nil {
		return fail(e)
	}
	if e := dirs.recheck(ctx); e != nil {
		return fail(e)
	}
	inputRef, _, e := inputs.snapshot()
	if e != nil {
		return fail(e)
	}
	raw, e := json.Marshal(inputRef)
	if e != nil {
		return fail(e)
	}
	inputs.mu.Lock()
	system := inputs.system
	if inputs.closed || system == nil {
		inputs.mu.Unlock()
		return fail(ErrUnknown)
	}
	system.mu.Lock()
	kernel := system.manifest.Kernel
	system.mu.Unlock()
	inputs.mu.Unlock()
	expected, e := LookupPhysicalKernelBuild(kernel.ProfileID)
	if e != nil || expected != kernel {
		return fail(fmt.Errorf("kmod kernel reference is not original registered build"))
	}
	if e = normalOrdinaryStockUdevKmodCurrentRelease(kernel.KernelRelease); e != nil {
		return fail(e)
	}
	env := normalOrdinaryStockUdevEnvironment()
	if e = validateNormalOrdinaryStockUdevKmodEnvironment(env); e != nil {
		return fail(e)
	}
	envSHA, e := normalSystemVectorSHA(env)
	if e != nil {
		return fail(e)
	}
	k := &normalOrdinaryStockUdevKmodInputs{inputs: inputs, dirs: dirs, pins: map[string]*os.File{}, ref: normalOrdinaryStockUdevKmodInputReference{ProtocolVersion: 1, Scope: "original-stock-udev-kmod-empty-inputs-v1", Binding: inputRef.Binding, InputsReferenceSHA256: physicalDigest(raw), Kernel: kernel, EnvironmentSHA256: envSHA, MissingSources: []string{"original-KernelPrep2-modules-disabled-opaque", "original-sameGuard-R2-three-actor-and-all-writers-Wait", "original-stock-exec-completion-and-sealed-projection", "full-continuous-normal-qualification"}}}
	ok := false
	defer func() {
		if !ok {
			k.close()
		}
	}()
	dirs.mu.Lock()
	if dirs.closed || dirs.phase != 1 || dirs.run == nil {
		dirs.mu.Unlock()
		return fail(fmt.Errorf("kmod capture is not original pre-stock runtime directory phase"))
	}
	fd, e := unix.FcntlInt(dirs.run.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if e == nil {
		k.run = os.NewFile(uintptr(fd), "stock-original-run-kmod-pin")
		k.ref.Run = dirs.runObject
	}
	dirs.mu.Unlock()
	if e != nil {
		return fail(e)
	}
	moduleRoot, e := normalOrdinaryStockUdevKmodReleaseRoot(kernel.KernelRelease)
	if e != nil {
		return fail(e)
	}
	for _, path := range []string{"/etc/modprobe.d", "/usr/local/lib/modprobe.d", "/lib/modprobe.d", moduleRoot} {
		ref, f, e := normalOrdinaryStockUdevKmodPinEmptyRoot(ctx, path)
		if e != nil {
			return fail(e)
		}
		k.ref.Roots = append(k.ref.Roots, ref)
		k.pins[path] = f
	}
	if e = normalOrdinaryStockUdevKmodRunAbsent(k.run, k.ref.Run); e != nil {
		return fail(e)
	}
	k.ref.Cmdline, k.pins["/proc/cmdline"], e = normalOrdinaryStockUdevKmodPinPseudo("/proc/cmdline", 64<<10, unix.PROC_SUPER_MAGIC)
	if e != nil {
		return fail(e)
	}
	k.ref.Compression, k.pins["/sys/module/compression"], e = normalOrdinaryStockUdevKmodPinCompression(kernel)
	if e != nil {
		return fail(e)
	}
	if e = ctx.Err(); e != nil {
		return fail(e)
	}
	ok = true
	return k, nil
}

// 确定路径的每个已存在component都不容未知alias。仅原System默认库布局的/lib→/usr/lib
// 祖先映射可保留；最终config/module根本身为symlink仍拒，库会跟随它读资源。
func normalOrdinaryStockUdevKmodNoUnknownAlias(path string) error {
	for at := path; ; at = filepath.Dir(at) {
		st, e := os.Lstat(at)
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		if e == nil && st.Mode()&os.ModeSymlink != 0 {
			real, e := filepath.EvalSymlinks(at)
			if e != nil || at != "/lib" || real != "/usr/lib" || at == path {
				return fmt.Errorf("kmod lookup has unsupported alias")
			}
		}
		if at == "/" {
			return nil
		}
	}
}
func normalOrdinaryStockUdevKmodEmptyAt(parent *os.File, name string) (*os.File, string, error) {
	if parent == nil || name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00\r\n") {
		return nil, "", ErrUnknown
	}
	var st unix.Stat_t
	if e := unix.Fstatat(int(parent.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); e == unix.ENOENT {
		return nil, "absent", nil
	} else if e != nil {
		return nil, "", e
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return nil, "", fmt.Errorf("kmod resource root is not directory; non-directories are not ignored")
	}
	f, e := normalFilesystemOpenAt(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW)
	if e != nil {
		return nil, "", e
	}
	var current unix.Stat_t
	if unix.Fstat(int(f.Fd()), &current) != nil || current.Dev != st.Dev || current.Ino != st.Ino {
		f.Close()
		return nil, "", fmt.Errorf("kmod lookup changed while opening")
	}
	names, e := normalFilesystemReadDirAt(int(f.Fd()), ".", 1)
	if e != nil || len(names) != 0 {
		f.Close()
		return nil, "", fmt.Errorf("kmod profile rejects every resource/config entry")
	}
	return f, "empty", nil
}
func normalOrdinaryStockUdevKmodPinEmptyRoot(ctx context.Context, path string) (normalOrdinaryStockUdevKmodRoot, *os.File, error) {
	var zero normalOrdinaryStockUdevKmodRoot
	if e := ctx.Err(); e != nil {
		return zero, nil, e
	}
	if e := normalOrdinaryStockUdevKmodNoUnknownAlias(path); e != nil {
		return zero, nil, e
	}
	// 找到最近真实目录，缺失必须由原RO父证明，不把省略文件当事实。
	anchor := filepath.Dir(path)
	for {
		if st, e := os.Stat(anchor); e == nil {
			if !st.IsDir() {
				return zero, nil, fmt.Errorf("kmod lookup ancestor is not directory")
			}
			break
		} else if !os.IsNotExist(e) {
			return zero, nil, e
		}
		if anchor == "/" {
			return zero, nil, ErrUnknown
		}
		anchor = filepath.Dir(anchor)
	}
	f, canonical, obj, e := normalOrdinaryStockUdevOpenRuleDirectory(anchor)
	if e != nil {
		return zero, nil, e
	}
	if anchor != filepath.Dir(path) {
		if _, e = os.Lstat(path); !os.IsNotExist(e) {
			f.Close()
			return zero, nil, fmt.Errorf("kmod absent root changed during pin")
		}
		return normalOrdinaryStockUdevKmodRoot{Path: path, State: "readonly-absent", AnchorPath: anchor, CanonicalPath: canonical, Object: obj}, f, nil
	}
	child, state, e := normalOrdinaryStockUdevKmodEmptyAt(f, filepath.Base(path))
	if e != nil {
		f.Close()
		return zero, nil, e
	}
	if state == "absent" {
		return normalOrdinaryStockUdevKmodRoot{Path: path, State: "readonly-absent", AnchorPath: anchor, CanonicalPath: canonical, Object: obj}, f, nil
	}
	f.Close()
	canonical, e = nativeBirthCanonical(path)
	if e != nil {
		child.Close()
		return zero, nil, e
	}
	if e = nativeBirthReadonlyPath(filepath.Join(path, ".kmod-empty-check")); e != nil {
		child.Close()
		return zero, nil, e
	}
	obj, e = normalFilesystemFDObject(int(child.Fd()))
	if e != nil || !obj.Readonly {
		child.Close()
		return zero, nil, fmt.Errorf("kmod empty root is not readonly")
	}
	return normalOrdinaryStockUdevKmodRoot{Path: path, State: "readonly-empty", AnchorPath: path, CanonicalPath: canonical, Object: obj}, child, nil
}
func normalOrdinaryStockUdevKmodRunAbsent(run *os.File, want normalFilesystemObject) error {
	obj, e := normalOrdinaryStockUdevRuntimeDirFact(run, 0, 0, 0755)
	if e != nil || obj != want {
		return fmt.Errorf("kmod original run pin/permissions differ")
	}
	var st unix.Stat_t
	if e = unix.Fstatat(int(run.Fd()), "modprobe.d", &st, unix.AT_SYMLINK_NOFOLLOW); e != unix.ENOENT {
		return fmt.Errorf("kmod runtime configuration path is not absent")
	}
	return nil
}
func normalOrdinaryStockUdevKmodPinPseudo(path string, limit int64, fsType int64) (normalOrdinaryStockUdevKmodPseudoInput, *os.File, error) {
	var zero normalOrdinaryStockUdevKmodPseudoInput
	fd, e := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return zero, nil, e
	}
	f := os.NewFile(uintptr(fd), "stock-original-kmod-pseudo-input")
	fail := func(e error) (normalOrdinaryStockUdevKmodPseudoInput, *os.File, error) {
		f.Close()
		return zero, nil, e
	}
	before, e := normalOrdinaryStockUdevKmodPseudoFact(f, path, fsType)
	if e != nil {
		return fail(e)
	}
	raw, e := io.ReadAll(io.NewSectionReader(f, 0, limit+1))
	if e != nil || len(raw) == 0 || int64(len(raw)) > limit || strings.ContainsRune(string(raw), 0) {
		return fail(fmt.Errorf("kmod pseudo input content unavailable or exceeds bound"))
	}
	after, e := normalOrdinaryStockUdevKmodPseudoFact(f, path, fsType)
	if e != nil || before != after {
		return fail(fmt.Errorf("kmod pseudo input identity changed"))
	}
	before.Bytes = uint64(len(raw))
	before.SHA256 = physicalDigest(raw)
	return before, f, nil
}
func normalOrdinaryStockUdevKmodPseudoFact(f *os.File, path string, fsType int64) (normalOrdinaryStockUdevKmodPseudoInput, error) {
	var st unix.Stat_t
	if f == nil || unix.Fstat(int(f.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != 0 || st.Mode&0222 != 0 {
		return normalOrdinaryStockUdevKmodPseudoInput{}, fmt.Errorf("kmod pseudo input type/owner/mode differs")
	}
	o, e := normalFilesystemFDObject(int(f.Fd()))
	if e != nil || o.Filesystem != fsType {
		return normalOrdinaryStockUdevKmodPseudoInput{}, fmt.Errorf("kmod pseudo input filesystem differs")
	}
	return normalOrdinaryStockUdevKmodPseudoInput{Path: path, Present: true, Object: o, Mode: st.Mode, UID: st.Uid, GID: st.Gid}, nil
}

// 仅准确18c的原compiled config966e6151... MODULE_COMPRESS=n，从而MODULE_DECOMPRESS
// 不编decompress.o；属性只该TU的CONFIG_SYSFS late_initcall创建。没有显式NONE=y。
// 证明锁835c4198...；出现节点/其它errno与该原build相矛盾，均Unknown。
// sysfs保留原RW热增语义，不把静态ext4的RO要求套到伪文件系统；原17graph/namespace与
// 此父pin由RuntimeDirs复核，KernelPrep2模块禁用仍由Root factory另消费opaque。
func normalOrdinaryStockUdevKmodCompressionMayBeAbsent(kernel PhysicalKernelBuildReference) bool {
	expected, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	return e == nil && kernel == expected && kernel.KernelConfigSHA256 == "966e615127358d6d0dcf4f6ca884a07702e957ad07a95909cf95feca4aae5441"
}
func normalOrdinaryStockUdevKmodPinCompression(kernel PhysicalKernelBuildReference) (normalOrdinaryStockUdevKmodPseudoInput, *os.File, error) {
	var zero normalOrdinaryStockUdevKmodPseudoInput
	if !normalOrdinaryStockUdevKmodCompressionMayBeAbsent(kernel) {
		return zero, nil, fmt.Errorf("kmod compression input has no exact original build proof")
	}
	dir, e := normalFilesystemOpenAt(unix.AT_FDCWD, "/sys/module", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW)
	if e != nil {
		return zero, nil, e
	}
	object, e := normalFilesystemFDObject(int(dir.Fd()))
	if e != nil || object.Filesystem != unix.SYSFS_MAGIC {
		dir.Close()
		return zero, nil, fmt.Errorf("kmod absent compression original sysfs pin differs")
	}
	var st unix.Stat_t
	if e = unix.Fstatat(int(dir.Fd()), "compression", &st, unix.AT_SYMLINK_NOFOLLOW); e != unix.ENOENT {
		dir.Close()
		return zero, nil, fmt.Errorf("kmod compression input contradicts exact original build absence")
	}
	return normalOrdinaryStockUdevKmodPseudoInput{Path: "/sys/module/compression", Present: false, Object: object}, dir, nil
}
func (k *normalOrdinaryStockUdevKmodInputs) recheck(ctx context.Context) error {
	if k == nil || ctx == nil || ctx.Err() != nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return ErrUnknown
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed || k.inputs == nil || k.dirs == nil {
		return ErrUnknown
	}
	if e := k.inputs.recheck(ctx); e != nil {
		return e
	}
	if e := k.dirs.recheck(ctx); e != nil {
		return e
	}
	input, _, e := k.inputs.snapshot()
	if e != nil {
		return e
	}
	b, e := json.Marshal(input)
	if e != nil || physicalDigest(b) != k.ref.InputsReferenceSHA256 {
		return ErrUnknown
	}
	if e = normalOrdinaryStockUdevKmodCurrentRelease(k.ref.Kernel.KernelRelease); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	env := normalOrdinaryStockUdevEnvironment()
	if e = validateNormalOrdinaryStockUdevKmodEnvironment(env); e != nil {
		return e
	}
	digest, e := normalSystemVectorSHA(env)
	if e != nil || digest != k.ref.EnvironmentSHA256 {
		return ErrUnknown
	}
	for _, old := range k.ref.Roots {
		now, f, e := normalOrdinaryStockUdevKmodPinEmptyRoot(ctx, old.Path)
		if e != nil {
			return errors.Join(ErrUnknown, e)
		}
		original := k.pins[old.Path]
		if original == nil {
			f.Close()
			return ErrUnknown
		}
		o, e := normalFilesystemFDObject(int(original.Fd()))
		f.Close()
		if e != nil || now != old || o != old.Object {
			return errors.Join(ErrUnknown, fmt.Errorf("kmod original static lookup state/pin changed"))
		}
	}
	if e = normalOrdinaryStockUdevKmodRunAbsent(k.run, k.ref.Run); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	for _, old := range []normalOrdinaryStockUdevKmodPseudoInput{k.ref.Cmdline, k.ref.Compression} {
		var now normalOrdinaryStockUdevKmodPseudoInput
		var f *os.File
		if old.Path == "/proc/cmdline" {
			now, f, e = normalOrdinaryStockUdevKmodPinPseudo(old.Path, 64<<10, unix.PROC_SUPER_MAGIC)
		} else {
			now, f, e = normalOrdinaryStockUdevKmodPinCompression(k.ref.Kernel)
		}
		if e != nil {
			return errors.Join(ErrUnknown, e)
		}
		original := k.pins[old.Path]
		if original == nil {
			f.Close()
			return ErrUnknown
		}
		o, e := normalFilesystemFDObject(int(original.Fd()))
		f.Close()
		if e != nil || now != old || o != old.Object {
			return errors.Join(ErrUnknown, fmt.Errorf("kmod original pseudo input changed"))
		}
	}
	return ctx.Err()
}
func (k *normalOrdinaryStockUdevKmodInputs) snapshot() (normalOrdinaryStockUdevKmodInputReference, error) {
	var out normalOrdinaryStockUdevKmodInputReference
	if k == nil {
		return out, ErrUnknown
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed || k.inputs == nil || k.dirs == nil || k.run == nil {
		return out, ErrUnknown
	}
	b, e := json.Marshal(k.ref)
	if e != nil {
		return out, e
	}
	e = json.Unmarshal(b, &out)
	return out, e
}
func (k *normalOrdinaryStockUdevKmodInputs) close() {
	if k == nil {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.closed {
		for _, f := range k.pins {
			if f != nil {
				f.Close()
			}
		}
		if k.run != nil {
			k.run.Close()
		}
		k.closed = true
	}
}
