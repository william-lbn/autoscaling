//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

type normalSystemLifecycleSource struct {
	mu       sync.Mutex
	closed   bool
	binding  normalSystemLifecycleBinding
	manifest NormalSystemSourceManifest
	observed normalSystemLifecycleObservation
	files    map[string]*os.File
	metadata map[string]nativeBirthELFMetadata
	logical  map[string]string
}

func (s *normalSystemLifecycleSource) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		for _, f := range s.files {
			f.Close()
		}
		s.closed = true
	}
}

// 入口独立读原ISO、封存runtime和policy绑定的构建字节，不反调Native Birth
// 总入口，避免其未来消费本producer时递归。raw只用于原字节核对，不自称信任。
func inspectNormalSystemLifecycleOriginal(ctx context.Context) (*normalSystemLifecycleSource, error) {
	fail := func(e error) (*normalSystemLifecycleSource, error) { return nil, errors.Join(ErrUnknown, e) }
	if ctx == nil || ctx.Err() != nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fail(fmt.Errorf("original System lifecycle requires bounded root context"))
	}
	runtimeRaw, e := nativeBirthReadISO(FixedRuntimePath, 64<<10)
	if e != nil {
		return fail(e)
	}
	if e = matchSealedFile(FixedSealedRuntimePath, runtimeRaw); e != nil {
		return fail(e)
	}
	policyRaw, e := nativeBirthReadISO(fixedNativeBirthPolicyPath, 16<<10)
	if e != nil {
		return fail(e)
	}
	f, e := nativeBirthOpenRO(fixedNativeBirthBuildPath)
	if e != nil {
		return fail(e)
	}
	buildRaw, e := nativeBirthReadBounded(f, 32<<10)
	f.Close()
	if e != nil {
		return fail(e)
	}
	source, e := decodeNativeBirthSource(runtimeRaw, policyRaw, buildRaw)
	if e != nil {
		return fail(e)
	}
	sealed, e := sealedRuntimeSource()
	if e != nil || !reflect.DeepEqual(source.resource, sealed) || !sealed.Config.SystemCgroup {
		return fail(fmt.Errorf("original System runtime source differs"))
	}
	ref := source.manifest.RuntimeClosure
	if ref == nil || ref.validate() != nil {
		return fail(fmt.Errorf("original System closure reference missing"))
	}
	f, e = nativeBirthOpenRO(NormalSystemSourceManifestPath)
	if e != nil {
		return fail(e)
	}
	manifestRaw, e := nativeBirthReadBounded(f, 1<<20)
	f.Close()
	if e != nil {
		return fail(e)
	}
	if physicalDigest(manifestRaw) != ref.SystemManifestSHA256 {
		return fail(fmt.Errorf("original System manifest digest differs"))
	}
	observed, e := CheckNormalSystemSource(ctx, manifestRaw)
	if e != nil {
		return fail(e)
	}
	m, e := DecodeNormalSystemSourceManifest(manifestRaw)
	if e != nil || !normalSystemLifecycleManifestVersion(m) {
		return fail(fmt.Errorf("original System lifecycle requires exact versioned v2/v3 manifest"))
	}
	var st unix.Stat_t
	if e = unix.Stat(systemCgroup, &st); e != nil {
		return fail(e)
	}
	dev, ino, e := systemCgroupIdentity(st.Dev)
	if e != nil {
		return fail(e)
	}
	system, e := splitReadNode(systemCgroup)
	if e != nil || !system.RootControlled || system.MemoryMaxBytes != sealed.Limits.SystemMemoryMaxBytes || system.SwapMaxBytes != 0 || !system.MemoryHighUnlimited {
		return fail(fmt.Errorf("original System hard controls differ"))
	}
	consumer, e := nativeBirthCurrentConsumerIdentity()
	if e != nil {
		return fail(e)
	}
	binding := normalSystemLifecycleBinding{RootConsumer: consumer, RuntimeDocumentSHA256: physicalDigest(runtimeRaw), NativePolicySHA256: physicalDigest(policyRaw), NativeBuildSHA256: physicalDigest(buildRaw), SystemManifestSHA256: physicalDigest(manifestRaw), GuestBootID: observed.GuestBootID, VMUID: sealed.Config.VMUID, ComputeInstanceID: sealed.Config.ComputeInstanceID, SystemDevice: dev, SystemInode: ino, SystemMemoryMaxBytes: uint64(sealed.Limits.SystemMemoryMaxBytes)}
	s := &normalSystemLifecycleSource{binding: binding, manifest: m, files: map[string]*os.File{}, metadata: map[string]nativeBirthELFMetadata{}, logical: map[string]string{}}
	ok := false
	defer func() {
		if !ok {
			s.close()
		}
	}()
	if e = s.readClosure(ctx); e != nil {
		return fail(e)
	}
	s.observed = normalSystemLifecycleObservation{Scope: normalSystemLifecycleScope, Binding: binding, OriginalMetadataBytesBound: true, ConditionalELFClosureVerified: true, ScriptInterpreterClosureVerified: true, ConfigInventoryVerified: observed.ConfigInventoryObserved, MissingSources: []string{"original-per-role-spawn-and-exec-environment-gate", "continuous-all-role-control-writer-enforcement", "original-host-image-ISO-process-lifecycle-qualification"}}
	names := make([]string, 0, len(s.metadata))
	for n := range s.metadata {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		f := s.files[n]
		var id unix.Stat_t
		if e = unix.Fstat(int(f.Fd()), &id); e != nil {
			return fail(e)
		}
		sha, e := nativeBirthFileHash(f)
		if e != nil {
			return fail(e)
		}
		m := s.metadata[n]
		s.observed.ELFs = append(s.observed.ELFs, normalSystemLifecycleELF{Path: n, SHA256: sha, Device: id.Dev, Inode: id.Ino, Interpreter: m.Interpreter, Needed: append([]string(nil), m.Needed...)})
	}
	if e = ctx.Err(); e != nil {
		return fail(e)
	}
	ok = true
	return s, nil
}

// 所有库必须显式出现在原System清单。default搜索的首个实际对象必须就是该
// canonical对象，禁止略过前面的影子库；不从ldconfig cache/RPATH猜实际加载。
func (s *normalSystemLifecycleSource) readClosure(ctx context.Context) error {
	kinds := map[string]string{}
	for _, entry := range s.manifest.Files {
		if e := ctx.Err(); e != nil {
			return e
		}
		s.logical[entry.Path] = entry.CanonicalPath
		if entry.Kind != "exec" && entry.Kind != "loader" {
			continue
		}
		if entry.Path == "/etc/ld.so.cache" {
			return fmt.Errorf("System lifecycle default loader rejects cache")
		}
		if entry.Kind == "exec" {
			kinds[entry.CanonicalPath] = "exec"
		}
		if prior := s.files[entry.CanonicalPath]; prior != nil {
			continue
		}
		f, e := nativeBirthOpenRO(entry.CanonicalPath)
		if e != nil {
			return fmt.Errorf("System ELF readonly source: %w", e)
		}
		s.files[entry.CanonicalPath] = f
		sha, e := nativeBirthFileHash(f)
		if e != nil || sha != entry.SHA256 {
			return fmt.Errorf("System ELF original digest differs")
		}
		metadata, e := nativeBirthParseELF(f)
		if e != nil {
			return fmt.Errorf("System ELF unsupported loader geometry: %w", e)
		}
		s.metadata[entry.CanonicalPath] = metadata
		if kinds[entry.CanonicalPath] != "exec" {
			kinds[entry.CanonicalPath] = entry.Kind
		}
	}
	// 两种准确loader的全局覆盖均拒，目录不存在不补造；存在时完整逻辑及
	// canonical祖先都须RO。musl的path文件不是glibc cache，必须独立检查。
	for _, p := range []string{"/etc/ld.so.cache", "/etc/ld.so.preload", "/etc/ld-musl-x86_64.path"} {
		if _, e := os.Lstat(p); !os.IsNotExist(e) {
			return fmt.Errorf("System loader has unsupported override")
		}
		if e := nativeBirthReadonlyPath(p); e != nil {
			return e
		}
	}
	for _, entry := range s.manifest.Files {
		if entry.Kind != "script" {
			continue
		}
		f, e := nativeBirthOpenRO(entry.CanonicalPath)
		if e != nil {
			return e
		}
		b := make([]byte, 128)
		n, _ := f.ReadAt(b, 0)
		f.Close()
		line, _, _ := strings.Cut(string(b[:n]), "\n")
		if line != "#!/neonvm/bin/sh" {
			return fmt.Errorf("System script has unsupported interpreter")
		}
		target := s.logical["/neonvm/bin/sh"]
		if target == "" || s.files[target] == nil {
			return fmt.Errorf("System script interpreter absent from original closure")
		}
	}
	visited := map[string]bool{}
	var visit func(string, string) error
	visit = func(path, profile string) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		key := profile + "\x00" + path
		if visited[key] {
			return nil
		}
		visited[key] = true
		m, ok := s.metadata[path]
		if !ok {
			return fmt.Errorf("System loader resolved an undeclared ELF")
		}
		dirs := []string{}
		switch profile {
		case "static":
			if len(m.Needed) != 0 || m.Interpreter != "" {
				return fmt.Errorf("static System ELF has dynamic inputs")
			}
			return nil
		case "glibc":
			dirs = nativeBirthLoaderDirs
		case "musl":
			dirs = []string{"/lib", "/usr/local/lib", "/usr/lib"}
		default:
			return fmt.Errorf("unsupported System loader profile")
		}
		for _, dir := range dirs {
			if _, e := os.Lstat(dir); os.IsNotExist(e) {
				continue
			} else if e != nil {
				return e
			}
			if e := nativeBirthReadonlyPath(filepath.Join(dir, ".source-check")); e != nil {
				return e
			}
			real, e := filepath.EvalSymlinks(dir)
			if e != nil {
				return e
			}
			if e = nativeBirthReadonlyPath(filepath.Join(real, ".source-check")); e != nil {
				return e
			}
			if profile == "glibc" {
				if _, e = os.Lstat(filepath.Join(dir, "glibc-hwcaps")); !os.IsNotExist(e) {
					return fmt.Errorf("System loader hwcaps unsupported")
				}
			}
		}
		for _, name := range m.Needed {
			if name == "" || strings.ContainsAny(name, "/$\x00") {
				return fmt.Errorf("System needed name unsupported")
			}
			resolved := ""
			for _, dir := range dirs {
				candidate := filepath.Join(dir, name)
				if _, e := os.Lstat(candidate); os.IsNotExist(e) {
					continue
				} else if e != nil {
					return e
				}
				var e error
				resolved, e = nativeBirthCanonical(candidate)
				if e != nil {
					return e
				}
				break
			}
			if resolved == "" {
				return fmt.Errorf("System dependency missing from default loader source")
			}
			if e := visit(resolved, profile); e != nil {
				return e
			}
		}
		return nil
	}
	for path, kind := range kinds {
		if kind != "exec" {
			continue
		}
		m := s.metadata[path]
		profile := "static"
		if m.Interpreter != "" {
			switch m.Interpreter {
			case "/lib64/ld-linux-x86-64.so.2", "/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2":
				profile = "glibc"
			case "/lib/ld-musl-x86_64.so.1":
				profile = "musl"
			default:
				return fmt.Errorf("System executable interpreter unsupported")
			}
			interp, e := nativeBirthCanonical(m.Interpreter)
			if e != nil {
				return e
			}
			if e = visit(interp, profile); e != nil {
				return e
			}
		}
		if e := visit(path, profile); e != nil {
			return e
		}
	}
	for path := range s.files {
		reached := false
		for key := range visited {
			reached = reached || strings.HasSuffix(key, "\x00"+path)
		}
		if !reached {
			return fmt.Errorf("System manifest has unused loader source")
		}
	}
	return nil
}

// 事实对象只从真实reader构造。生产许可必须另消费原role spawn/执行与持续写者
// 原始证据；当前缺项返回Unknown，不能仅因metadata引用存在就放行normal。
func (s *normalSystemLifecycleSource) requireContinuousQualification() error {
	return errors.Join(ErrUnknown, fmt.Errorf("System role spawn/exec and continuous writers are not yet qualified"))
}
func (s *normalSystemLifecycleSource) snapshot() (normalSystemLifecycleObservation, error) {
	if s == nil {
		return normalSystemLifecycleObservation{}, ErrUnknown
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return normalSystemLifecycleObservation{}, ErrUnknown
	}
	b, e := json.Marshal(s.observed)
	if e != nil {
		return normalSystemLifecycleObservation{}, e
	}
	var out normalSystemLifecycleObservation
	e = json.Unmarshal(b, &out)
	return out, e
}
