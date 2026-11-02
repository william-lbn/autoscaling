//go:build linux

package resourceguard

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

// 生产reader与构建器共用准确三种选择，不改变运行期RO/来源/原FD检查。
func normalSystemLoaderProfile(m nativeBirthELFMetadata) (string, error) {
	switch m.Interpreter {
	case "":
		return "static", nil
	case "/lib64/ld-linux-x86-64.so.2", "/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2":
		return "glibc", nil
	case "/lib/ld-musl-x86_64.so.1":
		return "musl", nil
	default:
		return "", fmt.Errorf("System executable interpreter unsupported")
	}
}
func normalSystemLoaderDirectories(m nativeBirthELFMetadata, profile string) ([]string, error) {
	switch profile {
	case "static":
		if len(m.Needed) != 0 || m.Interpreter != "" {
			return nil, fmt.Errorf("static System ELF has dynamic inputs")
		}
		return nil, nil
	case "glibc":
		return append([]string(nil), nativeBirthLoaderDirs...), nil
	case "musl":
		return []string{"/lib", "/usr/local/lib", "/usr/lib"}, nil
	default:
		return nil, fmt.Errorf("unsupported System loader profile")
	}
}

type normalSystemBuildFile struct {
	identity os.FileInfo
	sha      string
	bytes    uint64
	metadata nativeBirthELFMetadata
	first    []byte
}

// BuildNormalSystemClosureManifestJSON只输出构建声明。selector1固定派生registry
// 完整metadata18c五元组；它不是对实际运行内核/Host或持续normal的证明。
func BuildNormalSystemClosureManifestJSON(input []byte, canonical func(string) (string, error), open func(string) (*os.File, error), lstat func(string) (os.FileInfo, error)) ([]byte, error) {
	if runtime.GOARCH != "amd64" || canonical == nil || open == nil || lstat == nil {
		return nil, fmt.Errorf("System closure build requires Linux amd64 image readers")
	}
	m, e := DecodeNormalSystemSourceManifest(input)
	if e != nil {
		return nil, e
	}
	if m.ProtocolVersion != 3 {
		return nil, fmt.Errorf("System closure build requires System3")
	}
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil {
		return nil, e
	}
	m.Kernel = ref
	for _, name := range []string{"/etc/ld.so.cache", "/etc/ld.so.preload", "/etc/ld-musl-x86_64.path"} {
		if _, e = canonical(filepath.Dir(name)); e != nil {
			return nil, e
		}
		if _, e = lstat(name); !os.IsNotExist(e) {
			return nil, fmt.Errorf("System image loader override or unknown: %s", name)
		}
	}
	kinds := map[string]string{}
	logical := map[string]string{}
	for _, entry := range m.Files {
		if entry.Path == NormalSystemSourceManifestPath || entry.CanonicalPath == NormalSystemSourceManifestPath {
			return nil, fmt.Errorf("System image inventory cannot contain itself")
		}
		logical[entry.Path] = entry.CanonicalPath
		prior := kinds[entry.CanonicalPath]
		if prior != "" && prior != entry.Kind && !((prior == "exec" || prior == "loader") && (entry.Kind == "exec" || entry.Kind == "loader")) {
			return nil, fmt.Errorf("System image canonical object has conflicting file classes")
		}
		if entry.Kind == "exec" || prior == "" {
			kinds[entry.CanonicalPath] = entry.Kind
		}
	}
	files := map[string]*normalSystemBuildFile{}
	read := func(path, kind string) (out *normalSystemBuildFile, err error) {
		if old := files[path]; old != nil {
			return old, nil
		}
		actual, e := canonical(path)
		if e != nil || actual != path {
			return nil, fmt.Errorf("System image canonical file differs: %s", path)
		}
		f, e := open(path)
		if e != nil {
			return nil, e
		}
		defer f.Close()
		before, e := f.Stat()
		if e != nil {
			return nil, e
		}
		var st unix.Stat_t
		if e = unix.Fstat(int(f.Fd()), &st); e != nil {
			return nil, e
		}
		if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != 0 || st.Mode&0022 != 0 || st.Size <= 0 || st.Size > 256<<20 || (kind == "exec" || kind == "script") && st.Mode&0111 == 0 {
			return nil, fmt.Errorf("System image root regular/mode/size differs: %s", path)
		}
		out = &normalSystemBuildFile{bytes: uint64(st.Size)}
		if kind == "exec" || kind == "loader" {
			out.metadata, e = nativeBirthParseELF(f)
			if e != nil {
				return nil, e
			}
		}
		limit := int64(128)
		if path == NormalSystemOrdinaryV2ManifestPath {
			limit = 32 << 10
			if st.Size > limit {
				return nil, fmt.Errorf("System image controls exceeds bound")
			}
		}
		out.first, e = io.ReadAll(io.NewSectionReader(f, 0, limit))
		if e != nil {
			return nil, e
		}
		out.sha, e = nativeBirthFileHash(f)
		if e != nil {
			return nil, e
		}
		after, e := f.Stat()
		if e != nil || !NativeBirthBuildFileUnchanged(before, after) {
			return nil, fmt.Errorf("System image file changed while read: %s", path)
		}
		out.identity = after
		files[path] = out
		return out, nil
	}
	controls := false
	for _, entry := range m.Files {
		actual, e := canonical(entry.Path)
		if e != nil || actual != entry.CanonicalPath {
			return nil, fmt.Errorf("System image original logical alias differs: %s", entry.Path)
		}
		f, e := read(entry.CanonicalPath, kinds[entry.CanonicalPath])
		if e != nil {
			return nil, e
		}
		if f.bytes != entry.Bytes || f.sha != entry.SHA256 {
			return nil, fmt.Errorf("System image original file bytes differ: %s", entry.Path)
		}
		if entry.Path == NormalSystemOrdinaryV2ManifestPath {
			if controls || entry.Kind != "config" || entry.CanonicalPath != entry.Path {
				return nil, fmt.Errorf("System image requires fixed controls2")
			}
			controls = true
			if _, e = DecodeNormalSystemOrdinaryV2Manifest(f.first); e != nil {
				return nil, e
			}
		}
		if entry.Kind == "script" {
			line, _, _ := strings.Cut(string(f.first), "\n")
			if line != "#!/neonvm/bin/sh" {
				return nil, fmt.Errorf("System image script interpreter unsupported")
			}
		}
	}
	if !controls {
		return nil, fmt.Errorf("System image controls2 is missing")
	}
	sh := logical["/neonvm/bin/sh"]
	if sh == "" || files[sh] == nil || kinds[sh] != "exec" {
		return nil, fmt.Errorf("System image original shell interpreter missing")
	}
	visited := map[string]bool{}
	reached := map[string]bool{}
	var visit func(string, string) error
	visit = func(path, profile string) error {
		key := profile + "\x00" + path
		if visited[key] {
			return nil
		}
		visited[key] = true
		f := files[path]
		if f == nil {
			if len(m.Files) >= 256 {
				return fmt.Errorf("System image closure exceeds original file bound")
			}
			if !normalSystemAllowedManifestFile(m, path, "loader") {
				return fmt.Errorf("System image resolved loader outside fixed class: %s", path)
			}
			var e error
			f, e = read(path, "loader")
			if e != nil {
				return e
			}
			m.Files = append(m.Files, NormalSystemSourceFile{Path: path, CanonicalPath: path, Kind: "loader", Bytes: f.bytes, SHA256: f.sha})
			kinds[path] = "loader"
		}
		if kinds[path] != "exec" && kinds[path] != "loader" {
			return fmt.Errorf("System image loader aliases a non-ELF class")
		}
		reached[path] = true
		dirs, e := normalSystemLoaderDirectories(f.metadata, profile)
		if e != nil {
			return e
		}
		if profile == "static" {
			return nil
		}
		for _, dir := range dirs {
			if _, e = lstat(dir); os.IsNotExist(e) {
				continue
			} else if e != nil {
				return e
			}
			real, e := canonical(dir)
			if e != nil {
				return e
			}
			st, e := lstat(real)
			if e != nil || !st.IsDir() {
				return fmt.Errorf("System image loader directory differs: %s", dir)
			}
			if profile == "glibc" {
				if _, e = lstat(filepath.Join(dir, "glibc-hwcaps")); !os.IsNotExist(e) {
					return fmt.Errorf("System image loader hwcaps unsupported")
				}
			}
		}
		for _, name := range f.metadata.Needed {
			if name == "" || strings.ContainsAny(name, "/$\x00") {
				return fmt.Errorf("System needed name unsupported")
			}
			resolved := ""
			for _, dir := range dirs {
				candidate := filepath.Join(dir, name)
				if _, e = lstat(candidate); os.IsNotExist(e) {
					continue
				} else if e != nil {
					return e
				}
				resolved, e = canonical(candidate)
				if e != nil {
					return e
				}
				break
			}
			if resolved == "" {
				return fmt.Errorf("System image dependency missing from fixed default dirs: %s", name)
			}
			if e = visit(resolved, profile); e != nil {
				return e
			}
		}
		return nil
	}
	// 按稳定canonical执行入口排序；同一DSO在两种profile内仍分别走原目录图。
	executables := []string{}
	for path, kind := range kinds {
		if kind == "exec" {
			executables = append(executables, path)
		}
	}
	sort.Strings(executables)
	for _, path := range executables {
		meta := files[path].metadata
		profile, e := normalSystemLoaderProfile(meta)
		if e != nil {
			return nil, e
		}
		if meta.Interpreter != "" {
			interp, e := canonical(meta.Interpreter)
			if e != nil {
				return nil, e
			}
			if e = visit(interp, profile); e != nil {
				return nil, e
			}
		}
		if e = visit(path, profile); e != nil {
			return nil, e
		}
	}
	for path, kind := range kinds {
		if (kind == "exec" || kind == "loader") && !reached[path] {
			return nil, fmt.Errorf("System image contains unused declared loader: %s", path)
		}
	}
	// 对全部原config/script/ELF及新依赖再次核原对象与路径，发布不能保留中途旧快照。
	for path, expected := range files {
		actual, e := canonical(path)
		if e != nil || actual != path {
			return nil, fmt.Errorf("System image file alias changed before publication")
		}
		f, e := open(path)
		if e != nil {
			return nil, e
		}
		current, e := f.Stat()
		ce := f.Close()
		if e != nil || ce != nil || !NativeBirthBuildFileUnchanged(expected.identity, current) {
			return nil, fmt.Errorf("System image file changed before publication: %s", path)
		}
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	if e = m.Validate(); e != nil {
		return nil, e
	}
	raw, e := json.Marshal(m)
	if e != nil || len(raw) > 1<<20 {
		return nil, fmt.Errorf("System image final inventory exceeds original bound")
	}
	return raw, nil
}
