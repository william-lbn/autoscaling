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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// 仅钉住原System3静态输入；不产生stock处理完成、角色、FS或normal许可。
// 调用顺序为resource flock -> Inputs.mu -> System.mu；不得已持System.mu调用。
type normalOrdinaryStockUdevInputs struct {
	mu             sync.Mutex
	closed         bool
	system         *normalSystemLifecycleSource
	manifest       normalOrdinaryStockUdevManifest
	reference      normalOrdinaryStockUdevInputReference
	pins           map[string]*os.File
	directories    map[string]*os.File
	ruleLookupPins map[string]*os.File
}
type normalOrdinaryStockUdevInputFile struct {
	Path          string                 `json:"path"`
	CanonicalPath string                 `json:"canonical_path"`
	Kind          string                 `json:"kind"`
	Code          bool                   `json:"code"`
	Bytes         uint64                 `json:"bytes"`
	SHA256        string                 `json:"sha256"`
	Object        normalFilesystemObject `json:"object"`
	Mode          uint32                 `json:"mode"`
	UID           uint32                 `json:"uid"`
	GID           uint32                 `json:"gid"`
	Ctime         unix.Timespec          `json:"ctime"`
	Mtime         unix.Timespec          `json:"mtime"`
}
type normalOrdinaryStockUdevInputDirectory struct {
	Path          string                 `json:"path"`
	CanonicalPath string                 `json:"canonical_path"`
	Object        normalFilesystemObject `json:"object"`
}
type normalOrdinaryStockUdevInputReference struct {
	ProtocolVersion    uint32                                  `json:"protocol_version"`
	Scope              string                                  `json:"scope"`
	Binding            normalSystemLifecycleBinding            `json:"original_system"`
	SystemSourceSHA256 string                                  `json:"system_source_sha256"`
	ManifestSHA256     string                                  `json:"manifest_sha256"`
	PackageLockSHA256  string                                  `json:"package_lock_sha256"`
	RulesSHA256        string                                  `json:"rules_sha256"`
	Files              []normalOrdinaryStockUdevInputFile      `json:"files"`
	Directories        []normalOrdinaryStockUdevInputDirectory `json:"directories"`
	RuleLookups        []normalOrdinaryStockUdevRuleLookup     `json:"static_rule_lookups"`
	MissingSources     []string                                `json:"missing_sources"`
}

func pinNormalOrdinaryStockUdevInputs(ctx context.Context, system *normalSystemLifecycleSource) (*normalOrdinaryStockUdevInputs, error) {
	fail := func(e error) (*normalOrdinaryStockUdevInputs, error) { return nil, errors.Join(ErrUnknown, e) }
	if ctx == nil || ctx.Err() != nil || system == nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fail(fmt.Errorf("stock inputs require original root System source"))
	}
	i := &normalOrdinaryStockUdevInputs{system: system, pins: map[string]*os.File{}, directories: map[string]*os.File{}, ruleLookupPins: map[string]*os.File{}}
	ok := false
	defer func() {
		if !ok {
			i.close()
		}
	}()
	system.mu.Lock()
	defer system.mu.Unlock()
	if system.closed || system.manifest.ProtocolVersion != 3 || !normalSystemStockUdevSelected(system.manifest) {
		return fail(fmt.Errorf("stock inputs original System3 inventory absent"))
	}
	if e := system.manifest.Validate(); e != nil {
		return fail(e)
	}
	consumer, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || consumer != system.binding.RootConsumer {
		return fail(fmt.Errorf("stock inputs original consumer differs"))
	}
	// 不调用snapshot/referenceSHA256以免同mu重入；缓存摘要只绑定原对象，不证明当前RO。
	raw, e := json.Marshal(system.observed)
	if e != nil {
		return fail(e)
	}
	i.reference = normalOrdinaryStockUdevInputReference{ProtocolVersion: 1, Scope: "original-stock-udev-ro-inputs-v1", Binding: system.binding, SystemSourceSHA256: physicalDigest(raw), MissingSources: []string{"original-three-actor-and-nscd-socket-enforcement", "protected-run-udev-rules-and-writable-lookup-enforcement", "original-stock-exec-wait-processing-and-projection-seal", "continuous-role-and-normal-memory-qualification"}}
	entry := normalOrdinaryStockUdevSystemEntry(system.manifest, normalOrdinaryStockUdevManifestPath)
	if entry == nil || entry.Kind != "config" || entry.CanonicalPath != normalOrdinaryStockUdevManifestPath {
		return fail(fmt.Errorf("stock inputs original manifest entry differs"))
	}
	f, p, e := normalOrdinaryStockUdevPinFile(*entry, "manifest", false)
	if e != nil {
		return fail(e)
	}
	i.pins[p.Path] = f
	i.reference.Files = append(i.reference.Files, p)
	b, e := nativeBirthReadBounded(f, 64<<10)
	if e != nil {
		return fail(e)
	}
	if physicalDigest(b) != p.SHA256 {
		return fail(fmt.Errorf("stock input manifest changed while reading"))
	}
	i.manifest, e = decodeNormalOrdinaryStockUdevManifest(b)
	if e != nil {
		return fail(e)
	}
	if e = normalOrdinaryStockUdevMatchInventory(system.manifest, i.manifest); e != nil {
		return fail(e)
	}
	lockEntry, e := normalOrdinaryStockUdevPackageLockEntry(system.manifest, i.manifest)
	if e != nil {
		return fail(e)
	}
	lockFile, lockFact, e := normalOrdinaryStockUdevPinFile(*lockEntry, "package-lock", false)
	if e != nil {
		return fail(e)
	}
	i.pins[lockFact.Path] = lockFile
	i.reference.Files = append(i.reference.Files, lockFact)
	i.reference.ManifestSHA256 = p.SHA256
	i.reference.PackageLockSHA256 = i.manifest.PackageLockSHA256
	rules := []normalOrdinaryStockUdevFile{}
	for _, mf := range i.manifest.Files {
		if e = ctx.Err(); e != nil {
			return fail(e)
		}
		entry = normalOrdinaryStockUdevSystemEntry(system.manifest, mf.Path)
		f, p, e = normalOrdinaryStockUdevPinFile(*entry, mf.Kind, mf.Code)
		if e != nil {
			return fail(e)
		}
		i.pins[p.Path] = f
		i.reference.Files = append(i.reference.Files, p)
		if mf.Kind == "rule" || mf.Kind == "acl-override" {
			rules = append(rules, mf)
		}
	}
	sort.Slice(i.reference.Files, func(a, b int) bool { return i.reference.Files[a].Path < i.reference.Files[b].Path })
	raw, e = json.Marshal(rules)
	if e != nil {
		return fail(e)
	}
	i.reference.RulesSHA256 = physicalDigest(raw)
	if e = i.checkOriginalSystemManifestLocked(); e != nil {
		return fail(e)
	}
	if e = i.checkAccountsLocked(); e != nil {
		return fail(e)
	}
	if e = i.checkClosureLocked(ctx); e != nil {
		return fail(e)
	}
	if e = i.checkDirectoriesLocked(ctx, true); e != nil {
		return fail(e)
	}
	if e = i.checkStaticRuleLookupsLocked(ctx, true); e != nil {
		return fail(e)
	}
	if e = i.recheckFilesLocked(ctx); e != nil {
		return fail(e)
	}
	ok = true
	return i, nil
}

func normalOrdinaryStockUdevSystemEntry(m NormalSystemSourceManifest, path string) *NormalSystemSourceFile {
	for _, f := range m.Files {
		if f.Path == path {
			v := f
			return &v
		}
	}
	return nil
}

// 纯库存关联可以在模型中验证；它不打开文件也不生成Inputs。
func normalOrdinaryStockUdevMatchInventory(system NormalSystemSourceManifest, m normalOrdinaryStockUdevManifest) error {
	if system.ProtocolVersion != 3 || !normalSystemStockUdevSelected(system) {
		return fmt.Errorf("stock original System3 expansion absent")
	}
	want := map[string]normalOrdinaryStockUdevFile{}
	for _, f := range m.Files {
		if _, ok := want[f.Path]; ok {
			return fmt.Errorf("stock inventory duplicate")
		}
		want[f.Path] = f
		original := normalOrdinaryStockUdevSystemEntry(system, f.Path)
		kind, ok := normalSystemStockUdevFileKind(f.Path)
		if !ok || original == nil || original.Kind != kind || original.Bytes != uint64(f.Bytes) || original.SHA256 != f.SHA256 || f.Code != (kind == "exec" || kind == "loader") {
			return fmt.Errorf("stock file differs from original System inventory")
		}
	}
	// System中其它stock文件也必须在prep MF中，不能留下额外规则/辅助程序输入。
	for _, f := range system.Files {
		if f.Path == normalOrdinaryStockUdevManifestPath {
			continue
		}
		if class, _, ok := normalOrdinaryStockUdevFileClass(f.Path); ok && class != "dso" {
			if _, found := want[f.Path]; !found {
				return fmt.Errorf("original System has stock input absent from prep manifest")
			}
		}
	}
	return nil
}

func normalOrdinaryStockUdevPinFile(entry NormalSystemSourceFile, kind string, code bool) (*os.File, normalOrdinaryStockUdevInputFile, error) {
	var zero normalOrdinaryStockUdevInputFile
	canonical, e := nativeBirthCanonical(entry.Path)
	if e != nil || canonical != entry.CanonicalPath {
		return nil, zero, fmt.Errorf("stock original canonical source differs")
	}
	f, e := normalSystemOpenRO(canonical)
	if e != nil {
		return nil, zero, e
	}
	fail := func(e error) (*os.File, normalOrdinaryStockUdevInputFile, error) { f.Close(); return nil, zero, e }
	before, e := normalOrdinaryStockUdevFileFact(f, entry.Path, canonical, kind, code)
	if e != nil {
		return fail(e)
	}
	sha, e := nativeBirthFileHash(f)
	if e != nil {
		return fail(e)
	}
	after, e := normalOrdinaryStockUdevFileFact(f, entry.Path, canonical, kind, code)
	if e != nil || before != after || before.Bytes != entry.Bytes || sha != entry.SHA256 {
		return fail(fmt.Errorf("stock original file bytes or identity differ"))
	}
	before.SHA256 = sha
	return f, before, nil
}
func normalOrdinaryStockUdevFileFact(f *os.File, path, canonical, kind string, code bool) (normalOrdinaryStockUdevInputFile, error) {
	var z normalOrdinaryStockUdevInputFile
	var st unix.Stat_t
	if f == nil || unix.Fstat(int(f.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Size <= 0 || st.Uid != 0 || st.Mode&0022 != 0 {
		return z, fmt.Errorf("stock static file ownership or type differs")
	}
	obj, e := normalFilesystemFDObject(int(f.Fd()))
	if e != nil || !obj.Readonly {
		return z, fmt.Errorf("stock static file is not original readonly object")
	}
	return normalOrdinaryStockUdevInputFile{Path: path, CanonicalPath: canonical, Kind: kind, Code: code, Bytes: uint64(st.Size), Object: obj, Mode: st.Mode, UID: st.Uid, GID: st.Gid, Ctime: st.Ctim, Mtime: st.Mtim}, nil
}
func (i *normalOrdinaryStockUdevInputs) recheckFilesLocked(ctx context.Context) error {
	for _, old := range i.reference.Files {
		if e := ctx.Err(); e != nil {
			return e
		}
		f := i.pins[old.Path]
		now, e := normalOrdinaryStockUdevFileFact(f, old.Path, old.CanonicalPath, old.Kind, old.Code)
		if e != nil {
			return e
		}
		sha, e := nativeBirthFileHash(f)
		if e != nil {
			return e
		}
		now.SHA256 = sha
		if now != old {
			return fmt.Errorf("stock pinned file changed")
		}
		entry := NormalSystemSourceFile{Path: old.Path, CanonicalPath: old.CanonicalPath, Kind: "config", Bytes: old.Bytes, SHA256: old.SHA256}
		current, fact, e := normalOrdinaryStockUdevPinFile(entry, old.Kind, old.Code)
		if e != nil {
			return e
		}
		current.Close()
		if fact != old {
			return fmt.Errorf("stock logical lookup no longer resolves original inode")
		}
	}
	return nil
}

// 用原读者重新解析完整System ELF闭包，而不是信任旧Observed的布尔值。
// temp持独立FD；不替换原source缓存，也不在system.mu内调用其锁方法。
func (i *normalOrdinaryStockUdevInputs) checkClosureLocked(ctx context.Context) error {
	s := i.system
	raw, e := json.Marshal(s.manifest)
	if e != nil {
		return e
	}
	var m NormalSystemSourceManifest
	if e = json.Unmarshal(raw, &m); e != nil {
		return e
	}
	temp := &normalSystemLifecycleSource{manifest: m, files: map[string]*os.File{}, metadata: map[string]nativeBirthELFMetadata{}, logical: map[string]string{}}
	defer temp.close()
	if e = temp.readClosure(ctx); e != nil {
		return e
	}
	for path, f := range temp.files {
		original := s.files[path]
		if original == nil {
			return fmt.Errorf("stock original System closure pin absent")
		}
		a, e := normalFilesystemFDObject(int(f.Fd()))
		if e != nil {
			return e
		}
		b, e := normalFilesystemFDObject(int(original.Fd()))
		if e != nil || a != b {
			return fmt.Errorf("stock System ELF source replaced")
		}
	}
	stock := map[string]normalOrdinaryStockUdevFile{}
	canonical := map[string]bool{}
	for _, f := range i.manifest.Files {
		stock[f.Path] = f
		if f.Code {
			canonical[temp.logical[f.Path]] = true
		}
	}
	visited := map[string]bool{}
	var visit func(string) error
	visit = func(logical string) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		f, ok := stock[logical]
		if !ok || !f.Code {
			return fmt.Errorf("stock executable dependency absent from prep manifest")
		}
		path := temp.logical[logical]
		if visited[path] {
			return nil
		}
		visited[path] = true
		meta, ok := temp.metadata[path]
		if !ok || !canonical[path] {
			return fmt.Errorf("stock original executable metadata absent")
		}
		if meta.Interpreter != "" {
			if meta.Interpreter != "/lib/ld-musl-x86_64.so.1" {
				return fmt.Errorf("stock account lookup requires original musl loader profile")
			}
			if e := visit(meta.Interpreter); e != nil {
				return e
			}
		}
		for _, name := range meta.Needed {
			resolved := ""
			for _, dir := range []string{"/lib", "/usr/local/lib", "/usr/lib"} {
				candidate := filepath.Join(dir, name)
				if _, e := os.Lstat(candidate); os.IsNotExist(e) {
					continue
				} else if e != nil {
					return e
				}
				resolved = candidate
				break
			}
			if resolved == "" {
				return fmt.Errorf("stock dependency actual first lookup absent")
			}
			if e := visit(resolved); e != nil {
				return e
			}
		}
		return nil
	}
	for _, f := range i.manifest.Files {
		if f.Kind == "program" || f.Kind == "helper" {
			if e = visit(f.Path); e != nil {
				return e
			}
		}
	}
	for _, f := range i.manifest.Files {
		if f.Kind == "dso" && !visited[temp.logical[f.Path]] {
			return fmt.Errorf("stock prep manifest has unused loader source")
		}
	}
	return nil
}

// 只扫描/lib与/etc静态lookup库存，不把/run当RO，也不冒认nscd/角色边界已关闭。
// 固定两棵目录捕获所有规则、hwdb、helper和未知shadow文件；目录内不允许额外alias子树。
func (i *normalOrdinaryStockUdevInputs) checkDirectoriesLocked(ctx context.Context, initial bool) error {
	expected := map[string]bool{}
	expectedDirs := map[string]bool{"/lib/udev": true, "/etc/udev": true}
	for _, f := range i.manifest.Files {
		if strings.HasPrefix(f.Path, "/lib/udev/") || strings.HasPrefix(f.Path, "/etc/udev/") {
			expected[f.Path] = true
			for d := filepath.Dir(f.Path); d != "/lib" && d != "/etc"; d = filepath.Dir(d) {
				expectedDirs[d] = true
			}
		}
	}
	seen := map[string]bool{}
	facts := []normalOrdinaryStockUdevInputDirectory{}
	for _, root := range []string{"/lib/udev", "/etc/udev"} {
		canonical, e := nativeBirthCanonical(root)
		if e != nil {
			return e
		}
		e = filepath.WalkDir(canonical, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if e := ctx.Err(); e != nil {
				return e
			}
			rel, e := filepath.Rel(canonical, path)
			if e != nil {
				return e
			}
			logical := root
			if rel != "." {
				logical = filepath.Join(root, rel)
			}
			if strings.Count(rel, string(os.PathSeparator)) > 16 || len(seen)+len(facts) > 4096 {
				return fmt.Errorf("stock static inventory exceeds traversal bound")
			}
			if d.IsDir() {
				if !expectedDirs[logical] {
					return fmt.Errorf("stock static lookup has unlisted directory")
				}
				if e := nativeBirthReadonlyPath(filepath.Join(logical, ".source-check")); e != nil {
					return e
				}
				real, e := filepath.EvalSymlinks(logical)
				if e != nil || real != path {
					return fmt.Errorf("stock static directory alias differs")
				}
				fd, e := unix.Open(real, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
				if e != nil {
					return e
				}
				f := os.NewFile(uintptr(fd), "stock-original-ro-directory")
				obj, e := normalFilesystemFDObject(fd)
				if e != nil || !obj.Readonly {
					f.Close()
					return fmt.Errorf("stock lookup directory is not readonly")
				}
				fact := normalOrdinaryStockUdevInputDirectory{Path: logical, CanonicalPath: real, Object: obj}
				facts = append(facts, fact)
				if initial {
					i.directories[logical] = f
				} else {
					old := i.directories[logical]
					if old == nil {
						f.Close()
						return fmt.Errorf("stock new lookup directory")
					}
					pinned, e := normalFilesystemFDObject(int(old.Fd()))
					f.Close()
					if e != nil || pinned != obj {
						return fmt.Errorf("stock lookup directory identity changed")
					}
				}
				return nil
			}
			if !expected[logical] || seen[logical] {
				return fmt.Errorf("stock static lookup has unlisted file")
			}
			// 包内文件alias仍须逐原System canonical/FD核验；目录alias不能借此扩树。
			seen[logical] = true
			return nil
		})
		if e != nil {
			return e
		}
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("stock static lookup inventory incomplete")
	}
	sort.Slice(facts, func(a, b int) bool { return facts[a].Path < facts[b].Path })
	if initial {
		i.reference.Directories = facts
	} else if !reflect.DeepEqual(facts, i.reference.Directories) {
		return fmt.Errorf("stock static lookup directory graph changed")
	}
	return nil
}

// exact --enable-split-usr的额外静态规则根：别名必须就是原已钉规则目录；
// 独立目录只能真实RO空，缺失需钉最近实际RO父并每次重验。不会扩大prep MF规则路径。
type normalOrdinaryStockUdevRuleLookup struct {
	Path          string                 `json:"path"`
	State         string                 `json:"state"`
	CanonicalPath string                 `json:"canonical_path"`
	AnchorPath    string                 `json:"anchor_path"`
	Object        normalFilesystemObject `json:"object"`
}

func normalOrdinaryStockUdevRuleDirectoryDecision(canonical string, obj normalFilesystemObject, originals map[string]normalFilesystemObject, count int) (string, error) {
	if !obj.Readonly || obj.Inode == 0 || obj.MountID == 0 || count < 0 {
		return "", fmt.Errorf("stock static rule root is not original readonly object")
	}
	if original, ok := originals[canonical]; ok {
		if obj != original {
			return "", fmt.Errorf("stock rule alias is not same original directory")
		}
		return "original-alias", nil
	}
	if count != 0 {
		return "", fmt.Errorf("stock unsupported additional static rule root is nonempty")
	}
	return "readonly-empty", nil
}
func normalOrdinaryStockUdevOpenRuleDirectory(logical string) (*os.File, string, normalFilesystemObject, error) {
	canonical, e := nativeBirthCanonical(logical)
	if e != nil {
		return nil, "", normalFilesystemObject{}, e
	}
	if e = nativeBirthReadonlyPath(filepath.Join(logical, ".rule-root-check")); e != nil {
		return nil, "", normalFilesystemObject{}, e
	}
	if e = nativeBirthReadonlyPath(filepath.Join(canonical, ".rule-root-check")); e != nil {
		return nil, "", normalFilesystemObject{}, e
	}
	fd, e := unix.Open(canonical, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, "", normalFilesystemObject{}, e
	}
	f := os.NewFile(uintptr(fd), "stock-original-static-rule-root")
	obj, e := normalFilesystemFDObject(fd)
	if e != nil || !obj.Readonly {
		f.Close()
		return nil, "", normalFilesystemObject{}, fmt.Errorf("stock static rule lookup parent is not readonly")
	}
	return f, canonical, obj, nil
}
func normalOrdinaryStockUdevPinRuleLookup(ctx context.Context, logical string, originals map[string]normalFilesystemObject) (normalOrdinaryStockUdevRuleLookup, *os.File, error) {
	var zero normalOrdinaryStockUdevRuleLookup
	if e := ctx.Err(); e != nil {
		return zero, nil, e
	}
	if st, e := os.Stat(logical); e == nil {
		if !st.IsDir() {
			return zero, nil, fmt.Errorf("stock static rule lookup is not directory")
		}
		f, canonical, obj, e := normalOrdinaryStockUdevOpenRuleDirectory(logical)
		if e != nil {
			return zero, nil, e
		}
		names, e := normalFilesystemReadDirAt(int(f.Fd()), ".", 4096)
		if e != nil {
			f.Close()
			return zero, nil, e
		}
		state, e := normalOrdinaryStockUdevRuleDirectoryDecision(canonical, obj, originals, len(names))
		if e != nil {
			f.Close()
			return zero, nil, e
		}
		return normalOrdinaryStockUdevRuleLookup{Path: logical, State: state, CanonicalPath: canonical, AnchorPath: logical, Object: obj}, f, nil
	} else if !os.IsNotExist(e) {
		return zero, nil, e
	}
	if _, e := os.Lstat(logical); !os.IsNotExist(e) {
		return zero, nil, fmt.Errorf("stock static rule lookup has dangling or unsupported alias")
	}
	for anchor := filepath.Dir(logical); ; anchor = filepath.Dir(anchor) {
		if e := ctx.Err(); e != nil {
			return zero, nil, e
		}
		if st, e := os.Stat(anchor); e == nil {
			if !st.IsDir() {
				return zero, nil, fmt.Errorf("stock missing rule root has non-directory ancestor")
			}
			f, canonical, obj, e := normalOrdinaryStockUdevOpenRuleDirectory(anchor)
			if e != nil {
				return zero, nil, e
			}
			if _, e = os.Lstat(logical); !os.IsNotExist(e) {
				f.Close()
				return zero, nil, fmt.Errorf("stock missing rule lookup changed during pin")
			}
			return normalOrdinaryStockUdevRuleLookup{Path: logical, State: "readonly-absent", CanonicalPath: canonical, AnchorPath: anchor, Object: obj}, f, nil
		} else if !os.IsNotExist(e) {
			return zero, nil, e
		}
		if _, e := os.Lstat(anchor); !os.IsNotExist(e) {
			return zero, nil, fmt.Errorf("stock missing rule ancestor has dangling alias")
		}
		if anchor == "/" {
			return zero, nil, fmt.Errorf("stock readonly lookup anchor unavailable")
		}
	}
}
func (i *normalOrdinaryStockUdevInputs) checkStaticRuleLookupsLocked(ctx context.Context, initial bool) error {
	originals := map[string]normalFilesystemObject{}
	for _, file := range i.manifest.Files {
		if file.Kind != "rule" && file.Kind != "acl-override" {
			continue
		}
		logical := filepath.Dir(file.Path)
		for _, d := range i.reference.Directories {
			if d.Path == logical {
				originals[d.CanonicalPath] = d.Object
			}
		}
	}
	refs := []normalOrdinaryStockUdevRuleLookup{}
	for _, logical := range NormalSystemStockUdevStaticRuleDirectories() {
		if i.directories[logical] != nil {
			continue
		} // 原/lib与/etc目录已完整MF匹配/逐FD钉住。
		ref, f, e := normalOrdinaryStockUdevPinRuleLookup(ctx, logical, originals)
		if e != nil {
			return e
		}
		if initial {
			i.ruleLookupPins[logical] = f
		} else {
			old := i.ruleLookupPins[logical]
			if old == nil {
				f.Close()
				return fmt.Errorf("stock original static rule lookup pin missing")
			}
			obj, e := normalFilesystemFDObject(int(old.Fd()))
			f.Close()
			if e != nil || obj != ref.Object {
				return fmt.Errorf("stock original rule lookup directory replaced")
			}
		}
		refs = append(refs, ref)
	}
	if initial {
		i.reference.RuleLookups = refs
	} else if !reflect.DeepEqual(refs, i.reference.RuleLookups) {
		return fmt.Errorf("stock original static rule lookup state changed")
	}
	return nil
}

// 原musl本地文件解析；不使用os/user/NSS，不输出passwd、成员或原文件字节。
// 重复名字即使相同也拒，避免把多个有效解析次序冒成固定账户语义。
func normalOrdinaryStockUdevAccounts(passwd, group []byte) error {
	parse := func(raw []byte, n int) (map[string][]string, error) {
		if len(raw) == 0 || len(raw) > 64<<10 || raw[len(raw)-1] != '\n' || strings.ContainsAny(string(raw), "\x00\r") {
			return nil, fmt.Errorf("stock account file malformed")
		}
		out := map[string][]string{}
		for _, line := range strings.Split(string(raw), "\n") {
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			f := strings.Split(line, ":")
			if len(f) != n || f[0] == "" || strings.ContainsAny(f[0], " \t+/\\") || out[f[0]] != nil {
				return nil, fmt.Errorf("stock account name or record ambiguous")
			}
			number := []int{2}
			if n == 7 {
				number = append(number, 3)
			}
			for _, x := range number {
				v, e := strconv.ParseUint(f[x], 10, 32)
				if e != nil || strconv.FormatUint(v, 10) != f[x] {
					return nil, fmt.Errorf("stock account id is not canonical")
				}
			}
			out[f[0]] = f
		}
		return out, nil
	}
	p, e := parse(passwd, 7)
	if e != nil {
		return e
	}
	g, e := parse(group, 4)
	if e != nil {
		return e
	}
	if p["root"] == nil || p["root"][2] != "0" || p["root"][3] != "0" || g["root"] == nil || g["root"][2] != "0" {
		return fmt.Errorf("stock root account differs")
	}
	for _, name := range []string{"audio", "cdrom", "dialout", "disk", "input", "kmem", "kvm", "lp", "tape", "tty", "video"} {
		if g[name] == nil {
			return fmt.Errorf("stock rule symbolic group absent")
		}
	}
	return nil
}
func normalOrdinaryStockUdevReadAt(f *os.File, limit int64) ([]byte, error) {
	if f == nil || limit <= 0 || limit > 1<<20 {
		return nil, ErrUnknown
	}
	raw, e := io.ReadAll(io.NewSectionReader(f, 0, limit+1))
	if e != nil || int64(len(raw)) > limit {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("stock input read exceeds original bound"), e)
	}
	return raw, nil
}
func (i *normalOrdinaryStockUdevInputs) checkAccountsLocked() error {
	p, e := normalOrdinaryStockUdevReadAt(i.pins["/etc/passwd"], 64<<10)
	if e != nil {
		return e
	}
	g, e := normalOrdinaryStockUdevReadAt(i.pins["/etc/group"], 64<<10)
	if e != nil {
		return e
	}
	return normalOrdinaryStockUdevAccounts(p, g)
}
func (i *normalOrdinaryStockUdevInputs) checkOriginalSystemManifestLocked() error {
	f, e := nativeBirthOpenRO(NormalSystemSourceManifestPath)
	if e != nil {
		return e
	}
	raw, e := nativeBirthReadBounded(f, 1<<20)
	f.Close()
	if e != nil || physicalDigest(raw) != i.system.binding.SystemManifestSHA256 {
		return fmt.Errorf("stock original System manifest changed")
	}
	return nil
}
func (i *normalOrdinaryStockUdevInputs) recheck(ctx context.Context) error {
	if i == nil || ctx == nil || ctx.Err() != nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return ErrUnknown
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || i.system == nil {
		return ErrUnknown
	}
	s := i.system
	s.mu.Lock()
	defer s.mu.Unlock()
	consumer, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || s.closed || consumer != i.reference.Binding.RootConsumer || s.binding != i.reference.Binding {
		return errors.Join(ErrUnknown, fmt.Errorf("stock input original producer changed"))
	}
	raw, e := json.Marshal(s.observed)
	if e != nil || physicalDigest(raw) != i.reference.SystemSourceSHA256 {
		return ErrUnknown
	}
	if e = i.checkOriginalSystemManifestLocked(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	for _, check := range []func() error{func() error { return i.recheckFilesLocked(ctx) }, i.checkAccountsLocked, func() error { return i.checkClosureLocked(ctx) }, func() error { return i.checkDirectoriesLocked(ctx, false) }, func() error { return i.checkStaticRuleLookupsLocked(ctx, false) }} {
		if e = check(); e != nil {
			return errors.Join(ErrUnknown, e)
		}
	}
	return ctx.Err()
}

// 快照仅为缓存副本；生产者必须在使用前后显式recheck，不从此JSON重建opaque。
func (i *normalOrdinaryStockUdevInputs) snapshot() (normalOrdinaryStockUdevInputReference, normalOrdinaryStockUdevManifest, error) {
	var ref normalOrdinaryStockUdevInputReference
	var mf normalOrdinaryStockUdevManifest
	if i == nil {
		return ref, mf, ErrUnknown
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || i.system == nil || len(i.pins) == 0 {
		return ref, mf, ErrUnknown
	}
	b, e := json.Marshal(i.reference)
	if e != nil {
		return ref, mf, e
	}
	if e = json.Unmarshal(b, &ref); e != nil {
		return ref, mf, e
	}
	b, e = json.Marshal(i.manifest)
	if e != nil {
		return ref, mf, e
	}
	e = json.Unmarshal(b, &mf)
	return ref, mf, e
}
func (i *normalOrdinaryStockUdevInputs) close() {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.closed {
		for _, f := range i.pins {
			f.Close()
		}
		for _, f := range i.directories {
			f.Close()
		}
		for _, f := range i.ruleLookupPins {
			f.Close()
		}
		i.closed = true
	}
}

// 固定原RO包锁必须作为同一个System3的实际config对象存在；不是路径存在或MF常数。
// 返回副本以免后继调用者改原库存。首次pin及每次recheck仍核实际FD/只读祖先/hash。
func normalOrdinaryStockUdevPackageLockEntry(system NormalSystemSourceManifest, m normalOrdinaryStockUdevManifest) (*NormalSystemSourceFile, error) {
	e := normalOrdinaryStockUdevSystemEntry(system, NormalSystemOrdinaryV2PackageLockPath)
	if system.ProtocolVersion != 3 || e == nil || e.Kind != "config" || e.CanonicalPath != e.Path || e.Bytes == 0 || e.Bytes > 256<<10 || e.SHA256 != m.PackageLockSHA256 || !normalOrdinaryStockUdevDigest(e.SHA256) {
		return nil, fmt.Errorf("actual original stock package lock differs or is absent")
	}
	return e, nil
}
