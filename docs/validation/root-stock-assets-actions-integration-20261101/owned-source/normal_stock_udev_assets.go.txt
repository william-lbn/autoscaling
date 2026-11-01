package main

import (
	"bytes"
	"debug/elf"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

const normalStockAssetsLockScope = "normal-stock-udev-rootdisk-inputs-v1"
const normalStockAssetsACLPath = "/etc/udev/rules.d/99-neon-stock-acl.rules"
const normalStockAssetsACL = "SUBSYSTEM==\"input\", KERNEL==\"event*\", ATTRS{name}==\"Power Button\", OWNER=\"60006\", GROUP=\"60006\", MODE=\"0600\"\nSUBSYSTEM==\"ptp\", KERNEL==\"ptp0\", OWNER=\"60008\", GROUP=\"60008\", MODE=\"0600\"\n"
const normalStockAssetsVirtio = "SUBSYSTEM==\"virtio-ports\", KERNEL==\"vport*\", ATTR{name}==\"?*\", SYMLINK+=\"virtio-ports/$attr{name}\"\n"

// 构建私有溯源输入，不是Guest授权协议；inputs保存实际APK/tar/patch/build-record的SHA。
// files把每个provider实际member与最终rootdisk字节绑定。生成物账户/ACL/99virtio不冒APK。
type normalStockAssetFileLock struct {
	Path   string         `json:"path"`
	Bytes  control.Uint64 `json:"bytes"`
	SHA256 string         `json:"sha256"`
	Input  string         `json:"input"`
	Member string         `json:"member"`
}
type normalStockAssetPackageLock struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Scope           string                     `json:"scope"`
	Inputs          map[string]string          `json:"inputs"`
	Files           []normalStockAssetFileLock `json:"files"`
}
type normalStockAssetRead struct {
	raw       []byte
	canonical string
	mode      os.FileMode
}

func normalStockAssetDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s && s != strings.Repeat("0", 64)
}
func decodeNormalStockAssetPackageLock(raw []byte) (normalStockAssetPackageLock, error) {
	var z normalStockAssetPackageLock
	if len(raw) == 0 || len(raw) > 256<<10 {
		return z, fmt.Errorf("stock actual input lock outside bound")
	}
	if e := control.UnmarshalObject(raw, &z); e != nil {
		return z, e
	}
	if z.ProtocolVersion != 1 || z.Scope != normalStockAssetsLockScope || len(z.Inputs) == 0 || len(z.Inputs) > 256 || len(z.Files) == 0 || len(z.Files) > 100 {
		return z, fmt.Errorf("stock actual input lock contract differs")
	}
	for name, h := range z.Inputs {
		if name == "" || len(name) > 512 || strings.ContainsAny(name, "\x00\r\n") || !normalStockAssetDigest(h) {
			return z, fmt.Errorf("invalid stock original input digest")
		}
	}
	previous := ""
	for _, f := range z.Files {
		if f.Path <= previous || !filepath.IsAbs(f.Path) || filepath.Clean(f.Path) != f.Path || !normalStockAssetDigest(f.SHA256) || f.Bytes == 0 || f.Bytes > 64<<20 || z.Inputs[f.Input] == "" || f.Member == "" || filepath.IsAbs(f.Member) || filepath.Clean(f.Member) != f.Member || f.Member == ".." || strings.HasPrefix(f.Member, "../") || strings.ContainsAny(f.Path+f.Member, "\x00\r\n") {
			return z, fmt.Errorf("stock actual file provenance differs")
		}
		previous = f.Path
	}
	return z, nil
}

// 仅最终合并rootdisk，不跟随Host绝对alias；所有配置与输出祖先禁止别名。
func normalStockAssetReadFile(root, logical string, limit uint64, config bool) (normalStockAssetRead, error) {
	var out normalStockAssetRead
	canonical, e := normalImageCanonical(root, logical)
	if e != nil {
		return out, e
	}
	if config && canonical != logical {
		return out, fmt.Errorf("stock config alias: %s", logical)
	}
	f, e := os.OpenFile(filepath.Join(root, canonical), os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if e != nil {
		return out, e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Size() < 0 || uint64(st.Size()) > limit || st.Mode()&(os.ModeSetuid|os.ModeSetgid) != 0 {
		return out, fmt.Errorf("stock asset regular/mode/size differs: %s", logical)
	}
	raw, e := io.ReadAll(io.NewSectionReader(f, 0, int64(limit)+1))
	if e != nil || int64(len(raw)) != st.Size() {
		return out, fmt.Errorf("stock asset changed during read: %s", logical)
	}
	out = normalStockAssetRead{raw, canonical, st.Mode()}
	return out, nil
}

func normalStockAssetAccounts(passwd, group []byte, uid, gid uint32) ([]byte, []byte, error) {
	// 原base若已有完整九角色只保留原字节，任何局部/冲突账户都拒绝。
	if e := resourceguard.ValidateNormalSystemOrdinaryV2InstalledAccounts(passwd, group, uid, gid); e != nil {
		p, e := ordinaryV2AccountPlan(passwd, group, uid, gid)
		if e != nil {
			return nil, nil, e
		}
		passwd = append(append([]byte(nil), passwd...), p.PasswdAdditions...)
		group = append(append([]byte(nil), group...), p.GroupAdditions...)
	}
	if len(group) == 0 || group[len(group)-1] != '\n' {
		return nil, nil, fmt.Errorf("original group terminator differs")
	}
	names := map[string]uint32{}
	numbers := map[uint32]string{}
	for _, line := range strings.Split(strings.TrimSuffix(string(group), "\n"), "\n") {
		f := strings.Split(line, ":")
		if len(f) != 4 {
			return nil, nil, fmt.Errorf("original group record differs")
		}
		v, e := strconv.ParseUint(f[2], 10, 32)
		if e != nil || strconv.FormatUint(v, 10) != f[2] {
			return nil, nil, fmt.Errorf("original group ID differs")
		}
		names[f[0]] = uint32(v)
		numbers[uint32(v)] = f[0]
	}
	for i, name := range []string{"audio", "cdrom", "dialout", "disk", "input", "kmem", "kvm", "lp", "tape", "tty", "video"} {
		if _, exists := names[name]; exists {
			continue
		}
		candidate := uint32(61001 + i)
		if _, exists := numbers[candidate]; exists || candidate == gid || candidate == uid {
			return nil, nil, fmt.Errorf("fixed stock symbolic group ID collides: %s", name)
		}
		group = append(group, []byte(fmt.Sprintf("%s:x:%d:\n", name, candidate))...)
		names[name] = candidate
		numbers[candidate] = name
	}
	if e := resourceguard.ValidateNormalSystemStockUdevAssetsAccounts(passwd, group, uid, gid); e != nil {
		return nil, nil, e
	}
	return passwd, group, nil
}

func normalStockAssetCheckDirs(root string, expected map[string]string) error {
	known := map[string]bool{}
	for _, logical := range []string{"/lib/udev/rules.d", "/etc/udev/rules.d", "/etc/udev/hwdb.d"} {
		canonical, e := normalImageCanonical(root, logical)
		if e != nil {
			return e
		}
		if canonical != logical {
			return fmt.Errorf("stock config directory alias: %s", logical)
		}
		known[canonical] = true
		entries, e := os.ReadDir(filepath.Join(root, canonical))
		if e != nil {
			return e
		}
		for _, de := range entries {
			p := filepath.Join(logical, de.Name())
			if de.Type()&os.ModeSymlink != 0 || !de.Type().IsRegular() || expected[p] == "" {
				return fmt.Errorf("uncovered actual stock config: %s", p)
			}
		}
	}
	for _, logical := range resourceguard.NormalSystemStockUdevStaticRuleDirectories()[2:] {
		canonical, e := normalImageCanonical(root, logical)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		if known[canonical] {
			continue
		}
		entries, e := os.ReadDir(filepath.Join(root, canonical))
		if e != nil {
			return e
		}
		if len(entries) != 0 {
			return fmt.Errorf("uncovered alternate stock rules: %s", logical)
		}
	}
	for _, logical := range []string{"/lib/udev", "/usr/lib/udev"} {
		canonical, e := normalImageCanonical(root, logical)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		entries, e := os.ReadDir(filepath.Join(root, canonical))
		if e != nil {
			return e
		}
		for _, de := range entries {
			if de.IsDir() {
				if de.Name() != "rules.d" {
					return fmt.Errorf("uncovered helper subdirectory: %s", de.Name())
				}
				continue
			}
			p := "/lib/udev/" + de.Name()
			if expected[p] == "" && p != "/lib/udev/hwdb.bin" {
				return fmt.Errorf("uncovered actual stock helper input: %s", p)
			}
		}
	}
	return nil
}

func normalStockAssetELFClosure(root string, files map[string]normalStockAssetRead, paths map[string]string) error {
	queue := []string{}
	for p, k := range paths {
		if k == "exec" {
			queue = append(queue, p)
		}
	}
	sort.Strings(queue)
	seen := map[string]bool{}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		if seen[p] {
			continue
		}
		seen[p] = true
		if len(seen) > 32 {
			return fmt.Errorf("stock executable closure exceeds bound")
		}
		if _, ok := files[p]; !ok {
			_, limit, allowed := resourceguard.NormalSystemStockUdevAssetClass(p)
			if !allowed {
				return fmt.Errorf("stock dependency outside exact class: %s", p)
			}
			f, e := normalStockAssetReadFile(root, p, limit, false)
			if e != nil {
				return e
			}
			files[p] = f
			paths[p] = "loader"
		}
		f := files[p]
		if paths[p] == "exec" && f.mode&0111 == 0 {
			return fmt.Errorf("stock ELF lacks executable mode: %s", p)
		}
		binary, e := elf.NewFile(bytes.NewReader(f.raw))
		if e != nil {
			return fmt.Errorf("stock executable is not real ELF: %s: %w", p, e)
		}
		if binary.Class != elf.ELFCLASS64 || binary.Data != elf.ELFDATA2LSB || binary.Machine != elf.EM_X86_64 || (binary.Type != elf.ET_EXEC && binary.Type != elf.ET_DYN) {
			binary.Close()
			return fmt.Errorf("stock exact ELF architecture/type differs: %s", p)
		}
		for _, tag := range []elf.DynTag{elf.DT_RPATH, elf.DT_RUNPATH, elf.DT_AUDIT, elf.DT_DEPAUDIT, elf.DT_FILTER, elf.DT_AUXILIARY} {
			v, e := binary.DynValue(tag)
			if e != nil {
				binary.Close()
				return e
			}
			if len(v) != 0 {
				binary.Close()
				return fmt.Errorf("stock ELF has unapproved loader input: %s", p)
			}
		}
		for _, prog := range binary.Progs {
			if prog.Type == elf.PT_INTERP {
				raw, e := io.ReadAll(io.LimitReader(prog.Open(), 4097))
				if e != nil || len(raw) == 0 || len(raw) > 4096 || raw[len(raw)-1] != 0 || string(raw[:len(raw)-1]) != "/lib/ld-musl-x86_64.so.1" {
					binary.Close()
					return fmt.Errorf("stock interpreter differs: %s", p)
				}
				queue = append(queue, "/lib/ld-musl-x86_64.so.1")
			}
		}
		needed, e := binary.DynString(elf.DT_NEEDED)
		binary.Close()
		if e != nil {
			return e
		}
		for _, name := range needed {
			if name == "" || strings.ContainsAny(name, "/\x00\r\n") {
				return fmt.Errorf("stock needed name differs")
			}
			resolved := ""
			for _, dir := range []string{"/lib", "/usr/local/lib", "/usr/lib"} {
				candidate := filepath.Join(dir, name)
				_, e := normalImageCanonical(root, candidate)
				if os.IsNotExist(e) {
					continue
				}
				if e != nil {
					return e
				}
				if _, _, ok := resourceguard.NormalSystemStockUdevAssetClass(candidate); !ok {
					return fmt.Errorf("stock resolved DSO outside inventory: %s", candidate)
				}
				resolved = candidate
				break
			}
			if resolved == "" {
				return fmt.Errorf("stock needed DSO missing: %s", name)
			}
			queue = append(queue, resolved)
		}
	}
	return nil
}

// 新显式2构建分支。此入口真实写最终rootdisk静态资产；不执行udevd、不签Source。
// 完整预检后才改构建目录；清单最后提交，任何错误使本次镜像构建失败。
func prepareNormalSystemStockUdevAssets(root string, packageLockRaw []byte, computeUID, computeGID uint32, controlsVersion uint32) ([]byte, error) {
	if controlsVersion != 2 {
		return nil, fmt.Errorf("stock assets require explicit controls version 2")
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return nil, fmt.Errorf("canonical isolated final rootdisk required")
	}
	actual, e := filepath.EvalSymlinks(root)
	if e != nil || actual != root {
		return nil, fmt.Errorf("rootdisk alias differs")
	}
	lock, e := decodeNormalStockAssetPackageLock(packageLockRaw)
	if e != nil {
		return nil, e
	}
	for _, p := range []string{"/etc/ld.so.preload", "/etc/ld.so.cache", "/etc/ld-musl-x86_64.path"} {
		if _, e := normalImageCanonical(root, p); e == nil {
			return nil, fmt.Errorf("unapproved stock loader configuration: %s", p)
		} else if !os.IsNotExist(e) {
			return nil, e
		}
	}
	paths := resourceguard.NormalSystemSourceStockUdevRequiredFiles()
	delete(paths, resourceguard.NormalSystemStockUdevManifestPath)
	if e = normalStockAssetCheckDirs(root, paths); e != nil {
		return nil, e
	}
	files := map[string]normalStockAssetRead{}
	for p, k := range paths {
		if p == normalStockAssetsACLPath {
			continue
		}
		_, limit, ok := resourceguard.NormalSystemStockUdevAssetClass(p)
		if !ok {
			return nil, fmt.Errorf("fixed class absent")
		}
		f, e := normalStockAssetReadFile(root, p, limit, k == "config")
		if e != nil {
			return nil, e
		}
		files[p] = f
	}
	if !bytes.Equal(files["/lib/udev/rules.d/99-virtio-ports.rules"].raw, []byte(normalStockAssetsVirtio)) {
		return nil, fmt.Errorf("original fixed virtio rule drift")
	}
	for _, p := range []string{"/etc/udev/hwdb.bin", "/lib/udev/hwdb.bin"} {
		_, e := normalImageCanonical(root, p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, e
		}
		_, limit, _ := resourceguard.NormalSystemStockUdevAssetClass(p)
		f, e := normalStockAssetReadFile(root, p, limit, true)
		if e != nil {
			return nil, e
		}
		files[p] = f
		paths[p] = "config"
	}
	if e = normalStockAssetELFClosure(root, files, paths); e != nil {
		return nil, e
	}
	provider := map[string]normalStockAssetFileLock{}
	for _, f := range lock.Files {
		provider[f.Path] = f
	}
	generated := map[string]bool{"/etc/passwd": true, "/etc/group": true, normalStockAssetsACLPath: true, "/lib/udev/rules.d/99-virtio-ports.rules": true}
	for p, f := range files {
		if generated[p] {
			continue
		}
		q, ok := provider[p]
		if !ok || uint64(q.Bytes) != uint64(len(f.raw)) || q.SHA256 != normalDigest(f.raw) {
			return nil, fmt.Errorf("actual provider bytes absent/different in package lock: %s", p)
		}
		delete(provider, p)
	}
	if len(provider) != 0 {
		return nil, fmt.Errorf("package lock contains files outside actual stock closure")
	}
	passwd, group, e := normalStockAssetAccounts(files["/etc/passwd"].raw, files["/etc/group"].raw, computeUID, computeGID)
	if e != nil {
		return nil, e
	}
	files["/etc/passwd"] = normalStockAssetRead{raw: passwd}
	files["/etc/group"] = normalStockAssetRead{raw: group}
	files[normalStockAssetsACLPath] = normalStockAssetRead{raw: []byte(normalStockAssetsACL)}
	var entries []resourceguard.NormalSystemSourceFile
	for p, f := range files {
		entries = append(entries, resourceguard.NormalSystemSourceFile{Path: p, CanonicalPath: p, Kind: paths[p], Bytes: uint64(len(f.raw)), SHA256: normalDigest(f.raw)})
	}
	raw, e := resourceguard.NormalSystemStockUdevAssetsManifestJSON(normalDigest(packageLockRaw), entries)
	if e != nil {
		return nil, e
	}
	outputs := map[string][]byte{"/etc/passwd": passwd, "/etc/group": group, normalStockAssetsACLPath: []byte(normalStockAssetsACL), resourceguard.NormalSystemOrdinaryV2PackageLockPath: packageLockRaw, resourceguard.NormalSystemStockUdevManifestPath: raw}
	for p, want := range outputs {
		if p == "/etc/passwd" || p == "/etc/group" {
			continue
		}
		canonical, e := normalImageCanonical(root, p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil || canonical != p {
			return nil, fmt.Errorf("stock output alias differs: %s", p)
		}
		have, e := normalStockAssetReadFile(root, p, 256<<10, true)
		if e != nil || !bytes.Equal(have.raw, want) {
			return nil, fmt.Errorf("preexisting stock output differs: %s", p)
		}
	}
	if e = normalStockAssetPrepareInitialRoot(root, false); e != nil {
		return nil, e
	}
	if e = normalStockAssetPrepareInitialRoot(root, true); e != nil {
		return nil, e
	}
	// 固定输出父目录先逐段建Root0755；不借MkdirAll跟随未知alias。
	names := []string{"/etc/passwd", "/etc/group", normalStockAssetsACLPath, resourceguard.NormalSystemOrdinaryV2PackageLockPath, resourceguard.NormalSystemStockUdevManifestPath}
	for _, p := range names {
		if e = normalStockAssetWrite(root, p, outputs[p], p == "/etc/passwd" || p == "/etc/group"); e != nil {
			return nil, e
		}
	}
	return raw, nil
}

func normalStockAssetPrepareInitialRoot(root string, apply bool) error {
	// 只落实原RW anchors；运行期私有子目录必须留给P2原intent/identity/complete事务创建。
	// 特别是/run/neon-ordinary若存在必须空，不能在构建期预造其子目录绕过原空目录检查。
	for _, p := range []string{"/run", "/var/log", "/var/lib/chrony", "/run/neon-ordinary", "/run/chrony", "/var/log/vector", "/var/log/chrony"} {
		optional := p != "/run" && p != "/var/log" && p != "/var/lib/chrony"
		canonical, e := normalImageCanonical(root, p)
		if os.IsNotExist(e) {
			if !apply || optional {
				continue
			}
			if e = normalStockAssetParents(root, p+"/x"); e != nil {
				return e
			}
			canonical = p
		} else if e != nil {
			return e
		}
		if canonical != p {
			return fmt.Errorf("initial ordinary anchor alias: %s", p)
		}
		st, e := os.Lstat(filepath.Join(root, p))
		if e != nil || !st.IsDir() {
			return fmt.Errorf("initial ordinary anchor not directory: %s", p)
		}
		if p != "/run" && p != "/var/log" {
			entries, e := os.ReadDir(filepath.Join(root, p))
			if e != nil {
				return e
			}
			if len(entries) != 0 {
				return fmt.Errorf("initial ordinary anchor nonempty: %s", p)
			}
		}
		if apply {
			if e = os.Chown(filepath.Join(root, p), 0, 0); e != nil {
				return e
			}
			if e = os.Chmod(filepath.Join(root, p), 0755); e != nil {
				return e
			}
		}
	}
	dns, e := normalStockAssetReadFile(root, "/etc/resolv.conf", 64<<10, true)
	if e != nil {
		return e
	}
	_ = dns
	st, e := os.Lstat(filepath.Join(root, "/etc/resolv.conf"))
	if e != nil {
		return e
	}
	if st.Sys().(*syscall.Stat_t).Nlink != 1 {
		return fmt.Errorf("original DNS inode has another hardlink")
	}
	if apply {
		if e = os.Chown(filepath.Join(root, "/etc/resolv.conf"), 0, 0); e != nil {
			return e
		}
		return os.Chmod(filepath.Join(root, "/etc/resolv.conf"), 0644)
	}
	return nil
}

func normalStockAssetParents(root, p string) error {
	parts := strings.Split(strings.TrimPrefix(filepath.Dir(p), "/"), "/")
	cur := root
	for _, part := range parts {
		cur = filepath.Join(cur, part)
		st, e := os.Lstat(cur)
		if os.IsNotExist(e) {
			if e = os.Mkdir(cur, 0755); e != nil {
				return e
			}
			if e = os.Chown(cur, 0, 0); e != nil {
				return e
			}
			continue
		}
		if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("stock output parent differs: %s", cur)
		}
	}
	return nil
}
func normalStockAssetWrite(root, p string, raw []byte, account bool) error {
	if e := normalStockAssetParents(root, p); e != nil {
		return e
	}
	path := filepath.Join(root, p)
	if f, e := normalStockAssetReadFile(root, p, 256<<10, true); e == nil && bytes.Equal(f.raw, raw) {
		if e = os.Chown(path, 0, 0); e != nil {
			return e
		}
		mode := os.FileMode(0444)
		if account {
			mode = 0644
		}
		return os.Chmod(path, mode)
	}
	tmp, e := os.CreateTemp(filepath.Dir(path), ".stock-asset-")
	if e != nil {
		return e
	}
	name := tmp.Name()
	defer os.Remove(name)
	mode := os.FileMode(0444)
	if account {
		mode = 0644
	}
	if e = tmp.Chmod(mode); e == nil {
		e = tmp.Chown(0, 0)
	}
	if e == nil {
		_, e = tmp.Write(raw)
	}
	if e == nil {
		e = tmp.Sync()
	}
	closeErr := tmp.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	dir, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
