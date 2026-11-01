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
	"syscall"

	"golang.org/x/sys/unix"
)

// NativeBirthBuildFileUnchanged仅比较构建文件身份，排除读取本身可能更新的atime。
func NativeBirthBuildFileUnchanged(a, b os.FileInfo) bool {
	if a == nil || b == nil {
		return false
	}
	x, ok := a.Sys().(*syscall.Stat_t)
	y, good := b.Sys().(*syscall.Stat_t)
	return ok && good && x.Dev == y.Dev && x.Ino == y.Ino && x.Size == y.Size && x.Mode == y.Mode && x.Uid == y.Uid && x.Gid == y.Gid && x.Nlink == y.Nlink && x.Mtim == y.Mtim && x.Ctim == y.Ctim
}

// BuildNativeBirthManifestJSON只生成构建声明。访问器由Builder的隔离rootfs提供，
// 不产生运行来源/PG出生/normal资格；实际Guest仍逐阶段走原只读来源验证。
// 复用原raw ELF parser、清单validator和固定默认目录，不执行输入ELF。
func BuildNativeBirthManifestJSON(seedRaw, systemRaw []byte, canonical func(string) (string, error), open func(string) (*os.File, error), lstat func(string) (os.FileInfo, error)) ([]byte, error) {
	if runtime.GOARCH != "amd64" || len(seedRaw) == 0 || len(seedRaw) > 32<<10 || canonical == nil || open == nil || lstat == nil {
		return nil, fmt.Errorf("bounded native image seed and concrete rootfs readers required")
	}
	var m nativeBirthBuildManifest
	if e := json.Unmarshal(seedRaw, &m); e != nil {
		return nil, e
	}
	system, e := DecodeNormalSystemSourceManifest(systemRaw)
	if e != nil {
		return nil, fmt.Errorf("native image build requires exact final System3 bytes: %w", e)
	}
	if system.ProtocolVersion != 3 {
		return nil, fmt.Errorf("native image build requires System3")
	}
	identities := map[string]unix.Stat_t{}
	// 同System3也可能是旧controls1；必须实际读取该固定config并核原清单SHA，
	// 再复用完整ordinary2合同解码，不能只信CLI flag或一个stock selector路径。
	controlsFound := false
	for _, entry := range system.Files {
		if entry.Path != NormalSystemOrdinaryV2ManifestPath {
			continue
		}
		if controlsFound || entry.Kind != "config" || entry.CanonicalPath != entry.Path || entry.Bytes == 0 || entry.Bytes > 32<<10 {
			return nil, fmt.Errorf("native image requires one fixed original controls2 config")
		}
		controlsFound = true
		actual, e := canonical(entry.Path)
		if e != nil || actual != entry.CanonicalPath {
			return nil, fmt.Errorf("native image controls canonical path differs")
		}
		f, e := open(actual)
		if e != nil {
			return nil, e
		}
		var before, after unix.Stat_t
		if e = unix.Fstat(int(f.Fd()), &before); e != nil {
			f.Close()
			return nil, e
		}
		if before.Mode&unix.S_IFMT != unix.S_IFREG || before.Uid != 0 || before.Mode&0022 != 0 || before.Size != int64(entry.Bytes) {
			f.Close()
			return nil, fmt.Errorf("native image controls file differs")
		}
		raw, readErr := io.ReadAll(io.NewSectionReader(f, 0, 32<<10+1))
		statErr := unix.Fstat(int(f.Fd()), &after)
		closeErr := f.Close()
		if readErr != nil || statErr != nil || closeErr != nil || uint64(len(raw)) != entry.Bytes || physicalDigest(raw) != entry.SHA256 || before.Dev != after.Dev || before.Ino != after.Ino || before.Size != after.Size || before.Mode != after.Mode || before.Uid != after.Uid || before.Gid != after.Gid || before.Nlink != after.Nlink || before.Ctim != after.Ctim || before.Mtim != after.Mtim {
			return nil, fmt.Errorf("native image original controls bytes or identity changed")
		}
		if _, e = DecodeNormalSystemOrdinaryV2Manifest(raw); e != nil {
			return nil, e
		}
		identities[actual] = after
	}
	if !controlsFound {
		return nil, fmt.Errorf("native image controls2 entry missing")
	}
	for _, name := range []string{"/etc/ld.so.cache", "/etc/ld.so.preload"} {
		if _, e := canonical(filepath.Dir(name)); e != nil {
			return nil, e
		}
		if _, e := lstat(name); !os.IsNotExist(e) {
			return nil, fmt.Errorf("native image rejects loader control or unknown: %s", name)
		}
	}
	for _, dir := range nativeBirthLoaderDirs {
		if _, e := lstat(dir); os.IsNotExist(e) {
			continue
		} else if e != nil {
			return nil, e
		}
		resolved, e := canonical(dir)
		if e != nil {
			return nil, e
		}
		st, e := lstat(resolved)
		if e != nil || !st.IsDir() {
			return nil, fmt.Errorf("native image loader directory differs: %s", dir)
		}
		if _, e := lstat(filepath.Join(dir, "glibc-hwcaps")); !os.IsNotExist(e) {
			return nil, fmt.Errorf("native image rejects hwcaps or unknown: %s", dir)
		}
	}
	pg, e := canonical(m.LaunchPath)
	if e != nil {
		return nil, e
	}
	read := func(path string, executable bool) (nativeBirthELFMetadata, string, error) {
		var z nativeBirthELFMetadata
		resolved, e := canonical(path)
		if e != nil || resolved != path {
			return z, "", fmt.Errorf("native image canonical file differs: %s", path)
		}
		f, e := open(path)
		if e != nil {
			return z, "", e
		}
		defer f.Close()
		var before, after unix.Stat_t
		if e = unix.Fstat(int(f.Fd()), &before); e != nil {
			return z, "", e
		}
		if before.Mode&unix.S_IFMT != unix.S_IFREG || before.Uid != 0 || before.Mode&0022 != 0 || before.Size <= 0 || before.Size > 256<<20 || executable && before.Mode&0111 == 0 {
			return z, "", fmt.Errorf("native image bounded root-controlled regular file differs: %s", path)
		}
		meta, e := nativeBirthParseELF(f)
		if e != nil {
			return z, "", e
		}
		h, e := nativeBirthFileHash(f)
		if e != nil {
			return z, "", e
		}
		if e = unix.Fstat(int(f.Fd()), &after); e != nil {
			return z, "", e
		}
		if before.Dev != after.Dev || before.Ino != after.Ino || before.Size != after.Size || before.Mode != after.Mode || before.Uid != after.Uid || before.Gid != after.Gid || before.Nlink != after.Nlink || before.Ctim != after.Ctim || before.Mtim != after.Mtim {
			return z, "", fmt.Errorf("native image file changed while collecting: %s", path)
		}
		// 同FD前后身份之外也核路径仍指该对象；构建并发变动不能输出旧对象声明。
		current, e := open(path)
		if e != nil {
			return z, "", e
		}
		var now unix.Stat_t
		e = unix.Fstat(int(current.Fd()), &now)
		closeErr := current.Close()
		if e != nil || closeErr != nil || now.Dev != after.Dev || now.Ino != after.Ino || now.Mode != after.Mode || now.Uid != after.Uid || now.Gid != after.Gid || now.Nlink != after.Nlink || now.Size != after.Size || now.Ctim != after.Ctim || now.Mtim != after.Mtim {
			return z, "", fmt.Errorf("native image file path changed after collection: %s", path)
		}
		identities[path] = after
		return meta, h, nil
	}
	files := map[string]nativeBirthBuildFile{}
	var interpreter string
	var visit func(string) error
	visit = func(path string) error {
		if _, ok := files[path]; ok {
			return nil
		}
		if len(files) >= 64 {
			return fmt.Errorf("native image closure exceeds original 64-file bound")
		}
		meta, h, e := read(path, path == pg)
		if e != nil {
			return e
		}
		files[path] = nativeBirthBuildFile{Path: path, SHA256: h}
		if path == pg && meta.Interpreter != "" {
			interpreter, e = canonical(meta.Interpreter)
			if e != nil {
				return e
			}
		}
		for _, name := range meta.Needed {
			found := ""
			for _, dir := range nativeBirthLoaderDirs {
				candidate := filepath.Join(dir, name)
				if _, e := lstat(candidate); os.IsNotExist(e) {
					continue
				} else if e != nil {
					return e
				}
				found, e = canonical(candidate)
				if e != nil {
					return e
				}
				break // 第一命中不合格必须拒绝，不跳到后面的同名库。
			}
			if found == "" {
				return fmt.Errorf("native image dependency missing in fixed loader dirs: %s", name)
			}
			if e = visit(found); e != nil {
				return e
			}
		}
		return nil
	}
	if e = visit(pg); e != nil {
		return nil, e
	}
	if interpreter != "" {
		if e = visit(interpreter); e != nil {
			return nil, e
		}
	}
	if m.ArrayHelper != nil {
		meta, h, e := read(m.ArrayHelper.Path, true)
		if e != nil || h != m.ArrayHelper.SHA256 || meta.Interpreter != "" || len(meta.Needed) != 0 {
			return nil, fmt.Errorf("native image original static array helper differs")
		}
		if _, reached := files[m.ArrayHelper.Path]; reached {
			return nil, fmt.Errorf("native image helper is not a PG dependency")
		}
	}
	for path, expected := range identities {
		f, e := open(path)
		if e != nil {
			return nil, e
		}
		var current unix.Stat_t
		e = unix.Fstat(int(f.Fd()), &current)
		closeErr := f.Close()
		if e != nil || closeErr != nil || current.Dev != expected.Dev || current.Ino != expected.Ino || current.Size != expected.Size || current.Mode != expected.Mode || current.Uid != expected.Uid || current.Gid != expected.Gid || current.Nlink != expected.Nlink || current.Ctim != expected.Ctim || current.Mtim != expected.Mtim {
			return nil, fmt.Errorf("native image closure changed before publication: %s", path)
		}
	}
	m.PostgresPath, m.InterpreterPath = pg, interpreter
	m.Files = nil
	for _, item := range files {
		m.Files = append(m.Files, item)
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	m.RuntimeClosure = &nativeRuntimeClosureBuild{ProtocolVersion: 1, SystemManifestSHA256: physicalDigest(systemRaw)}
	if e = m.validate(); e != nil {
		return nil, e
	}
	raw, e := json.Marshal(m)
	if e != nil || len(raw) > 32<<10 {
		return nil, fmt.Errorf("native image manifest exceeds original bound")
	}
	return raw, nil
}
