//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Inspect只读取；不落WAL、不设置权限、不挂载、不exec、不下载。
// 复用Source7完整readonly祖先链，不以单文件pin或raw参数中的自报bool放宽。
func InspectNormalSystemSource(ctx context.Context, raw []byte) (NormalSystemSourceObservation, error) {
	var o NormalSystemSourceObservation
	m, e := DecodeNormalSystemSourceManifest(raw)
	if e != nil {
		return o, e
	}
	if e = ctx.Err(); e != nil {
		return o, e
	}
	release, e := kernelText("/proc/sys/kernel/osrelease")
	if e != nil {
		return o, e
	}
	notes, e := physicalKernelNotes()
	if e != nil {
		return o, e
	}
	if release != m.Kernel.KernelRelease || physicalDigest(notes) != m.Kernel.KernelNotesSHA256 {
		return o, fmt.Errorf("system source real kernel differs")
	}
	boot, e := kernelText("/proc/sys/kernel/random/boot_id")
	if e != nil {
		return o, e
	}
	if e = normalSystemCheckRuntimeOverrides("/neonvm/runtime"); e != nil {
		return o, e
	}
	o = NormalSystemSourceObservation{Scope: normalSystemSourceScope, ManifestSHA256: physicalDigest(raw), GuestBootID: boot, Kernel: m.Kernel, RuntimeOverridesAbsent: true}
	canonical, e := json.Marshal(m)
	if e != nil {
		return NormalSystemSourceObservation{}, e
	}
	o.ManifestCanonicalSHA256 = physicalDigest(canonical)
	for _, f := range m.Files {
		if e = ctx.Err(); e != nil {
			return NormalSystemSourceObservation{}, e
		}
		actual, e := nativeBirthCanonical(f.Path)
		if e != nil {
			return NormalSystemSourceObservation{}, e
		}
		if actual != f.CanonicalPath {
			return NormalSystemSourceObservation{}, fmt.Errorf("system source canonical identity differs: %s", f.Path)
		}
		h, e := normalSystemOpenRO(actual)
		if e != nil {
			return NormalSystemSourceObservation{}, e
		}
		var before, after unix.Stat_t
		if e = unix.Fstat(int(h.Fd()), &before); e != nil {
			h.Close()
			return NormalSystemSourceObservation{}, e
		}
		if before.Size <= 0 || uint64(before.Size) != f.Bytes || ((f.Kind == "exec" || f.Kind == "script") && before.Mode&0111 == 0) {
			h.Close()
			return NormalSystemSourceObservation{}, fmt.Errorf("system source file size differs: %s", f.Path)
		}
		digest, e := nativeBirthFileHash(h)
		if e != nil {
			h.Close()
			return NormalSystemSourceObservation{}, e
		}
		if f.Kind == "exec" || f.Kind == "loader" && f.Path != "/etc/ld.so.cache" {
			var magic [4]byte
			_, e = h.ReadAt(magic[:], 0)
			if e != nil || !bytes.Equal(magic[:], []byte{0x7f, 'E', 'L', 'F'}) {
				h.Close()
				return NormalSystemSourceObservation{}, fmt.Errorf("system executable/loader is not ELF")
			}
		}
		if f.Path == "/etc/inittab" {
			b, e := nativeBirthReadBounded(h, 64<<10)
			if e != nil {
				h.Close()
				return NormalSystemSourceObservation{}, e
			}
			if e = normalSystemCheckVersionInittab(m.ProtocolVersion, b); e != nil {
				h.Close()
				return NormalSystemSourceObservation{}, e
			}
			o.InittabSHA256 = digest
			o.FixedInittabObserved = true
		}
		e = unix.Fstat(int(h.Fd()), &after)
		h.Close()
		if e != nil {
			return NormalSystemSourceObservation{}, e
		}
		if before.Dev != after.Dev || before.Ino != after.Ino || before.Size != after.Size || before.Mtim != after.Mtim || before.Ctim != after.Ctim || digest != f.SHA256 {
			return NormalSystemSourceObservation{}, fmt.Errorf("system source changed or hash differs: %s", f.Path)
		}
		o.Files = append(o.Files, NormalSystemSourceFileObservation{Path: f.Path, CanonicalPath: actual, SHA256: digest, Device: uint64(before.Dev), Inode: before.Ino, Bytes: uint64(before.Size)})
	}

	for _, dir := range NormalSystemSourceConfigDirectories() {
		if e = nativeBirthReadonlyPath(filepath.Join(dir, ".source-inventory-check")); e != nil {
			return NormalSystemSourceObservation{}, e
		}
		// 空glob目录没有listed文件的第二条父链兜底；logical symlink及目标本身
		// RO不证明canonical祖先RO，必须对真实base独立核同一完整父链。
		base, err := filepath.EvalSymlinks(dir)
		if err != nil {
			return NormalSystemSourceObservation{}, err
		}
		if err = nativeBirthReadonlyPath(filepath.Join(base, ".source-inventory-check")); err != nil {
			return NormalSystemSourceObservation{}, err
		}
	}
	o.ConfigInventorySHA256, e = normalSystemConfigInventory("/", m)
	if e != nil {
		return NormalSystemSourceObservation{}, e
	}
	o.ConfigInventoryObserved = true
	o.ReadonlyAncestorsObserved = true
	o.MissingSources = []string{"original-image-ISO-process-lifecycle-binding", "complete-static-ELF-loader-and-config-closure", "fixed-argv-environment-actual-exec", "continuous-role-and-kernel-control-writer-enforcement"}
	return o, nil
}

// hash使用SectionReader、ELF magic用ReadAt，均不改变同一已钉FD的offset。
// Gate始终走同一真实reader，不接受外传Observation或bool替代真实事实。
// nil只表示此组件检查成功，不表示原镜像/ISO认证、ELF闭包或normal许可。
func CheckNormalSystemSource(ctx context.Context, raw []byte) (NormalSystemSourceObservation, error) {
	return InspectNormalSystemSource(ctx, raw)
}

func normalSystemForbiddenOverrides() []string {
	return []string{"mounts.sh", "sysctl.conf", "env.sh", "command.sh", "args.sh", "resize-swap-internal.sh"}
}
func normalSystemCheckRuntimeOverrides(dir string) error {
	st, e := os.Lstat(dir)
	if e != nil {
		return e
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("system runtime path is not original directory")
	}
	for _, n := range normalSystemForbiddenOverrides() {
		if _, e := os.Lstat(filepath.Join(dir, n)); !os.IsNotExist(e) {
			return fmt.Errorf("system source forbids runtime executable/tuning override: %s", n)
		}
	}
	return nil
}

// root参数仅给U夹具复用库存算法，生产调用永远是字面量"/"，没有请求path入口。
// RO权限由真实reader在调用前逐祖先验证；这里不以测试参数伪造RO或原来源。
func normalSystemConfigInventory(root string, m NormalSystemSourceManifest) (string, error) {
	expected := map[string]bool{}
	for _, f := range m.Files {
		if f.Kind == "config" {
			expected[f.Path] = true
		}
	}
	var observed []string
	count := 0
	for _, logical := range NormalSystemSourceConfigDirectories() {
		base, e := filepath.EvalSymlinks(filepath.Join(root, logical))
		if e != nil {
			return "", e
		}
		st, e := os.Stat(base)
		if e != nil {
			return "", e
		}
		if !st.IsDir() {
			return "", fmt.Errorf("system config root is not directory")
		}
		e = filepath.WalkDir(base, func(path string, d fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			count++
			if count > 4096 {
				return fmt.Errorf("system config inventory exceeds bound")
			}
			rel, e := filepath.Rel(base, path)
			if e != nil {
				return e
			}
			if rel == "." {
				return nil
			}
			if len(strings.Split(rel, string(filepath.Separator))) > 16 {
				return fmt.Errorf("system config inventory depth exceeds bound")
			}
			if d.IsDir() {
				return nil
			}
			logicalFile := filepath.Join(logical, rel)
			if !expected[logicalFile] {
				return fmt.Errorf("system config has unlisted file: %s", logicalFile)
			}
			info, e := os.Stat(path)
			if e != nil {
				return e
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("system config object is not regular")
			}
			observed = append(observed, logicalFile)
			return nil
		})
		if e != nil {
			return "", e
		}
	}
	sort.Strings(observed)
	return physicalDigest([]byte(strings.Join(observed, "\n"))), nil
}

// 特殊文件不能在类型核验之前阻塞：保持Source7完整RO父链和root权限要求，
// 仅本新reader用NONBLOCK先打开、Fstat拒非regular，不修改或放宽Source7。
func normalSystemOpenRO(path string) (*os.File, error) {
	if e := nativeBirthReadonlyPath(path); e != nil {
		return nil, e
	}
	if e := secureParents(filepath.Dir(path)); e != nil {
		return nil, e
	}
	fd, e := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), path)
	var st unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(fd, &st) != nil || unix.Fstatfs(fd, &fs) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != 0 || st.Mode&0022 != 0 || st.Size <= 0 || st.Size > 256<<20 || fs.Flags&unix.ST_RDONLY == 0 {
		f.Close()
		return nil, fmt.Errorf("system source is not bounded root read-only regular file")
	}
	return f, nil
}
