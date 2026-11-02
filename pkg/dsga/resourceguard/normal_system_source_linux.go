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
	"sync"
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
	files, inittabSHA, fixedInittab, e := normalSystemInspectSourceFiles(ctx, m.ProtocolVersion, m.Files)
	if e != nil {
		return NormalSystemSourceObservation{}, e
	}
	o.Files = files
	o.InittabSHA256 = inittabSHA
	o.FixedInittabObserved = fixedInittab

	for _, dir := range NormalSystemSourceConfigDirectoriesForManifest(m) {
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

// 固定两个独立reader；每次调用仍完整核所有文件，不跨调用保存结果或共享FD。
// 内部取消只停止分配新job；已分配job按原ctx读完并close，join后才返回。
const normalSystemSourceFileWorkers = 2

type normalSystemParallelFileResult struct {
	file                 NormalSystemSourceFileObservation
	inittabSHA256        string
	fixedInittabObserved bool
	err                  error
	done                 bool
}

func normalSystemInspectSourceFiles(ctx context.Context, version uint32, files []NormalSystemSourceFile) ([]NormalSystemSourceFileObservation, string, bool, error) {
	if ctx == nil {
		return nil, "", false, fmt.Errorf("source file context absent")
	}
	if e := ctx.Err(); e != nil {
		return nil, "", false, e
	}
	results := make([]normalSystemParallelFileResult, len(files))
	jobs := make(chan int)
	scheduling, stop := context.WithCancel(ctx)
	defer stop()
	var workers sync.WaitGroup
	for n := 0; n < normalSystemSourceFileWorkers; n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				result, e := normalSystemInspectSourceFile(ctx, version, files[index])
				result.err = e
				result.done = true
				results[index] = result
				if e != nil {
					stop()
				}
			}
		}()
	}
dispatch:
	for index := range files {
		if scheduling.Err() != nil {
			break
		}
		select {
		case <-scheduling.Done():
			break dispatch
		case jobs <- index:
		}
	}
	close(jobs)
	workers.Wait()
	// 按原清单顺序报告首个实际错误；没有部分观测从失败调用流出。
	for _, result := range results {
		if result.err != nil {
			return nil, "", false, result.err
		}
	}
	if e := ctx.Err(); e != nil {
		return nil, "", false, e
	}
	for _, result := range results {
		if !result.done {
			return nil, "", false, fmt.Errorf("source file worker did not complete")
		}
	}
	observed := make([]NormalSystemSourceFileObservation, len(files))
	tab := ""
	fixed := false
	for index, result := range results {
		observed[index] = result.file
		if result.fixedInittabObserved {
			tab = result.inittabSHA256
			fixed = true
		}
	}
	return observed, tab, fixed, nil
}

// 逐文件原检查保持原顺序；本reader只拥有本次open的FD，返回前必close。
func normalSystemInspectSourceFile(ctx context.Context, version uint32, f NormalSystemSourceFile) (normalSystemParallelFileResult, error) {
	var result normalSystemParallelFileResult
	var e error
	if e = ctx.Err(); e != nil {
		return normalSystemParallelFileResult{}, e
	}
	actual, e := nativeBirthCanonical(f.Path)
	if e != nil {
		return normalSystemParallelFileResult{}, e
	}
	if actual != f.CanonicalPath {
		return normalSystemParallelFileResult{}, fmt.Errorf("system source canonical identity differs: %s", f.Path)
	}
	h, e := normalSystemOpenRO(actual)
	if e != nil {
		return normalSystemParallelFileResult{}, e
	}
	defer h.Close()
	var before, after unix.Stat_t
	if e = unix.Fstat(int(h.Fd()), &before); e != nil {
		return normalSystemParallelFileResult{}, e
	}
	if before.Size <= 0 || uint64(before.Size) != f.Bytes || ((f.Kind == "exec" || f.Kind == "script") && before.Mode&0111 == 0) {
		return normalSystemParallelFileResult{}, fmt.Errorf("system source file size differs: %s", f.Path)
	}
	digest, e := nativeBirthFileHash(h)
	if e != nil {
		return normalSystemParallelFileResult{}, e
	}
	if f.Kind == "exec" || f.Kind == "loader" && f.Path != "/etc/ld.so.cache" {
		var magic [4]byte
		_, e = h.ReadAt(magic[:], 0)
		if e != nil || !bytes.Equal(magic[:], []byte{0x7f, 'E', 'L', 'F'}) {

			return normalSystemParallelFileResult{}, fmt.Errorf("system executable/loader is not ELF")
		}
	}
	if f.Path == "/etc/inittab" {
		b, e := nativeBirthReadBounded(h, 64<<10)
		if e != nil {

			return normalSystemParallelFileResult{}, e
		}
		if e = normalSystemCheckRuntimeInittab(version, b); e != nil {

			return normalSystemParallelFileResult{}, e
		}
		result.inittabSHA256 = digest
		result.fixedInittabObserved = true
	}
	e = unix.Fstat(int(h.Fd()), &after)
	if e != nil {
		return normalSystemParallelFileResult{}, e
	}
	if before.Dev != after.Dev || before.Ino != after.Ino || before.Size != after.Size || before.Mtim != after.Mtim || before.Ctim != after.Ctim || digest != f.SHA256 {
		return normalSystemParallelFileResult{}, fmt.Errorf("system source changed or hash differs: %s", f.Path)
	}
	result.file = NormalSystemSourceFileObservation{Path: f.Path, CanonicalPath: actual, SHA256: digest, Device: uint64(before.Dev), Inode: before.Ino, Bytes: uint64(before.Size)}

	if e = ctx.Err(); e != nil {
		return normalSystemParallelFileResult{}, e
	}
	return result, nil
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
	for _, logical := range NormalSystemSourceConfigDirectoriesForManifest(m) {
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

// 只读部署合同必须取原sealed Runtime；裸table/model不产生Source或normal资格。
// 无新模式时保留旧完整version检查，trusted仍逐字匹配自己的唯一固定表。
func normalSystemCheckRuntimeInittab(version uint32, raw []byte) error {
	source, e := sealedRuntimeSource()
	if e != nil {
		return normalSystemCheckVersionInittab(version, raw)
	}
	return normalSystemCheckDeploymentInittab(version, source.Config.ExecutionModel, raw)
}
func normalSystemCheckDeploymentInittab(version uint32, model string, raw []byte) error {
	if model == "" {
		return normalSystemCheckVersionInittab(version, raw)
	}
	if model != TrustedControllerExecutionModel || version != 3 || !bytes.Equal(raw, []byte(strings.Join(TrustedControllerInitEntries(), "\n")+"\n")) {
		return fmt.Errorf("system inittab differs from sealed fixed deployment contract")
	}
	return nil
}
