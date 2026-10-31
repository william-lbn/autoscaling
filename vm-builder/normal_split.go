package main

import (
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"syscall"

	"github.com/alessio/shellescape"
	"github.com/distribution/reference"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

const normalSplitBuildMode = "normal-split-v1"
const normalPlanPath = "/neonvm/config/dsga-normal-build-plan.json"

// 这是构建输入，不是Guest授权协议。原ISO/镜像来源、固定角色和ELF依赖闭包
// 仍须独立封存；不因用户给了合格JSON就发normal许可。Env不打印到构建日志。
type normalBuildPlan struct {
	ProtocolVersion uint32   `json:"protocol_version"`
	ComputeUID      uint32   `json:"compute_uid"`
	ComputeGID      uint32   `json:"compute_gid"`
	ComputeArgv     []string `json:"compute_argv"`
	ComputeEnv      []string `json:"compute_env"`
	LoaderPaths     []string `json:"loader_paths"`
}

var normalEnvName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

func normalDigest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func decodeNormalBuildPlan(raw []byte) (normalBuildPlan, error) {
	var p normalBuildPlan
	if len(raw) == 0 || len(raw) > 64<<10 {
		return p, fmt.Errorf("normal build plan exceeds bound")
	}
	if e := control.UnmarshalObject(raw, &p); e != nil {
		return p, e
	}
	if p.ProtocolVersion != 1 || p.ComputeEnv == nil || p.LoaderPaths == nil || p.ComputeUID == 0 || p.ComputeGID == 0 || p.ComputeUID > 1<<31-1 || p.ComputeGID > 1<<31-1 || len(p.ComputeArgv) == 0 || len(p.ComputeArgv) > 64 || p.ComputeArgv[0] != "/usr/local/bin/compute_ctl" || len(p.ComputeEnv) > 128 || len(p.LoaderPaths) > 128 {
		return p, fmt.Errorf("normal build plan requires exact nonroot compute identity and fixed compute_ctl")
	}
	for _, a := range p.ComputeArgv {
		if len(a) > 4096 || strings.ContainsAny(a, "\x00\r\n") {
			return p, fmt.Errorf("invalid fixed compute argv")
		}
	}
	pgdata := ""
	for i, a := range p.ComputeArgv {
		if a == "-D" || a == "--pgdata" {
			if pgdata != "" || i+1 >= len(p.ComputeArgv) {
				return p, fmt.Errorf("exact PGDATA argument required")
			}
			pgdata = p.ComputeArgv[i+1]
		} else if strings.HasPrefix(a, "--pgdata=") {
			if pgdata != "" {
				return p, fmt.Errorf("duplicate PGDATA")
			}
			pgdata = strings.TrimPrefix(a, "--pgdata=")
		}
	}
	if filepath.Dir(pgdata) != "/var/db/postgres" || filepath.Clean(pgdata) != pgdata || strings.ContainsAny(pgdata, "\x00\r\n") {
		return p, fmt.Errorf("normal profile requires explicit PGDATA child of original persistent parent")
	}
	prev := ""
	for _, v := range p.ComputeEnv {
		k, _, ok := strings.Cut(v, "=")
		if !ok || !normalEnvName.MatchString(k) || k <= prev || strings.HasPrefix(k, "LD_") || k == "ENV" || k == "BASH_ENV" || k == "IFS" || k == "SHELLOPTS" || strings.ContainsAny(v, "\x00\r\n") || len(v) > 8192 {
			return p, fmt.Errorf("compute env must be explicit sorted unique safe entries")
		}
		prev = k
	}
	prev = ""
	for _, v := range p.LoaderPaths {
		if v <= prev || !normalLoaderPath(v) {
			return p, fmt.Errorf("loader inventory must be fixed sorted unique paths")
		}
		prev = v
	}
	return p, nil
}
func normalLoaderPath(p string) bool {
	if !filepath.IsAbs(p) || filepath.Clean(p) != p || strings.ContainsAny(p, "\x00\r\n") {
		return false
	}
	for _, s := range []string{"/lib/", "/lib64/", "/usr/lib/", "/usr/lib64/", "/neonvm/lib/"} {
		if strings.HasPrefix(p, s) {
			return true
		}
	}
	return p == "/etc/ld.so.cache"
}
func validateNormalBuildSelection(mode, arch string, spec *imageSpec, args []string, plan []byte) error {
	if mode == "legacy" {
		if len(plan) != 0 {
			return fmt.Errorf("normal plan cannot affect legacy build")
		}
		return nil
	}
	if mode != normalSplitBuildMode || arch != targetArchLinuxAmd64 {
		return fmt.Errorf("unsupported fixed System build mode or architecture")
	}
	// spec即便内容空也不接受：不让后继增加字段悄悄开任意Docker/shell入口。
	if spec != nil || len(args) != 0 {
		return fmt.Errorf("normal build forbids custom spec and build-arg overrides")
	}
	_, e := decodeNormalBuildPlan(plan)
	return e
}
func normalComputeScript(p normalBuildPlan) string {
	args := []string{"/neonvm/bin/su-exec", fmt.Sprintf("%d:%d", p.ComputeUID, p.ComputeGID), "/neonvm/bin/busybox", "env", "-i"}
	args = append(args, p.ComputeEnv...)
	args = append(args, p.ComputeArgv...)
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = shellescape.Quote(a)
	}
	// 不生成可变vmstarter.sh，不读runtime command/env，不使用eval或二次shell引用。
	return "#!/neonvm/bin/sh\nset -eu\n/neonvm/bin/dsga-resource-guard check-guest >/dev/null || exit 1\n[ -f /run/neon-dsga/vmstart.allowed ] || exit 1\nexec /neonvm/bin/busybox flock -x /run/neon-dsga/vmstart.lock " + strings.Join(quoted, " ") + "\n"
}
func normalBuilderExecutable() ([]byte, error) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return nil, fmt.Errorf("normal build final inventory tool requires Linux amd64")
	}
	p, e := os.Executable()
	if e != nil {
		return nil, e
	}
	f, e := elf.Open(p)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	for _, s := range f.Progs {
		if s.Type == elf.PT_INTERP {
			return nil, fmt.Errorf("normal final inventory tool must be static (CGO_ENABLED=0)")
		}
	}
	b, e := os.ReadFile(p)
	if e != nil || len(b) == 0 || len(b) > 256<<20 {
		return nil, fmt.Errorf("static inventory tool outside bound")
	}
	return b, nil
}

// 在构建容器里解析最终rootdisk中的绝对symlink，绝不让/image/lib→/lib
// 跳出到构建Host。调用者给的root仅构建工具使用，不是生产观察API。
func normalImageCanonical(root, logical string) (string, error) {
	if !filepath.IsAbs(root) || !filepath.IsAbs(logical) || filepath.Clean(logical) != logical || strings.ContainsAny(logical, "\x00\r\n") {
		return "", fmt.Errorf("invalid image path")
	}
	pending := strings.Split(strings.TrimPrefix(logical, "/"), "/")
	var done []string
	links := 0
	for len(pending) > 0 {
		c := pending[0]
		pending = pending[1:]
		if c == "" || c == "." {
			continue
		}
		if c == ".." {
			if len(done) == 0 {
				return "", fmt.Errorf("image alias escapes root")
			}
			done = done[:len(done)-1]
			continue
		}
		at := filepath.Join(append([]string{root}, append(done, c)...)...)
		st, e := os.Lstat(at)
		if e != nil {
			return "", e
		}
		if st.Mode()&os.ModeSymlink == 0 {
			done = append(done, c)
			continue
		}
		links++
		if links > 40 {
			return "", fmt.Errorf("image alias depth exceeds bound")
		}
		v, e := os.Readlink(at)
		if e != nil {
			return "", e
		}
		if filepath.IsAbs(v) {
			done = nil
		}
		pending = append(strings.Split(v, "/"), pending...)
	}
	return "/" + filepath.Join(done...), nil
}
func generateNormalSystemManifest(root string, plan normalBuildPlan, baseSHA string) ([]byte, error) {
	if len(baseSHA) != 64 {
		return nil, fmt.Errorf("original base image digest absent")
	}
	b, e := hex.DecodeString(baseSHA)
	if e != nil || hex.EncodeToString(b) != baseSHA {
		return nil, fmt.Errorf("base image digest not canonical")
	}
	ref, e := resourceguard.LookupPhysicalKernelBuild(resourceguard.NormalSplitV1PhysicalKernelProfileID)
	if e != nil {
		return nil, e
	}
	argv, _ := json.Marshal(plan.ComputeArgv)
	env, _ := json.Marshal(plan.ComputeEnv)
	m := resourceguard.NormalSystemSourceManifest{ProtocolVersion: 2, ProfileID: resourceguard.NormalSystemSourceV2ProfileID, Kernel: ref, DeclaredImageSHA256: baseSHA, ComputePath: "/usr/local/bin/compute_ctl", ComputeArgvSHA256: normalDigest(argv), ComputeEnvironmentSHA256: normalDigest(env)}
	paths, e := resourceguard.NormalSystemSourceRequiredFilesForVersion(2)
	if e != nil {
		return nil, e
	}
	for _, p := range plan.LoaderPaths {
		paths[p] = "loader"
	}
	// 固定四棵配置目录全量收录，不让空glob或未声明新文件绕过库存。别跟随目录alias。
	count := 0
	for _, d := range resourceguard.NormalSystemSourceConfigDirectories() {
		base, e := normalImageCanonical(root, d)
		if e != nil {
			return nil, e
		}
		e = filepath.WalkDir(filepath.Join(root, base), func(p string, de os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			count++
			if count > 4096 {
				return fmt.Errorf("config inventory exceeds bound")
			}
			rel, e := filepath.Rel(filepath.Join(root, base), p)
			if e != nil {
				return e
			}
			if rel == "." {
				return nil
			}
			if len(strings.Split(rel, string(filepath.Separator))) > 16 {
				return fmt.Errorf("config depth exceeds bound")
			}
			if de.IsDir() {
				return nil
			}
			if de.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("config alias unsupported")
			}
			if !de.Type().IsRegular() {
				return fmt.Errorf("config must be regular")
			}
			paths[filepath.Join(d, rel)] = "config"
			return nil
		})
		if e != nil {
			return nil, e
		}
	}
	names := make([]string, 0, len(paths))
	for p := range paths {
		names = append(names, p)
	}
	sort.Strings(names)
	var total int64
	for _, p := range names {
		canonical, e := normalImageCanonical(root, p)
		if e != nil {
			return nil, e
		}
		// 清单入口不等待FIFO/设备；先以非阻塞且不跟随末端别名打开，再以同FD核普通文件。
		f, e := os.OpenFile(filepath.Join(root, canonical), os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
		if e != nil {
			return nil, e
		}
		st, e := f.Stat()
		if e != nil || !st.Mode().IsRegular() || st.Size() <= 0 || st.Size() > 256<<20 {
			f.Close()
			return nil, fmt.Errorf("image source file invalid: %s", p)
		}
		total += st.Size()
		if total > 512<<20 {
			f.Close()
			return nil, fmt.Errorf("source inventory aggregate exceeds bound")
		}
		if paths[p] == "exec" || (paths[p] == "loader" && p != "/etc/ld.so.cache") {
			var magic [4]byte
			if _, e := f.ReadAt(magic[:], 0); e != nil || magic != [4]byte{0x7f, 'E', 'L', 'F'} {
				f.Close()
				return nil, fmt.Errorf("final source executable is not ELF: %s", p)
			}
		}
		if paths[p] == "exec" || paths[p] == "script" {
			if st.Mode()&0111 == 0 {
				f.Close()
				return nil, fmt.Errorf("source executable lacks mode: %s", p)
			}
		}
		h := sha256.New()
		n, e := io.Copy(h, io.LimitReader(f, 256<<20+1))
		if n != st.Size() {
			f.Close()
			return nil, fmt.Errorf("source changed during final inventory: %s", p)
		}
		closeErr := f.Close()
		if e != nil {
			return nil, e
		}
		if closeErr != nil {
			return nil, closeErr
		}
		m.Files = append(m.Files, resourceguard.NormalSystemSourceFile{Path: p, CanonicalPath: canonical, Kind: paths[p], Bytes: uint64(st.Size()), SHA256: hex.EncodeToString(h.Sum(nil))})
	}
	if e = m.Validate(); e != nil {
		return nil, e
	}
	return json.Marshal(m)
}

// inspect后的mutable tag不能再作FROM，否则声明的原base与实际rootfs可漂移。
// 这里只固定构建输入；镜像的业务资格仍须原Source认证。旧legacy流程不调用。
func normalPinnedBase(input, id string, digests []string) (string, string, error) {
	if !strings.HasPrefix(id, "sha256:") || len(id) != 71 {
		return "", "", fmt.Errorf("original inspected config digest absent")
	}
	b, e := hex.DecodeString(strings.TrimPrefix(id, "sha256:"))
	if e != nil || hex.EncodeToString(b) != strings.TrimPrefix(id, "sha256:") {
		return "", "", fmt.Errorf("noncanonical config digest")
	}
	named, e := reference.ParseNormalizedNamed(input)
	if e != nil {
		return "", "", e
	}
	if c, ok := named.(reference.Canonical); ok {
		if c.Digest().Algorithm().String() != "sha256" {
			return "", "", fmt.Errorf("base requires sha256")
		}
		return c.String(), strings.TrimPrefix(id, "sha256:"), nil
	}
	repo := reference.TrimNamed(named).Name()
	var candidates []string
	for _, v := range digests {
		n, e := reference.ParseNormalizedNamed(v)
		if e != nil {
			continue
		}
		c, ok := n.(reference.Canonical)
		if ok && reference.TrimNamed(n).Name() == repo && c.Digest().Algorithm().String() == "sha256" {
			candidates = append(candidates, c.String())
		}
	}
	if len(candidates) == 0 {
		return "", "", fmt.Errorf("normal base requires original immutable RepoDigest; publish local image before building")
	}
	sort.Strings(candidates)
	return candidates[0], strings.TrimPrefix(id, "sha256:"), nil
}
func normalRequirePinnedImage(v string) error {
	n, e := reference.ParseNormalizedNamed(v)
	if e != nil {
		return fmt.Errorf("normal runtime image is invalid")
	}
	c, ok := n.(reference.Canonical)
	if !ok || c.Digest().Algorithm().String() != "sha256" {
		return fmt.Errorf("normal runtime image requires immutable sha256 reference")
	}
	return nil
}
