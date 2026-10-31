//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const FixedNormalSplitTopologyPolicyPath = "/neonvm/runtime/dsga-normal-split.json"
const NormalSplitTopologyKernelArg = "neonvm.dsga.normal_split=split-v1"

func (g *Guard) PrepareNormalSplitTopology(ctx context.Context) (NormalSplitTopologyResult, error) {
	in, err := g.normalSplitRuntimeInputs(ctx)
	if err != nil {
		return NormalSplitTopologyResult{}, err
	}
	return g.normalSplit(ctx, in, normalSplitPrepare)
}
func (g *Guard) ObserveNormalSplitTopology(ctx context.Context) (NormalSplitTopologyResult, error) {
	in, err := g.normalSplitRuntimeInputs(ctx)
	if err != nil {
		return NormalSplitTopologyResult{}, err
	}
	return g.normalSplit(ctx, in, normalSplitObserve)
}
func (g *Guard) ReplayNormalSplitTopology(ctx context.Context) (NormalSplitTopologyResult, error) {
	in, err := g.normalSplitRuntimeInputs(ctx)
	if err != nil {
		return NormalSplitTopologyResult{}, err
	}
	return g.normalSplit(ctx, in, normalSplitReplay)
}
func (g *Guard) normalSplitRuntimeInputs(ctx context.Context) (normalSplitInputs, error) {
	var in normalSplitInputs
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return in, fmt.Errorf("split topology requires real root")
	}
	source, runtimeRaw, err := runtimeSource()
	if err != nil {
		return in, err
	}
	if source.Config != g.config {
		return in, fmt.Errorf("split runtime configuration differs")
	}
	cmdline, err := kernelText("/proc/cmdline")
	if err != nil {
		return in, err
	}
	if err := normalSplitBootMarker(cmdline); err != nil {
		return in, err
	}
	f, err := rootFile(FixedNormalSplitTopologyPolicyPath, false)
	if err != nil {
		return in, err
	}
	defer f.Close()
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &fs); err != nil {
		return in, err
	}
	if fs.Type != unix.ISOFS_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY == 0 {
		return in, fmt.Errorf("split policy requires original read-only ISO")
	}
	raw, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return in, err
	}
	if len(raw) > 64*1024 {
		return in, fmt.Errorf("split policy too large")
	}
	var policy NormalSplitTopologyPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return in, err
	}
	if policy.RuntimeDocumentSHA256 != physicalDigest(runtimeRaw) {
		return in, fmt.Errorf("split runtime exact bytes differ")
	}
	origin, err := g.ObservePhysicalKernelOrigin(ctx)
	if err != nil {
		return in, err
	}
	ref, err := origin.ReferenceSHA256()
	if err != nil {
		return in, err
	}
	var doc RuntimeDocument
	if err := json.Unmarshal(runtimeRaw, &doc); err != nil {
		return in, err
	}
	in = normalSplitInputs{source: source, sourceRaw: append([]byte(nil), doc.SourceJSON...), policy: policy, policyRaw: append([]byte(nil), raw...), origin: origin, originSHA: ref}
	if err := in.validate(g.config); err != nil {
		return normalSplitInputs{}, err
	}
	return in, nil
}

// 裸key/重复key/空值同样是歧义，不能只数合法值而忽略另一份原启动选择。
func normalSplitBootMarker(cmdline string) error {
	count := 0
	for _, token := range strings.Fields(cmdline) {
		key, _, _ := strings.Cut(token, "=")
		if key == "neonvm.dsga.normal_split" {
			count++
			if token != NormalSplitTopologyKernelArg || count != 1 {
				return fmt.Errorf("ambiguous original split boot marker")
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("original split boot marker absent")
	}
	return nil
}

func normalSplitLockedDirectory(s *lockedState, current NormalSplitTopologySnapshot) error {
	var st unix.Stat_t
	if err := unix.Fstat(int(s.dir.Fd()), &st); err != nil {
		return err
	}
	if control.Uint64(st.Dev) != current.StateDevice || control.Uint64(st.Ino) != current.StateInode {
		return fmt.Errorf("split original state directory changed")
	}
	return nil
}

// 所有生产路径固定在原PG父组下，绝不接受来自请求的path。读取目录/控制文件
// 都要求cgroup2、root不可委派；未知子组、任何后代或parent任务均立即失败。
func splitReadNode(path string) (normalSplitNode, error) {
	var n normalSplitNode
	if err := secureParents(path); err != nil {
		return n, err
	}
	var st unix.Stat_t
	var fs unix.Statfs_t
	if err := unix.Lstat(path, &st); err != nil {
		return n, err
	}
	if err := unix.Statfs(path, &fs); err != nil {
		return n, err
	}
	if fs.Type != unix.CGROUP2_SUPER_MAGIC || st.Uid != 0 || st.Mode&0022 != 0 || st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return n, fmt.Errorf("split node not root-controlled cgroup2")
	}
	n.Device, n.Inode = control.Uint64(st.Dev), control.Uint64(st.Ino)
	read := func(file string, unlimited bool) (control.Uint64, error) {
		v, err := readNumber(path+"/"+file, unlimited)
		return control.Uint64(v), err
	}
	var err error
	if n.MemoryMaxBytes, err = read("memory.max", true); err != nil {
		return n, err
	}
	if n.PidsMaxTasks, err = read("pids.max", true); err != nil {
		return n, err
	}
	if n.MemoryCurrentBytes, err = read("memory.current", false); err != nil {
		return n, err
	}
	if n.SwapCurrentBytes, err = read("memory.swap.current", false); err != nil {
		return n, err
	}
	if n.SwapMaxBytes, err = read("memory.swap.max", true); err != nil {
		return n, err
	}
	high, err := readNumber(path+"/memory.high", true)
	if err != nil {
		return n, err
	}
	n.MemoryHighUnlimited = high == math.MaxUint64
	typ, err := kernelText(path + "/cgroup.type")
	if err != nil || typ != "domain" {
		return n, fmt.Errorf("split node must remain domain")
	}
	procs, err := kernelText(path + "/cgroup.procs")
	if err != nil {
		return n, err
	}
	threads, err := kernelText(path + "/cgroup.threads")
	if err != nil {
		return n, err
	}
	events, err := kernelText(path + "/cgroup.events")
	if err != nil {
		return n, err
	}
	fields := strings.Fields(events)
	populated := -1
	for j := 0; j < len(fields); j += 2 {
		if j+1 >= len(fields) {
			return n, fmt.Errorf("malformed split cgroup.events")
		}
		if fields[j] == "populated" {
			if populated != -1 || fields[j+1] != "0" && fields[j+1] != "1" {
				return n, fmt.Errorf("ambiguous split populated")
			}
			if fields[j+1] == "0" {
				populated = 0
			} else {
				populated = 1
			}
		}
	}
	if populated < 0 {
		return n, fmt.Errorf("split recursive populated missing")
	}
	n.Empty = procs == "" && threads == "" && populated == 0
	controls, err := kernelText(path + "/cgroup.subtree_control")
	if err != nil {
		return n, err
	}
	tokens := strings.Fields(controls)
	sort.Strings(tokens)
	n.Controllers = strings.Join(tokens, " ")
	// rootFile逐个核控制权限，不能仅核目录就接受可由PG放宽的文件。
	for _, file := range []string{"memory.max", "memory.high", "memory.swap.max", "pids.max", "cgroup.procs", "cgroup.threads", "cgroup.subtree_control"} {
		f, e := rootFile(path+"/"+file, false)
		if e != nil {
			return n, e
		}
		f.Close()
	}
	n.RootControlled = true
	return n, nil
}
func splitChildren(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("split cgroup contains symlink")
		}
		if info.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}
func (k *linuxKernel) normalSplitRead() (NormalSplitTopologySnapshot, error) {
	var s NormalSplitTopologySnapshot
	var err error
	s.Identity, err = k.Identity()
	if err != nil {
		return s, err
	}
	s.Parent, err = splitReadNode(pgCgroup)
	if err != nil {
		return s, err
	}
	// populated递归不用于拒绝已完成后的合法叶任务，但父组自身永远不准直接执行。
	for _, file := range []string{"cgroup.procs", "cgroup.threads"} {
		v, e := kernelText(pgCgroup + "/" + file)
		if e != nil {
			return s, e
		}
		if v != "" {
			return s, fmt.Errorf("split normal parent must never contain tasks")
		}
	}
	names, err := splitChildren(pgCgroup)
	if err != nil {
		return s, err
	}
	for _, name := range names {
		if name != "private" && name != "arrays" {
			return s, fmt.Errorf("unknown direct split child")
		}
		children, e := splitChildren(pgCgroup + "/" + name)
		if e != nil {
			return s, e
		}
		if len(children) != 0 {
			return s, fmt.Errorf("split leaf descendants forbidden")
		}
		n, e := splitReadNode(pgCgroup + "/" + name)
		if e != nil {
			return s, e
		}
		if name == "private" {
			s.Private = &n
		} else {
			s.Arrays = &n
		}
	}
	var st unix.Stat_t
	if err := unix.Lstat(k.config.StateDir, &st); err != nil {
		return s, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != 0 || st.Mode&0077 != 0 {
		return s, fmt.Errorf("split state must remain private root directory")
	}
	s.StateDevice, s.StateInode = control.Uint64(st.Dev), control.Uint64(st.Ino)
	return s, nil
}
func (k *linuxKernel) normalSplitEnable() error {
	s, err := k.normalSplitRead()
	if err != nil {
		return err
	}
	if err := splitEmpty(s); err != nil {
		return err
	}
	if s.Parent.Controllers == "memory pids" {
		return nil
	}
	if s.Parent.Controllers != "" {
		return fmt.Errorf("split cannot adopt unknown parent controllers")
	}
	controllers, err := kernelText(pgCgroup + "/cgroup.controllers")
	if err != nil {
		return err
	}
	set := map[string]bool{}
	for _, c := range strings.Fields(controllers) {
		set[c] = true
	}
	if !set["memory"] || !set["pids"] {
		return fmt.Errorf("split memory/pids controllers unavailable")
	}
	f, err := rootFile(pgCgroup+"/cgroup.subtree_control", true)
	if err != nil {
		return err
	}
	defer f.Close()
	b := []byte("+memory +pids")
	n, err := f.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}
func splitLeafPath(name string) (string, error) {
	if name != "private" && name != "arrays" {
		return "", fmt.Errorf("invalid fixed split leaf")
	}
	return pgCgroup + "/" + name, nil
}
func (k *linuxKernel) normalSplitCreate(name string) error {
	path, err := splitLeafPath(name)
	if err != nil {
		return err
	}
	if err := secureParents(pgCgroup); err != nil {
		return err
	}
	// EEXIST不能当成功：没有已fsync的原inode凭据就不允许收养/重建。
	return os.Mkdir(path, 0755)
}
func (k *linuxKernel) normalSplitSet(name string, cap, pids control.Uint64) error {
	path, err := splitLeafPath(name)
	if err != nil {
		return err
	}
	n, err := splitReadNode(path)
	if err != nil {
		return err
	}
	if !n.Empty || n.MemoryCurrentBytes != 0 || n.SwapCurrentBytes != 0 || n.Controllers != "" || cap == 0 || uint64(cap) >= math.MaxInt64 || uint64(cap)%(64<<10) != 0 || pids == 0 {
		return fmt.Errorf("split only bounds original empty leaf")
	}
	if n.MemoryMaxBytes != control.Uint64(math.MaxUint64) && n.MemoryMaxBytes != cap || n.PidsMaxTasks != control.Uint64(math.MaxUint64) && n.PidsMaxTasks != pids || n.SwapMaxBytes != control.Uint64(math.MaxUint64) && n.SwapMaxBytes != 0 || !n.MemoryHighUnlimited {
		return fmt.Errorf("split existing leaf caps differ from original intent")
	}
	for _, v := range []struct {
		name  string
		value uint64
	}{{"memory.swap.max", 0}, {"memory.max", uint64(cap)}, {"pids.max", uint64(pids)}} {
		if err := writeNumber(path+"/"+v.name, v.value); err != nil {
			return err
		}
	}
	return nil
}
