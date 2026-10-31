//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const FixedNormalV1KernelBootstrapPolicyPath = "/neonvm/runtime/dsga-kernel-bootstrap.json"
const NormalV1KernelBootstrapArg = "neonvm.dsga.kernel_bootstrap=normal-split-v1"

// 三入口仅供原sysinit在普通入口前调用；不接受请求policy/PID/path。
// marker/ISO/当前空域不是完整启动历史证明，未来写者尚未封闭时始终不赋权。
func (g *Guard) PrepareKernelBeforeOrigin(ctx context.Context) (NormalV1KernelBootstrapEvidence, error) {
	in, e := g.normalKernelBootstrapInputs()
	if e != nil {
		return NormalV1KernelBootstrapEvidence{}, e
	}
	return g.normalKernelPreparation(ctx, in, kernelPrepPrepare)
}
func (g *Guard) ObserveKernelBeforeOrigin(ctx context.Context) (NormalV1KernelBootstrapEvidence, error) {
	in, e := g.normalKernelBootstrapInputs()
	if e != nil {
		return NormalV1KernelBootstrapEvidence{}, e
	}
	return g.normalKernelPreparation(ctx, in, kernelPrepObserve)
}
func (g *Guard) ReplayKernelBeforeOrigin(ctx context.Context) (NormalV1KernelBootstrapEvidence, error) {
	in, e := g.normalKernelBootstrapInputs()
	if e != nil {
		return NormalV1KernelBootstrapEvidence{}, e
	}
	return g.normalKernelPreparation(ctx, in, kernelPrepReplay)
}

func kernelBootstrapMarker(cmdline string) error {
	count := 0
	for _, token := range strings.Fields(cmdline) {
		key, _, _ := strings.Cut(token, "=")
		if key != "neonvm.dsga.kernel_bootstrap" {
			continue
		}
		count++
		if token != NormalV1KernelBootstrapArg {
			return fmt.Errorf("kernel preparation boot marker differs")
		}
	}
	if count != 1 {
		return fmt.Errorf("original kernel preparation marker missing or duplicated")
	}
	return nil
}

func readKernelBootstrapISO(path string) ([]byte, error) {
	f, e := rootFile(path, false)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	var fs unix.Statfs_t
	if e = unix.Fstatfs(int(f.Fd()), &fs); e != nil {
		return nil, e
	}
	if fs.Type != unix.ISOFS_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY == 0 {
		return nil, fmt.Errorf("kernel preparation requires root read-only runtime ISO")
	}
	raw, e := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if e != nil {
		return nil, e
	}
	if len(raw) > 64*1024 {
		return nil, fmt.Errorf("kernel ISO policy exceeds bound")
	}
	return raw, nil
}
func (g *Guard) normalKernelBootstrapInputs() (kernelPrepRawInputs, error) {
	var in kernelPrepRawInputs
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return in, fmt.Errorf("original kernel preparation requires real root")
	}
	source, raw, e := runtimeSource()
	if e != nil {
		return in, e
	}
	if source.Config != g.config {
		return in, fmt.Errorf("kernel original runtime config differs")
	}
	in.RuntimeRaw = raw
	cmdline, e := kernelText("/proc/cmdline")
	if e != nil {
		return in, e
	}
	if e = kernelBootstrapMarker(cmdline); e != nil {
		return in, e
	}
	in.PhysicalPolicyRaw, e = readKernelBootstrapISO(FixedPhysicalKernelPolicyPath)
	if e != nil {
		return in, e
	}
	in.PolicyRaw, e = readKernelBootstrapISO(FixedNormalV1KernelBootstrapPolicyPath)
	if e != nil {
		return in, e
	}
	_, e = in.decode(g.config)
	return in, e
}

func kernelBootstrapNamespaces() ([4]control.Uint64, error) {
	var result [4]control.Uint64
	for i, n := range []string{"pid", "mnt", "ipc", "user"} {
		var self, init unix.Stat_t
		if e := unix.Stat("/proc/self/ns/"+n, &self); e != nil {
			return result, e
		}
		if e := unix.Stat("/proc/1/ns/"+n, &init); e != nil {
			return result, e
		}
		if self.Dev != init.Dev || self.Ino != init.Ino || self.Ino == 0 {
			return result, fmt.Errorf("kernel preparation differs from original PID1 namespaces")
		}
		result[i] = control.Uint64(self.Ino)
	}
	return result, nil
}

// 独立raw reader允许原allocator默认值，但不放宽PhysicalObservation的规则。
// 初始J已由准确369 early硬源给出，pid_max必须原样等hard，完全没有晚写补签。
func (k *linuxKernel) kernelPrepRead() (kernelPrepSnapshot, error) {
	var s kernelPrepSnapshot
	if runtime.GOARCH != "amd64" || os.Getpagesize() != 4096 {
		return s, fmt.Errorf("kernel preparation requires exact amd64 base pages")
	}
	var e error
	s.Identity, e = k.Identity()
	if e != nil {
		return s, e
	}
	s.KernelRelease, e = kernelText("/proc/sys/kernel/osrelease")
	if e != nil {
		return s, e
	}
	notes, e := physicalKernelNotes()
	if e != nil {
		return s, e
	}
	s.KernelNotesSHA256 = physicalDigest(notes)
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if e != nil || !ref.StrictPageCounterIRQBoundary() || s.KernelRelease != ref.KernelRelease || s.KernelNotesSHA256 != ref.KernelNotesSHA256 {
		return s, fmt.Errorf("kernel preparation exact strict build differs")
	}
	s.CommandLine, e = kernelText("/proc/cmdline")
	if e != nil {
		return s, e
	}
	if e = kernelBootstrapMarker(s.CommandLine); e != nil {
		return s, e
	}
	s.HardPIDMax, e = readPhysicalKernelPIDHardLimit(s.CommandLine)
	if e != nil {
		return s, e
	}
	pid, e := readNumber("/proc/sys/kernel/pid_max", false)
	if e != nil || pid != uint64(s.HardPIDMax) {
		return s, fmt.Errorf("kernel preparation runtime PID bound differs from original hard source")
	}
	s.PIDMax = uint32(pid)
	s.Capabilities, e = readPhysicalKernelRuntimeCapabilities()
	if e != nil {
		return s, e
	}
	h := s.HardPIDMax
	s.Capabilities.HardPIDMaxLimit = &h
	boot, e := kernelText("/proc/sys/kernel/random/boot_id")
	if e != nil || boot != s.Identity.GuestBootID {
		return s, fmt.Errorf("kernel preparation boot differs")
	}
	for _, p := range []string{"/sys/kernel/mm/transparent_hugepage", "/sys/kernel/mm/ksm"} {
		if _, e := os.Lstat(p); !os.IsNotExist(e) {
			return s, fmt.Errorf("THP/KSM must remain compiled out")
		}
	}
	swaps, e := kernelText("/proc/swaps")
	if e != nil || strings.Join(strings.Fields(swaps), " ") != "Filename Type Size Used Priority" {
		return s, fmt.Errorf("kernel preparation requires no swap")
	}
	online, e := kernelText("/sys/devices/system/memory/auto_online_blocks")
	if e != nil || online != "online_movable" {
		return s, fmt.Errorf("kernel preparation hotplug policy differs")
	}
	possible, e := kernelText("/sys/devices/system/cpu/possible")
	if e != nil {
		return s, e
	}
	s.PossibleCPUs, e = physicalPossibleCPUs(possible)
	if e != nil {
		return s, e
	}
	zones, e := kernelText("/proc/zoneinfo")
	if e != nil {
		return s, e
	}
	s.Zones, e = parsePhysicalZoneInfo(zones)
	if e != nil {
		return s, e
	}
	modules, e := kernelText("/proc/modules")
	if e != nil {
		return s, e
	}
	s.LoadedModules = []string{}
	for _, line := range strings.Split(modules, "\n") {
		if fields := strings.Fields(line); len(fields) > 0 {
			s.LoadedModules = append(s.LoadedModules, fields[0])
		}
	}
	value, e := readNumber("/proc/sys/kernel/modules_disabled", false)
	if e != nil || value > 1 {
		return s, fmt.Errorf("invalid modules_disabled source")
	}
	s.ModulesDisabled = uint32(value)
	value, e = readNumber("/proc/sys/kernel/numa_balancing", false)
	if e != nil || value > 3 {
		return s, fmt.Errorf("invalid NUMA balancing source")
	}
	s.NUMABalancing = uint32(value)
	// 准确369配置NUMA+MIGRATION，必须实读现有控制；ENOENT不能猜成无demotion。
	demotion, e := kernelText("/sys/kernel/mm/numa/demotion_enabled")
	if e != nil {
		return s, e
	}
	if demotion != "true" && demotion != "false" {
		return s, fmt.Errorf("unrecognized demotion source")
	}
	s.DemotionEnabled = demotion == "true"
	s.Namespaces, e = kernelBootstrapNamespaces()
	if e != nil {
		return s, e
	}
	var fs unix.Statfs_t
	var st, root unix.Stat_t
	if e = unix.Statfs(k.config.StateDir, &fs); e != nil {
		return s, e
	}
	if e = unix.Stat(k.config.StateDir, &st); e != nil {
		return s, e
	}
	if e = unix.Stat("/", &root); e != nil {
		return s, e
	}
	if fs.Type != unix.EXT4_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY != 0 || st.Dev == root.Dev || st.Ino == 0 {
		return s, fmt.Errorf("kernel original intent requires separate writable persistent ext4 state")
	}
	s.StateDevice, s.StateInode = control.Uint64(st.Dev), control.Uint64(st.Ino)
	return s, nil
}

// root cgroup.events的populated为子树事实；同时逐目录核procs/threads，拒符号链接、
// 过深/过大的未知子树。只读，不能通过只看顶层空组漏掉后台子域。
func (k *linuxKernel) kernelPrepPGEmpty() error {
	events, e := kernelText(pgCgroup + "/cgroup.events")
	if e != nil {
		return e
	}
	populated := false
	for _, line := range strings.Split(events, "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[0] == "populated" {
			if populated || f[1] != "0" {
				return fmt.Errorf("PG subtree populated or ambiguous")
			}
			populated = true
		}
	}
	if !populated {
		return fmt.Errorf("PG recursive populated source missing")
	}
	pending := []string{pgCgroup}
	count := 0
	for len(pending) > 0 {
		dir := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		count++
		if count > 4096 || strings.Count(strings.TrimPrefix(dir, pgCgroup), "/") > 16 {
			return fmt.Errorf("PG subtree exceeds reviewed bound")
		}
		for _, name := range []string{"cgroup.procs", "cgroup.threads"} {
			text, e := kernelText(dir + "/" + name)
			if e != nil {
				return e
			}
			if text != "" {
				return fmt.Errorf("kernel preparation requires empty recursive PG")
			}
		}
		entries, e := os.ReadDir(dir)
		if e != nil {
			return e
		}
		for _, entry := range entries {
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("PG subtree symlink not allowed")
			}
			if entry.IsDir() {
				pending = append(pending, dir+"/"+entry.Name())
			}
		}
	}
	return nil
}

func (k *linuxKernel) kernelPrepWrite(name string, value uint64) error {
	paths := map[string]string{"modules_disabled": "/proc/sys/kernel/modules_disabled", "numa_balancing": "/proc/sys/kernel/numa_balancing", "demotion_enabled": "/sys/kernel/mm/numa/demotion_enabled", "watermark_boost_factor": "/proc/sys/vm/watermark_boost_factor", "compaction_proactiveness": "/proc/sys/vm/compaction_proactiveness", "percpu_pagelist_high_fraction": "/proc/sys/vm/percpu_pagelist_high_fraction", "watermark_scale_factor": "/proc/sys/vm/watermark_scale_factor", "min_free_kbytes": "/proc/sys/vm/min_free_kbytes"}
	path, ok := paths[name]
	if !ok {
		return fmt.Errorf("unrecognized kernel bootstrap write")
	}
	if (name == "modules_disabled" && value != 1) || ((name == "numa_balancing" || name == "demotion_enabled" || name == "watermark_boost_factor" || name == "compaction_proactiveness") && value != 0) {
		return fmt.Errorf("kernel bootstrap cannot relax force controls")
	}
	// modules_disabled=1的不可逆处理依赖准确kernel源码。当前读回不能证明旧inflight
	// module_load已不存在；受控sysinit调用图/未来写者仍属全来源尚未闭的独立门槛。
	return writeNumber(path, value)
}
func (k *linuxKernel) kernelPrepCensus(hard uint32) error {
	_, _, e := preSystemTaskCensus(hard)
	return e
}

var _ json.Unmarshaler = (*NormalV1KernelBootstrapPolicy)(nil)
