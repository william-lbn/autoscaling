//go:build linux

package resourceguard

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// 观察层只读，绝不修改sysctl/挂载/cgroup。它不是持续强制或原启动历史；
// 后继资格必须绑定KernelPrep、原Host和全部写者。raw读取有固定上限，错误关门。
func readNormalInflightSource(id Identity) (normalInflightSourceFacts, error) {
	var f normalInflightSourceFacts
	ref, err := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if err != nil {
		return f, err
	}
	o, err := readPhysicalKernelComponent(id)
	if err != nil {
		return f, err
	}
	read := func(path string) (string, error) {
		h, e := os.Open(path)
		if e != nil {
			return "", e
		}
		defer h.Close()
		b, e := io.ReadAll(io.LimitReader(h, 1024*1024+1))
		if e != nil {
			return "", e
		}
		if len(b) > 1024*1024 {
			return "", fmt.Errorf("inflight source exceeds bound: %s", path)
		}
		return strings.TrimSpace(string(b)), nil
	}
	f.Build = ref
	f.Physical = o
	zoneinfo, e := read("/proc/zoneinfo")
	if e != nil {
		return normalInflightSourceFacts{}, e
	}
	f.NonMovableManagedPages, e = normalInflightManagedLowPages(zoneinfo)
	if e != nil {
		return normalInflightSourceFacts{}, e
	}

	// balloon本身分配未进memcg的HIGHUSER_MOVABLE页，不能用nokmem或K冒充覆盖。
	// 观察无设备只属当前事实；原Host必须另封设备集合及未来hotplug写者。
	devices, e := filepath.Glob("/sys/bus/virtio/devices/*")
	if e != nil {
		return normalInflightSourceFacts{}, e
	}
	inventory := []string{}
	for _, d := range devices {
		raw, e := read(filepath.Join(d, "device"))
		if e != nil {
			return normalInflightSourceFacts{}, e
		}
		n, e := strconv.ParseUint(strings.TrimPrefix(raw, "0x"), 16, 32)
		if e != nil {
			return normalInflightSourceFacts{}, e
		}
		if n == 5 {
			return normalInflightSourceFacts{}, fmt.Errorf("inflight unaccounted MOVABLE balloon device is unsupported")
		}
		canonical, e := filepath.EvalSymlinks(d)
		if e != nil {
			return normalInflightSourceFacts{}, e
		}
		inventory = append(inventory, canonical+"="+raw)
	}
	sort.Strings(inventory)
	f.VirtioInventorySHA256 = physicalDigest([]byte(strings.Join(inventory, "\n")))
	f.NoBalloonDevice = true
	hyper, e := read("/sys/hypervisor/type")
	if e != nil && !os.IsNotExist(e) {
		return normalInflightSourceFacts{}, e
	}
	if e == nil && hyper == "xen" {
		return normalInflightSourceFacts{}, fmt.Errorf("inflight requires native x86 KVM source, not Xen balloon/PV entry")
	}
	f.NoXenHypervisor = true

	controls := []struct {
		path, want string
		dst        *bool
	}{
		{"/proc/sys/kernel/modules_disabled", "1", &f.ModulesDisabled},
		{"/proc/modules", "", &f.ModulesEmpty},
		{"/proc/sys/kernel/numa_balancing", "0", &f.NUMABalancingDisabled},
		{"/sys/kernel/mm/numa/demotion_enabled", "false", &f.DemotionDisabled},
		{"/sys/module/memory_hotplug/parameters/memmap_on_memory", "N", &f.MemmapOnMemoryDisabled},
	}
	for _, v := range controls {
		s, e := read(v.path)
		if e != nil {
			return normalInflightSourceFacts{}, e
		}
		*v.dst = s == v.want
		if !*v.dst {
			return normalInflightSourceFacts{}, fmt.Errorf("inflight prerequisite differs: %s", v.path)
		}
	}
	states, err := filepath.Glob("/sys/kernel/mm/hugepages/hugepages-*kB")
	if err != nil || len(states) == 0 {
		return normalInflightSourceFacts{}, fmt.Errorf("inflight Huge state inventory unavailable")
	}
	for _, state := range states {
		for _, key := range []string{"nr_hugepages", "free_hugepages", "resv_hugepages", "surplus_hugepages", "nr_overcommit_hugepages"} {
			s, e := read(filepath.Join(state, key))
			if e != nil {
				return normalInflightSourceFacts{}, e
			}
			if s != "0" {
				return normalInflightSourceFacts{}, fmt.Errorf("inflight Huge state is not zero")
			}
		}
	}
	f.HugeStatesZero = true
	raw, err := read("/proc/self/mountinfo")
	if err != nil {
		return normalInflightSourceFacts{}, err
	}
	fs, err := normalInflightMountTypes(raw)
	if err != nil {
		return normalInflightSourceFacts{}, err
	}
	f.MountInfoSHA256 = physicalDigest([]byte(raw))
	f.MountFilesystems = fs
	if err := f.validate(); err != nil {
		return normalInflightSourceFacts{}, err
	}
	return f, nil
}

func normalInflightMountTypes(raw string) ([]string, error) {
	if len(raw) == 0 || len(raw) > 1024*1024 {
		return nil, fmt.Errorf("inflight invalid mount inventory")
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(raw, "\n") {
		parts := strings.Split(line, " - ")
		if len(parts) != 2 {
			return nil, fmt.Errorf("inflight malformed mount source")
		}
		left := strings.Fields(parts[0])
		right := strings.Fields(parts[1])
		if len(left) < 6 || len(right) < 3 {
			return nil, fmt.Errorf("inflight truncated mount source")
		}

		for _, opts := range []string{left[5], right[2]} {
			for _, opt := range strings.Split(opts, ",") {
				if opt == "dax" || strings.HasPrefix(opt, "dax=") {
					return nil, fmt.Errorf("inflight DAX is unsupported")
				}
			}
		}

		seen[right[0]] = true
	}
	var fs []string
	for s := range seen {
		fs = append(fs, s)
	}
	sort.Strings(fs)
	return fs, nil
}

// managed是物理已管理容量，非当前free/RSS；原非MOVABLE不可移除/balloon的
// 生命周期仍须Host/Root来源封闭。本函数不把一次观察自动改成持续floor。
func normalInflightManagedLowPages(raw string) (uint64, error) {
	var total uint64
	zone := ""
	seen := map[string]bool{}
	pending := false
	for _, line := range strings.Split(raw, "\n") {
		v := strings.Fields(line)
		if len(v) == 4 && v[0] == "Node" && v[2] == "zone" {
			if pending {
				return 0, fmt.Errorf("inflight missing managed zone field")
			}
			zone = v[3]
			key := v[1] + zone
			if seen[key] {
				return 0, fmt.Errorf("inflight duplicate zone")
			}
			seen[key] = true
			pending = true
			continue
		}
		if len(v) == 2 && v[0] == "managed" {
			if !pending {
				return 0, fmt.Errorf("inflight orphan managed field")
			}
			n, e := strconv.ParseUint(v[1], 10, 64)
			if e != nil {
				return 0, e
			}
			if zone != "Movable" {
				total, e = normalInflightSum(total, n)
				if e != nil {
					return 0, e
				}
			}
			pending = false
		}
	}
	if pending || len(seen) == 0 || total == 0 {
		return 0, fmt.Errorf("inflight lowmem managed source absent")
	}
	return total, nil
}
