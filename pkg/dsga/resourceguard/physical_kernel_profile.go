package resourceguard

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const physicalKernelObservationScope = "linux-6.12.26-physical-component-observation-only"

// 此观察 profile 逐字冻结本项目已构建 artifact 的完整 release，不接受任意
// 6.12.26 前缀。原收据 /data/neon-build/manifests/kernel-binary.json 对应
// vmlinuz SHA256 0ab3f71456c783452a49d50752a9b1945c2da399588febed409637d7af6891a0。
// release 相等只说明匹配观察目标；实际 notes/image/config/patch 的认证及
// 原出生封存仍须独立可信来源，不能据此认定该 artifact 已在 Guest 启动。
const physicalKernelRelease = "6.12.26-william-bd33f7c78026"

// PhysicalKernelZone 记录物理容量，不是当前分配量或 RSS。
// present 包括 reserved/unmanaged 页；若改用 managed/free，balloon 等操作
// 会使所谓持续包络随瞬时状态收缩，不能作为原固定 non-MOVABLE 上界。
type PhysicalKernelZone struct {
	Node         uint32         `json:"node"`
	Name         string         `json:"name"`
	PresentPages control.Uint64 `json:"present_pages"`
}

// PhysicalKernelObservation 只是拟议 PG17 受限 profile 的不赋权观察组件。
// 它不是原出生封存、Outside 预留、完整 counter/在途预算或 memory.max 降限许可；
// 当前生产资源写者与预算消费者均未接入。旧 VM 可以只读观察，但绝不能借观察
// 补签“PG 出生前已强制”的历史。取消/超时不写资源，不更换原 boot/revision/Held。
// notes 只有与独立可信 Runner/image 收据及准确 config/patch 对上后才能认证构建；
// 摘要格式、相同 release 或未来 Match 成功均不能独立认证这一来源。
type PhysicalKernelObservation struct {
	ProtocolVersion      uint32               `json:"protocol_version"`
	VMShrinkPermit       bool                 `json:"vm_shrink_permit"`
	Scope                string               `json:"scope"`
	Identity             Identity             `json:"identity"`
	KernelRelease        string               `json:"kernel_release"`
	KernelNotesSHA256    string               `json:"kernel_notes_sha256"`
	CommandLineSHA256    string               `json:"command_line_sha256"`
	PageSizeBytes        control.Uint64       `json:"page_size_bytes"`
	PossibleCPUs         uint32               `json:"possible_cpus"`
	PIDMax               uint32               `json:"pid_max"`
	Zones                []PhysicalKernelZone `json:"zones"`
	NonMovableUpperBytes control.Uint64       `json:"non_movable_upper_bytes"`
	// 这里只含已填充的 per-CPU folio charge stock，不是完整 I。
	// batch charge 成功至 refill 之间可抢占，每个在途任务还可能持另一批；
	// active_memcg worker 也未必计入 PG pids。未来消费者必须独立证明全部
	// 在途/延迟 folio/上下文包络，不能将本字段改名当作完整安全余量。
	CPUChargeStockUpperBytes control.Uint64 `json:"cpu_charge_stock_upper_bytes"`
}

func physicalDigest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func physicalDigestValid(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}

func physicalNonMovable(zones []PhysicalKernelZone) (uint64, error) {
	var total uint64
	previous := ""
	for _, z := range zones {
		// 首观察范围为 x86-64、single NUMA、无 ZONE_DEVICE。DMA 若存在必须
		// 和 DMA32/Normal 一样计入 K；当前原 artifact config 虽未开 ZONE_DMA，
		// 观察层保守求和不能凭该配置猜不存在或漏掉正常 DMA 物理成本。
		if z.Node != 0 || (z.Name != "DMA" && z.Name != "DMA32" && z.Name != "Normal" && z.Name != "Movable") || z.Name <= previous {
			return 0, fmt.Errorf("unsupported, duplicate, or unordered physical zone")
		}
		previous = z.Name
		if z.Name == "Movable" {
			continue
		}
		if uint64(z.PresentPages) > math.MaxUint64/4096-total {
			return 0, fmt.Errorf("physical zone capacity overflow")
		}
		total += uint64(z.PresentPages)
	}
	if total == 0 {
		return 0, fmt.Errorf("missing fixed non-MOVABLE physical capacity")
	}
	return total * 4096, nil
}

func (o PhysicalKernelObservation) Validate() error {
	if o.ProtocolVersion != 1 || o.VMShrinkPermit || o.Scope != physicalKernelObservationScope || o.Identity.Validate() != nil || o.KernelRelease != physicalKernelRelease || !physicalDigestValid(o.KernelNotesSHA256) || !physicalDigestValid(o.CommandLineSHA256) || o.PageSizeBytes != 4096 || o.PossibleCPUs == 0 || o.PossibleCPUs > 1024 || o.PIDMax < 301 || o.PIDMax > 4096 || len(o.Zones) == 0 || len(o.Zones) > 4 {
		return fmt.Errorf("invalid restricted physical kernel component observation")
	}
	k, err := physicalNonMovable(o.Zones)
	if err != nil {
		return err
	}
	if o.NonMovableUpperBytes != control.Uint64(k) || o.CPUChargeStockUpperBytes != control.Uint64(o.PossibleCPUs)*64*4096 {
		return fmt.Errorf("physical capacity or per-CPU stock component was changed")
	}
	return nil
}

// Match 只核对本物理观察组件，允许 MOVABLE 增减，拒绝 non-MOVABLE 拓扑/容量、
// CPU/PID 空间、kernel 或原 boot 漂移。它不证明任一观察早于 PG 出生，也不阻止
// 未来特权写者改变拓扑；这些都须由尚未完成的原封存及生产强制另行提供。
func (o PhysicalKernelObservation) Match(current PhysicalKernelObservation) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := current.Validate(); err != nil {
		return err
	}
	withoutMovable := func(zs []PhysicalKernelZone) []PhysicalKernelZone {
		out := make([]PhysicalKernelZone, 0, len(zs))
		for _, z := range zs {
			if z.Name != "Movable" {
				out = append(out, z)
			}
		}
		return out
	}
	a, b := o, current
	a.Zones, b.Zones = withoutMovable(a.Zones), withoutMovable(b.Zones)
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("physical kernel component source or fixed envelope changed")
	}
	return nil
}

func parsePhysicalZoneInfo(text string) ([]PhysicalKernelZone, error) {
	var zones []PhysicalKernelZone
	var current *PhysicalKernelZone
	present := false
	finish := func() error {
		if current != nil {
			if !present {
				return fmt.Errorf("zone present counter missing")
			}
			zones = append(zones, *current)
		}
		return nil
	}
	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "Node ") {
			if err := finish(); err != nil {
				return nil, err
			}
			fields := strings.Fields(line)
			if len(fields) != 4 || fields[0] != "Node" || !strings.HasSuffix(fields[1], ",") || fields[2] != "zone" {
				return nil, fmt.Errorf("invalid physical zone header")
			}
			n, err := physicalUint(strings.TrimSuffix(fields[1], ","))
			if err != nil || n > math.MaxUint32 {
				return nil, fmt.Errorf("invalid physical zone node")
			}
			current = &PhysicalKernelZone{Node: uint32(n), Name: fields[3]}
			present = false
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 0 && fields[0] == "present" {
			if current == nil || present || len(fields) != 2 {
				return nil, fmt.Errorf("duplicate or unbound present counter")
			}
			n, err := physicalUint(fields[1])
			if err != nil {
				return nil, err
			}
			current.PresentPages, present = control.Uint64(n), true
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if err := finish(); err != nil {
		return nil, err
	}
	sort.Slice(zones, func(i, j int) bool {
		if zones[i].Node != zones[j].Node {
			return zones[i].Node < zones[j].Node
		}
		return zones[i].Name < zones[j].Name
	})
	if _, err := physicalNonMovable(zones); err != nil {
		return nil, err
	}
	return zones, nil
}

func physicalUint(s string) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != s {
		return 0, fmt.Errorf("invalid canonical physical kernel counter")
	}
	return n, nil
}

func physicalPossibleCPUs(s string) (uint32, error) {
	if s == "0" {
		return 1, nil
	}
	if !strings.HasPrefix(s, "0-") {
		return 0, fmt.Errorf("profile requires contiguous possible CPUs starting at zero")
	}
	n, err := physicalUint(strings.TrimPrefix(s, "0-"))
	if err != nil || n == 0 || n >= 1024 {
		return 0, fmt.Errorf("unsupported possible CPU range")
	}
	return uint32(n + 1), nil
}

func physicalBootPolicy(cmdline string) error {
	values := make(map[string]string)
	for _, token := range strings.Fields(cmdline) {
		key, value, ok := strings.Cut(token, "=")
		if key != "cgroup.memory" && key != "memhp_default_state" {
			continue
		}
		if !ok {
			return fmt.Errorf("missing physical profile boot value")
		}
		if _, duplicate := values[key]; duplicate {
			return fmt.Errorf("duplicate physical profile boot control")
		}
		values[key] = value
	}
	if values["memhp_default_state"] != "online_movable" {
		return fmt.Errorf("hotplug default is not explicitly MOVABLE from boot")
	}
	flags := make(map[string]bool)
	for _, flag := range strings.Split(values["cgroup.memory"], ",") {
		if flags[flag] || (flag != "nokmem" && flag != "nosocket" && flag != "nobpf") {
			return fmt.Errorf("unsupported or duplicate memory accounting boot flag")
		}
		flags[flag] = true
	}
	if len(flags) != 3 {
		return fmt.Errorf("kernel/socket/BPF accounting exclusion not explicit from boot")
	}
	return nil
}
