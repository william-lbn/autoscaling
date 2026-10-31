package resourceguard

import (
	"fmt"
	"math"
)

// 本组件只生产准确369构建可独立复核的在途来源和分项。它不接请求中的
// “已闭合”bool，不签发normal/VM许可；完整IRQ/角色/持有引用尚须原强制来源。
// 已收费的GUP/pipe/IO引用仍在memcg账内，不能再按其容量重复加入I。
const normalInflightSourceScope = "linux-6.12.26-inflight-source-component-v1"

// 以下常数绑定physical lookup中的准确369 config/source，不来自当前CPU采样。
// NR_CPUS是编译上界64；possible=2本身不证明以后不能CPU热增。
const (
	normalInflightCompiledCPUCeiling  uint64 = 64
	normalInflightHardTasks           uint64 = 512
	normalInflightBasePage            uint64 = 4096
	normalInflightPagevec             uint64 = 31
	normalInflightMigrationBatch      uint64 = 512
	normalInflightKernelStack         uint64 = 16384
	normalInflightPageblock           uint64 = 512
	normalInflightMaxBuddyPages       uint64 = 1024
	normalInflightIRQStackBytes       uint64 = 16384
	normalInflightExceptionStackBytes uint64 = 4 * 8192
	normalInflightPTRegsBytes         uint64 = 168
)

type normalInflightSourceFacts struct {
	NonMovableManagedPages uint64                       `json:"non_movable_managed_pages"`
	NoBalloonDevice        bool                         `json:"no_balloon_device"`
	NoXenHypervisor        bool                         `json:"no_xen_hypervisor"`
	VirtioInventorySHA256  string                       `json:"virtio_inventory_sha256"`
	Build                  PhysicalKernelBuildReference `json:"build"`
	Physical               PhysicalKernelObservation    `json:"physical"`
	ModulesDisabled        bool                         `json:"modules_disabled"`
	ModulesEmpty           bool                         `json:"modules_empty"`
	NUMABalancingDisabled  bool                         `json:"numa_balancing_disabled"`
	DemotionDisabled       bool                         `json:"demotion_disabled"`
	MemmapOnMemoryDisabled bool                         `json:"memmap_on_memory_disabled"`
	HugeStatesZero         bool                         `json:"huge_states_zero"`
	MountInfoSHA256        string                       `json:"mountinfo_sha256"`
	MountFilesystems       []string                     `json:"mount_filesystems"`
}

type normalInflightSourceTerms struct {
	IRQContextCeiling           uint64 `json:"irq_context_ceiling"`
	IRQConditionalOccupiedBytes uint64 `json:"irq_conditional_occupied_bytes"`
	Scope                       string `json:"scope"`
	HardTaskCeiling             uint64 `json:"hard_task_ceiling"`
	CompiledCPUCeiling          uint64 `json:"compiled_cpu_ceiling"`
	CurrentPossibleCPUs         uint32 `json:"current_possible_cpus"`
	PageBytes                   uint64 `json:"page_bytes"`
	// Migration是memcg迁账前新dst的物理影子，不把仍计账的原src再加一次。
	MigrationDestinationsBytes uint64 `json:"migration_destinations_bytes"`
	TaskPrechargeBytes         uint64 `json:"task_precharge_bytes"`
	TaskRetireBytes            uint64 `json:"task_retire_bytes"`
	TaskSpeculativeLookupBytes uint64 `json:"task_speculative_lookup_bytes"`
	LateExitStackSlots         uint64 `json:"late_exit_stack_slots"`
	LateExitSingleRetireBytes  uint64 `json:"late_exit_single_retire_bytes"`
	// 这是已空闲但暂隔离的allocator可用性预留，绝不是占用中的folio或RSS。
	CompactionIsolatedFreeReserveBytes uint64   `json:"compaction_isolated_free_reserve_bytes"`
	CPUStockPaidBytes                  uint64   `json:"cpu_stock_paid_bytes"`
	CounterHierarchyGapBytes           uint64   `json:"counter_hierarchy_gap_bytes"`
	KnownConditionalOccupiedBytes      uint64   `json:"known_conditional_occupied_bytes"`
	MissingSources                     []string `json:"missing_sources"`
	AllSourcesClosed                   bool     `json:"all_sources_closed"`
	NormalDecreaseAuthorized           bool     `json:"normal_decrease_authorized"`
}

func normalInflightMultiply(a, b uint64) (uint64, error) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, fmt.Errorf("inflight arithmetic overflow")
	}
	return a * b, nil
}
func normalInflightSum(parts ...uint64) (uint64, error) {
	var n uint64
	for _, p := range parts {
		if p > math.MaxUint64-n {
			return 0, fmt.Errorf("inflight arithmetic overflow")
		}
		n += p
	}
	return n, nil
}

func (f normalInflightSourceFacts) validate() error {
	expected, err := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if err != nil || f.Build != expected || !f.Build.StrictPageCounterIRQBoundary() {
		return fmt.Errorf("inflight requires exact reviewed369 build")
	}
	if err := f.Physical.Validate(); err != nil {
		return err
	}
	o := f.Physical
	c := o.RuntimeCapabilities
	if o.KernelRelease != expected.KernelRelease || o.KernelNotesSHA256 != expected.KernelNotesSHA256 ||
		o.PageSizeBytes != 4096 || o.PossibleCPUs == 0 || uint64(o.PossibleCPUs) > normalInflightCompiledCPUCeiling ||
		c == nil || c.HardPIDMaxLimit == nil || *c.HardPIDMaxLimit != 512 || o.PIDMax != 512 {
		return fmt.Errorf("inflight original hard PID/build/page/CPU source differs")
	}
	if !f.ModulesDisabled || !f.ModulesEmpty || !f.NUMABalancingDisabled || !f.DemotionDisabled ||
		!f.MemmapOnMemoryDisabled || !f.HugeStatesZero || !f.NoBalloonDevice || !f.NoXenHypervisor {
		return fmt.Errorf("inflight runtime prerequisite is missing")
	}
	if c.WatermarkBoostFactor != 0 || c.CompactionProactiveness != 0 || c.PerCPUPagelistHighFraction < 8 {
		return fmt.Errorf("inflight requires explicit allocator preparation")
	}
	if f.NonMovableManagedPages == 0 || f.NonMovableManagedPages > uint64(f.Physical.NonMovableUpperBytes)/4096 {
		return fmt.Errorf("inflight stable lowmem managed source invalid")
	}
	if !physicalDigestValid(f.MountInfoSHA256) || !physicalDigestValid(f.VirtioInventorySHA256) || len(f.MountFilesystems) == 0 {
		return fmt.Errorf("inflight mount source absent")
	}
	seen := map[string]bool{}
	for _, fs := range f.MountFilesystems {
		// pseudo FS不是PG数据域许可；设备、可读文件与挂载写者还须原角色来源封闭。
		switch fs {
		case "rootfs", "ramfs", "tmpfs", "ext4", "iso9660", "proc", "sysfs", "cgroup2", "devtmpfs", "devpts", "securityfs":
		default:
			return fmt.Errorf("inflight unsupported filesystem %q", fs)
		}
		if seen[fs] {
			return fmt.Errorf("inflight duplicate filesystem fact")
		}
		seen[fs] = true
	}
	return nil
}

// 只计算已有源码支持的条件分项。Q=1还依赖全域数据路径/FS/角色强制，
// 因此KnownConditionalOccupiedBytes不是完整I。失败不会输出部分合格结果。
// 退出后PID已不可用，不能仍以J约束晚put：尚在执行的VMAP任务栈至少16KiB，
// 且栈物理分配在nonMOVABLE域，所以以原K/16KiB额外保守计数；不扣已在K的栈。
func normalInflightCalculate(f normalInflightSourceFacts) (normalInflightSourceTerms, error) {
	if err := f.validate(); err != nil {
		return normalInflightSourceTerms{}, err
	}
	p := normalInflightBasePage
	j := normalInflightHardTasks
	c := normalInflightCompiledCPUCeiling
	t := normalInflightSourceTerms{Scope: normalInflightSourceScope, HardTaskCeiling: j, CompiledCPUCeiling: c,
		CurrentPossibleCPUs: f.Physical.PossibleCPUs, PageBytes: p, LateExitStackSlots: uint64(f.Physical.NonMovableUpperBytes) / normalInflightKernelStack}
	// x86 IDT完整pt_regs=168B，实际entry调用C前压入15通用寄存器、origAX和IRET帧。
	// hardirq_stack_inuse在切栈前置位；嵌套IRQ不重置栈顶，因此任意重开IRQ
	// 也必须保留先前帧。保守连实际4个8KiB异常栈(AMD_MEM_ENCRYPT=n，VC/VC2物理size0)一起计算，不靠handler无嵌套。
	// NMI嵌套只改repeat_nmi返回点、直接iret，不再进入C/覆写仍活跃C frame。
	// 另加每CPU3槽覆盖最多两个保留在task/外栈的首入帧（用户IRQ及
	// 首次切IRQ栈的内核IRQ），以及一个无独立IDT帧的softirq。
	// NMI的C体已包含在异常栈容量中；nested_nmi只安排重放，不再进C。
	// 这不是任意kernel代码可自选/重置栈的证明，仍依赖固定可信构建/无注入来源。
	stacks := normalInflightIRQStackBytes + normalInflightExceptionStackBytes
	t.IRQContextCeiling = c * ((stacks+normalInflightPTRegsBytes-1)/normalInflightPTRegsBytes + 3)
	var err error
	values := []struct {
		dest *uint64
		a, b uint64
	}{
		{&t.MigrationDestinationsBytes, j * normalInflightMigrationBatch, p},
		{&t.TaskPrechargeBytes, j, p},
		{&t.IRQConditionalOccupiedBytes, t.IRQContextCeiling * (normalInflightPagevec + 1 + 1 + 1 + 1), p},
		{&t.TaskRetireBytes, j * (normalInflightPagevec + 1 + 1), p},
		{&t.TaskSpeculativeLookupBytes, j, p},
		{&t.LateExitSingleRetireBytes, t.LateExitStackSlots, p},
		{&t.CompactionIsolatedFreeReserveBytes, j * (normalInflightPageblock + normalInflightMaxBuddyPages - 1), p},
		{&t.CPUStockPaidBytes, c * 64, p},
		{&t.CounterHierarchyGapBytes, c * 4 * 128, p},
	}
	for _, v := range values {
		if *v.dest, err = normalInflightMultiply(v.a, v.b); err != nil {
			return normalInflightSourceTerms{}, err
		}
	}
	t.KnownConditionalOccupiedBytes, err = normalInflightSum(t.MigrationDestinationsBytes, t.TaskPrechargeBytes, t.TaskRetireBytes, t.TaskSpeculativeLookupBytes, t.LateExitSingleRetireBytes, t.IRQConditionalOccupiedBytes)
	if err != nil {
		return normalInflightSourceTerms{}, err
	}
	t.MissingSources = []string{
		"original-Q1-data-FS-and-all-role-enforcement",
		"immutable-x86-entry-no-alternate-stack-or-kernel-injection-role-source",
		"post-transfer-speculative-reference-caller-closure",
		"late-exit-pipe-ops-and-task-frag-role-closure",
		"direct-compaction-capture-and-reentrancy-closure",
		"force-Normal-folios-never-migrate-out-and-all-force-callers",
		"original-cpuset-device-BPF-mount-and-future-control-writers",
		"allocator-watermark-PCP-and-original-QMP-maximum-availability",
	}
	return t, nil
}

// 这些参数仅供原来源形成前的预算规划，绝不是从请求补签Mmax的生产接口。
// 后继真正消费者必须从同VM的原QMP双backend/region及Origin取对应输入，
// 并封闭single-NUMA/唯一MOVABLE zone和kernel controls未来写者。
// normalLeavesBytes必须含全部数组计划+实际helper余量，不能把AS叫RSS来省预算。
type normalInflightPlanningInput struct {
	MaximumBytes      uint64 `json:"candidate_maximum_bytes"`
	NormalLeavesBytes uint64 `json:"candidate_normal_leaves_bytes"`
	SystemBytes       uint64 `json:"candidate_system_bytes"`
}
type normalInflightPlanningResult struct {
	ConditionalNormalTargetBytes     uint64 `json:"conditional_normal_target_bytes"`
	ConditionalForceChargeBytes      uint64 `json:"conditional_force_charge_bytes"`
	PCPTotalFreeReserveBytes         uint64 `json:"pcp_total_free_reserve_bytes"`
	MovableWatermarkReserveBytes     uint64 `json:"movable_watermark_reserve_bytes"`
	OutsideKernelAndInitialRootBytes uint64 `json:"outside_kernel_and_initial_root_bytes"`
	TotalRequiredBytes               uint64 `json:"total_required_bytes"`
	CandidateSpareBytes              uint64 `json:"candidate_spare_bytes"`
	CandidateFits                    bool   `json:"candidate_fits"`
	CandidateOnly                    bool   `json:"candidate_only"`
	MmaxAuthorized                   bool   `json:"mmax_authorized"`
	NormalDecreaseAuthorized         bool   `json:"normal_decrease_authorized"`
}

func normalInflightCeilDiv(a, b uint64) (uint64, error) {
	if b == 0 {
		return 0, fmt.Errorf("inflight zero divisor")
	}
	q := a / b
	if a%b != 0 {
		q++
	}
	return q, nil
}

func normalInflightPlan(f normalInflightSourceFacts, in normalInflightPlanningInput) (normalInflightPlanningResult, error) {
	var r normalInflightPlanningResult
	t, e := normalInflightCalculate(f)
	if e != nil {
		return r, e
	}
	p := normalInflightBasePage
	c := normalInflightCompiledCPUCeiling
	k := uint64(f.Physical.NonMovableUpperBytes)
	if in.MaximumBytes == 0 || in.MaximumBytes > 1<<52 || in.MaximumBytes%p != 0 || in.NormalLeavesBytes == 0 || in.SystemBytes == 0 {
		return r, fmt.Errorf("inflight invalid candidate plan")
	}
	pages := in.MaximumBytes / p
	caps := f.Physical.RuntimeCapabilities
	// page_alloc.c: zone_batchsize<=63；固定fraction使high_min==high_max。
	// 每CPU对单MOVABLE zone保守不使用nr_split_cpus除数，high<=max(M/f,4B)。
	// 另加3类PCP list各一批及8页高阶插入窗口；即使PCP更新异步仍用原Mmax，
	// 不拿当前free/当前request作界。初始化前旧PCP页由原base-only K覆盖。
	high, e := normalInflightCeilDiv(pages, uint64(caps.PerCPUPagelistHighFraction))
	if e != nil {
		return r, e
	}
	high, e = normalInflightSum(high, 4*63, 3*63, 8)
	if e != nil {
		return r, e
	}
	count, e := normalInflightMultiply(c, high)
	if e != nil {
		return r, e
	}
	r.PCPTotalFreeReserveBytes, e = normalInflightMultiply(count, p)
	if e != nil {
		return r, e
	}
	// MOVABLE WMARK_MIN<=128；距离仍使用未截断tmp，不能误用min/4。
	// tmp=(min_free_pages*managedMOVABLE)/managedNonMovable，后取tmp/4或scale。
	// 取PROMO的3段距离，比只HIGH更保守；boost必须原持续0。
	minBytes, e := normalInflightMultiply(uint64(caps.MinFreeKBytes), 1024)
	if e != nil {
		return r, e
	}
	weighted, e := normalInflightMultiply(minBytes/p, pages)
	if e != nil {
		return r, e
	}
	distance, e := normalInflightCeilDiv(weighted, f.NonMovableManagedPages)
	if e != nil {
		return r, e
	}
	distance, e = normalInflightCeilDiv(distance, 4)
	if e != nil {
		return r, e
	}
	scaled, e := normalInflightMultiply(pages, uint64(caps.WatermarkScaleFactor))
	if e != nil {
		return r, e
	}
	scaled, e = normalInflightCeilDiv(scaled, 10000)
	if e != nil {
		return r, e
	}
	if scaled > distance {
		distance = scaled
	}
	distance, e = normalInflightMultiply(distance, 3)
	if e != nil {
		return r, e
	}
	distance, e = normalInflightSum(distance, 128)
	if e != nil {
		return r, e
	}
	r.MovableWatermarkReserveBytes, e = normalInflightMultiply(distance, p)
	if e != nil {
		return r, e
	}
	// F_force<=K只在全force入口+禁止Normal迁出闭合时才成立，当前不给资格。
	// 同一K在normal里覆盖force、Outside里覆盖kernel+initial-root，重复预留明确保留。
	r.ConditionalForceChargeBytes = k
	r.ConditionalNormalTargetBytes, e = normalInflightSum(in.NormalLeavesBytes, k, t.CounterHierarchyGapBytes)
	if e != nil {
		return r, e
	}
	r.OutsideKernelAndInitialRootBytes, e = normalInflightMultiply(k, 2)
	if e != nil {
		return r, e
	}
	r.TotalRequiredBytes, e = normalInflightSum(r.ConditionalNormalTargetBytes, in.SystemBytes, r.OutsideKernelAndInitialRootBytes, t.KnownConditionalOccupiedBytes, t.CompactionIsolatedFreeReserveBytes, t.CPUStockPaidBytes, r.PCPTotalFreeReserveBytes, r.MovableWatermarkReserveBytes)
	if e != nil {
		return r, e
	}
	r.CandidateFits = r.TotalRequiredBytes <= in.MaximumBytes
	r.CandidateOnly = true
	if r.CandidateFits {
		r.CandidateSpareBytes = in.MaximumBytes - r.TotalRequiredBytes
	}
	return r, nil
}
