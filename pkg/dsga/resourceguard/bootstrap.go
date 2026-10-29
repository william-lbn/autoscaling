package resourceguard

import (
	"context"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// Limits是PG出生前也能建立的硬约束范围。不得伪造一个Postmaster PID来套用运行
// 期命令；初始化时PG尚不存在，真实Guest/cgroup及整组限额仍须独立读回。
type Limits struct {
	MemoryMaxBytes control.Uint64 `json:"memory_max_bytes"`
	// 0/缺省保持旧未约束来源，绝不补造默认。有限任务数覆盖线程，原创建后冻结；
	// 它不是私有字节上界，也不保证后台工作进程仍有出生余量。
	PidsMaxTasks control.Uint64 `json:"pids_max_tasks,omitempty"`
	// 原创建可明确限制每个进程的整个虚拟地址空间。0保持旧未约束来源；
	// 非零必须与有限任务数一起使用。它包括共享VA/占位，绝不能直接当私有
	// RSS或从预算扣除；实际soft/hard与不可提权属性须出生后逐进程核验。
	AddressSpaceMaxBytes control.Uint64 `json:"address_space_max_bytes,omitempty"`
	// 系统计费域额度包含于OutsideUpperBytes，不能再次相加；0保持旧未约束来源。
	SystemMemoryMaxBytes control.Uint64 `json:"system_memory_max_bytes,omitempty"`
	OutsideUpperBytes    control.Uint64 `json:"outside_upper_bytes"`
	HugePools            []HugePool     `json:"huge_pools"`
}

type BootstrapCommand struct {
	ProtocolVersion    uint32         `json:"protocol_version"`
	Identity           Identity       `json:"identity"`
	IssuerEpoch        control.Uint64 `json:"issuer_epoch"`
	BudgetCommitmentID string         `json:"budget_commitment_id"`
	Limits             Limits         `json:"limits"`
}

type bootstrapRecord struct {
	Original BootstrapCommand `json:"original"`
	Applied  bool             `json:"applied"`
	// 限额确认与启动归组是两个事实；归组未确认时不得出生PG。
	SystemPlacementObserved bool `json:"system_placement_observed,omitempty"`
}

func bootstrapMatches(e Evidence, id Identity, l Limits) error {
	if e.CgroupPopulated || (l.PidsMaxTasks != 0 && (e.TaskLimit == nil || e.TaskLimit.CurrentTasks != 0)) {
		return fmt.Errorf("bootstrap cgroup is not empty")
	}
	return e.matchesLimits(id, l)
}

func (c Command) limits() Limits {
	return Limits{MemoryMaxBytes: c.MemoryMaxBytes, PidsMaxTasks: c.PidsMaxTasks, SystemMemoryMaxBytes: c.SystemMemoryMaxBytes, AddressSpaceMaxBytes: c.AddressSpaceMaxBytes, OutsideUpperBytes: c.OutsideUpperBytes, HugePools: c.HugePools}
}

func (l Limits) Validate() error {
	if err := validateAddressSpaceLimit(l.AddressSpaceMaxBytes, l.PidsMaxTasks); err != nil {
		return err
	}
	if l.SystemMemoryMaxBytes > l.OutsideUpperBytes {
		return fmt.Errorf("system charged-domain cap exceeds retained outside bound")
	}
	if l.PidsMaxTasks > MaxPidsTasks {
		return fmt.Errorf("task limit exceeds supported finite Linux range")
	}
	if l.MemoryMaxBytes == 0 || l.OutsideUpperBytes == 0 || len(l.HugePools) == 0 || len(l.HugePools) > 32 {
		return fmt.Errorf("incomplete resource bounds")
	}
	var previous control.Uint64
	for _, p := range l.HugePools {
		if p.PageSizeKB == 0 || p.PageSizeKB <= previous || p.PageSizeKB > control.Uint64(math.MaxUint64/1024) || p.Pages > control.Uint64(math.MaxUint64)/(p.PageSizeKB*1024) {
			return fmt.Errorf("unordered, duplicate or overflowing HugeTLB pool")
		}
		previous = p.PageSizeKB
	}
	_, err := l.totalUpper()
	return err
}

func (l Limits) totalUpper() (uint64, error) {
	total := uint64(l.MemoryMaxBytes)
	parts := []uint64{uint64(l.OutsideUpperBytes)}
	for _, p := range l.HugePools {
		parts = append(parts, uint64(p.PageSizeKB)*1024*uint64(p.Pages))
	}
	for _, p := range parts {
		if p > math.MaxUint64-total {
			return 0, fmt.Errorf("Guest resource budget overflows")
		}
		total += p
	}
	return total, nil
}

func (c BootstrapCommand) Validate() error {
	if c.ProtocolVersion != 1 {
		return fmt.Errorf("unsupported bootstrap resource command")
	}
	if err := validateResourceContext(c.Identity, c.IssuerEpoch, c.BudgetCommitmentID); err != nil {
		return err
	}
	if err := systemIdentityMatches(c.Identity, c.Limits.SystemMemoryMaxBytes); err != nil {
		return err
	}
	return c.Limits.Validate()
}

// InspectIdentity只有内核身份读取，无初始化/epoch推进；原bootstrap调用者须先
// 冻结返回的真实身份再持久提交完整命令。该返回值不含预算、准入或VM许可。
func (g *Guard) InspectIdentity() (Identity, error) {
	id, err := g.kernel.Identity()
	if err != nil {
		return Identity{}, err
	}
	if id.VMUID != g.config.VMUID || id.ComputeInstanceID != g.config.ComputeInstanceID || g.config.SystemCgroup != (id.SystemCgroupInode != 0) {
		return Identity{}, fmt.Errorf("resource inspection binding differs")
	}
	return id, nil
}

// Bootstrap写前保存原创建约束，PG尚不存在时先落实真实限额。首次动作只能面对
// 不存在的日志；已有裸Initialize/旧运行期记录不能被升级成新创建，避免绕过旧
// 不确定性。回复丢失后普通调用仅观察，显式ReplayBootstrap才可原样再次写。
func (g *Guard) Bootstrap(ctx context.Context, c BootstrapCommand) (Evidence, error) {
	return g.bootstrap(ctx, c, false, false)
}
func (g *Guard) ObserveBootstrap(ctx context.Context, c BootstrapCommand) (Evidence, error) {
	return g.bootstrap(ctx, c, true, false)
}
func (g *Guard) ReplayBootstrap(ctx context.Context, c BootstrapCommand) (Evidence, error) {
	return g.bootstrap(ctx, c, false, true)
}

func (g *Guard) bootstrap(ctx context.Context, c BootstrapCommand, observe, replay bool) (Evidence, error) {
	var evidence Evidence
	if err := c.Validate(); err != nil {
		return evidence, err
	}
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		return evidence, err
	}
	defer s.close()
	id, err := g.InspectIdentity()
	if err != nil {
		return evidence, err
	}
	if id != c.Identity {
		return evidence, fmt.Errorf("bootstrap Guest/cgroup differs")
	}
	r, err := s.load()
	if err != nil {
		return evidence, err
	}
	if r == nil {
		if observe || replay {
			return evidence, ErrUnknown
		}
		r = &record{Version: 1, Identity: id, Floor: c.IssuerEpoch}
	} else if err := r.validate(id); err != nil {
		return evidence, err
	} else if r.Floor < c.IssuerEpoch || r.Bootstrap == nil || !reflect.DeepEqual(r.Bootstrap.Original, c) || r.Original != nil || r.Held != nil {
		return evidence, fmt.Errorf("bootstrap differs from retained creation or runtime has started")
	} else if r.Bootstrap.Applied || observe {
		evidence, err = g.kernel.ReadBounds(id, c.Limits)
		if err != nil {
			return evidence, err
		}
		if err := bootstrapMatches(evidence, id, c.Limits); err != nil {
			return evidence, fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		if !r.Bootstrap.Applied {
			r.Bootstrap.Applied = true
			if err := s.save(r); err != nil {
				return evidence, fmt.Errorf("%w: bootstrap completion persistence: %v", ErrUnknown, err)
			}
		}
		return evidence, nil
	} else if !replay {
		return evidence, ErrUnknown
	}
	if r.Floor != c.IssuerEpoch {
		return evidence, fmt.Errorf("bootstrap write is fenced")
	}
	before, err := g.kernel.ReadBounds(id, c.Limits)
	if err != nil {
		return evidence, err
	}
	upper, err := c.Limits.totalUpper()
	if err != nil || upper > uint64(before.GuestMemTotalBytes) || before.MemoryCurrentBytes > c.Limits.MemoryMaxBytes || before.SwapCurrentBytes != 0 || before.CgroupPopulated || (c.Limits.PidsMaxTasks != 0 && (before.TaskLimit == nil || before.TaskLimit.CurrentTasks != 0)) {
		return evidence, fmt.Errorf("bootstrap target does not fit actual Guest occupancy")
	}
	if c.Limits.SystemMemoryMaxBytes != 0 && (before.System == nil || before.System.ProcessCount != 0) {
		return evidence, fmt.Errorf("initial system cap requires empty charged domain")
	}
	// 空域也须来自完整读取；不能用缺失/伪造的AS事实先保存并写入新资源。
	if err := before.matchesAddressSpace(c.Limits); err != nil {
		return evidence, err
	}
	if err := ctx.Err(); err != nil {
		return evidence, err
	}
	copy := c
	copy.Limits.HugePools = append([]HugePool(nil), c.Limits.HugePools...)
	r.Bootstrap = &bootstrapRecord{Original: copy}
	if err := s.save(r); err != nil {
		return evidence, fmt.Errorf("%w: bootstrap intent persistence: %v", ErrUnknown, err)
	}
	if err := g.kernel.ApplyBounds(c.Limits); err != nil {
		return evidence, fmt.Errorf("%w: bootstrap write may have applied: %v", ErrUnknown, err)
	}
	evidence, err = g.kernel.ReadBounds(id, c.Limits)
	if err != nil {
		return evidence, fmt.Errorf("%w: bootstrap readback: %v", ErrUnknown, err)
	}
	if err := bootstrapMatches(evidence, id, c.Limits); err != nil {
		return evidence, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	r.Bootstrap.Applied = true
	if err := s.save(r); err != nil {
		return evidence, fmt.Errorf("%w: bootstrap completion persistence: %v", ErrUnknown, err)
	}
	return evidence, nil
}

func (v *Limits) UnmarshalJSON(data []byte) error {
	type plain Limits
	var n plain
	if err := control.UnmarshalObject(data, &n); err != nil {
		return err
	}
	*v = Limits(n)
	return nil
}
func (v *BootstrapCommand) UnmarshalJSON(data []byte) error {
	type plain BootstrapCommand
	var n plain
	if err := control.UnmarshalObject(data, &n); err != nil {
		return err
	}
	*v = BootstrapCommand(n)
	return nil
}
func (v *bootstrapRecord) UnmarshalJSON(data []byte) error {
	type plain bootstrapRecord
	var n plain
	if err := control.UnmarshalObject(data, &n); err != nil {
		return err
	}
	*v = bootstrapRecord(n)
	return nil
}
