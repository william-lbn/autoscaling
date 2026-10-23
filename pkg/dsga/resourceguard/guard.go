// Package resourceguard执行Guest内的资源硬约束。它不是预算估算器，也不发VM许可。
// 生产入口仅接受root持有的固定配置；网络身份/租约仍须由调用方认证。PG boot不参与
// 资源floor生命周期：PG重启不能解除尚未完成的VM动作所依赖的旧资源承诺。
package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

var ErrUnknown = errors.New("resource command requires original readback or explicit replay")

// Config必须来自原VM的可信只读配置，不能从远程请求构造路径。StateDir位于Guest
// 持久盘，独占本VM incarnation；不得放/tmp、重启即清空的/run或PG数据目录内。
type Config struct {
	VMUID             string `json:"vm_uid"`
	ComputeInstanceID string `json:"compute_instance_id"`
	StateDir          string `json:"state_dir"`
}

type Identity struct {
	VMUID             string         `json:"vm_uid"`
	ComputeInstanceID string         `json:"compute_instance_id"`
	GuestBootID       string         `json:"guest_boot_id"`
	CgroupDevice      control.Uint64 `json:"cgroup_device"`
	CgroupInode       control.Uint64 `json:"cgroup_inode"`
}

type HugePool struct {
	PageSizeKB control.Uint64 `json:"page_size_kb"`
	Pages      control.Uint64 `json:"pages"`
}

// Command是完整原意图，调用方须在外层账本持久化后调用。OutsideUpperBytes是已
// 保留的系统/过渡/安全上界，不是本执行器证明的OS硬限；不得将本回执单独当作
// FinalizeBudget。PostmasterPID/StartTicks来自当前受信PG进程，防止PID复用。
// HugePools必须覆盖Guest全部实际hstate，包括目标0的池；不接受只列默认2MiB池。
type Command struct {
	ProtocolVersion      uint32         `json:"protocol_version"`
	Identity             Identity       `json:"identity"`
	IssuerEpoch          control.Uint64 `json:"issuer_epoch"`
	Revision             control.Uint64 `json:"revision"`
	BudgetCommitmentID   string         `json:"budget_commitment_id"`
	PostmasterPID        int            `json:"postmaster_pid"`
	PostmasterStartTicks control.Uint64 `json:"postmaster_start_ticks"`
	MemoryMaxBytes       control.Uint64 `json:"memory_max_bytes"`
	OutsideUpperBytes    control.Uint64 `json:"outside_upper_bytes"`
	HugePools            []HugePool     `json:"huge_pools"`
}

// Evidence只描述当前硬约束与核验时的实际占用，不是物理回收/Host释放或无OOM证明。
// Pool上界覆盖整个Guest池（含空闲）；memory controller必须未计HugeTLB，才可
// 与MemoryMax相加。内核临时超限、异步释放仍须安全余量及目标环境验收。
type Evidence struct {
	ProtocolVersion        uint32         `json:"protocol_version"`
	VMShrinkPermit         bool           `json:"vm_shrink_permit"`
	Identity               Identity       `json:"identity"`
	MemoryMaxBytes         control.Uint64 `json:"memory_max_bytes"`
	MemoryCurrentBytes     control.Uint64 `json:"memory_current_bytes"`
	SwapMaxBytes           control.Uint64 `json:"swap_max_bytes"`
	SwapCurrentBytes       control.Uint64 `json:"swap_current_bytes"`
	GuestMemTotalBytes     control.Uint64 `json:"guest_mem_total_bytes"`
	HugePools              []HugePool     `json:"huge_pools"`
	WholePoolUpperBytes    control.Uint64 `json:"whole_pool_upper_bytes"`
	HugeOvercommitDisabled bool           `json:"huge_overcommit_disabled"`
	HugeSurplusPages       control.Uint64 `json:"huge_surplus_pages"`
}

type record struct {
	Version  uint32         `json:"version"`
	Identity Identity       `json:"identity"`
	Floor    control.Uint64 `json:"floor"`
	Original *Command       `json:"original"`
	Held     *Command       `json:"held"`
	Applied  bool           `json:"applied"`
}

// kernel所有写入均同步完成。调用者ctx到期不能把正在内核中执行的写分离成后台
// goroutine，再提前放锁；否则旧owner可能在新约束后迟到放宽memory.max。
type kernel interface {
	Identity() (Identity, error)
	Read(Command) (Evidence, error)
	Apply(Command) error
}

type Guard struct {
	config Config
	kernel kernel
}

func (c Command) Validate() error {
	if c.ProtocolVersion != 1 || c.IssuerEpoch == 0 || c.Revision == 0 || c.PostmasterPID <= 1 || c.PostmasterStartTicks == 0 || c.MemoryMaxBytes == 0 || c.OutsideUpperBytes == 0 || len(c.HugePools) == 0 || len(c.HugePools) > 32 {
		return fmt.Errorf("incomplete resource command")
	}
	if control.ValidateID(c.Identity.VMUID, "VM UID") != nil || control.ValidateID(c.Identity.ComputeInstanceID, "Compute instance") != nil || !validGuestBoot(c.Identity.GuestBootID) || c.Identity.CgroupInode == 0 || control.ValidateID(c.BudgetCommitmentID, "budget commitment") != nil {
		return fmt.Errorf("invalid resource identity")
	}
	var previous control.Uint64
	for _, pool := range c.HugePools {
		if pool.PageSizeKB == 0 || pool.PageSizeKB <= previous || pool.PageSizeKB > control.Uint64(math.MaxUint64/1024) || pool.Pages > control.Uint64(math.MaxUint64)/(pool.PageSizeKB*1024) {
			return fmt.Errorf("unordered, duplicate or overflowing HugeTLB pool")
		}
		previous = pool.PageSizeKB
	}
	_, err := c.totalUpper()
	return err
}

// Linux boot_id保留内核的规范UUID格式；不能直接套用本项目32字符VM incarnation。
func validGuestBoot(s string) bool {
	return len(s) == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-' && control.ValidateIncarnation(strings.ReplaceAll(s, "-", "")) == nil
}

func (c Command) totalUpper() (uint64, error) {
	total := uint64(c.MemoryMaxBytes)
	parts := []uint64{uint64(c.OutsideUpperBytes)}
	for _, pool := range c.HugePools {
		parts = append(parts, uint64(pool.PageSizeKB)*1024*uint64(pool.Pages))
	}
	for _, part := range parts {
		if part > math.MaxUint64-total {
			return 0, fmt.Errorf("Guest resource budget overflows")
		}
		total += part
	}
	return total, nil
}

func (e Evidence) matches(c Command) error {
	if e.ProtocolVersion != 1 || e.VMShrinkPermit || e.Identity != c.Identity || e.MemoryMaxBytes != c.MemoryMaxBytes || e.MemoryCurrentBytes > c.MemoryMaxBytes || e.SwapMaxBytes != 0 || e.SwapCurrentBytes != 0 || !e.HugeOvercommitDisabled || e.HugeSurplusPages != 0 || !reflect.DeepEqual(e.HugePools, c.HugePools) {
		return fmt.Errorf("actual resource limits, occupancy or identity differ")
	}
	total, err := c.totalUpper()
	if err != nil || total > uint64(e.GuestMemTotalBytes) {
		return fmt.Errorf("resource budget exceeds actual Guest MemTotal")
	}
	poolBytes := uint64(0)
	for _, p := range e.HugePools {
		poolBytes += uint64(p.PageSizeKB) * 1024 * uint64(p.Pages)
	}
	if e.WholePoolUpperBytes != control.Uint64(poolBytes) {
		return fmt.Errorf("whole HugeTLB pool differs")
	}
	return nil
}

// withRecord锁序固定为本VM资源文件锁→持久记录→内核读取/同步写入→记录fsync。
// 锁独立于进程和PG boot；正常返回、HTTP超时、上层取消均不删除已持久的原意图。
func (g *Guard) withRecord(ctx context.Context, fn func(*record, *lockedState) error) error {
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		return err
	}
	defer s.close()
	id, err := g.kernel.Identity()
	if err != nil {
		return err
	}
	if id.VMUID != g.config.VMUID || id.ComputeInstanceID != g.config.ComputeInstanceID {
		return fmt.Errorf("resource config binding differs")
	}
	r, err := s.load()
	if err != nil {
		return err
	}
	if r == nil {
		return fmt.Errorf("resource journal missing; explicit original-bootstrap initialization required")
	}
	if r.Version != 1 || r.Floor == 0 || r.Identity != id || (r.Applied && r.Original == nil) {
		return fmt.Errorf("resource journal identity changed; controlled recovery required")
	}
	if r.Original != nil && (r.Original.Validate() != nil || r.Original.Identity != id || r.Original.IssuerEpoch > r.Floor) {
		return fmt.Errorf("invalid retained resource command")
	}
	if r.Held != nil && (r.Held.Validate() != nil || r.Held.Identity != id || r.Held.IssuerEpoch > r.Floor || r.Original == nil || r.Held.Revision > r.Original.Revision) {
		return fmt.Errorf("invalid retained held constraint")
	}
	if r.Applied && (r.Held == nil || !reflect.DeepEqual(r.Held, r.Original)) {
		return fmt.Errorf("applied resource journal lacks original held constraint")
	}
	if !r.Applied && r.Original != nil {
		if r.Held == nil {
			if r.Original.Revision != 1 {
				return fmt.Errorf("first resource command revision differs")
			}
		} else if r.Held.Revision == control.Uint64(math.MaxUint64) || r.Original.Revision != r.Held.Revision+1 {
			return fmt.Errorf("pending resource command lost held revision")
		}
	}
	return fn(r, s)
}

// Initialize是原VM创建流程的一次性动作，不是恢复失败后的自动fallback。调用方
// 必须已证明这是原bootstrap且不存在旧Guest/Runner动作；缺日志不能由Fence补建。
// 即使旧文件为空/损坏也不覆盖。后续恢复必须使用既有记录，PG重启永不调用本入口。
func (g *Guard) Initialize(ctx context.Context, epoch control.Uint64) (Identity, error) {
	var id Identity
	if epoch == 0 {
		return id, fmt.Errorf("zero initialization epoch")
	}
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		return id, err
	}
	defer s.close()
	r, err := s.load()
	if err != nil {
		return id, err
	}
	if r != nil {
		return id, fmt.Errorf("resource journal already initialized; use fence or original observation")
	}
	id, err = g.kernel.Identity()
	if err != nil {
		return id, err
	}
	if id.VMUID != g.config.VMUID || id.ComputeInstanceID != g.config.ComputeInstanceID {
		return id, fmt.Errorf("resource initialization binding differs")
	}
	err = s.save(&record{Version: 1, Identity: id, Floor: epoch})
	return id, err
}

// Fence先持久化更高代次，随后旧进程的任何写都必须在同一锁内重新核验。未知旧
// 原意图仍保留；新owner只能先读回或进行受控恢复，不能抹掉旧承诺另起revision。
func (g *Guard) Fence(ctx context.Context, epoch control.Uint64) (Identity, error) {
	var id Identity
	if epoch == 0 {
		return id, fmt.Errorf("zero resource epoch")
	}
	err := g.withRecord(ctx, func(r *record, s *lockedState) error {
		if epoch < r.Floor {
			return fmt.Errorf("stale resource epoch")
		}
		if epoch > r.Floor {
			r.Floor = epoch
			if err := s.save(r); err != nil {
				return err
			}
		}
		id = r.Identity
		return nil
	})
	return id, err
}

func (g *Guard) Apply(ctx context.Context, command Command) (Evidence, error) {
	return g.apply(ctx, command, false)
}

// ReplayOriginal只接受当前floor下完全相同的未确认原意图；普通Apply对未知命令
// 不自动重放。旧epoch查询可核实结果，却不能改epoch后写同一份命令。
func (g *Guard) ReplayOriginal(ctx context.Context, command Command) (Evidence, error) {
	return g.apply(ctx, command, true)
}

func (g *Guard) apply(ctx context.Context, command Command, replay bool) (Evidence, error) {
	var evidence Evidence
	if err := command.Validate(); err != nil {
		return evidence, err
	}
	err := g.withRecord(ctx, func(r *record, s *lockedState) error {
		if command.Identity != r.Identity || command.IssuerEpoch != r.Floor {
			return fmt.Errorf("resource command is fenced")
		}
		same := r.Original != nil && reflect.DeepEqual(*r.Original, command)
		if r.Original != nil && !r.Applied && (!same || !replay) {
			return ErrUnknown
		}
		if same && r.Applied {
			var err error
			evidence, err = g.kernel.Read(command)
			if err != nil {
				return err
			}
			return evidence.matches(command)
		}
		if !same {
			if replay {
				return fmt.Errorf("replay differs from retained original")
			}
			previous := control.Uint64(0)
			if r.Original != nil {
				previous = r.Original.Revision
			}
			if previous == control.Uint64(math.MaxUint64) || command.Revision != previous+1 {
				return fmt.Errorf("resource revision is not consecutive")
			}
		}
		// 内核预检可以拒绝明显不安全的缩小，但采样不是排空/预算证明。调用者必须
		// 先关准入并完成V3排空；本层仍在写后独立核验实际占用和全部文件读回。
		before, err := g.kernel.Read(command)
		if err != nil {
			return err
		}
		total, err := command.totalUpper()
		if err != nil || total > uint64(before.GuestMemTotalBytes) || before.MemoryCurrentBytes > command.MemoryMaxBytes || before.SwapCurrentBytes != 0 {
			return fmt.Errorf("Guest occupancy or usable memory cannot admit target limits")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// 必须先保存未知，再做第一项可能部分生效的系统调用。save失败即不写内核；
		// 内核写失败、读回失败和最终fsync失败均保留原意图，不回滚放宽旧约束。
		copy := command
		copy.HugePools = append([]HugePool(nil), command.HugePools...)
		r.Original = &copy
		r.Applied = false
		if err := s.save(r); err != nil {
			return fmt.Errorf("%w: original resource intent persistence: %v", ErrUnknown, err)
		}
		if err := g.kernel.Apply(command); err != nil {
			return fmt.Errorf("%w: resource write may have applied: %v", ErrUnknown, err)
		}
		evidence, err = g.kernel.Read(command)
		if err != nil {
			return fmt.Errorf("%w: resource write readback: %v", ErrUnknown, err)
		}
		if err := evidence.matches(command); err != nil {
			return fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		r.Applied = true
		r.Held = r.Original
		if err := s.save(r); err != nil {
			return fmt.Errorf("%w: resource completion persistence: %v", ErrUnknown, err)
		}
		return nil
	})
	return evidence, err
}

// Observe必须带完整原意图。not-found/内容不匹配不证明NoEffect；只有当前所有
// 约束、进程身份、占用和Guest预算均吻合才能确认，并持久保留原命令与floor。
func (g *Guard) Observe(ctx context.Context, command Command) (Evidence, error) {
	var evidence Evidence
	if err := command.Validate(); err != nil {
		return evidence, err
	}
	err := g.withRecord(ctx, func(r *record, s *lockedState) error {
		if r.Original == nil || !reflect.DeepEqual(*r.Original, command) {
			return ErrUnknown
		}
		var err error
		evidence, err = g.kernel.Read(command)
		if err != nil {
			return err
		}
		if err := evidence.matches(command); err != nil {
			return fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		if !r.Applied {
			r.Applied = true
			r.Held = r.Original
			return s.save(r)
		}
		return nil
	})
	return evidence, err
}

func sortedPools(p []HugePool) {
	sort.Slice(p, func(i, j int) bool { return p[i].PageSizeKB < p[j].PageSizeKB })
}

func (v *Command) UnmarshalJSON(data []byte) error {
	type plain Command
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	*v = Command(next)
	return nil
}

func (v *Config) UnmarshalJSON(data []byte) error {
	type plain Config
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	*v = Config(next)
	return nil
}
func (v *Identity) UnmarshalJSON(data []byte) error {
	type plain Identity
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	*v = Identity(next)
	return nil
}
func (v *HugePool) UnmarshalJSON(data []byte) error {
	type plain HugePool
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	*v = HugePool(next)
	return nil
}
func (v *record) UnmarshalJSON(data []byte) error {
	type plain record
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	*v = record(next)
	return nil
}

// 编译时保留严格JSON入口，避免未来以map[string]any丢失uint64原值。
var _ json.Unmarshaler = (*Command)(nil)
