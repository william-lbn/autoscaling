package resourceguard

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const SystemChargedScope = "system-cgroup-charged-pages"

// SystemEvidence只证明固定系统组的计费页约束和本次观察的归组。Linux迁入进程
// 不迁移既有memory charge；根组旧页、内核未计费项等仍须单独预算，绝不能把它
// 命名为完整SystemOutsidePG或用当前RSS补足。额度在原创建后冻结，缩容不调低
// 管理平面额度；0模式不返回此字段。ProcessCount不是字节数，也不是持续活性证明。
type SystemEvidence struct {
	Scope              string         `json:"scope"`
	MemoryMaxBytes     control.Uint64 `json:"memory_max_bytes"`
	MemoryCurrentBytes control.Uint64 `json:"memory_current_bytes"`
	SwapMaxBytes       control.Uint64 `json:"swap_max_bytes"`
	SwapCurrentBytes   control.Uint64 `json:"swap_current_bytes"`
	ProcessCount       control.Uint64 `json:"process_count"`
	PID1Member         bool           `json:"pid1_member"`
	RootUserspaceCount control.Uint64 `json:"root_userspace_count"`
}

func (v *SystemEvidence) UnmarshalJSON(b []byte) error {
	type plain SystemEvidence
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	*v = SystemEvidence(p)
	return nil
}
func systemIdentityMatches(id Identity, cap control.Uint64) error {
	if (cap != 0) != (id.SystemCgroupInode != 0) || (id.SystemCgroupInode == 0 && id.SystemCgroupDevice != 0) || (cap != 0 && id.SystemCgroupDevice != id.CgroupDevice) {
		return fmt.Errorf("system charged-domain identity differs from original mode")
	}
	return nil
}
func (r *record) systemLimit() control.Uint64 {
	if r.Bootstrap == nil {
		return 0
	}
	return r.Bootstrap.Original.Limits.SystemMemoryMaxBytes
}
func (e Evidence) matchesSystem(cap control.Uint64) error {
	if err := systemIdentityMatches(e.Identity, cap); err != nil {
		return err
	}
	if cap == 0 {
		if e.System != nil {
			return fmt.Errorf("unexpected system evidence for legacy source")
		}
		return nil
	}
	s := e.System
	if s == nil || s.Scope != SystemChargedScope || s.MemoryMaxBytes != cap || s.MemoryCurrentBytes > cap || s.SwapMaxBytes != 0 || s.SwapCurrentBytes != 0 {
		return fmt.Errorf("system charged-domain bound is absent, drifted or exceeded")
	}
	return nil
}
func (e Evidence) systemPlacementMatches(cap control.Uint64) error {
	if err := e.matchesSystem(cap); err != nil {
		return err
	}
	if cap != 0 && (!e.System.PID1Member || e.System.RootUserspaceCount != 0) {
		return fmt.Errorf("system boot placement is not currently observed")
	}
	return nil
}

// placeSystem与PG出生、资源写共用唯一文件锁，顺序为已fsync原Bootstrap→实际
// 限额读回→同步迁移→归组/限额读回→fsync确认。在此之前PG出生入口拒绝；不迁移
// 任意请求PID、不杀进程、不清除旧Birth。部分迁移或保存失败保留原意图及false，
// sysinit只能重试同一归组范围。确认后重试只读，发现漂移绝不自动迁移修复。
func (g *Guard) placeSystem(ctx context.Context, move func(context.Context) error) (Evidence, error) {
	var e Evidence
	err := g.withRecord(ctx, func(r *record, s *lockedState) error {
		if r.Bootstrap == nil || !r.Bootstrap.Applied || r.Original != nil || r.Held != nil || r.Birth != nil {
			return fmt.Errorf("system placement requires original applied bootstrap before PG birth")
		}
		l := r.Bootstrap.Original.Limits
		var err error
		e, err = g.kernel.ReadBounds(r.Identity, l)
		if err != nil {
			return err
		}
		if err = bootstrapMatches(e, r.Identity, l); err != nil {
			return err
		}
		if l.SystemMemoryMaxBytes == 0 {
			return nil
		}
		if r.Bootstrap.SystemPlacementObserved {
			return e.systemPlacementMatches(l.SystemMemoryMaxBytes)
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = move(ctx); err != nil {
			return fmt.Errorf("%w: partial system placement: %v", ErrUnknown, err)
		}
		e, err = g.kernel.ReadBounds(r.Identity, l)
		if err != nil {
			return fmt.Errorf("%w: system placement readback: %v", ErrUnknown, err)
		}
		if err = bootstrapMatches(e, r.Identity, l); err != nil {
			return fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		if err = e.systemPlacementMatches(l.SystemMemoryMaxBytes); err != nil {
			return fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		r.Bootstrap.SystemPlacementObserved = true
		if err = s.save(r); err != nil {
			return fmt.Errorf("%w: system placement persistence: %v", ErrUnknown, err)
		}
		return nil
	})
	return e, err
}

type systemProcess struct {
	pid   int
	ticks uint64
}
type bootPlacement interface {
	rootUserspace() ([]systemProcess, error)
	moveSystem(systemProcess) error
}

// PID1先迁入，后续常规子进程继承固定系统组；重新扫描吸收此前fork的子进程。
// 只做有界同步4轮，不以无限循环掩盖持续逃逸，也不因ctx取消留下后台写。扫描
// 不是原子快照；本层依赖sysinit在普通服务/PG之前、root独占迁移入口这一前提。
func moveBootUserspace(ctx context.Context, p bootPlacement) error {
	for pass := 0; pass < 4; pass++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		list, err := p.rootUserspace()
		if err != nil {
			return err
		}
		if len(list) == 0 {
			return nil
		}
		sort.Slice(list, func(i, j int) bool { return list[i].pid < list[j].pid })
		previous := systemProcess{}
		for _, proc := range list {
			if proc.pid < 1 {
				return fmt.Errorf("invalid root userspace PID")
			}
			if proc.pid == previous.pid {
				if proc == previous {
					continue
				}
				return fmt.Errorf("conflicting root process identity")
			}
			previous = proc
			if err = ctx.Err(); err != nil {
				return err
			}
			if err = p.moveSystem(proc); err != nil {
				return err
			}
		}
	}
	list, err := p.rootUserspace()
	if err != nil {
		return err
	}
	if len(list) != 0 {
		return fmt.Errorf("root userspace did not settle during boot placement")
	}
	return nil
}

// comm可含括号和空格，flags为第9字段（后缀6），starttime第22（后缀19）。
// PF_KTHREAD明确排除内核线程；PID1的启动tick可为0，不能套用要求非零的PG解析。
func bootProcessStat(text string, pid int) (systemProcess, bool, error) {
	p := systemProcess{pid: pid}
	start := strings.Index(text, " (")
	end := strings.LastIndex(text, ")")
	if pid < 1 || len(text) > 64*1024 || start < 0 || end <= start || text[:start] != strconv.Itoa(pid) {
		return p, false, fmt.Errorf("invalid boot process stat")
	}
	f := strings.Fields(text[end+1:])
	if len(f) < 20 {
		return p, false, fmt.Errorf("short boot process stat")
	}
	flags, err := strconv.ParseUint(f[6], 10, 64)
	if err != nil {
		return p, false, err
	}
	p.ticks, err = strconv.ParseUint(f[19], 10, 64)
	if err != nil {
		return p, false, err
	}
	return p, flags&0x00200000 != 0, nil
}
