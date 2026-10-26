package resourceguard

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// View是同一资源锁内读取的原日志和当前限额。Original/Held属于独立于PG boot
// 的资源域，不能从Native PG revision推导资源revision，也不能因外层历史淘汰
// 猜测计数。它不是原命令完成回执，尤其不能拿新PG的Actual替代旧PID的Observe。
type View struct {
	ProtocolVersion         uint32           `json:"protocol_version"`
	VMShrinkPermit          bool             `json:"vm_shrink_permit"`
	Identity                Identity         `json:"identity"`
	IssuerFloorEpoch        control.Uint64   `json:"issuer_floor_epoch"`
	Bootstrap               BootstrapCommand `json:"bootstrap"`
	BootstrapApplied        bool             `json:"bootstrap_applied"`
	SystemPlacementObserved bool             `json:"system_placement_observed,omitempty"`
	Original                *Command         `json:"original"`
	Held                    *Command         `json:"held"`
	Applied                 bool             `json:"applied"`
	Birth                   *ProcessIdentity `json:"birth"`
	Actual                  Evidence         `json:"actual"`
}

func (v View) Validate() error {
	if v.ProtocolVersion != 1 || v.VMShrinkPermit || v.Actual.ProtocolVersion != 1 || v.Actual.VMShrinkPermit || v.Actual.Identity != v.Identity {
		return fmt.Errorf("invalid resource journal observation")
	}
	r := record{Version: 1, Identity: v.Identity, Floor: v.IssuerFloorEpoch, Original: v.Original, Held: v.Held, Applied: v.Applied, Bootstrap: &bootstrapRecord{Original: v.Bootstrap, Applied: v.BootstrapApplied, SystemPlacementObserved: v.SystemPlacementObserved}, Birth: v.Birth}
	return r.validate(v.Identity)
}
func (v *View) UnmarshalJSON(b []byte) error {
	type plain View
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	next := View(p)
	if err := next.Validate(); err != nil {
		return err
	}
	*v = next
	return nil
}

// ConfirmedLimits仅说明当前限额仍与已落盘Held/Bootstrap吻合；不含Postmaster
// 生存、完整外部上界或VM效果的证明。未知命令一律保留，不能把旧Held的读回
// 变成新Original的NoEffect或完成。首次资源绑定可用此检查拒绝私自漂移的约束。
func (v View) ConfirmedLimits() (Limits, error) {
	if err := v.Validate(); err != nil {
		return Limits{}, err
	}
	if !v.BootstrapApplied || (v.Bootstrap.Limits.SystemMemoryMaxBytes != 0 && !v.SystemPlacementObserved) || (v.Original != nil && !v.Applied) {
		return Limits{}, ErrUnknown
	}
	l := v.Bootstrap.Limits
	if v.Held != nil {
		l = v.Held.limits()
	}
	if err := v.Actual.matchesLimits(v.Identity, l); err != nil {
		// 当前占用/异步回收或限额漂移不能确认时保持owner和承诺，等待原查询；
		// 不为一次内核暂态不断换owner/floor，进而使旧未知命令更难受控恢复。
		return Limits{}, fmt.Errorf("%w: held constraints not confirmed: %v", ErrUnknown, err)
	}
	if err := v.Actual.systemPlacementMatches(l.SystemMemoryMaxBytes); err != nil {
		return Limits{}, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	l.HugePools = append([]HugePool(nil), l.HugePools...)
	return l, nil
}

func (g *Guard) InspectState(ctx context.Context) (View, error) {
	var v View
	err := g.withRecord(ctx, func(r *record, _ *lockedState) error {
		if r.Bootstrap == nil {
			return fmt.Errorf("original resource bootstrap missing")
		}
		limits := r.Bootstrap.Original.Limits
		if r.Held != nil {
			limits = r.Held.limits()
		}
		actual, err := g.kernel.ReadBounds(r.Identity, limits)
		if err != nil {
			return err
		}
		v = View{ProtocolVersion: 1, Identity: r.Identity, IssuerFloorEpoch: r.Floor, Bootstrap: r.Bootstrap.Original, BootstrapApplied: r.Bootstrap.Applied, SystemPlacementObserved: r.Bootstrap.SystemPlacementObserved, Original: r.Original, Held: r.Held, Applied: r.Applied, Birth: r.Birth, Actual: actual}
		return v.Validate()
	})
	return v, err
}
