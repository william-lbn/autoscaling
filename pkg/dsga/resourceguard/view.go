package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

type stateObservationError struct {
	stage string
	err   error
}

func (e *stateObservationError) Error() string { return e.err.Error() }
func (e *stateObservationError) Unwrap() error { return e.err }

func stateObservationDiagnostic(err error) (stage, reason string) {
	stage = "open_original_record"
	var observation *stateObservationError
	if errors.As(err, &observation) {
		stage = observation.stage
	}
	switch {
	case errors.Is(err, errAddressSpaceChurn):
		reason = "process_membership_changed"
	case errors.Is(err, syscall.EINTR):
		reason = "interrupted"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		reason = "request_ended"
	case errors.Is(err, os.ErrNotExist), errors.Is(err, syscall.ESRCH):
		reason = "source_disappeared"
	case errors.Is(err, os.ErrPermission):
		reason = "source_permission"
	case errors.Is(err, ErrUnknown):
		reason = "source_unconfirmed"
	default:
		reason = "source_rejected"
	}
	return stage, reason
}

// View是同一资源锁内读取的原日志和当前限额。Original/Held属于独立于PG boot
// 的资源域，不能从Native PG revision推导资源revision，也不能因外层历史淘汰
// 猜测计数。它不是原命令完成回执，尤其不能拿新PG的Actual替代旧PID的Observe。
type View struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	// 原只读创建profile所需内部协议，不是授权/normal许可；旧v1精确省略。
	// 在线降限只用于仍持有原硬预算的 PG；零值表示没有协商该版本。
	OnlineShrinkProtocolVersion   uint32           `json:"online_shrink_protocol_version,omitempty"`
	NormalQuotaProtocolVersion    uint32           `json:"normal_quota_protocol_version,omitempty"`
	ArrayAuthorityProtocolVersion uint32           `json:"array_authority_protocol_version,omitempty"`
	VMShrinkPermit                bool             `json:"vm_shrink_permit"`
	Identity                      Identity         `json:"identity"`
	IssuerFloorEpoch              control.Uint64   `json:"issuer_floor_epoch"`
	Bootstrap                     BootstrapCommand `json:"bootstrap"`
	BootstrapApplied              bool             `json:"bootstrap_applied"`
	SystemPlacementObserved       bool             `json:"system_placement_observed,omitempty"`
	Original                      *Command         `json:"original"`
	Held                          *Command         `json:"held"`
	Applied                       bool             `json:"applied"`
	Cancelled                     *Command         `json:"cancelled,omitempty"`
	Birth                         *ProcessIdentity `json:"birth"`
	Actual                        Evidence         `json:"actual"`
	// 可选只读能力v1；旧Guest保持省略，新发布者缺项或unavailable必须failclosed。
	GuestQualification            *GuestQualificationArtifact    `json:"guest_qualification,omitempty"`
	GuestQualificationUnavailable *GuestQualificationUnavailable `json:"guest_qualification_unavailable,omitempty"`
}

func (v View) Validate() error {
	if v.GuestQualification != nil && v.GuestQualificationUnavailable != nil {
		return fmt.Errorf("Guest qualification response has conflicting availability")
	}
	if v.GuestQualification != nil && (v.GuestQualification.Validate() != nil || v.GuestQualification.Identity != v.Identity) {
		return fmt.Errorf("Guest qualification observation changed identity")
	}
	if v.GuestQualificationUnavailable != nil && v.GuestQualificationUnavailable.Validate() != nil {
		return fmt.Errorf("invalid Guest qualification availability")
	}
	if v.OnlineShrinkProtocolVersion > 1 || (v.OnlineShrinkProtocolVersion == 1 && (v.NormalQuotaProtocolVersion != 1 || v.ArrayAuthorityProtocolVersion != 1)) {
		return fmt.Errorf("online shrink requires original normal quota and array authority")
	}
	if v.NormalQuotaProtocolVersion > 1 || v.ArrayAuthorityProtocolVersion > 1 || v.ProtocolVersion != 1 || v.VMShrinkPermit || v.Actual.ProtocolVersion != 1 || v.Actual.VMShrinkPermit || v.Actual.Identity != v.Identity {
		return fmt.Errorf("invalid resource journal observation")
	}
	r := record{Version: 1, Identity: v.Identity, Floor: v.IssuerFloorEpoch, Original: v.Original, Held: v.Held, Applied: v.Applied, Cancelled: v.Cancelled, Bootstrap: &bootstrapRecord{Original: v.Bootstrap, Applied: v.BootstrapApplied, SystemPlacementObserved: v.SystemPlacementObserved}, Birth: v.Birth}
	if v.Cancelled != nil {
		r.Version = 2
	}
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
	err := g.withRecord(ctx, func(r *record, s *lockedState) error {
		if err := g.checkNormalQuotaObservationLocked(s, r); err != nil {
			return &stateObservationError{"quota_durability", err}
		}
		if r.Bootstrap == nil {
			return &stateObservationError{"bootstrap_record", fmt.Errorf("original resource bootstrap missing")}
		}
		limits := r.Bootstrap.Original.Limits
		if r.Held != nil {
			limits = r.Held.limits()
		}
		actual, err := g.kernel.ReadBounds(r.Identity, limits)
		if err != nil {
			return &stateObservationError{"kernel_bounds", err}
		}
		v = r.view(actual)
		if err := v.Validate(); err != nil {
			return &stateObservationError{"view_validation", err}
		}
		return nil
	})
	return v, err
}

// view只能在原文件锁内、实际约束读回后调用。取消屏障不改变已持有上界，
// 所以RootAggregate仍引用相同Held/Bootstrap；屏障只控制未来命令的revision。
func (r *record) view(actual Evidence) View {
	return View{ProtocolVersion: 1, VMShrinkPermit: false, Identity: r.Identity, IssuerFloorEpoch: r.Floor, Bootstrap: r.Bootstrap.Original, BootstrapApplied: r.Bootstrap.Applied, SystemPlacementObserved: r.Bootstrap.SystemPlacementObserved, Original: r.Original, Held: r.Held, Applied: r.Applied, Cancelled: r.Cancelled, Birth: r.Birth, Actual: actual}
}

// NextRevision同时越过已接收命令和已持久取消屏障。不能在取消后重用那个
// revision，否则同owner已在网络排队的旧Apply可能被误认为下一条合法命令。
func (v View) NextRevision() (control.Uint64, error) {
	if err := v.Validate(); err != nil {
		return 0, err
	}
	return nextResourceRevision(v.Original, v.Cancelled)
}
