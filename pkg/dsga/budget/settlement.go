package budget

import (
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// ObjectRef固定真实Kubernetes对象UID，不能仅按同名ConfigMap收养重建的承诺。
// 对象必须由对应可信预算/Host生产者建立，持有到显式结清；没有TTL或租约到期
// 自动释放。这里校验引用结构，不把自报UID、immutable或JSON摘要当来源认证。
type ObjectRef struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

func (r ObjectRef) Validate() error {
	if control.ValidateID(r.Name, "reservation object") != nil || control.ValidateID(r.UID, "reservation UID") != nil || strings.Contains(r.Name, "_") {
		return fmt.Errorf("invalid immutable reservation reference")
	}
	return nil
}

// OutsideReservation是预算域为原操作保留的完整系统外部/过渡/安全承诺。系统组
// 仅作为组成部分，ResidualProfileID引用实际平台资格/约束来源，不能拿RSS补齐。
// 本结构不认证资格或生成额度；只有可信生产者建立并持续持有的原immutable对象
// 可被执行器消费。生产者/平台资格尚缺时保持阻断，不能现场创建一份声明来放行。
type OutsideReservation struct {
	ProtocolVersion             uint32                 `json:"protocol_version"`
	VMShrinkPermit              bool                   `json:"vm_shrink_permit"`
	Request                     control.Request        `json:"request"`
	Identity                    resourceguard.Identity `json:"identity"`
	Incarnation                 string                 `json:"incarnation"`
	ResidualProfileID           string                 `json:"residual_profile_id"`
	SystemOutsidePG             Bound                  `json:"system_outside_pg"`
	Transition                  Bound                  `json:"transition"`
	Safety                      Bound                  `json:"safety"`
	HoldUntilExplicitSettlement bool                   `json:"hold_until_explicit_settlement"`
}

func (r OutsideReservation) ValidateFor(p Prepared, incarnation string) error {
	if r.ProtocolVersion != 1 || r.VMShrinkPermit || !r.HoldUntilExplicitSettlement || r.Request != p.Request || r.Identity != p.Identity || r.Incarnation != incarnation || control.ValidateIncarnation(r.Incarnation) != nil || control.ValidateID(r.ResidualProfileID, "qualified residual profile") != nil || r.SystemOutsidePG != p.SystemOutsidePG || r.Transition != p.Transition || r.Safety != p.Safety {
		return fmt.Errorf("outside reservation differs from original prepared commitment")
	}
	return p.ValidateFor(p.Request, p.Identity)
}

// HostReservation来自Host/Scheduler真实持有对象，而不是VM spec PATCH成功。
// UID引用由外层记录；源绑定原VM/Guest/Runner/Node及完整请求，必须不早于目标
// 内存持有并持续到显式结清。新owner不能重命名此对象或把它改成新epoch的命令。
type HostReservation struct {
	ProtocolVersion             uint32                 `json:"protocol_version"`
	VMShrinkPermit              bool                   `json:"vm_shrink_permit"`
	Request                     control.Request        `json:"request"`
	Identity                    resourceguard.Identity `json:"identity"`
	Incarnation                 string                 `json:"incarnation"`
	SourceID                    string                 `json:"source_id"`
	RunnerPodName               string                 `json:"runner_pod_name"`
	RunnerPodUID                string                 `json:"runner_pod_uid"`
	NodeName                    string                 `json:"node_name"`
	NodeUID                     string                 `json:"node_uid"`
	ReservedMemoryBytes         control.Uint64         `json:"reserved_memory_bytes"`
	HostOverheadBytes           control.Uint64         `json:"host_overhead_bytes"`
	HoldUntilExplicitSettlement bool                   `json:"hold_until_explicit_settlement"`
}

func (h HostReservation) ValidateFor(p Prepared, incarnation string) error {
	if h.ProtocolVersion != 1 || h.VMShrinkPermit || !h.HoldUntilExplicitSettlement || h.Request != p.Request || h.Identity != p.Identity || h.Incarnation != incarnation || control.ValidateIncarnation(h.Incarnation) != nil || h.RunnerPodName != p.RunnerPodName || h.HostOverheadBytes == 0 || uint64(h.HostOverheadBytes) > math.MaxUint64-uint64(p.Request.TargetVMMemoryBytes) || h.ReservedMemoryBytes < p.Request.TargetVMMemoryBytes+h.HostOverheadBytes {
		return fmt.Errorf("host reservation differs from original VM/Guest/Runner/target")
	}
	for _, s := range []string{h.SourceID, h.RunnerPodName, h.RunnerPodUID, h.NodeName, h.NodeUID} {
		if control.ValidateID(s, "host binding") != nil {
			return fmt.Errorf("invalid host reservation identity")
		}
	}
	return nil
}

// VMSettlement只由最终fenced/串行Runner和Host域确认后写入原ResizeVM/ObserveVM。
// 数字相等、VM状态、无pending自报都不能单独生成此事实；实际执行器须认证来源。
// Finalize再次检查原Host对象/真实Pod与Node/root/PG，不能替代最终QMP执行协议。
type VMSettlement struct {
	ProtocolVersion        uint32                 `json:"protocol_version"`
	VMShrinkPermit         bool                   `json:"vm_shrink_permit"`
	Request                control.Request        `json:"request"`
	Identity               resourceguard.Identity `json:"identity"`
	HostClaim              ObjectRef              `json:"host_claim"`
	Host                   HostReservation        `json:"host"`
	RunnerRestartCount     int32                  `json:"runner_restart_count"`
	RunnerFenceEpoch       control.Uint64         `json:"runner_fence_epoch"`
	RunnerCommandID        string                 `json:"runner_command_id"`
	GuestUsableMemoryBytes control.Uint64         `json:"guest_usable_memory_bytes"`
	ActualMemoryBytes      control.Uint64         `json:"actual_memory_bytes"`
	QMPObservedMemoryBytes control.Uint64         `json:"qmp_observed_memory_bytes"`
	PGResourceSourceID     string                 `json:"pg_resource_source_id"`
	NoPendingCommands      bool                   `json:"no_pending_commands"`
	MigrationBlocked       bool                   `json:"migration_blocked"`
}

func (v VMSettlement) ValidateFor(p Prepared, a RootAggregate, incarnation string) error {
	if v.ProtocolVersion != 1 || v.VMShrinkPermit || v.Request != p.Request || v.Identity != p.Identity || v.HostClaim.Validate() != nil || v.Host.ValidateFor(p, incarnation) != nil || v.RunnerRestartCount != p.RunnerRestartCount || v.RunnerFenceEpoch != p.Request.Key.IssuerEpoch || control.ValidateID(v.RunnerCommandID, "original runner command") != nil || v.GuestUsableMemoryBytes != p.GuestUsableMemoryBytes || v.ActualMemoryBytes != p.Request.TargetVMMemoryBytes || v.QMPObservedMemoryBytes != v.ActualMemoryBytes || ((p.Request.Kind == control.Shrink && v.PGResourceSourceID != a.Bound.SourceID) || (p.Request.Kind == control.Grow && v.PGResourceSourceID != "")) || !v.NoPendingCommands || !v.MigrationBlocked {
		return fmt.Errorf("final VM/Host/Runner settlement is incomplete or replaced")
	}
	return a.Validate()
}
func (r *ObjectRef) UnmarshalJSON(b []byte) error {
	type plain ObjectRef
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	*r = ObjectRef(p)
	return nil
}
func (r *OutsideReservation) UnmarshalJSON(b []byte) error {
	type plain OutsideReservation
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	*r = OutsideReservation(p)
	return nil
}
func (r *HostReservation) UnmarshalJSON(b []byte) error {
	type plain HostReservation
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	*r = HostReservation(p)
	return nil
}
func (r *VMSettlement) UnmarshalJSON(b []byte) error {
	type plain VMSettlement
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	*r = VMSettlement(p)
	return nil
}

// SettlementSources绑定持续持有对象及最终执行回执摘要；PG聚合另由Finalized中
// 原Bound固定。不能按“同数值”换一份新Host/Outside对象。
type SettlementSources struct {
	OutsideClaim    ObjectRef `json:"outside_claim"`
	HostClaim       ObjectRef `json:"host_claim"`
	VMReceiptSHA256 string    `json:"vm_receipt_sha256"`
}

func (s *SettlementSources) UnmarshalJSON(b []byte) error {
	type plain SettlementSources
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	*s = SettlementSources(p)
	return nil
}

func (s SettlementSources) Validate() error {
	b, err := hex.DecodeString(s.VMReceiptSHA256)
	if s.OutsideClaim.Validate() != nil || s.HostClaim.Validate() != nil || err != nil || len(b) != 32 || hex.EncodeToString(b) != s.VMReceiptSHA256 {
		return fmt.Errorf("invalid finalized source references")
	}
	return nil
}
