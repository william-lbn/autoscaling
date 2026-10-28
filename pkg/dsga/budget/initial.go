package budget

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// Initial是首次服务的完整启动预算，独立于任何Resize请求。不能伪造Grow/
// VM完成/Host操作回执来初始化它；原PG尚未提交容量变更，Root仅有原Bootstrap，
// Host来自QEMU启动之前就持有的原Pod placement。资格与原对象须持续保存，
// 后续正常/实际预算覆盖并显式结清前，不因空Active或历史淘汰删除本来源。
// 本合同只核完整绑定、去重与算术；认证、持久化和开放分别由真实执行器完成。
type Initial struct {
	ProtocolVersion        uint32                     `json:"protocol_version"`
	VMShrinkPermit         bool                       `json:"vm_shrink_permit"`
	Instance               control.Instance           `json:"instance"`
	Incarnation            string                     `json:"incarnation"`
	IssuerEpoch            control.Uint64             `json:"issuer_epoch"`
	Qualification          QualificationSource        `json:"qualification"`
	PGAggregate            RootAggregate              `json:"pg_aggregate"`
	Shared                 control.SharedCostResponse `json:"shared"`
	VMMemoryBytes          control.Uint64             `json:"vm_memory_bytes"`
	GuestUsableMemoryBytes control.Uint64             `json:"guest_usable_memory_bytes"`
	RunnerBirth            ObjectRef                  `json:"runner_birth"`
	RunnerIdentitySHA256   string                     `json:"runner_identity_sha256"`
	RunnerPodName          string                     `json:"runner_pod_name"`
	RunnerPodUID           string                     `json:"runner_pod_uid"`
	RunnerRestartCount     int32                      `json:"runner_restart_count"`
	NodeName               string                     `json:"node_name"`
	NodeUID                string                     `json:"node_uid"`
	HostProfileID          string                     `json:"host_profile_id"`
	HostMemoryBytes        control.Uint64             `json:"host_memory_bytes"`
	HostOverheadBytes      control.Uint64             `json:"host_overhead_bytes"`
	SystemOutsidePG        Bound                      `json:"system_outside_pg"`
	Transition             Bound                      `json:"transition"`
	Safety                 Bound                      `json:"safety"`
}

func (i Initial) Validate() error {
	r, s := i.PGAggregate, i.Shared.Observed
	if i.ProtocolVersion != 1 || i.VMShrinkPermit || i.Instance.Validate() != nil || control.ValidateIncarnation(i.Incarnation) != nil || i.IssuerEpoch == 0 || i.Qualification.Validate() != nil || r.Validate() != nil || r.Held != nil || r.Identity.VMUID != i.Instance.VMUID || r.Identity.ComputeInstanceID != i.Instance.ComputeInstanceID || r.Bootstrap.IssuerEpoch > i.IssuerEpoch || r.Bootstrap.Limits.PidsMaxTasks == 0 || r.Bootstrap.Limits.SystemMemoryMaxBytes == 0 || i.Shared.ValidateFor(i.Instance) != nil || s.IssuerFloorEpoch != i.IssuerEpoch || s.CapacityGeneration != 1 || s.LowBuffers != s.HighBuffers || s.Operation != nil || s.AdmissionOpen || s.AdmissionRevision != 0 {
		return fmt.Errorf("initial budget requires original initial PG capacity and complete root bootstrap")
	}
	if i.RunnerBirth.Validate() != nil || !validDigest(i.RunnerIdentitySHA256) || i.RunnerRestartCount < 0 || i.VMMemoryBytes == 0 || i.VMMemoryBytes > math.MaxInt64 || i.GuestUsableMemoryBytes == 0 || i.GuestUsableMemoryBytes > i.VMMemoryBytes || i.HostMemoryBytes < i.VMMemoryBytes || i.HostOverheadBytes == 0 || uint64(i.HostMemoryBytes) > math.MaxUint64-uint64(i.HostOverheadBytes) {
		return fmt.Errorf("initial VM/Guest/Host capacity or original Runner context is incomplete")
	}
	for _, id := range []string{i.RunnerPodName, i.RunnerPodUID, i.NodeName, i.NodeUID, i.HostProfileID} {
		if control.ValidateID(id, "initial original placement") != nil {
			return fmt.Errorf("invalid original initial placement identity")
		}
	}
	for _, a := range s.Arrays {
		if a.AllocationUnknown {
			return fmt.Errorf("initial native allocation is unknown")
		}
	}
	// PG聚合已含normal及整HugeTLB池（含空闲），不再把Shared/DSM/私有/
	// LFC分类加一遍。Shared只用于检查原生当前成本能被该聚合容纳；不能
	// 将全部HugeTLB共享成本强压进normal上限而重复计费。
	if i.Shared.CurrentMainAndArraysUpperBytes > r.Bound.UpperBytes || i.SystemOutsidePG.UpperBytes < r.Bootstrap.Limits.SystemMemoryMaxBytes || i.Transition.UpperBytes == 0 || i.Safety.UpperBytes == 0 {
		return fmt.Errorf("initial aggregate cannot contain retained native/system commitments")
	}
	seen, outside := map[string]bool{}, uint64(0)
	for _, b := range []Bound{i.SystemOutsidePG, i.Transition, i.Safety} {
		if (b.Kind != HardLimit && b.Kind != CommittedUpper) || control.ValidateID(b.SourceID, "initial outside commitment") != nil || seen[b.SourceID] || uint64(b.UpperBytes) > math.MaxUint64-outside {
			return fmt.Errorf("initial outside commitments overlap or overflow")
		}
		seen[b.SourceID] = true
		outside += uint64(b.UpperBytes)
	}
	if outside != uint64(r.Bootstrap.Limits.OutsideUpperBytes) || uint64(r.Bound.UpperBytes) > math.MaxUint64-outside || uint64(r.Bound.UpperBytes)+outside > uint64(i.GuestUsableMemoryBytes) {
		return fmt.Errorf("initial complete commitments exceed guaranteed usable Guest memory")
	}
	return nil
}

// MatchQualification在消费者重新GET原UID/摘要后，将完整启动预算绑定到
// 平台认证的确切启动规格。不接受“同数值另一份证书”，也不按目标VM比例
// 猜测启动值；Boot的root限额必须已经真实等于资格，不在这里调整它。
func (i Initial) MatchQualification(q PlatformQualification) error {
	if err := i.Validate(); err != nil {
		return err
	}
	t, err := q.Target(i.VMMemoryBytes)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(i.PGAggregate.Bootstrap)
	if err != nil {
		return err
	}
	system, transition, safety := t.Bounds(i.Qualification)
	digest := sha256.Sum256(raw)
	if q.Identity != i.PGAggregate.Identity || q.Incarnation != i.Incarnation || q.RootBootstrapSHA256 != hex.EncodeToString(digest[:]) || q.RunnerBirth != i.RunnerBirth || q.RunnerIdentitySHA256 != i.RunnerIdentitySHA256 || q.RunnerPodName != i.RunnerPodName || q.RunnerPodUID != i.RunnerPodUID || q.RunnerRestartCount != i.RunnerRestartCount || q.NodeName != i.NodeName || q.NodeUID != i.NodeUID || t.GuestUsableMemoryBytes != i.GuestUsableMemoryBytes || !reflect.DeepEqual(t.Limits, i.PGAggregate.Bootstrap.Limits) || system != i.SystemOutsidePG || transition != i.Transition || safety != i.Safety {
		return fmt.Errorf("initial complete budget differs from exact original platform qualification")
	}
	return nil
}

// MatchCurrent只证明已认证读者给出的当前成本/约束仍被原启动预算覆盖。
// Floor可因合法接管前进，门禁可因原开放或维护改变；这些都不是重置预算的
// 理由。容量代次/前缀、原root、启动成本及实际VM不允许静默替换。它不选择
// 是否开放、不证明Host/Runner当前状态，也不将NotFound当成原动作无效果。
func (i Initial) MatchCurrent(shared control.SharedCostResponse, root RootAggregate, epoch control.Uint64, vmBytes, guestUsable control.Uint64) error {
	if i.Validate() != nil || shared.ValidateFor(i.Instance) != nil || root.Validate() != nil || !reflect.DeepEqual(root, i.PGAggregate) || epoch < i.IssuerEpoch || shared.Observed.IssuerFloorEpoch != epoch || vmBytes != i.VMMemoryBytes || guestUsable < i.GuestUsableMemoryBytes {
		return fmt.Errorf("current initial budget source, floor or actual capacity changed")
	}
	s, original := shared.Observed, i.Shared.Observed
	if s.CapacityGeneration != original.CapacityGeneration || s.LowBuffers != original.LowBuffers || s.HighBuffers != original.HighBuffers || s.MaximumBuffers != original.MaximumBuffers || shared.StartupMainUpperBytes != i.Shared.StartupMainUpperBytes || shared.StartupArraysUpperBytes != i.Shared.StartupArraysUpperBytes || shared.CurrentMainAndArraysUpperBytes > root.Bound.UpperBytes || s.Operation != nil && s.Operation.Phase == control.Preparing {
		return fmt.Errorf("current native allocation exceeds or replaces original initial capacity")
	}
	for _, a := range s.Arrays {
		if a.AllocationUnknown {
			return fmt.Errorf("current initial native allocation is unknown")
		}
	}
	return nil
}

func (i *Initial) UnmarshalJSON(raw []byte) error {
	type plain Initial
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	next := Initial(p)
	if err := next.Validate(); err != nil {
		return err
	}
	*i = next
	return nil
}
