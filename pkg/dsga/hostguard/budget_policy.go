package hostguard

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	core "k8s.io/api/core/v1"
)

const KubernetesBudgetScope = "kubernetes-node-memory-budget-v1"

// KubernetesBudgetPolicy 是可信启动配置中的容量策略，不是整台 Host 内核的物理
// 上界证明。Capacity-Allocatable 已由 Kubernetes 扣除，不能再记入 OutsideBytes。
// SystemReservationBytes 只保留 Allocatable 内尚未扣除的系统预算；所有 Pod/VM
// 仍须以完整硬限额和固定开销入原账本。此策略不替代 Guest 的持续硬约束证据。
type KubernetesBudgetPolicy struct {
	ProtocolVersion        uint32         `json:"protocol_version"`
	Scope                  string         `json:"scope"`
	NodeName               string         `json:"node_name"`
	NodeUID                string         `json:"node_uid"`
	CapacityBytes          control.Uint64 `json:"capacity_bytes"`
	AllocatableBytes       control.Uint64 `json:"allocatable_bytes"`
	SystemReservationBytes control.Uint64 `json:"system_reservation_bytes"`
	ProfileSHA256          string         `json:"profile_sha256"`
}

func (p KubernetesBudgetPolicy) Validate() error {
	h, err := hex.DecodeString(p.ProfileSHA256)
	if p.ProtocolVersion != 1 || p.Scope != KubernetesBudgetScope || control.ValidateID(p.NodeName, "budget Node") != nil || control.ValidateID(p.NodeUID, "budget Node UID") != nil || p.CapacityBytes == 0 || p.AllocatableBytes == 0 || p.AllocatableBytes > p.CapacityBytes || p.SystemReservationBytes == 0 || p.SystemReservationBytes >= p.AllocatableBytes || err != nil || len(h) != sha256.Size || hex.EncodeToString(h) != p.ProfileSHA256 {
		return fmt.Errorf("explicit Kubernetes Node budget requires fixed identity, exact capacity and in-allocatable reservation")
	}
	return nil
}

func (p *KubernetesBudgetPolicy) UnmarshalJSON(raw []byte) error {
	type plain KubernetesBudgetPolicy
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	policy := KubernetesBudgetPolicy(next)
	if err := policy.Validate(); err != nil {
		return err
	}
	*p = policy
	return nil
}

func ProfileSHA256(profile Profile) (string, error) {
	if err := profile.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(profile)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:]), nil
}

// Inspect 使用真实 Node 读取值；任何 UID、容量或原配置漂移均拒绝新资格/授予。
// 返回成功只说明预算合同一致，不认证宿主机未计费内核开销的绝对上界。
func (p KubernetesBudgetPolicy) Inspect(node *core.Node, profile Profile, outside control.Uint64) error {
	if err := p.Validate(); err != nil {
		return err
	}
	digest, err := ProfileSHA256(profile)
	if err != nil || digest != p.ProfileSHA256 || outside != p.SystemReservationBytes || node == nil || node.Name != p.NodeName || string(node.UID) != p.NodeUID || node.ResourceVersion == "" || node.DeletionTimestamp != nil {
		return fmt.Errorf("original Kubernetes budget identity/profile/reservation changed")
	}
	capacity, exactCapacity := node.Status.Capacity.Memory().AsInt64()
	allocatable, exactAllocatable := node.Status.Allocatable.Memory().AsInt64()
	if !exactCapacity || !exactAllocatable || capacity <= 0 || allocatable <= 0 || control.Uint64(capacity) != p.CapacityBytes || control.Uint64(allocatable) != p.AllocatableBytes {
		return fmt.Errorf("original Kubernetes budget capacity/allocatable changed")
	}
	return nil
}
