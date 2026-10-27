// Package hostguard保存节点分配域的持久内存账本。这里的Held只表示容量已经
// 条件记账，不是HostReservation完成证明、物理分配或VM许可；生产Scheduler
// 必须把该域纳入全部授予/恢复路径，再由独立原VM/Pod/Runner核验发布来源。
package hostguard

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"

	"k8s.io/apimachinery/pkg/types"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

const (
	MaxHolds    = 256
	MaxRawBytes = 192 * 1024
)

// Binding来自已认证的节点/分配域启动，固定原Node和Lease真实UID。旧Lease
// 过期不能换同名新Lease，Node重建也不能沿用这个域的持有。
type Binding struct {
	NodeName  string    `json:"node_name"`
	NodeUID   types.UID `json:"node_uid"`
	Namespace string    `json:"namespace"`
	LeaseName string    `json:"lease_name"`
	LeaseUID  types.UID `json:"lease_uid"`
}

func (b Binding) Validate() error {
	for _, v := range []string{b.NodeName, string(b.NodeUID), b.Namespace, b.LeaseName, string(b.LeaseUID)} {
		if control.ValidateID(v, "original Host binding") != nil {
			return fmt.Errorf("invalid original Host binding")
		}
	}
	return nil
}

func (b *Binding) UnmarshalJSON(raw []byte) error {
	type plain Binding
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	*b = Binding(p)
	return b.Validate()
}

// Plan保存完整原请求与容量维度，不能由同目标数值替换另一原Key。账本不认证
// 输入的PG/Root预算，真实生产调用方仍须从原VM UID/RV WAL及Pod/Node独立取得。
type Plan struct {
	Namespace         string          `json:"namespace"`
	VMName            string          `json:"vm_name"`
	Incarnation       string          `json:"incarnation"`
	RunnerPodUID      string          `json:"runner_pod_uid"`
	Request           control.Request `json:"request"`
	BeforeBytes       control.Uint64  `json:"before_bytes"`
	HostOverheadBytes control.Uint64  `json:"host_overhead_bytes"`
}

func (p Plan) Bytes() (control.Uint64, error) {
	if p.Request.Validate() != nil || control.ValidateIncarnation(p.Incarnation) != nil || p.BeforeBytes == 0 || p.HostOverheadBytes == 0 ||
		(p.Request.Kind == control.Grow && p.Request.TargetVMMemoryBytes < p.BeforeBytes) || (p.Request.Kind == control.Shrink && p.Request.TargetVMMemoryBytes > p.BeforeBytes) {
		return 0, fmt.Errorf("invalid complete Host reservation plan")
	}
	for _, v := range []string{p.Namespace, p.VMName, p.RunnerPodUID} {
		if control.ValidateID(v, "original Host allocation") != nil {
			return 0, fmt.Errorf("invalid original Host allocation identity")
		}
	}
	beforeAndAfter := max(p.BeforeBytes, p.Request.TargetVMMemoryBytes)
	if p.HostOverheadBytes > control.Uint64(math.MaxUint64)-beforeAndAfter {
		return 0, fmt.Errorf("host reservation plus overhead overflows")
	}
	return beforeAndAfter + p.HostOverheadBytes, nil
}

func (p *Plan) UnmarshalJSON(raw []byte) error {
	type plain Plan
	var v plain
	if err := control.UnmarshalObject(raw, &v); err != nil {
		return err
	}
	*p = Plan(v)
	_, err := p.Bytes()
	return err
}

type Hold struct {
	Plan     Plan   `json:"plan"`
	SourceID string `json:"source_id"`
	// Claim在原Node容量已持久保留、原不可变CM已读回后才CAS固定。
	// nil表示尚未发布，消费者不得据此执行。固定后不收养同名新UID；
	// CREATE或最终CAS丢回复只读恢复原UID，不因Lease/超时清除此字段。
	Claim *budget.ObjectRef `json:"claim,omitempty"`
}

// Model没有TTL/自动释放或通用reset。v4仅在原owner核原VM/Runner成功结清后
// 收敛旧峰值，目标/当前来源与高水位继续持有；整个Pod退出和取消仍需独立
// 关闭证明，不能把“Pod不见了”当物理归还。v5在终态原Pod/原QEMU Wait
// 证明后显式移除整Pod持有，并要求容量写者重读终态；旧v4程序拒绝v5。
type Model struct {
	ProtocolVersion uint32         `json:"protocol_version"`
	Binding         Binding        `json:"binding"`
	Origin          string         `json:"origin"`
	LeaseFloor      journal.Owner  `json:"lease_floor"`
	Sequence        control.Uint64 `json:"sequence"`
	// OutsideBytes是域外资源的分配保留，初始化显式冻结；本账本不能自行
	// 认证它覆盖了全部其他Pod/Host系统，也不能把该字段当资格证书。
	OutsideBytes control.Uint64 `json:"outside_bytes"`
	Holds        []Hold         `json:"holds"`
	// v3把首次Pod/VM及其他工作负载纳入同一持久容量域。指针保证空列表
	// 序列化为[]而不是被omitempty抹掉；旧v1/v2缺字段仅保留原语义读取。
	Profile    *Profile     `json:"profile,omitempty"`
	Placements *[]Placement `json:"placements,omitempty"`
}

// SourceID只给出原完整计划在该分配域中的稳定标识，不是容量持有或发布授权。
func SourceID(origin string, p Plan) string {
	raw, _ := json.Marshal([]any{origin, p})
	h := sha256.Sum256(append([]byte("neon-dsga-node-held-memory-v1\x00"), raw...))
	return hex.EncodeToString(h[:])
}

func (m Model) Validate() error {
	if (m.ProtocolVersion < 1 || m.ProtocolVersion > 5) || m.Binding.Validate() != nil || control.ValidateIncarnation(m.Origin) != nil || m.LeaseFloor.Validate() != nil || m.Sequence == 0 || m.Holds == nil || len(m.Holds) > MaxHolds {
		return fmt.Errorf("invalid durable Host allocation model")
	}
	for i, h := range m.Holds {
		if _, err := h.Plan.Bytes(); err != nil || h.SourceID != SourceID(m.Origin, h.Plan) {
			return fmt.Errorf("host hold changed original complete plan/source")
		}
		if h.Claim != nil && (m.ProtocolVersion < 2 || h.Claim.Validate() != nil) {
			return fmt.Errorf("published Host hold requires original claim UID and model v2")
		}
		for j := range i {
			old := m.Holds[j].Plan
			if old.Request.Key.Instance.VMUID != h.Plan.Request.Key.Instance.VMUID {
				continue
			}
			if old.Namespace != h.Plan.Namespace || old.VMName != h.Plan.VMName || old.Incarnation != h.Plan.Incarnation {
				return fmt.Errorf("host allocation changed original VM name/incarnation")
			}
			if old.Request.Key == h.Plan.Request.Key {
				return fmt.Errorf("duplicate original Host key in held ledger")
			}
			if old.RunnerPodUID == h.Plan.RunnerPodUID {
				if old.Request.Key.Instance != h.Plan.Request.Key.Instance || compare(old.Request.Key, h.Plan.Request.Key) >= 0 {
					return fmt.Errorf("host allocation changed original PG boot/high water")
				}
			}
		}
	}
	if err := m.validatePlacements(); err != nil {
		return err
	}
	_, err := m.UsedBytes()
	return err
}

type allocation struct {
	VMUID        string
	Incarnation  string
	RunnerPodUID string
}

// 同一原VM/Pod多个在途/已结算承诺取最大值，不能叠加同一物理Guest；不同Pod
// 即使VM UID相同也分开相加，避免新Pod出现时把尚未证明退出的旧QEMU成本抹掉。
func (m Model) UsedBytes() (control.Uint64, error) {
	peaks := make(map[allocation]control.Uint64)
	if m.ProtocolVersion >= 3 && m.Placements != nil {
		for _, p := range *m.Placements {
			b, err := p.Bytes()
			if err != nil {
				return 0, err
			}
			key := allocation{VMUID: p.VMUID, Incarnation: p.Incarnation, RunnerPodUID: p.PodUID}
			peaks[key] = b
		}
	}
	for _, h := range m.Holds {
		if _, closed := m.settledPlacement(h.Plan); closed {
			continue
		}
		b, err := h.Plan.Bytes()
		if err != nil {
			return 0, err
		}
		key := allocation{VMUID: h.Plan.Request.Key.Instance.VMUID, Incarnation: h.Plan.Incarnation, RunnerPodUID: h.Plan.RunnerPodUID}
		peaks[key] = max(peaks[key], b)
	}
	used := m.OutsideBytes
	for _, b := range peaks {
		if b > control.Uint64(math.MaxUint64)-used {
			return 0, fmt.Errorf("host allocation sum overflows")
		}
		used += b
	}
	return used, nil
}

func compare(a, b control.Key) int {
	if a.IssuerEpoch < b.IssuerEpoch || a.IssuerEpoch == b.IssuerEpoch && a.Revision < b.Revision {
		return -1
	}
	if a.IssuerEpoch > b.IssuerEpoch || a.Revision > b.Revision {
		return 1
	}
	return 0
}

func (m Model) Clone() Model {
	if m.Profile != nil {
		profile := *m.Profile
		m.Profile = &profile
	}
	m.Placements = clonePlacements(m.Placements)
	m.Holds = append([]Hold{}, m.Holds...)
	for i := range m.Holds {
		if m.Holds[i].Claim != nil {
			ref := *m.Holds[i].Claim
			m.Holds[i].Claim = &ref
		}
	}
	return m
}

func (m *Model) UnmarshalJSON(raw []byte) error {
	type plain Model
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, MaxRawBytes, 16384); err != nil {
		return err
	}
	*m = Model(p)
	return m.Validate()
}

func (m Model) reserve(actor journal.Owner, p Plan, capacity control.Uint64) (Model, Hold, error) {
	var none Hold
	if m.Validate() != nil || actor != m.LeaseFloor {
		return m, none, fmt.Errorf("host allocator owner is not the current fenced floor")
	}
	if _, err := p.Bytes(); err != nil {
		return m, none, err
	}
	if _, closed := m.settledPlacement(p); closed {
		return m, none, fmt.Errorf("original Host request has been settled; execution reservation cannot reopen")
	}
	for _, h := range m.Holds {
		if h.Plan.Request.Key == p.Request.Key {
			if h.Plan != p {
				return m, none, fmt.Errorf("same original Host key changed complete context")
			}
			return m, h, nil
		}
	}
	if m.Sequence == control.Uint64(math.MaxUint64) || len(m.Holds) == MaxHolds {
		return m, none, fmt.Errorf("host ledger sequence/hold bound exhausted; explicit settlement required")
	}
	next := m.Clone()
	h := Hold{Plan: p, SourceID: SourceID(m.Origin, p), Claim: nil}
	next.Holds = append(next.Holds, h)
	next.Sequence++
	if err := next.Validate(); err != nil {
		return m, none, err
	}
	used, err := next.UsedBytes()
	if err != nil || used > capacity {
		return m, none, fmt.Errorf("insufficient original Node capacity for complete Host hold")
	}
	return next, h, nil
}
