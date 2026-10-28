package budget

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// QualificationSource只能由可信配置选择，不能取自变配请求。UID固定来源对象，
// SHA256固定其完整原始内容；两者都不是平台认证本身。资格的签发者必须在目标
// Linux平台完成约束/可用内存认证，保持原对象到所有引用显式结清，无TTL续租。
type QualificationSource struct {
	Claim  ObjectRef `json:"claim"`
	SHA256 string    `json:"sha256"`
}

func validDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}

func (s QualificationSource) Validate() error {
	if s.Claim.Validate() != nil || !validDigest(s.SHA256) {
		return fmt.Errorf("invalid original platform qualification reference")
	}
	return nil
}

// QualifiedTarget是独立平台认证给出的某一确切VM规格，不在运行期按比例推算。
// SystemOutsidePG包含系统计费组及未计费残余，Transition/Safety也须有持久
// 保证；不能用本次RSS/空闲页替代。所有字节上界互不重复，系统组不再另加一次。
type QualifiedTarget struct {
	VMMemoryBytes          control.Uint64       `json:"vm_memory_bytes"`
	GuestUsableMemoryBytes control.Uint64       `json:"guest_usable_memory_bytes"`
	Limits                 resourceguard.Limits `json:"limits"`
	SystemOutsidePGBytes   control.Uint64       `json:"system_outside_pg_bytes"`
	TransitionBytes        control.Uint64       `json:"transition_bytes"`
	SafetyBytes            control.Uint64       `json:"safety_bytes"`
}

// PlatformQualification由独立可信平台资格签发者提供，本预算生产者只读、不创建。
// 它固定原Guest/root bootstrap、Runner出生与Pod/Node，故不能把另一内核/镜像、
// 重启后Runner或更小VM的测试数字搬来放行。ReportID指向实际验收记录；摘要和
// 格式正确不能代替该验收。首次接入、跨boot/迁移必须取得新的显式资格选择。
type PlatformQualification struct {
	ProtocolVersion             uint32                 `json:"protocol_version"`
	Identity                    resourceguard.Identity `json:"identity"`
	Incarnation                 string                 `json:"incarnation"`
	ReportID                    string                 `json:"report_id"`
	RootBootstrapSHA256         string                 `json:"root_bootstrap_sha256"`
	RunnerBirth                 ObjectRef              `json:"runner_birth"`
	RunnerIdentitySHA256        string                 `json:"runner_identity_sha256"`
	RunnerPodName               string                 `json:"runner_pod_name"`
	RunnerPodUID                string                 `json:"runner_pod_uid"`
	RunnerRestartCount          int32                  `json:"runner_restart_count"`
	NodeName                    string                 `json:"node_name"`
	NodeUID                     string                 `json:"node_uid"`
	HoldUntilExplicitSettlement bool                   `json:"hold_until_explicit_settlement"`
	Targets                     []QualifiedTarget      `json:"targets"`
}

func (q PlatformQualification) Validate() error {
	if q.ProtocolVersion != 1 || q.Identity.Validate() != nil || q.Identity.SystemCgroupInode == 0 || control.ValidateIncarnation(q.Incarnation) != nil || !q.HoldUntilExplicitSettlement || !validDigest(q.RootBootstrapSHA256) || q.RunnerBirth.Validate() != nil || !validDigest(q.RunnerIdentitySHA256) || q.RunnerRestartCount < 0 || len(q.Targets) == 0 || len(q.Targets) > 32 {
		return fmt.Errorf("incomplete original platform qualification")
	}
	for _, id := range []string{q.ReportID, q.RunnerPodName, q.RunnerPodUID, q.NodeName, q.NodeUID} {
		if control.ValidateID(id, "qualified platform identity") != nil {
			return fmt.Errorf("invalid qualified platform identity")
		}
	}
	var previous control.Uint64
	for _, t := range q.Targets {
		if t.Limits.Validate() != nil || t.Limits.PidsMaxTasks == 0 || t.Limits.SystemMemoryMaxBytes == 0 || t.VMMemoryBytes <= previous || t.VMMemoryBytes > math.MaxInt64 || t.GuestUsableMemoryBytes == 0 || t.GuestUsableMemoryBytes > t.VMMemoryBytes || t.SystemOutsidePGBytes < t.Limits.SystemMemoryMaxBytes || t.TransitionBytes == 0 || t.SafetyBytes == 0 {
			return fmt.Errorf("invalid or unordered exact qualified VM target")
		}
		outside := uint64(t.SystemOutsidePGBytes)
		for _, n := range []control.Uint64{t.TransitionBytes, t.SafetyBytes} {
			if uint64(n) > math.MaxUint64-outside {
				return fmt.Errorf("qualified outside commitments overflow")
			}
			outside += uint64(n)
		}
		total, err := t.Limits.TotalUpperBytes()
		if err != nil || outside != uint64(t.Limits.OutsideUpperBytes) || total > uint64(t.GuestUsableMemoryBytes) {
			return fmt.Errorf("qualified complete budget exceeds guaranteed usable memory")
		}
		previous = t.VMMemoryBytes
	}
	return nil
}

func (q PlatformQualification) Target(n control.Uint64) (QualifiedTarget, error) {
	if err := q.Validate(); err != nil {
		return QualifiedTarget{}, err
	}
	for _, t := range q.Targets {
		if t.VMMemoryBytes == n {
			return t, nil
		}
	}
	return QualifiedTarget{}, fmt.Errorf("exact VM size has no retained platform qualification")
}

// Bounds的来源名由原资格完整摘要及确切规格派生，不接受调用者自报kind或名称。
// 固定前缀只用于去重；认证仍由原Kube对象UID/RBAC、完整内容与持有寿命提供。
func (t QualifiedTarget) Bounds(source QualificationSource) (Bound, Bound, Bound) {
	prefix := fmt.Sprintf("qualified-%s-%d-", source.SHA256, t.VMMemoryBytes)
	return Bound{Kind: CommittedUpper, UpperBytes: t.SystemOutsidePGBytes, SourceID: prefix + "system"},
		Bound{Kind: CommittedUpper, UpperBytes: t.TransitionBytes, SourceID: prefix + "transition"},
		Bound{Kind: CommittedUpper, UpperBytes: t.SafetyBytes, SourceID: prefix + "safety"}
}

// ValidateSource区分历史v1与具有可重新认证资格引用的v2。v2不能省略来源后退回
// v1，v1也不能夹带新来源；旧二进制的严格JSON读者将拒绝新增字段。
func (r OutsideReservation) ValidateSource() error {
	if r.ProtocolVersion == 1 && r.Qualification == nil {
		return nil
	}
	if r.ProtocolVersion == 2 && r.Qualification != nil && r.Qualification.Validate() == nil && r.ResidualProfileID == "qualified-"+r.Qualification.SHA256 {
		return nil
	}
	return fmt.Errorf("outside source version or original qualification differs")
}

func (q PlatformQualification) MatchOutside(out OutsideReservation) error {
	if out.ValidateSource() != nil || out.Qualification == nil || out.Identity != q.Identity || out.Incarnation != q.Incarnation || out.Request.Validate() != nil {
		return fmt.Errorf("outside reservation belongs to another original qualification")
	}
	t, err := q.Target(out.Request.TargetVMMemoryBytes)
	if err != nil {
		return err
	}
	system, transition, safety := t.Bounds(*out.Qualification)
	if out.SystemOutsidePG != system || out.Transition != transition || out.Safety != safety {
		return fmt.Errorf("outside upper bounds differ from exact qualified VM size")
	}
	return nil
}

func (q *PlatformQualification) UnmarshalJSON(raw []byte) error {
	type plain PlatformQualification
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	next := PlatformQualification(p)
	if err := next.Validate(); err != nil {
		return err
	}
	*q = next
	return nil
}
