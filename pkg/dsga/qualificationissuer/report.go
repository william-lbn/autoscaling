// Package qualificationissuer 只发布独立平台签发者已明确认证的原资格。
// 真实 Linux 验收和持续硬约束证明由独立受限主体提供；本包不从 RSS、空闲量
// 或报告自报布尔生成认证。调用方须固定可信配置、真实 Kubernetes 客户端和本地 Agent。
package qualificationissuer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

const (
	ReportScope                 = "neon-dsga-linux-platform-report-v1"
	KubernetesBudgetReportScope = "neon-dsga-linux-guest-kubernetes-budget-report-v2"
	MaxReportBytes              = 64 * 1024
	MaxArtifactBytes            = 1024 * 1024
	ReportAnnotation            = "internal.autoscaling.neon.tech/qualification-report-sha256"
	LinuxAnnotation             = "internal.autoscaling.neon.tech/qualification-linux-sha256"
	ContinuousAnnotation        = "internal.autoscaling.neon.tech/qualification-continuous-sha256"
)

// Config 只能来自独立 issuer 的 root 保护启动配置，不能取自租户请求或普通 VM 注解。
// ReportSHA256 是信任锚；报告里的摘要本身不是认证。ResumeClaimUID 仅用于操作者
// 已独立选定原 UID 的显式恢复，绝不按同名对象自动收养 CREATE 的未知结果。
type Config struct {
	ProtocolVersion        uint32          `json:"protocol_version"`
	Binding                journal.Binding `json:"binding"`
	NodeName               string          `json:"node_name"`
	NodeUID                string          `json:"node_uid"`
	ReportSHA256           string          `json:"report_sha256"`
	ControlSocketPath      string          `json:"control_socket_path"`
	TimeoutSeconds         uint32          `json:"timeout_seconds"`
	ReportPath             string          `json:"report_path"`
	LinuxArtifactPath      string          `json:"linux_artifact_path"`
	ContinuousArtifactPath string          `json:"continuous_artifact_path"`
	ResumeClaimUID         string          `json:"resume_claim_uid,omitempty"`
}

func (c Config) Validate() error {
	if c.ProtocolVersion != 1 || c.Binding.Namespace == "" || c.Binding.Name == "" || control.ValidateID(string(c.Binding.VMUID), "VM") != nil || control.ValidateIncarnation(c.Binding.Incarnation) != nil || control.ValidateID(c.NodeName, "Node") != nil || control.ValidateID(c.NodeUID, "Node UID") != nil || !validSHA(c.ReportSHA256) || c.TimeoutSeconds < 1 || c.TimeoutSeconds > 120 {
		return fmt.Errorf("issuer requires fixed trusted binding, report SHA and finite timeout")
	}
	if c.ResumeClaimUID != "" && control.ValidateID(c.ResumeClaimUID, "original qualification UID") != nil {
		return fmt.Errorf("invalid explicit qualification recovery UID")
	}
	return nil
}
func (c *Config) UnmarshalJSON(raw []byte) error {
	type plain Config
	var v plain
	if err := control.UnmarshalObject(raw, &v); err != nil {
		return err
	}
	next := Config(v)
	if err := next.Validate(); err != nil {
		return err
	}
	*c = next
	return nil
}

// Report 是 issuer 私有输入，不改变上层资格/控制协议。资格目标必须全部来自独立
// Linux 平台报告；本包不补目标、不按比例外推，也不将文件摘要解释为内核事实。
// 两份 artifact 保留真实 Linux 验收与持续强制合同原文；root 配置固定本报告原 SHA。
type Report struct {
	ProtocolVersion          uint32                       `json:"protocol_version"`
	Scope                    string                       `json:"scope"`
	Qualification            budget.PlatformQualification `json:"qualification"`
	ConfigSecretUID          string                       `json:"config_secret_uid"`
	ResourceSourceSHA256     string                       `json:"resource_source_sha256"`
	LinuxArtifactSHA256      string                       `json:"linux_artifact_sha256"`
	ContinuousArtifactSHA256 string                       `json:"continuous_artifact_sha256"`
	HostBinding              hostguard.Binding            `json:"host_binding"`
	HostProfileSHA256        string                       `json:"host_profile_sha256"`
	HostSystemBytes          control.Uint64               `json:"host_system_bytes"`
	// v2 将 Host 容量预算与 Guest 物理证明分离；公开资格 DTO 仍为原 v1。
	HostKubernetesBudget *hostguard.KubernetesBudgetPolicy `json:"host_kubernetes_budget,omitempty"`
	// 新collector显式绑定完整原Guest投影；省略仍为既有独立外部报告合同。
	// 不改变公开Qualification DTO，已选该能力的报告不能退回缺投影发布。
	GuestArtifactSHA256 string `json:"guest_artifact_sha256,omitempty"`
}

func (r Report) Validate() error {
	if r.GuestArtifactSHA256 != "" && (r.ProtocolVersion != 2 || r.Scope != KubernetesBudgetReportScope || !validSHA(r.GuestArtifactSHA256)) {
		return fmt.Errorf("original Guest artifact requires versioned Kubernetes budget report")
	}
	if r.Qualification.Validate() != nil || control.ValidateID(r.ConfigSecretUID, "original config Secret UID") != nil || !validSHA(r.ResourceSourceSHA256) || !validSHA(r.LinuxArtifactSHA256) || !validSHA(r.ContinuousArtifactSHA256) || r.HostBinding.Validate() != nil || !validSHA(r.HostProfileSHA256) || r.HostSystemBytes == 0 || r.HostBinding.NodeName != r.Qualification.NodeName || string(r.HostBinding.NodeUID) != r.Qualification.NodeUID {
		return fmt.Errorf("complete independent Linux report and persistent constraint sources required")
	}
	switch r.ProtocolVersion {
	case 1:
		if r.Scope != ReportScope || r.HostKubernetesBudget != nil {
			return fmt.Errorf("legacy report cannot change Host proof semantics")
		}
	case 2:
		p := r.HostKubernetesBudget
		if r.Scope != KubernetesBudgetReportScope || p == nil || p.Validate() != nil || p.NodeName != r.HostBinding.NodeName || p.NodeUID != string(r.HostBinding.NodeUID) || p.ProfileSHA256 != r.HostProfileSHA256 || p.SystemReservationBytes != r.HostSystemBytes {
			return fmt.Errorf("v2 requires explicit original Kubernetes budget policy; Guest proof remains mandatory")
		}
	default:
		return fmt.Errorf("unsupported private platform report version")
	}
	return nil
}
func (r *Report) UnmarshalJSON(raw []byte) error {
	type plain Report
	var v plain
	if err := control.UnmarshalObjectBounded(raw, &v, MaxReportBytes, 4096); err != nil {
		return err
	}
	next := Report(v)
	if err := next.Validate(); err != nil {
		return err
	}
	*r = next
	return nil
}

type Bundle struct{ Report, LinuxArtifact, ContinuousArtifact []byte }

func SHA256(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func validSHA(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

func (c Config) authenticate(b Bundle) (Report, error) {
	var r Report
	if c.Validate() != nil || len(b.Report) == 0 || len(b.Report) > MaxReportBytes || SHA256(b.Report) != c.ReportSHA256 || json.Unmarshal(b.Report, &r) != nil {
		return r, fmt.Errorf("trusted original platform report missing, malformed or SHA mismatch")
	}
	if len(b.LinuxArtifact) == 0 || len(b.LinuxArtifact) > MaxArtifactBytes || SHA256(b.LinuxArtifact) != r.LinuxArtifactSHA256 || len(b.ContinuousArtifact) == 0 || len(b.ContinuousArtifact) > MaxArtifactBytes || SHA256(b.ContinuousArtifact) != r.ContinuousArtifactSHA256 {
		return r, fmt.Errorf("original Linux or continuous-enforcement artifact missing or changed")
	}
	q := r.Qualification
	if q.Identity.VMUID != string(c.Binding.VMUID) || q.Incarnation != c.Binding.Incarnation || q.NodeName != c.NodeName || q.NodeUID != c.NodeUID {
		return r, fmt.Errorf("trusted report belongs to another original VM or Node")
	}
	return r, nil
}
