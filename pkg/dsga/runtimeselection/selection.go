// Package runtimeselection 是可信控制面选择原资格与自动策略的版本合同。
// 它只表达意图；资格仍由独立 Issuer 签发，所有物理动作仍由原 Owner/Scheduler 执行。
package runtimeselection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

const DataKey = "selection.json"
const LabelKey = "internal.autoscaling.neon.tech/runtime-selection"
const LabelValue = "v1"

type Selection struct {
	Namespace       string                     `json:"namespace"`
	Name            string                     `json:"name"`
	ProtocolVersion uint32                     `json:"protocol_version"`
	Instance        control.Instance           `json:"instance"`
	Incarnation     string                     `json:"incarnation"`
	Generation      control.Uint64             `json:"generation"`
	Qualification   budget.QualificationSource `json:"qualification"`
	// nil 明确关闭新自动建议，不取消已接受/未知动作，也不撤销现有安全预算。
	Automatic *policy.Config `json:"automatic,omitempty"`
}

func (s Selection) Validate() error {
	if control.ValidateID(s.Namespace, "namespace") != nil || control.ValidateID(s.Name, "VM name") != nil || s.ProtocolVersion != 1 || s.Instance.Validate() != nil || control.ValidateIncarnation(s.Incarnation) != nil || s.Generation == 0 || s.Qualification.Validate() != nil || s.Automatic != nil && s.Automatic.Validate() != nil {
		return fmt.Errorf("invalid trusted runtime selection")
	}
	return nil
}

func (s *Selection) UnmarshalJSON(raw []byte) error {
	type plain Selection
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	next := Selection(p)
	if err := next.Validate(); err != nil {
		return err
	}
	*s = next
	return nil
}

func (s Selection) Digest() string {
	raw, _ := json.Marshal(s)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func Name(vmUID string) string {
	sum := sha256.Sum256([]byte(vmUID))
	return "dsga-runtime-" + hex.EncodeToString(sum[:])[:48]
}

// Status 区分意图被 Owner 采用与真实资源变化；applied 绝不等于内存动作完成。
// 字段只含公开身份/摘要与固定状态码，不含凭据或来源正文。
type Status struct {
	DesiredGeneration control.Uint64 `json:"desired_generation"`
	AppliedGeneration control.Uint64 `json:"applied_generation"`
	DesiredSHA256     string         `json:"desired_sha256,omitempty"`
	AppliedSHA256     string         `json:"applied_sha256,omitempty"`
	State             string         `json:"state"`
}

type Reader struct {
	core      coreclient.CoreV1Interface
	binding   journal.Binding
	uid       string
	namespace string
}

func NewReader(client coreclient.CoreV1Interface, binding journal.Binding, namespace string) (*Reader, error) {
	if client == nil || !validBinding(binding) || control.ValidateID(namespace, "selection namespace") != nil || namespace == binding.Namespace {
		return nil, fmt.Errorf("runtime selection requires original binding")
	}
	return &Reader{core: client, binding: binding, namespace: namespace}, nil
}

// Read 由同一 Owner 串行调用。首次缺失只等待；一旦读到原 UID，同名替换不被收养。
// 安全边界是受限平台写入身份 + 准入保护，而不是普通租户可写的 VM 注解。
func (r *Reader) Read(ctx context.Context) (*Selection, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cm, err := r.core.ConfigMaps(r.namespace).Get(ctx, Name(string(r.binding.VMUID)), metav1.GetOptions{})
	if apierrors.IsNotFound(err) && r.uid == "" {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("original runtime selection unavailable")
	}
	selected, err := Decode(cm, r.binding, r.namespace)
	if err != nil {
		return nil, err
	}
	if r.uid != "" && string(cm.UID) != r.uid {
		return nil, fmt.Errorf("runtime selection UID changed")
	}
	r.uid = string(cm.UID)
	return &selected, nil
}

func Decode(cm *core.ConfigMap, binding journal.Binding, namespace string) (Selection, error) {
	var s Selection
	if !validBinding(binding) || cm == nil || cm.Name != Name(string(binding.VMUID)) || cm.Namespace != namespace || namespace == binding.Namespace || cm.UID == "" || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || len(cm.Data) != 1 || len(cm.BinaryData) != 0 || cm.Labels[LabelKey] != LabelValue {
		return s, fmt.Errorf("runtime selection object differs from original binding")
	}
	// 选择位于平台专用命名空间，不能放入租户/Agent 可 CREATE 的 VM 命名空间。
	// 跨命名空间 ownerReference 在 Kubernetes 中无效；完整 VM 身份在文档中绑定。
	if len(cm.OwnerReferences) != 0 {
		return s, fmt.Errorf("platform selection must not use cross-namespace ownership")
	}
	if err := json.Unmarshal([]byte(cm.Data[DataKey]), &s); err != nil {
		return s, err
	}
	if s.Namespace != binding.Namespace || s.Name != binding.Name || s.Instance.VMUID != string(binding.VMUID) || s.Incarnation != binding.Incarnation {
		return Selection{}, fmt.Errorf("runtime selection incarnation differs")
	}
	return s, nil
}

func validBinding(b journal.Binding) bool {
	return control.ValidateID(b.Namespace, "namespace") == nil && control.ValidateID(b.Name, "VM name") == nil && control.ValidateID(string(b.VMUID), "VM UID") == nil && control.ValidateIncarnation(b.Incarnation) == nil
}
