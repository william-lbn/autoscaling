package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// StoppedPodKind是可信Scheduler核验整Pod终态后产生的独立来源，不是
// ProcessExit的新版本。原Runner在Start后崩溃可能永远没有Wait；此时只在
// 原Pod全部app/init/sidecar均已终止且Never重启时允许结清整个Pod承诺。
// 不制造launch nonce、PID、Wait结果或本地WAL摘要，也不完成原未知resize。
const StoppedPodKind = "pod-terminal-v1"

type StoppedPod struct {
	ProtocolVersion uint32           `json:"protocol_version"`
	Identity        Identity         `json:"identity"`
	BirthClaim      budget.ObjectRef `json:"birth_claim"`
	StateClaim      budget.ObjectRef `json:"state_claim"`
	PodName         string           `json:"pod_name"`
	NodeName        string           `json:"node_name"`
	TerminalSHA256  string           `json:"terminal_sha256"`
	ContainerID     string           `json:"container_id"`
}

func (p StoppedPod) Validate() error {
	if p.ProtocolVersion != 1 || p.Identity.Validate() != nil || p.BirthClaim.Validate() != nil || p.StateClaim.Validate() != nil || control.ValidateID(p.PodName, "original stopped Pod") != nil || control.ValidateID(p.NodeName, "original stopped Node") != nil || !digestValid(p.TerminalSHA256) || control.ValidateID(p.ContainerID, "original stopped Runner container") != nil {
		return fmt.Errorf("invalid original whole Pod termination source")
	}
	return nil
}

func (p *StoppedPod) UnmarshalJSON(raw []byte) error {
	type plain StoppedPod
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*p = StoppedPod(next)
	return p.Validate()
}

func StoppedPodClaimName(binding journal.Binding, birth Birth) string {
	// 与Wait来源不同名；一旦Pod关闭意图保存本来源，后来到达的Wait不替换它。
	raw, _ := json.Marshal([]any{binding, birth})
	return "dsga-t-" + SourceDigest(raw)[:56]
}

// stoppedPodFact只验证完整事实，不从标签或调用者布尔值取得授权。原Node
// UID由Scheduler的节点owner/CAS另外核验；本处固定Birth与原VM/Pod/容器。
// 原Pod删除不影响终态读取，但Pod404、同名换UID、重启及残留sidecar均拒绝。
func stoppedPodFact(binding journal.Binding, birth Birth, ref budget.ObjectRef, pod *core.Pod) (StoppedPod, error) {
	var none StoppedPod
	if birth.Validate() != nil || ref.Validate() != nil || ref.Name != BindingName(binding) || pod == nil || pod.Namespace != binding.Namespace || pod.Spec.RestartPolicy != core.RestartPolicyNever || birth.Identity.VMUID != string(binding.VMUID) || birth.Identity.Incarnation != binding.Incarnation {
		return none, fmt.Errorf("stopped original Pod birth or restart policy differs")
	}
	candidate := pod.DeepCopy()
	candidate.DeletionTimestamp = nil
	if err := originalPodIdentity(candidate, binding, pod.Name, birth.Identity.RunnerPodUID, pod.Spec.NodeName); err != nil {
		return none, err
	}
	digest, err := hostguard.TerminalPodDigest(pod)
	if err != nil {
		return none, err
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "neonvm-runner" && status.RestartCount == 0 {
			fact := StoppedPod{ProtocolVersion: 1, Identity: birth.Identity, BirthClaim: ref, StateClaim: birth.StateClaim, PodName: pod.Name, NodeName: pod.Spec.NodeName, TerminalSHA256: digest, ContainerID: status.ContainerID}
			return fact, fact.Validate()
		}
	}
	return none, fmt.Errorf("original stopped Runner container is absent or restarted")
}

// PublishStoppedPod只由可信节点Scheduler在原Node fence后调用。先重新GET
// 原Pod核同UID/完整终态，再CREATE确定名称的不可变来源并真实GET；网络不持
// 本地锁，未知不换名重建。来源UID由随后原Pod关闭意图持久固定；Node CAS
// 前后仍须再读原Pod/来源。它从不触碰Runner WAL、QMP或外层业务日志。
func PublishStoppedPod(ctx context.Context, client coreclient.CoreV1Interface, binding journal.Binding, birth Birth, ref budget.ObjectRef, pod *core.Pod) (*core.ConfigMap, error) {
	if client == nil {
		return nil, fmt.Errorf("stopped Pod source requires original API reader")
	}
	expected, err := stoppedPodFact(binding, birth, ref, pod)
	if err != nil {
		return nil, err
	}
	actual, err := client.Pods(binding.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	observed, err := stoppedPodFact(binding, birth, ref, actual)
	if err != nil || observed != expected || actual.Spec.NodeName != pod.Spec.NodeName {
		return nil, errors.Join(fmt.Errorf("original whole Pod termination changed before source creation"), err)
	}
	raw, err := json.Marshal(expected)
	if err != nil {
		return nil, err
	}
	cm := newClaim(binding, StoppedPodClaimName(binding, birth), StoppedPodKind, true, raw)
	_, createErr := client.ConfigMaps(binding.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	got, err := client.ConfigMaps(binding.Namespace).Get(ctx, cm.Name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Join(createErr, err)
	}
	if _, err := InspectStoppedPod(got, binding, birth, ref, actual); err != nil {
		return nil, err
	}
	return got, nil
}

// InspectStoppedPod可读取正在GC但仍受保护的原UID来源。返回对象不代表Node
// 结清；调用者另核原Node/终态并固定本CM UID和完整原文摘要。旧Wait读者因
// 来源类型/字段不同而拒绝，不能将其解释成伪造的原QEMU Wait。
func InspectStoppedPod(cm *core.ConfigMap, binding journal.Binding, birth Birth, ref budget.ObjectRef, pod *core.Pod) (StoppedPod, error) {
	fact, err := InspectStoppedPodSource(cm, binding, birth, ref)
	if err != nil {
		return fact, err
	}
	expected, err := stoppedPodFact(binding, birth, ref, pod)
	if err != nil || fact != expected {
		return fact, errors.Join(fmt.Errorf("original stopped Pod source binding differs"), err)
	}
	return fact, nil
}

// InspectStoppedPodSource只证明原Scheduler已经封存不可撤回的关闭事实。
// 活动读者即使在Pod GC之后也必须拒绝旧QMP发布；Node释放路径仍须调用
// InspectStoppedPod重核实际完整Pod终态，不能单用此函数取得释放许可。
func InspectStoppedPodSource(cm *core.ConfigMap, binding journal.Binding, birth Birth, ref budget.ObjectRef) (StoppedPod, error) {
	var fact StoppedPod
	if birth.Validate() != nil || ref.Validate() != nil || ref.Name != BindingName(binding) || cm == nil || cm.Name != StoppedPodClaimName(binding, birth) || birth.Identity.VMUID != string(binding.VMUID) || birth.Identity.Incarnation != binding.Incarnation {
		return fact, fmt.Errorf("original stopped Pod source identity differs")
	}
	claim := budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)}
	if validClaimState(cm, binding, claim, StoppedPodKind, true, true) != nil || json.Unmarshal([]byte(cm.Data[ClaimKey]), &fact) != nil || fact.Identity != birth.Identity || fact.BirthClaim != ref || fact.StateClaim != birth.StateClaim {
		return fact, fmt.Errorf("original stopped Pod source is replaced or differs from complete termination")
	}
	return fact, nil
}
