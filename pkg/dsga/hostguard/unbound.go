package hostguard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const UnboundRetirementAnnotation = "internal.autoscaling.neon.tech/dsga-unbound-retirement-v1"

// UnboundRetirement独立记录从未绑定的关闭，不伪造Runner退出或容量退款。
// 各配置Node的原owner先用CAS确认该Pod/VM从未占账；全部原域证明完成后，
// Scheduler才可移除Pod保护。任何已有placement/hold仍走原责任恢复路径。
type UnboundRetirement struct {
	ProtocolVersion uint32        `json:"protocol_version"`
	Namespace       string        `json:"namespace"`
	PodName         string        `json:"pod_name"`
	PodUID          string        `json:"pod_uid"`
	PodSHA256       string        `json:"pod_sha256"`
	VMName          string        `json:"vm_name"`
	VMUID           string        `json:"vm_uid"`
	Incarnation     string        `json:"incarnation"`
	Domains         []Binding     `json:"domains"`
	Confirmed       []UnboundNode `json:"confirmed"`
}
type UnboundNode struct {
	Binding        Binding        `json:"binding"`
	Origin         string         `json:"origin"`
	BeforeSequence control.Uint64 `json:"before_sequence"`
}

func (r UnboundRetirement) Validate(complete bool) error {
	if r.ProtocolVersion != 1 || !retirementDigest(r.PodSHA256) || control.ValidateIncarnation(r.Incarnation) != nil || len(r.Domains) == 0 || len(r.Domains) > 128 || r.Confirmed == nil || len(r.Confirmed) > len(r.Domains) || complete && len(r.Confirmed) != len(r.Domains) {
		return fmt.Errorf("invalid never-bound closure")
	}
	for _, v := range []string{r.Namespace, r.PodName, r.PodUID, r.VMName, r.VMUID} {
		if control.ValidateID(v, "unbound identity") != nil {
			return fmt.Errorf("invalid never-bound identity")
		}
	}
	for i, b := range r.Domains {
		if b.Validate() != nil || i > 0 && r.Domains[i-1].NodeName >= b.NodeName {
			return fmt.Errorf("never-bound domains must be unique and sorted")
		}
	}
	for i, n := range r.Confirmed {
		if !slices.Contains(r.Domains, n.Binding) || n.BeforeSequence == 0 || control.ValidateIncarnation(n.Origin) != nil {
			return fmt.Errorf("invalid never-bound Node confirmation")
		}
		for _, old := range r.Confirmed[:i] {
			if old.Binding.NodeName == n.Binding.NodeName {
				return fmt.Errorf("duplicate never-bound Node confirmation")
			}
		}
	}
	return nil
}

func UnboundPodDigest(pod *core.Pod) (string, error) {
	if pod == nil || pod.UID == "" || pod.ResourceVersion == "" || pod.DeletionTimestamp == nil || pod.Spec.NodeName != "" || pod.Spec.RestartPolicy != core.RestartPolicyNever || pod.Status.Phase != core.PodFailed || pod.Status.StartTime != nil || pod.Status.HostIP != "" || len(pod.Status.HostIPs) != 0 || pod.Status.PodIP != "" || len(pod.Status.PodIPs) != 0 || len(pod.Status.ContainerStatuses) != 0 || len(pod.Status.InitContainerStatuses) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 || len(pod.Spec.EphemeralContainers) != 0 || len(pod.Spec.Containers) == 0 {
		return "", fmt.Errorf("Pod is not a deleted never-bound original")
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == core.PodScheduled && c.Status == core.ConditionTrue {
			return "", fmt.Errorf("Pod has original scheduling history")
		}
	}
	o := metav1.GetControllerOf(pod)
	if o == nil || len(pod.OwnerReferences) != 1 || o.APIVersion != vmv1.SchemeGroupVersion.String() || o.Kind != "VirtualMachine" || o.UID == "" {
		return "", fmt.Errorf("never-bound Pod requires unique original VM controller")
	}
	raw, err := json.Marshal([]any{pod.Namespace, pod.Name, pod.UID, pod.Spec, pod.Status, pod.OwnerReferences, pod.DeletionTimestamp})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (r UnboundRetirement) InspectPod(pod *core.Pod) error {
	hash, err := UnboundPodDigest(pod)
	if err != nil {
		return err
	}
	o := metav1.GetControllerOf(pod)
	if r.Validate(false) != nil || pod.Namespace != r.Namespace || pod.Name != r.PodName || string(pod.UID) != r.PodUID || hash != r.PodSHA256 || o.Name != r.VMName || string(o.UID) != r.VMUID {
		return fmt.Errorf("never-bound original Pod changed")
	}
	return nil
}

func (r UnboundRetirement) InspectVM(vm *vmv1.VirtualMachine) error {
	if r.Validate(false) != nil || vm == nil || vm.DeletionTimestamp == nil || vm.ResourceVersion == "" || vm.Spec.DSGABootstrap == nil || vm.Namespace != r.Namespace || vm.Name != r.VMName || string(vm.UID) != r.VMUID || vm.Spec.DSGABootstrap.Incarnation != r.Incarnation || vm.Status.PodName != r.PodName {
		return fmt.Errorf("never-bound original VM changed")
	}
	copy := vm.DeepCopy()
	copy.Status.PodName = ""
	held, err := vm.DSGABootstrapHeld()
	b := journal.Binding{Namespace: vm.Namespace, Name: vm.Name, VMUID: vm.UID, Incarnation: r.Incarnation}
	snapshot, readErr := journal.Inspect(vm, b)
	if err != nil || held || !copy.DSGAUndispatched() || readErr != nil || !journal.PristineBootstrap(snapshot.State()) {
		return fmt.Errorf("never-bound VM has dispatch or operation history")
	}
	return nil
}

func unboundAbsent(m Model, r UnboundRetirement) error {
	if m.ProtocolVersion < 3 || m.Placements == nil {
		return fmt.Errorf("never-bound closure requires complete Host ledger")
	}
	for _, p := range *m.Placements {
		if p.PodUID == r.PodUID || p.VMUID == r.VMUID {
			return fmt.Errorf("never-bound original allocation remains held")
		}
	}
	for _, h := range m.Holds {
		if h.Plan.RunnerPodUID == r.PodUID || h.Plan.Request.Key.Instance.VMUID == r.VMUID {
			return fmt.Errorf("never-bound original peak remains held")
		}
	}
	return nil
}

// SealUnboundAbsence只推进原Node序号，绝不删除placement/hold。删除后CAS
// 使删除前已读Pod的在途Reserve快照失效；取得新Node快照的写者必须重读
// 已关闭Pod并被原准入拒绝。未知写只保留保护，后续重读/再封存不退还容量。
func (s *Store) SealUnboundAbsence(ctx context.Context, n *Snapshot, r UnboundRetirement) (UnboundNode, error) {
	var none UnboundNode
	if n == nil || n.model.Binding != s.binding || r.Validate(false) != nil || !slices.Contains(r.Domains, s.binding) {
		return none, fmt.Errorf("never-bound absence requires original Node")
	}
	actor, err := s.authorize(ctx, n.node)
	if err != nil {
		return none, err
	}
	if actor != n.model.LeaseFloor {
		return none, fmt.Errorf("never-bound Node owner is not fenced")
	}
	pod, err := s.core.Pods(r.Namespace).Get(ctx, r.PodName, metav1.GetOptions{})
	if err != nil {
		return none, err
	}
	if err := r.InspectPod(pod); err != nil {
		return none, err
	}
	if !slices.Contains(pod.Finalizers, PodProtectionFinalizer) {
		return none, fmt.Errorf("never-bound original Pod lacks protection")
	}
	if err := unboundAbsent(n.model, r); err != nil {
		return none, err
	}
	if n.model.Sequence == control.Uint64(math.MaxUint64) {
		return none, fmt.Errorf("never-bound sequence exhausted")
	}
	m := n.model.Clone()
	m.ProtocolVersion = max(m.ProtocolVersion, 5)
	m.Sequence++
	if _, err := s.write(ctx, n.node, m, false); err != nil {
		return none, err
	}
	proof := UnboundNode{Binding: s.binding, Origin: n.model.Origin, BeforeSequence: n.model.Sequence}
	current, err := s.Load(ctx)
	if err != nil {
		return none, err
	}
	if err := InspectUnboundNode(current.Node(), r, proof); err != nil {
		return none, err
	}
	return proof, nil
}

func InspectUnboundNode(node *core.Node, r UnboundRetirement, proof UnboundNode) error {
	if r.Validate(false) != nil || !slices.Contains(r.Domains, proof.Binding) || proof.BeforeSequence == 0 || control.ValidateIncarnation(proof.Origin) != nil {
		return fmt.Errorf("invalid original never-bound proof")
	}
	n, err := inspect(node, proof.Binding)
	if err != nil || node.DeletionTimestamp != nil || n.model.ProtocolVersion < 5 || n.model.Origin != proof.Origin || n.model.Sequence <= proof.BeforeSequence {
		return fmt.Errorf("never-bound Node barrier is unconfirmed")
	}
	return unboundAbsent(n.model, r)
}

// UnmarshalJSON保持严格字段/身份校验；顶层按128个完整域及各域回执设定
// 256KiB/8192值节点的独立上界，覆盖最大长度身份而不使用默认64KiB/2048。
// 各子对象继续使用原严格小对象上限；Pod/VM最终还受API注解总量约束。
func (r *UnboundRetirement) UnmarshalJSON(raw []byte) error {
	type plain UnboundRetirement
	var v plain
	if err := control.UnmarshalObjectBounded(raw, &v, 256*1024, 8192); err != nil {
		return err
	}
	*r = UnboundRetirement(v)
	return r.Validate(false)
}
func (n *UnboundNode) UnmarshalJSON(raw []byte) error {
	type plain UnboundNode
	var v plain
	if err := control.UnmarshalObject(raw, &v); err != nil {
		return err
	}
	*n = UnboundNode(v)
	if n.Binding.Validate() != nil || control.ValidateIncarnation(n.Origin) != nil || n.BeforeSequence == 0 {
		return fmt.Errorf("invalid never-bound Node confirmation")
	}
	return nil
}
