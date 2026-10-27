package hostguard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

const (
	PodProtectionFinalizer = "autoscaling.neon.tech/dsga-host-pod-protection"
	RetirementAnnotation   = "internal.autoscaling.neon.tech/dsga-host-retirement-v1"
	VMRetirementAnnotation = "internal.autoscaling.neon.tech/dsga-host-vm-retirement-v1"
)

// Retirement在移除Node容量前持久固定到原Pod。它是关闭意图/恢复锚点，不是
// “已经释放”的票据；必须再核同origin的v5 Node中原Pod及其所有Hold均已移除。
// 原Pod终态不可回到可运行态，v5旧读者拒绝，所有v5容量写者重新GET原Pod；
// 因此结清后可删除行而无需永久增长Node墓碑。Pod404本身从不触发本结清。
type Retirement struct {
	ProtocolVersion uint32            `json:"protocol_version"`
	Binding         Binding           `json:"binding"`
	Origin          string            `json:"origin"`
	BeforeSequence  control.Uint64    `json:"before_sequence"`
	Placement       Placement         `json:"placement"`
	TerminalSHA256  string            `json:"terminal_sha256"`
	RunnerBirth     *budget.ObjectRef `json:"runner_birth"`
	RunnerExit      *budget.ObjectRef `json:"runner_exit"`
	RunnerExitSHA   string            `json:"runner_exit_sha256"`
}

func retirementDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

func (r Retirement) Validate() error {
	if r.ProtocolVersion != 1 || r.Binding.Validate() != nil || control.ValidateIncarnation(r.Origin) != nil || r.BeforeSequence == 0 || !retirementDigest(r.TerminalSHA256) {
		return fmt.Errorf("invalid original Pod retirement")
	}
	if _, err := r.Placement.Bytes(); err != nil {
		return err
	}
	if r.Placement.Managed {
		if r.RunnerBirth == nil || r.RunnerBirth.Validate() != nil || r.RunnerExit == nil || r.RunnerExit.Validate() != nil || !retirementDigest(r.RunnerExitSHA) {
			return fmt.Errorf("managed retirement requires original Runner birth/exit UID")
		}
	} else if r.RunnerBirth != nil || r.RunnerExit != nil || r.RunnerExitSHA != "" {
		return fmt.Errorf("ordinary retirement cannot project Runner sources")
	}
	return nil
}

func (r *Retirement) UnmarshalJSON(raw []byte) error {
	type plain Retirement
	var v plain
	if err := control.UnmarshalObject(raw, &v); err != nil {
		return err
	}
	*r = Retirement(v)
	return r.Validate()
}

// TerminalPodDigest只接受实际Kubelet终态及完整容器集合。终态、所有app/init
// 均terminated、原容器ID与退出时间缺一不可；Pod删除标记/Ready=false/404/
// waiting不是停止证明。原生sidecar也必须终止；ephemeral不在已认证成本域。
// 此处认证协议事实，不替代目标环境Kubelet/CRI/cgroup回收资格与RBAC验收。
func TerminalPodDigest(pod *core.Pod) (string, error) {
	if pod == nil || pod.UID == "" || pod.ResourceVersion == "" || pod.Spec.NodeName == "" || len(pod.Spec.Containers) == 0 || len(pod.Spec.EphemeralContainers) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 || pod.Status.Phase != core.PodSucceeded && pod.Status.Phase != core.PodFailed {
		return "", fmt.Errorf("original Pod has no complete terminal container proof")
	}
	type stopped struct {
		Name        string                        `json:"name"`
		ContainerID string                        `json:"container_id"`
		Restarts    int32                         `json:"restarts"`
		Result      core.ContainerStateTerminated `json:"result"`
	}
	rows := []stopped{}
	for index, containers := range [][]core.Container{pod.Spec.InitContainers, pod.Spec.Containers} {
		statuses := pod.Status.InitContainerStatuses
		if index == 1 {
			statuses = pod.Status.ContainerStatuses
		}
		if len(statuses) != len(containers) {
			return "", fmt.Errorf("original Pod terminal status omits a container")
		}
		seen := make(map[string]bool)
		for _, c := range containers {
			var status *core.ContainerStatus
			for i := range statuses {
				if statuses[i].Name == c.Name {
					if status != nil {
						return "", fmt.Errorf("duplicate original terminal container")
					}
					status = &statuses[i]
				}
			}
			if seen[c.Name] || status == nil || status.RestartCount < 0 || status.State.Terminated == nil || status.State.Running != nil || status.State.Waiting != nil || control.ValidateID(status.ContainerID, "terminal container") != nil {
				return "", fmt.Errorf("original Pod container is not authoritatively terminated")
			}
			result := status.State.Terminated
			if result.ContainerID != status.ContainerID || result.StartedAt.IsZero() || result.FinishedAt.IsZero() || result.FinishedAt.Before(&result.StartedAt) {
				return "", fmt.Errorf("original container termination identity/time differs")
			}
			seen[c.Name] = true
			rows = append(rows, stopped{Name: c.Name, ContainerID: status.ContainerID, Restarts: status.RestartCount, Result: *result})
		}
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return "", err
	}
	return digestBytes(raw), nil
}

func digestBytes(raw []byte) string {
	// SourceID同样使用SHA256；独立域只作为原文绑定，不当来源认证。
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}

func retirementPod(pod *core.Pod, r Retirement) error {
	p := r.Placement
	if r.Validate() != nil || pod == nil || pod.Name != p.PodName || pod.Namespace != p.Namespace || string(pod.UID) != p.PodUID || pod.Spec.NodeName != r.Binding.NodeName {
		return fmt.Errorf("original retiring Pod identity differs")
	}
	hash, err := TerminalPodDigest(pod)
	if err != nil || hash != r.TerminalSHA256 {
		return fmt.Errorf("original Pod terminal proof changed")
	}
	if p.VMUID != "" {
		owner := metav1.GetControllerOf(pod)
		if len(pod.OwnerReferences) != 1 || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != p.VMName || string(owner.UID) != p.VMUID {
			return fmt.Errorf("retiring Pod changed original VM owner")
		}
	}
	return nil
}

func InspectRetirement(pod *core.Pod) (Retirement, error) {
	var r Retirement
	if pod == nil || json.Unmarshal([]byte(pod.Annotations[RetirementAnnotation]), &r) != nil || retirementPod(pod, r) != nil {
		return r, fmt.Errorf("original Pod retirement intent is absent or changed")
	}
	return r, nil
}

// Retire只由节点owner在独立核完原退出来源后调用。Pod上的原意图必须已持久，
// 同一Node UID/RV CAS移除这一个Pod的Placement及所有峰值；别的Pod/VM不动。
// 响应丢失时用原Pod意图和Node origin/sequence恢复，不用本地removed列表猜测。
func (s *Store) Retire(ctx context.Context, snapshot *Snapshot, r Retirement) (*Snapshot, error) {
	if snapshot == nil || r.Validate() != nil || r.Binding != s.binding || r.Origin != snapshot.model.Origin || snapshot.model.ProtocolVersion < 3 || snapshot.model.Sequence < r.BeforeSequence {
		return nil, fmt.Errorf("host retirement changed original Node intent")
	}
	actor, err := s.authorize(ctx, snapshot.node)
	if err != nil || actor != snapshot.model.LeaseFloor {
		return nil, fmt.Errorf("host retirement owner is no longer fenced")
	}
	pod, err := s.core.Pods(r.Placement.Namespace).Get(ctx, r.Placement.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	actual, err := InspectRetirement(pod)
	got, _ := json.Marshal(actual)
	wanted, _ := json.Marshal(r)
	if err != nil || string(got) != string(wanted) || !slices.Contains(pod.Finalizers, PodProtectionFinalizer) {
		return nil, fmt.Errorf("original protected Pod retirement intent changed")
	}
	row, err := snapshot.model.Placement(r.Placement.PodUID)
	if err != nil {
		if snapshot.model.ProtocolVersion < 5 || snapshot.model.Sequence <= r.BeforeSequence {
			return nil, fmt.Errorf("host retirement has no original row/confirmed closure")
		}
		return snapshot, nil
	}
	if !row.sameOriginal(r.Placement) || snapshot.model.Sequence == control.Uint64(math.MaxUint64) {
		return nil, fmt.Errorf("host retirement changed original allocation or sequence exhausted")
	}
	next := snapshot.model.Clone()
	next.ProtocolVersion, next.Sequence = max(next.ProtocolVersion, 5), next.Sequence+1
	next.Holds = slices.DeleteFunc(next.Holds, func(h Hold) bool { return h.Plan.RunnerPodUID == row.PodUID })
	*next.Placements = slices.DeleteFunc(*next.Placements, func(p Placement) bool { return p.PodUID == row.PodUID })
	return s.write(ctx, snapshot.node, next, false)
}

// InspectRetiredPod供直接APIReader确认实际CAS结果。删除标记不能代替本确认；
// 原Node重建/原账本损坏、未知RV或容量行仍在时一律保留Pod/VM保护。
func InspectRetiredPod(node *core.Node, pod *core.Pod) error {
	r, err := InspectRetirement(pod)
	if err != nil {
		return err
	}
	return InspectRetirementCompletion(node, r)
}

// InspectRetirementCompletion只用于可信Scheduler已写入原VM的关闭锚点。
// 该锚点在移除Pod保护前持久，解决Pod GC先于Controller重读的恢复窗口；
// 不能从远程请求或任意annotation自报构造释放许可。
func InspectRetirementCompletion(node *core.Node, r Retirement) error {
	if r.Validate() != nil {
		return fmt.Errorf("original retirement completion is malformed")
	}
	n, err := inspect(node, r.Binding)
	if err != nil || node.DeletionTimestamp != nil || n.model.ProtocolVersion < 5 || n.model.Origin != r.Origin || n.model.Sequence <= r.BeforeSequence {
		return fmt.Errorf("original Node has no confirmed Pod retirement")
	}
	for _, p := range *n.model.Placements {
		if p.PodUID == r.Placement.PodUID {
			return fmt.Errorf("original Pod allocation is still held")
		}
	}
	for _, h := range n.model.Holds {
		if h.Plan.RunnerPodUID == r.Placement.PodUID {
			return fmt.Errorf("original Pod operation peak is still held")
		}
	}
	return nil
}

// InspectVMUnheld供删除原VM前核查全部实际Node；不以Pod404或某个节点没有
// 记录推定其他节点也已结清。未知/损坏账本必须保留VM finalizer供恢复。
func InspectVMUnheld(node *core.Node, vmUID string) error {
	if node == nil || control.ValidateID(vmUID, "original VM") != nil {
		return fmt.Errorf("original VM Node check is incomplete")
	}
	if node.Annotations[LedgerAnnotation] == "" {
		if node.Annotations[OriginAnnotation] != "" || slices.Contains(node.Finalizers, ProtectionFinalizer) {
			return fmt.Errorf("original Node ledger is missing")
		}
		return nil
	}
	var m Model
	if json.Unmarshal([]byte(node.Annotations[LedgerAnnotation]), &m) != nil {
		return fmt.Errorf("original Node ledger is malformed")
	}
	if _, err := inspect(node, m.Binding); err != nil {
		return err
	}
	if m.Placements != nil {
		for _, p := range *m.Placements {
			if p.VMUID == vmUID {
				return fmt.Errorf("original VM still has a Pod allocation")
			}
		}
	}
	for _, h := range m.Holds {
		if h.Plan.Request.Key.Instance.VMUID == vmUID {
			return fmt.Errorf("original VM still has an operation peak")
		}
	}
	return nil
}
