// Package budgetclaims用可信Kubernetes客户端读取原UID的持续持有对象。它不创建
// 承诺、不按声明推算系统上界，不访问QMP，也不接受请求指定集群/namespace。
package budgetclaims

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

const ClaimKey = "reservation.json"

// ProtectionFinalizer阻止VM删除/GC在旧QMP/预算未结清时清掉持有对象。
// 只有对应领域核验完整关闭后才能显式移除，普通Agent只读，不能代为清理。
const ProtectionFinalizer = "autoscaling.neon.tech/dsga-reservation-protection"

const (
	ClaimLabel  = "autoscaling.neon.tech/dsga-reservation"
	OutsideKind = "outside-v1"
	HostKind    = "host-v1"
)

type Reader struct {
	core    coreclient.CoreV1Interface
	binding journal.Binding
}

func New(client coreclient.CoreV1Interface, binding journal.Binding) (*Reader, error) {
	if client == nil || binding.Namespace == "" || binding.VMUID == "" {
		return nil, fmt.Errorf("reservation reader requires fixed trusted Kubernetes client/VM")
	}
	return &Reader{core: client, binding: binding}, nil
}

// HostClaimName固定到完整原请求/VM incarnation，供真实Scheduler生产者与Agent
// 消费者共用。名称仅用于找到原对象；仍须真实GET固定UID并校验完整持有内容，
// 不允许按同数值标签List任选一份来源，也不在对象缺失时由Agent临时创建。
func HostClaimName(binding journal.Binding, request control.Request) (string, error) {
	if request.Validate() != nil || request.Key.Instance.VMUID != string(binding.VMUID) || control.ValidateIncarnation(binding.Incarnation) != nil {
		return "", fmt.Errorf("invalid original Host claim binding")
	}
	raw, err := json.Marshal([]any{binding.Namespace, binding.Name, string(binding.VMUID), binding.Incarnation, request})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(append([]byte("neon-dsga-host-reservation-v1\x00"), raw...))
	return "dsga-h-" + hex.EncodeToString(h[:28]), nil
}

func (r *Reader) HostReference(ctx context.Context, request control.Request) (budget.ObjectRef, error) {
	name, err := HostClaimName(r.binding, request)
	if err != nil {
		return budget.ObjectRef{}, err
	}
	cm, err := r.core.ConfigMaps(r.binding.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return budget.ObjectRef{}, err
	}
	if cm == nil {
		return budget.ObjectRef{}, fmt.Errorf("original Host reservation unavailable")
	}
	ref := budget.ObjectRef{Name: name, UID: string(cm.UID)}
	if _, err := r.claim(ctx, ref, HostKind); err != nil {
		return budget.ObjectRef{}, err
	}
	return ref, nil
}

// observed只作跨网络二次复核的本进程事实，不是可由调用者构造的持有票据。
type Observed struct {
	outside, host *core.ConfigMap
	pod           *core.Pod
	node          *core.Node
	Host          budget.HostReservation
}

func (r *Reader) claim(ctx context.Context, ref budget.ObjectRef, kind string) (*core.ConfigMap, error) {
	if ref.Validate() != nil {
		return nil, fmt.Errorf("invalid original reservation reference")
	}
	cm, err := r.core.ConfigMaps(r.binding.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if cm == nil || cm.Name != ref.Name || string(cm.UID) != ref.UID || cm.Namespace != r.binding.Namespace || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || cm.Immutable == nil || !*cm.Immutable || !slices.Contains(cm.Finalizers, ProtectionFinalizer) || cm.Labels[ClaimLabel] != kind || len(cm.Data) != 1 || len(cm.BinaryData) != 0 || len(cm.Data[ClaimKey]) == 0 || len(cm.Data[ClaimKey]) > journal.MaxObservationBytes {
		return nil, fmt.Errorf("original immutable reservation is absent, changed or releasing")
	}
	owner := metav1.GetControllerOf(cm)
	if owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != r.binding.Name || owner.UID != r.binding.VMUID || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
		return nil, fmt.Errorf("reservation belongs to another original VM")
	}
	return cm, nil
}

// Read只消费已由可信预算/Host生产者落实的持久对象。immutable不是资格证明，
// 标签也不是认证；真实RBAC必须只授对应生产者创建/释放权限。缺对象/404不能
// 解释为NoEffect、预算0或自动重建，UID相同且完整body匹配才继续。
func (r *Reader) Read(ctx context.Context, s *journal.Snapshot, p budget.Prepared, hostRef budget.ObjectRef) (*Observed, error) {
	if s == nil || s.VirtualMachine().UID != r.binding.VMUID || p.OutsideClaim == nil {
		return nil, fmt.Errorf("original outside reservation unavailable")
	}
	outside, err := r.claim(ctx, *p.OutsideClaim, OutsideKind)
	if err != nil {
		return nil, err
	}
	var o budget.OutsideReservation
	if json.Unmarshal([]byte(outside.Data[ClaimKey]), &o) != nil || o.ValidateFor(p, r.binding.Incarnation) != nil {
		return nil, fmt.Errorf("outside object differs from original prepared budget")
	}
	host, err := r.claim(ctx, hostRef, HostKind)
	if err != nil {
		return nil, err
	}
	var h budget.HostReservation
	if json.Unmarshal([]byte(host.Data[ClaimKey]), &h) != nil || h.ValidateFor(p, r.binding.Incarnation) != nil {
		return nil, fmt.Errorf("host object differs from original operation")
	}
	pod, err := r.core.Pods(r.binding.Namespace).Get(ctx, h.RunnerPodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if pod == nil {
		return nil, fmt.Errorf("Runner pod observation missing")
	}
	vm := s.VirtualMachine()
	owner := metav1.GetControllerOf(pod)
	if pod == nil || pod.Namespace != r.binding.Namespace || pod.Name != h.RunnerPodName || string(pod.UID) != h.RunnerPodUID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning || pod.Spec.NodeName != h.NodeName || pod.Status.PodIP != vm.Status.PodIP || vm.Status.PodName != pod.Name || vm.Status.Node != h.NodeName || vm.Status.RestartCount != p.RunnerRestartCount || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.UID != r.binding.VMUID || owner.Name != r.binding.Name {
		return nil, fmt.Errorf("original live Runner pod changed")
	}
	node, err := r.core.Nodes().Get(ctx, h.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if node == nil || node.Name != h.NodeName || string(node.UID) != h.NodeUID || node.ResourceVersion == "" || node.DeletionTimestamp != nil {
		return nil, fmt.Errorf("original Host node changed")
	}
	return &Observed{outside: outside, host: host, pod: pod, node: node, Host: h}, nil
}

// Recheck在完成落账/开放之前再次读取源对象及真实Pod/Node。允许Node正常心跳的
// RV变化，但所有承诺/身份必须同一UID且关键内容不变；ConfigMap不能删后同名重建。
// 该读取仍不能替代最终Runner fence或生产者持有寿命协议。
func (r *Reader) Recheck(ctx context.Context, s *journal.Snapshot, p budget.Prepared, hostRef budget.ObjectRef, original *Observed) error {
	if original == nil {
		return fmt.Errorf("reservation observation missing")
	}
	latest, err := r.Read(ctx, s, p, hostRef)
	if err != nil {
		return err
	}
	if original.outside.UID != latest.outside.UID || original.outside.ResourceVersion != latest.outside.ResourceVersion || !reflect.DeepEqual(original.outside.Data, latest.outside.Data) || original.host.UID != latest.host.UID || original.host.ResourceVersion != latest.host.ResourceVersion || !reflect.DeepEqual(original.host.Data, latest.host.Data) || original.pod.UID != latest.pod.UID || original.node.UID != latest.node.UID || original.Host != latest.Host {
		return fmt.Errorf("original reservation changed during reconciliation")
	}
	return nil
}
