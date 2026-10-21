package v1

import (
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// DSGABootstrap在CREATE中先持久化随机incarnation/Compute身份，实际VM UID
// 由apiserver分配后才能登记journal及安全Compute配置。held→ready只允许一次；
// 解除必须同时固定实际VM UID及owned配置Secret UID，不以同名Secret代替身份。
// 这是启动屏障，不是预算/领导权/运行期缩容许可；完整journal和Secret内容在
// Controller/Webhook外层用可信reader进一步校验，避免API包循环依赖journal。
// +kubebuilder:validation:XValidation:rule="self.protocolVersion == oldSelf.protocolVersion && self.incarnation == oldSelf.incarnation && self.computeInstanceID == oldSelf.computeInstanceID",message="DSGA bootstrap identity is immutable"
// +kubebuilder:validation:XValidation:rule="!oldSelf.ready || self == oldSelf",message="ready DSGA bootstrap cannot change"
// +kubebuilder:validation:XValidation:rule="self.ready ? size(self.vmUID) > 0 && size(self.configSecretUID) > 0 : size(self.vmUID) == 0 && size(self.configSecretUID) == 0",message="DSGA release requires both immutable UIDs"
// +kubebuilder:validation:XValidation:rule="self.incarnation != '00000000000000000000000000000000'",message="DSGA incarnation cannot be zero"
type DSGABootstrap struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1
	ProtocolVersion uint32 `json:"protocolVersion"`
	// +kubebuilder:validation:MinLength=32
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{32}$`
	Incarnation string `json:"incarnation"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[!-~]+$`
	ComputeInstanceID string `json:"computeInstanceID"`
	// held时为空；释放时与metadata.uid准确相等，之后不可更改。
	// +kubebuilder:validation:MaxLength=128
	VMUID string `json:"vmUID"`
	// held时为空；释放时指向同namespace/name且归属同一VM的实际Secret UID。
	// +kubebuilder:validation:MaxLength=128
	ConfigSecretUID string `json:"configSecretUID"`
	Ready           bool   `json:"ready"`
}

// DSGAUndispatched只用于创建屏障/初始化，不把清空PodName当可重用历史。
// status中的资源、revision、网络/密钥分配也表明Controller已经执行过创建，
// 均禁止收养为新受管VM。条件列表/普通metadata/finalizer不属于执行事实。
func (vm *VirtualMachine) DSGAUndispatched() bool {
	return vm != nil && (vm.Status.Phase == "" || vm.Status.Phase == VmPending) && vm.Status.RestartCount == 0 &&
		vm.Status.PodName == "" && vm.Status.PodIP == "" && vm.Status.Node == "" && vm.Status.ExtraNetIP == "" && vm.Status.ExtraNetMask == "" &&
		vm.Status.CPUs == nil && vm.Status.MemorySize == nil && vm.Status.CurrentRevision == nil && vm.Status.SSHSecretName == "" && vm.Status.TLSSecretName == ""
}

// DSGABootstrapHeld允许Controller在任何资源动作前失败关闭。nil只属于明确
// 未受管legacy；存在标记却丢屏障、未知版本、损坏或身份不匹配不能回落旧路径。
// 本函数只验API结构/标记；ready的完整journal/配置仍必须经外层可信reader核验。
func (vm *VirtualMachine) DSGABootstrapHeld() (bool, error) {
	if vm == nil {
		return true, fmt.Errorf("missing VM bootstrap object")
	}
	bootstrap := vm.Spec.DSGABootstrap
	if bootstrap == nil {
		if control.Managed(vm.Annotations) {
			return true, fmt.Errorf("managed VM bootstrap record is missing")
		}
		return false, nil
	}
	if bootstrap.ProtocolVersion != 1 || control.ValidateIncarnation(bootstrap.Incarnation) != nil || control.ValidateID(bootstrap.ComputeInstanceID, "Compute") != nil {
		return true, fmt.Errorf("malformed DSGA bootstrap identity")
	}
	if !bootstrap.Ready {
		if bootstrap.VMUID != "" || bootstrap.ConfigSecretUID != "" || !vm.DSGAUndispatched() {
			return true, fmt.Errorf("held DSGA VM already has dispatch history or a release identity")
		}
		if !control.Managed(vm.Annotations) {
			return true, nil
		}
	} else if vm.UID == "" || bootstrap.VMUID != string(vm.UID) || control.ValidateID(bootstrap.ConfigSecretUID, "config Secret") != nil {
		return true, fmt.Errorf("DSGA bootstrap release identity differs")
	}
	if !control.Managed(vm.Annotations) || control.ValidateManagedTransition(vm.Annotations, vm.Annotations, false) != nil || vm.Annotations[control.ModeAnnotation] != "1:"+bootstrap.Incarnation {
		return true, fmt.Errorf("DSGA bootstrap journal marker differs")
	}
	return !bootstrap.Ready, nil
}

func (vm *VirtualMachine) validateDSGABootstrapUpdate(before *VirtualMachine) error {
	old, next := before.Spec.DSGABootstrap, vm.Spec.DSGABootstrap
	if old == nil && next == nil {
		if control.Managed(before.Annotations) || control.Managed(vm.Annotations) {
			return fmt.Errorf("DSGA requires a bootstrap hold from VM creation")
		}
		return nil
	}
	if old == nil || next == nil {
		return fmt.Errorf("DSGA bootstrap cannot be added or removed after creation")
	}
	if vm.UID != before.UID || vm.Name != before.Name || vm.Namespace != before.Namespace || old.ProtocolVersion != next.ProtocolVersion || old.Incarnation != next.Incarnation || old.ComputeInstanceID != next.ComputeInstanceID {
		return fmt.Errorf("DSGA bootstrap identity is immutable")
	}
	if old.Ready && !reflect.DeepEqual(old, next) {
		return fmt.Errorf("ready DSGA bootstrap cannot change")
	}
	if _, err := vm.DSGABootstrapHeld(); err != nil {
		return err
	}
	if !old.Ready && next.Ready && (before.DeletionTimestamp != nil || vm.DeletionTimestamp != nil || !before.DSGAUndispatched() || !vm.DSGAUndispatched()) {
		return fmt.Errorf("DSGA bootstrap release requires the original undispatched VM")
	}
	return nil
}
