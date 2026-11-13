package hostscheduler

import (
	"context"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var errRunnerBirthAbsent = errors.New("original Runner birth is absent")

// unusedPreBirthJournal只用于删除关闭，不放宽初始化的PristineBootstrap。
// Active为空不能证明未执行：资源/PG身份、高水位、历史及淘汰计数、预算和服务
// 生命周期都必须为空。允许启动失败前仅owner fencing推进sequence/epoch，
// 但一旦选定关闭意图，后续重读还必须逐字匹配该原日志摘要。
func unusedPreBirthJournal(m journal.Model) bool {
	return m.Validate() == nil && m.ProtocolVersion == 1 && m.HighWater == nil && m.Active == nil && len(m.History) == 0 && m.ExpiredResults == 0 && m.ResourceIdentity == nil && m.PGInstance == nil && m.BudgetHeld == nil && m.CapacityHeld == nil && m.InitialHeld == nil && m.BudgetBoundary == nil && m.ServiceClosure == nil && m.ServiceLifecycle == nil && m.RetainedCoverage == nil && m.RetainedContextSHA256 == ""
}

func (s *Service) preBirthTerminal(ctx context.Context, pod *core.Pod, p hostguard.Placement, expected *hostguard.PreBirthStop) (hostguard.PreBirthStop, error) {
	var none hostguard.PreBirthStop
	if !p.Managed || p.Settled != nil || p.Barrier != nil || hostguard.PreBirthTerminalPod(pod) != nil {
		return none, fmt.Errorf("pre-birth cleanup lacks original unused managed terminal Pod")
	}
	binding := journal.Binding{Namespace: p.Namespace, Name: p.VMName, VMUID: types.UID(p.VMUID), Incarnation: p.Incarnation}
	vm, err := s.vm.NeonvmV1().VirtualMachines(p.Namespace).Get(ctx, p.VMName, metav1.GetOptions{})
	if err != nil {
		return none, err
	}
	// 本次fresh GET须重新验证完整物理身份，不能借terminalRunner前一次
	// 读取通过就接受两次读之间替换的UID、bootstrap或incarnation。
	if vm.UID != binding.VMUID || vm.ResourceVersion == "" || vm.Spec.DSGABootstrap == nil || vm.Spec.DSGABootstrap.Incarnation != binding.Incarnation {
		return none, fmt.Errorf("pre-birth cleanup original VM identity changed on reread")
	}
	owner := metav1.GetControllerOf(pod)
	if pod.Namespace != p.Namespace || pod.Name != p.PodName || string(pod.UID) != p.PodUID || len(pod.OwnerReferences) != 1 || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != p.VMName || owner.UID != binding.VMUID || vm.DeletionTimestamp == nil {
		return none, fmt.Errorf("pre-birth cleanup original deleting VM/Pod identity differs")
	}
	snapshot, err := journal.Inspect(vm, binding)
	if err != nil {
		return none, err
	}
	if !unusedPreBirthJournal(snapshot.State()) {
		return none, fmt.Errorf("pre-birth cleanup cannot adopt a used VM journal")
	}
	source := hostguard.PreBirthStop{VMJournalSHA256: vmguard.SourceDigest([]byte(vm.Annotations[control.JournalAnnotation]))}
	if expected != nil && *expected != source {
		return none, fmt.Errorf("pre-birth cleanup original VM journal changed")
	}
	if err := vmguard.ConfirmBirthSourcesAbsent(ctx, s.core, binding); err != nil {
		return none, err
	}
	return source, nil
}

func (s *Service) verifyTerminalRunner(ctx context.Context, d domain, pod *core.Pod, r hostguard.Retirement) error {
	if r.ProtocolVersion == 3 {
		_, err := s.preBirthTerminal(ctx, pod, r.Placement, r.PreBirth)
		return err
	}
	_, _, _, err := s.terminalRunner(ctx, d, pod, r.Placement, &r)
	return err
}
