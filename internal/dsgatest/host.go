// Package dsgatest提供明确的协议fixture；只能由测试导入，不是Host资格或生产者。
// Node/额度/Lease/来源为测试声明，不运行真实Scheduler或认证物理内存。
package dsgatest

import (
	"encoding/json"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

const Origin = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

func Plan(binding journal.Binding, p budget.Prepared, h budget.HostReservation) hostguard.Plan {
	return hostguard.Plan{Namespace: binding.Namespace, VMName: binding.Name, Incarnation: binding.Incarnation, RunnerPodUID: h.RunnerPodUID, Request: p.Request, BeforeBytes: p.VMBeforeMemoryBytes, HostOverheadBytes: h.HostOverheadBytes}
}

func Host(binding journal.Binding, p budget.Prepared, h budget.HostReservation) budget.HostReservation {
	h.ProtocolVersion = 2
	h.SourceID = hostguard.SourceID(Origin, Plan(binding, p, h))
	return h
}

func Node(original *core.Node, binding journal.Binding, p budget.Prepared, h budget.HostReservation, ref budget.ObjectRef) *core.Node {
	n := original.DeepCopy()
	if n.Annotations == nil {
		n.Annotations = map[string]string{}
	}
	n.Status.Allocatable = core.ResourceList{core.ResourceMemory: resource.MustParse("16Gi")}
	n.Finalizers = append(n.Finalizers, hostguard.ProtectionFinalizer)
	m := hostguard.Model{ProtocolVersion: 2, Binding: hostguard.Binding{NodeName: n.Name, NodeUID: n.UID, Namespace: "fixture-system", LeaseName: "fixture-node-allocator", LeaseUID: "fixture-host-lease"}, Origin: Origin, LeaseFloor: journal.Owner{HolderID: "fixture-host-owner", Epoch: 1}, Sequence: 3, OutsideBytes: 1 << 30, Holds: []hostguard.Hold{{Plan: Plan(binding, p, h), SourceID: h.SourceID, Claim: &ref}}, Profile: nil, Placements: nil}
	raw, err := json.Marshal(m)
	if err != nil || m.Validate() != nil || strings.TrimSpace(string(raw)) == "" {
		panic("invalid explicit Host ledger fixture")
	}
	n.Annotations[hostguard.LedgerAnnotation] = string(raw)
	n.Annotations[hostguard.OriginAnnotation] = "1:" + Origin
	return n
}

// InitialNode是既有Runner/Agent协议用例新增前置条件的显式fixture，不执行
// Scheduler，不认证平台上界。本批Scheduler集成用例另用真实Reserve生成账本。
func InitialNode(original *core.Node, binding journal.Binding, podName, podUID string, memory control.Uint64) *core.Node {
	n := original.DeepCopy()
	if n.Annotations == nil {
		n.Annotations = make(map[string]string)
	}
	n.Status.Allocatable = core.ResourceList{core.ResourceMemory: resource.MustParse("16Gi")}
	n.Finalizers = append(n.Finalizers, hostguard.ProtectionFinalizer)
	p := hostguard.Placement{Namespace: binding.Namespace, PodName: podName, PodUID: podUID, VMName: binding.Name, VMUID: string(binding.VMUID), Incarnation: binding.Incarnation, Managed: true, MemoryBytes: memory, HostOverheadBytes: 64 << 20}
	rows := []hostguard.Placement{p}
	profile := hostguard.Profile{ID: "explicit-test-fixture", SchedulerName: "neonvm-scheduler", VMOverheadBytes: 64 << 20, PodOverheadBytes: 16 << 20}
	m := hostguard.Model{ProtocolVersion: 3, Binding: hostguard.Binding{NodeName: n.Name, NodeUID: n.UID, Namespace: "host-fixture-system", LeaseName: "node-one-allocator", LeaseUID: "original-host-lease-uid"}, Origin: Origin, LeaseFloor: journal.Owner{HolderID: "host-fixture-holder", Epoch: 1}, Sequence: 1, OutsideBytes: 1 << 30, Holds: []hostguard.Hold{}, Profile: &profile, Placements: &rows}
	raw, err := json.Marshal(m)
	if err != nil || m.Validate() != nil {
		panic("invalid explicit initial Host allocation fixture")
	}
	n.Annotations[hostguard.LedgerAnnotation], n.Annotations[hostguard.OriginAnnotation] = string(raw), "1:"+m.Origin
	return n
}
