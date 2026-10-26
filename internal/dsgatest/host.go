// Package dsgatest提供明确的协议fixture；只能由测试导入，不是Host资格或生产者。
// Node/额度/Lease/来源为测试声明，不运行真实Scheduler或认证物理内存。
package dsgatest

import (
	"encoding/json"
	"strings"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

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
	m := hostguard.Model{ProtocolVersion: 2, Binding: hostguard.Binding{NodeName: n.Name, NodeUID: n.UID, Namespace: "fixture-system", LeaseName: "fixture-node-allocator", LeaseUID: "fixture-host-lease"}, Origin: Origin, LeaseFloor: journal.Owner{HolderID: "fixture-host-owner", Epoch: 1}, Sequence: 3, OutsideBytes: 1 << 30, Holds: []hostguard.Hold{{Plan: Plan(binding, p, h), SourceID: h.SourceID, Claim: &ref}}}
	raw, err := json.Marshal(m)
	if err != nil || m.Validate() != nil || strings.TrimSpace(string(raw)) == "" {
		panic("invalid explicit Host ledger fixture")
	}
	n.Annotations[hostguard.LedgerAnnotation] = string(raw)
	n.Annotations[hostguard.OriginAnnotation] = "1:" + Origin
	return n
}
