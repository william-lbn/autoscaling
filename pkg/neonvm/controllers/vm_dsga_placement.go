package controllers

import (
	core "k8s.io/api/core/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
)

// dsgaAffinityForVirtualMachine保持原受管VM冻结的每个required分支，再收紧
// linux/目标架构。Kubernetes的NodeSelectorTerms之间是OR；若另加一个仅
// linux/arch的新term，节点名单、区域等原限制就会被绕过，可能把PG17调度
// 到未授权节点。这里对每个原非空term分别追加AND条件，不引入新OR分支。
//
// 仅DSGA VM走此路径，普通VM仍沿用原实现。必须先深拷贝，不能修改缓存中的
// VM或其原持久计划；重试不能靠上次调用产生的新约束来判断原配置。已有空
// selector/空term按Kubernetes语义是不匹配任何节点，不能补条件将它放开。
func dsgaAffinityForVirtualMachine(vm *vmv1.VirtualMachine) *core.Affinity {
	affinity := vm.Spec.Affinity.DeepCopy()
	if affinity == nil {
		affinity = &core.Affinity{}
	}
	if affinity.NodeAffinity == nil {
		affinity.NodeAffinity = &core.NodeAffinity{}
	}
	required := []core.NodeSelectorRequirement{{Key: "kubernetes.io/os", Operator: core.NodeSelectorOpIn, Values: []string{"linux"}}}
	if vm.Spec.TargetArchitecture != nil {
		required = append(required, core.NodeSelectorRequirement{Key: "kubernetes.io/arch", Operator: core.NodeSelectorOpIn, Values: []string{string(*vm.Spec.TargetArchitecture)}})
	}
	selector := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if selector == nil {
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &core.NodeSelector{NodeSelectorTerms: []core.NodeSelectorTerm{{MatchExpressions: required}}}
		return affinity
	}
	for i := range selector.NodeSelectorTerms {
		term := &selector.NodeSelectorTerms[i]
		if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
			continue
		}
		term.MatchExpressions = append(term.MatchExpressions, required...)
	}
	return affinity
}
