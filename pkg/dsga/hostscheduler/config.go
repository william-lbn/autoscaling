// Package hostscheduler接入实际Scheduler的持久容量预留及节点owner循环。
// 默认不启用；配置只能来自可信启动参数，不能按Pod请求动态创建分配域。
package hostscheduler

import (
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

type NodeConfig struct {
	Binding hostguard.Binding `json:"binding"`
	Profile hostguard.Profile `json:"profile"`
	// 旧模式为原持续硬约束系统域；显式 KubernetesBudget 模式下仅为
	// Allocatable 内额外系统预算，不声称整台 Host 内核绝对硬上界。
	SystemBytes      control.Uint64                    `json:"system_bytes"`
	KubernetesBudget *hostguard.KubernetesBudgetPolicy `json:"kubernetes_budget,omitempty"`
	// 仅首次引导使用固定原RV，重试不能改成当前RV。已初始化后只读恢复。
	InitializeAtResourceVersion string `json:"initialize_at_resource_version,omitempty"`
}

type Config struct {
	Nodes []NodeConfig `json:"nodes"`
}

func (c Config) Validate(schedulerName string) error {
	if len(c.Nodes) == 0 || len(c.Nodes) > 128 {
		return fmt.Errorf("host domains must contain 1..128 explicit original Nodes")
	}
	for i, n := range c.Nodes {
		if n.Binding.Validate() != nil || n.Profile.Validate() != nil || n.Profile.SchedulerName != schedulerName || n.SystemBytes == 0 {
			return fmt.Errorf("invalid original Node domain/profile/system upper bound")
		}
		if n.KubernetesBudget != nil {
			policy := *n.KubernetesBudget
			digest, err := hostguard.ProfileSHA256(n.Profile)
			if policy.Validate() != nil || err != nil || policy.ProfileSHA256 != digest || policy.NodeName != n.Binding.NodeName || policy.NodeUID != string(n.Binding.NodeUID) || policy.SystemReservationBytes != n.SystemBytes {
				return fmt.Errorf("Kubernetes budget differs from original Node/profile/system reservation")
			}
		}
		for _, old := range c.Nodes[:i] {
			if old.Binding.NodeName == n.Binding.NodeName || old.Binding.NodeUID == n.Binding.NodeUID || old.Binding.Namespace == n.Binding.Namespace && old.Binding.LeaseName == n.Binding.LeaseName {
				return fmt.Errorf("duplicate original Node or Host Lease domain")
			}
		}
	}
	return nil
}
