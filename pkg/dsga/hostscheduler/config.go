// Package hostscheduler接入实际Scheduler的持久容量预留及节点owner循环。
// 默认不启用；配置只能来自可信启动参数，不能按Pod请求动态创建分配域。
package hostscheduler

import (
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
)

// KubernetesSnapshotBootstrap 只授权对原 UID/Lease/预算策略做首次建账。
// resourceVersion 是单次快照的 CAS 条件，不是部署期间必须保持不变的身份。
const KubernetesSnapshotBootstrap = "kubernetes-current-snapshot-v1"
const CPUAccountingUpgradeSnapshot = "original-placement-snapshot-v1"

type NodeConfig struct {
	CPUAccountingUpgradeMode string            `json:"cpu_accounting_upgrade_mode,omitempty"`
	Binding                  hostguard.Binding `json:"binding"`
	Profile                  hostguard.Profile `json:"profile"`
	// 旧模式为原持续硬约束系统域；显式 KubernetesBudget 模式下仅为
	// Allocatable 内额外系统预算，不声称整台 Host 内核绝对硬上界。
	SystemBytes      control.Uint64                    `json:"system_bytes"`
	KubernetesBudget *hostguard.KubernetesBudgetPolicy `json:"kubernetes_budget,omitempty"`
	// 兼容旧显式冻结模式：原 RV 不得在重试中刷新。与 InitializeMode 互斥。
	InitializeAtResourceVersion string `json:"initialize_at_resource_version,omitempty"`
	// 新模式每次重新读取完整 Node/Pod 快照，并用此次 Node RV 做原子 CAS。
	// 必须同时提供不可变的原 KubernetesBudget；已有或损坏账本绝不重建。
	InitializeMode string `json:"initialize_mode,omitempty"`
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
		if n.InitializeMode != "" && (n.InitializeMode != KubernetesSnapshotBootstrap || n.InitializeAtResourceVersion != "" || n.KubernetesBudget == nil) {
			return fmt.Errorf("snapshot bootstrap requires its explicit original budget and excludes a frozen resourceVersion")
		}
		if n.CPUAccountingUpgradeMode != "" && (n.CPUAccountingUpgradeMode != CPUAccountingUpgradeSnapshot ||
			n.Profile.CPUAccountingVersion != hostguard.CPUAccountingVersion) {
			return fmt.Errorf("CPU upgrade requires explicit original-placement snapshot mode")
		}
		if n.Profile.CPUAccountingVersion == hostguard.CPUAccountingVersion &&
			(n.KubernetesBudget == nil || n.KubernetesBudget.ProtocolVersion != 2) {
			return fmt.Errorf("fixed CPU Host profile requires explicit Kubernetes budget policy v2")
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
