// Package runnercpu 描述原 Runner cgroup 的 CPU 带宽事实，不授予内存或 Host 释放。
package runnercpu

import (
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const (
	ProtocolVersion = uint32(1)
	Scope           = "original-runner-cgroup-v2"
	PeriodUS        = uint64(100000)
	MaximumVCPUs    = uint32(64)
)

type Identity struct {
	VMUID             string `json:"vm_uid"`
	ComputeInstanceID string `json:"compute_instance_id"`
	Incarnation       string `json:"incarnation"`
	RunnerPodUID      string `json:"runner_pod_uid"`
	RunnerBootID      string `json:"runner_boot_id"`
	NodeUID           string `json:"node_uid"`
}

func (id Identity) Validate() error {
	for kind, value := range map[string]string{"VM": id.VMUID, "Compute": id.ComputeInstanceID, "RunnerPod": id.RunnerPodUID, "Node": id.NodeUID} {
		if err := control.ValidateID(value, kind); err != nil {
			return err
		}
	}
	if err := control.ValidateIncarnation(id.RunnerBootID); err != nil {
		return err
	}
	return control.ValidateIncarnation(id.Incarnation)
}

// Command.Generation 来自当前同 UID 的 VM，Runner 必须直接向原 API 验证，不能自报提升。
type Command struct {
	ProtocolVersion uint32   `json:"protocol_version"`
	Identity        Identity `json:"identity"`
	Generation      int64    `json:"vm_generation"`
}

func (c Command) Validate() error {
	if c.ProtocolVersion != ProtocolVersion || c.Generation <= 0 {
		return fmt.Errorf("invalid original CPU command version or generation")
	}
	return c.Identity.Validate()
}

type Statistics struct {
	Periods             uint64 `json:"periods"`
	ThrottledPeriods    uint64 `json:"throttled_periods"`
	ThrottledUS         uint64 `json:"throttled_us"`
	PressureSomeTotalUS uint64 `json:"pressure_some_total_us"`
}

// Observation 额度包含原 Runner、QEMU、控制与 I/O 线程的同组成本。
// 这只是该组的带宽上界；weight/低使用量都不保证最低吞吐，也不是小规格出生资格。
type Observation struct {
	NodeCommitment      NodeCommitment `json:"node_commitment"`
	MaximumMillicores   uint32         `json:"maximum_millicores"`
	CommittedMillicores uint32         `json:"committed_millicores"`
	// QEMUPID 属于原 Runner 的 PID namespace，不是可脱离 Pod/boot 使用的宿主PID。
	QEMUPID          uint32     `json:"qemu_pid"`
	QEMUStartTicks   uint64     `json:"qemu_start_ticks"`
	ProtocolVersion  uint32     `json:"protocol_version"`
	Scope            string     `json:"scope"`
	Identity         Identity   `json:"identity"`
	Generation       int64      `json:"vm_generation"`
	CgroupDevice     uint64     `json:"cgroup_device"`
	CgroupInode      uint64     `json:"cgroup_inode"`
	QuotaUS          uint64     `json:"quota_us"`
	PeriodUS         uint64     `json:"period_us"`
	Weight           uint64     `json:"weight"`
	OnlineVCPUs      uint32     `json:"online_vcpus"`
	MaximumVCPUs     uint32     `json:"maximum_vcpus"`
	TargetMillicores uint32     `json:"target_millicores"`
	Pending          bool       `json:"pending"`
	Statistics       Statistics `json:"statistics"`
}

// Millicores 用整数换算，拒绝 max、零/溢出和不能精确表达的带宽，不取逻辑目标补值。
func (o Observation) Millicores() (uint32, error) {
	if o.PeriodUS < 1000 || o.PeriodUS > 1000000 || o.QuotaUS < 1000 ||
		o.QuotaUS > uint64(MaximumVCPUs)*o.PeriodUS ||
		o.QuotaUS*1000%o.PeriodUS != 0 {
		return 0, fmt.Errorf("invalid or inexact original CPU quota")
	}
	return uint32(o.QuotaUS * 1000 / o.PeriodUS), nil
}

func (o Observation) Validate() error {
	if err := o.NodeCommitment.Validate(); err != nil {
		return err
	}
	if o.NodeCommitment.CommittedMillicores < uint64(o.CommittedMillicores) {
		return fmt.Errorf("Node commitment does not cover original Runner CPU")
	}
	if err := o.Identity.Validate(); err != nil {
		return err
	}
	if o.ProtocolVersion != ProtocolVersion || o.Scope != Scope || o.Generation < 0 ||
		o.CgroupDevice == 0 || o.CgroupInode == 0 || o.QEMUPID == 0 || o.QEMUStartTicks == 0 || o.Weight == 0 || o.Weight > 10000 ||
		o.MaximumVCPUs == 0 || o.MaximumVCPUs > MaximumVCPUs || o.OnlineVCPUs == 0 || o.OnlineVCPUs > o.MaximumVCPUs ||
		o.MaximumMillicores < 10 || o.MaximumMillicores > o.MaximumVCPUs*1000 || (o.MaximumMillicores+999)/1000 != o.MaximumVCPUs || o.CommittedMillicores < o.MaximumMillicores || o.CommittedMillicores > MaximumVCPUs*1000 || o.TargetMillicores < 10 || o.TargetMillicores > o.MaximumMillicores {
		return fmt.Errorf("invalid original CPU observation")
	}
	current, err := o.Millicores()
	if err != nil {
		return err
	}
	if current > o.MaximumMillicores {
		return fmt.Errorf("observed CPU quota exceeds fixed topology")
	}
	if !o.Pending && (current != o.TargetMillicores || o.OnlineVCPUs != (current+999)/1000) {
		return fmt.Errorf("completed CPU observation differs from actual quota or topology")
	}
	return nil
}
