package cpuscaling

import (
	"errors"
	"fmt"
	"slices"
)

type CPUStater interface {
	OnlineCPUs() ([]int, error)
	OfflineCPUs() ([]int, error)
	SetState(cpuID int, cpuState cpuState) error
}

type cpuState string

const (
	cpuOnline  cpuState = "online"
	cpuOffline cpuState = "offline"
)

type CPUScaler struct {
	cpuState CPUStater
}

func NewCPUScaler() *CPUScaler {
	return &CPUScaler{
		cpuState: &cpuSysfsState{},
	}
}

func (c *CPUScaler) ReconcileOnlineCPU(targetCount int) error {
	// CPU0必须持续在线；非法目标在任何sysfs读写之前拒绝，不能先关闭
	// 其他CPU再发现无法达到0/负数。此门禁不授予受管Guest的写权限，
	// 调用者仍负责认证、原实例fence及CPU操作串行化。
	if targetCount < 1 {
		return fmt.Errorf("target CPU count must be positive")
	}
	online, err := c.cpuState.OnlineCPUs()
	if err != nil {
		return err
	}

	if len(online) == targetCount {
		return nil
	}

	if len(online) > targetCount {
		diff := len(online) - targetCount
		// offline 'diff' CPUs that are currently online
		// reverse online slice so that we offline in the reverse order of onlining.
		slices.Reverse(online)
		if err := c.setStateTo(cpuOffline, diff, online); err != nil {
			return err
		}

	} else if len(online) < targetCount {
		offline, err := c.cpuState.OfflineCPUs()
		if err != nil {
			// 原实现吞掉此错误，导致daemon回200、Runner记住目标，实际
			// 却未扩任何CPU。读取失败不是无操作成功，必须保留原错误。
			return err
		}

		diff := targetCount - len(online)
		// online 'diff' CPUs that are currently offline
		if err := c.setStateTo(cpuOnline, diff, offline); err != nil {
			return err
		}
	}

	// sysfs写被接受不替代完成读回。包含部分写后失败、读回错误或数量
	// 未匹配时均返回错误，不自动回滚已经发生的CPU状态；下次协调先读
	// 当前事实再决定差值。该读回不证明内存、预算或Host已经结清。
	actual, err := c.ActiveCPUsCount()
	if err != nil {
		return err
	}
	if actual != targetCount {
		return fmt.Errorf("CPU target remains unconfirmed: target %d, online %d", targetCount, actual)
	}
	return nil
}

func (c *CPUScaler) setStateTo(state cpuState, count int, candidateCPUs []int) error {
	for _, cpuID := range candidateCPUs {
		if cpuID == 0 {
			// Not allowed to change the status of CPU 0
			continue
		}

		if err := c.cpuState.SetState(cpuID, state); err != nil {
			return err
		}

		count -= 1
		// nothing left to do
		if count <= 0 {
			return nil
		}
	}

	// Got through the entire list but didn't change the state of enough CPUs
	return errors.New("could not change the state of enough CPUs")
}

// ActiveCPUsCount() returns the count of online CPUs.
func (c *CPUScaler) ActiveCPUsCount() (int, error) {
	onlineCPUs, err := c.cpuState.OnlineCPUs()
	if err != nil {
		return 0, err
	}
	return len(onlineCPUs), nil
}
