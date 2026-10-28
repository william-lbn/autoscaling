package cpuscaling

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// faultState只模拟sysfs读写边界；执行正式CPUScaler，不把本测试作为
// Linux CPU热插拔、Runner认证或任何内存回收证明。部分写不回滚，后续
// 协调须由真实读回决定余下动作，不能通过记录目标补造成功。
type faultState struct {
	*MockState
	onlineReads   int
	writes        int
	onlineError   error
	offlineError  error
	readbackError error
	writeError    error
	failWrite     int
	ignoreWrites  bool
}

func (s *faultState) OnlineCPUs() ([]int, error) {
	s.onlineReads++
	if s.onlineError != nil {
		return nil, s.onlineError
	}
	if s.onlineReads > 1 && s.readbackError != nil {
		return nil, s.readbackError
	}
	return s.MockState.OnlineCPUs()
}

func (s *faultState) OfflineCPUs() ([]int, error) {
	if s.offlineError != nil {
		return nil, s.offlineError
	}
	return s.MockState.OfflineCPUs()
}

func (s *faultState) SetState(id int, state cpuState) error {
	s.writes++
	if s.writes == s.failWrite {
		return s.writeError
	}
	if s.ignoreWrites {
		return nil
	}
	return s.MockState.SetState(id, state)
}

func newFaultState() *faultState {
	return &faultState{
		MockState: NewMockState(4), onlineReads: 0, writes: 0,
		onlineError: nil, offlineError: nil, readbackError: nil, writeError: nil,
		failWrite: 0, ignoreWrites: false,
	}
}

func TestCPUReconcileRejectsInvalidBeforeIO(t *testing.T) {
	for _, target := range []int{0, -1} {
		s := newFaultState()
		scaler := &CPUScaler{cpuState: s}
		require.Error(t, scaler.ReconcileOnlineCPU(target))
		require.Zero(t, s.onlineReads)
		require.Zero(t, s.writes)
	}
}

func TestCPUReconcilePreservesReadAndPartialWriteFailures(t *testing.T) {
	for _, scenario := range []string{"online", "offline", "readback", "partial-write"} {
		t.Run(scenario, func(t *testing.T) {
			s := newFaultState()
			cause := errors.New("injected sysfs failure")
			switch scenario {
			case "online":
				s.onlineError = cause
			case "offline":
				s.offlineError = cause
			case "readback":
				s.readbackError = cause
			case "partial-write":
				s.failWrite, s.writeError = 2, cause
			}
			scaler := &CPUScaler{cpuState: s}
			require.ErrorIs(t, scaler.ReconcileOnlineCPU(3), cause)
			switch scenario {
			case "online", "offline":
				require.Zero(t, s.writes)
				require.Equal(t, 1, s.onlineReads)
			case "readback":
				require.Equal(t, 2, s.writes)
				require.Equal(t, 2, s.onlineReads)
			case "partial-write":
				require.Equal(t, 2, s.writes)
				require.Equal(t, cpuOnline, s.state[1], "先发生的效果必须保留，不能回滚猜测")
				require.Equal(t, cpuOffline, s.state[2])
				// 同一个执行器从实际部分状态继续，不重做已完成CPU写。
				s.failWrite, s.writeError = 0, nil
				require.NoError(t, scaler.ReconcileOnlineCPU(3))
				require.Equal(t, 3, s.writes)
				require.Equal(t, cpuOnline, s.state[0])
			}
		})
	}
}

func TestCPUReconcileRequiresActualCompletionReadback(t *testing.T) {
	for _, direction := range []string{"grow", "shrink"} {
		t.Run(direction, func(t *testing.T) {
			s := newFaultState()
			target := 3
			if direction == "shrink" {
				s.state[1], s.state[2] = cpuOnline, cpuOnline
				target = 1
			}
			s.ignoreWrites = true
			scaler := &CPUScaler{cpuState: s}
			require.ErrorContains(t, scaler.ReconcileOnlineCPU(target), "remains unconfirmed")
			require.Equal(t, 2, s.writes)
			require.Equal(t, 2, s.onlineReads)
			require.Equal(t, cpuOnline, s.state[0], "CPU0不能关闭")
		})
	}
}
