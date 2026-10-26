package resourceguard

import (
	"fmt"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// MaxPidsTasks对应受支持64位Linux6.12的PID_MAX_LIMIT；不是平台配置默认值。
// 目标内核实际可用pid_max还须在写前读取核验，不把协议接受当内核一定接受。
const MaxPidsTasks control.Uint64 = 1 << 22

// TaskLimitEvidence只读真实pids.max/current；current含线程和整个子树。
// 迁入及调低上限允许current>max，故仅有max文件绝不能作为数量上界证明。
// 真PG身份、root独占迁移和当前<=max仍须由各自边界核验；不发私有预算/VM许可。
type TaskLimitEvidence struct {
	MaxTasks     control.Uint64 `json:"max_tasks"`
	CurrentTasks control.Uint64 `json:"current_tasks"`
}

func (v *TaskLimitEvidence) UnmarshalJSON(b []byte) error {
	type plain TaskLimitEvidence
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	*v = TaskLimitEvidence(p)
	return nil
}

func (e Evidence) matchesTasks(want control.Uint64) error {
	if want == 0 {
		if e.TaskLimit != nil {
			return fmt.Errorf("unexpected task limit for legacy unbounded source")
		}
		return nil
	}
	if want > MaxPidsTasks || e.TaskLimit == nil || e.TaskLimit.MaxTasks != want || e.TaskLimit.CurrentTasks > want {
		return fmt.Errorf("finite task limit is missing, drifted or currently exceeded")
	}
	// populated不包含僵尸，而pids.current在release时才释放，可出现populated=false但
	// current>0；这不是0任务的证据。Bootstrap/下一次出生必须另外要求计数确为0。
	return nil
}

func (r *record) taskLimit() control.Uint64 {
	if r.Bootstrap == nil {
		return 0
	}
	return r.Bootstrap.Original.Limits.PidsMaxTasks
}

// 只开启原配置所需controller，不静默补pids或从子进程数猜默认。内核不存在该
// controller时在原bootstrap内核写前失败，已有只memory的来源保持原行为。
func requiredControllers(available string, limits Limits) (string, error) {
	set := make(map[string]bool)
	for _, name := range strings.Fields(available) {
		set[name] = true
	}
	if !set["memory"] {
		return "", fmt.Errorf("Guest memory controller unavailable")
	}
	if limits.PidsMaxTasks == 0 {
		return "+memory\n", nil
	}
	if !set["pids"] {
		return "", fmt.Errorf("Guest task controller unavailable")
	}
	return "+memory +pids\n", nil
}
