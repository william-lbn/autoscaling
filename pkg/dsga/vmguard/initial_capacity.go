package vmguard

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// InitialConfirmed是本次认证读取的原启动容量，刻意不包含Resize Receipt。
// 只允许原Runner从未登记任何内存命令的状态；无Pending并不足够，因为
// 已完成/取消的Last和HighWater也不能被当前相同数值抹去。它不是缓存许可，
// 消费者仍须复核PG/Root/Host来源，并以原VM/Node UID和RV完成最终CAS。
type InitialConfirmed struct {
	Sources budget.InitialVMSources
	Birth   Birth
	Actual  Observation
}

func (r *Reader) InitialCapacityFor(ctx context.Context, initial budget.Initial, actor journal.Owner) (InitialConfirmed, error) {
	var none InitialConfirmed
	if initial.Validate() != nil || initial.Instance.VMUID != string(r.binding.VMUID) || initial.Incarnation != r.binding.Incarnation {
		return none, fmt.Errorf("initial Runner capacity differs from fixed original VM budget")
	}
	birth, ref, p, err := r.Inspect(ctx, &initial.RunnerBirth, actor)
	if err != nil {
		return none, err
	}
	raw, err := json.Marshal(birth.Identity)
	if err != nil {
		return none, err
	}
	if ref != initial.RunnerBirth || SourceDigest(raw) != initial.RunnerIdentitySHA256 || birth.Identity.RunnerPodUID != initial.RunnerPodUID || birth.Identity.NodeUID != initial.NodeUID ||
		p.State.Pending != nil || p.State.Last != nil || p.State.HighWater != nil || p.State.LastPublished != nil || p.Actual.RequestedBytes != initial.VMMemoryBytes || p.Actual.ActualBytes != initial.VMMemoryBytes {
		return none, fmt.Errorf("initial Runner capacity has command history, changed birth or different actual memory")
	}
	source := budget.InitialVMSources{Instance: initial.Instance, BirthClaim: ref, StateClaim: birth.StateClaim, IdentitySHA256: SourceDigest(raw), MemoryBytes: initial.VMMemoryBytes}
	if err := source.MatchInitial(initial); err != nil {
		return none, err
	}
	return InitialConfirmed{Sources: source, Birth: birth, Actual: *p.Actual}, nil
}

// CheckInitialCapacity同时认证原不可变Birth给出的State UID与永久预算来源。
// 不允许用同名重建State、当前观测值或另一份Initial替换已保存来源；失败只
// 保留原持有并返回错误，不创建来源、不发送QMP、不合成取消/完成回执。
func (r *Reader) CheckInitialCapacity(ctx context.Context, source budget.InitialVMSources, initial budget.Initial, actor journal.Owner) (InitialConfirmed, error) {
	var none InitialConfirmed
	if err := source.MatchInitial(initial); err != nil {
		return none, err
	}
	proof, err := r.InitialCapacityFor(ctx, initial, actor)
	if err != nil {
		return none, err
	}
	if proof.Sources != source {
		return none, fmt.Errorf("initial Runner capacity lost original State UID or source")
	}
	return proof, nil
}
