package budgetexec

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 原Finalized不能借用VM状态或另一个同目标回执。新生产构造器有Runner读者，
// 必须实际读取原Birth/State/Receipt UID和当前floor/无pending/实际目标；旧缺
// 来源仅供历史显式路径，不能补一份“已完成”声明来升级当前预算。
func (e *Executor) checkRunner(ctx context.Context, p budget.Prepared, prepared, guest, vm *journal.Step, fact budget.VMSettlement) error {
	if e.runner == nil {
		if fact.Runner != nil {
			return fmt.Errorf("runner-backed settlement requires original authenticated reader")
		}
		return nil
	}
	if fact.Runner == nil || prepared == nil || prepared.Observation == nil || guest == nil || guest.Observation == nil || vm == nil {
		return fmt.Errorf("production finalization requires original persistent Runner sources")
	}
	var cmd vmguard.Command
	if json.Unmarshal(vm.Command, &cmd) != nil || cmd.Validate() != nil || cmd.Request != p.Request || cmd.BeforeBytes != p.VMBeforeMemoryBytes || cmd.BindingClaim != fact.Runner.BirthClaim || cmd.HostClaim != fact.HostClaim || vm.CommandSHA256 != fact.Runner.JournalSHA256 || vm.TargetID != cmd.Identity.RunnerPodUID || cmd.Identity.RunnerPodUID != fact.Host.RunnerPodUID || cmd.Identity.NodeUID != fact.Host.NodeUID {
		return fmt.Errorf("original typed VM command/receipt binding changed before finalization")
	}
	proof, err := e.runner.CheckRetained(ctx, *fact.Runner, p.Request, digest(prepared.Observation.Data), digest(guest.Observation.Data), fact.HostClaim, e.actor)
	if err != nil {
		return err
	}
	if proof.Receipt.Intent.Command != cmd || proof.Actual.ActualBytes != fact.ActualMemoryBytes || proof.Sources.CommandSHA256 != fact.RunnerCommandID {
		return fmt.Errorf("original Runner completion differs from final VM fact")
	}
	return nil
}
