package pgexec

import (
	"context"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// finishUnstartedAbort只关闭“整个原计划零派发”的取消，不产生PG/Root/VM
// 回执，也不释放任何资源。原执行链的PG/Guest/VM动作须先有原VM WAL的
// Start票据；全Planned因此只证明该原计划未获派发，不证明外部没有承诺。
// Scheduler可能已独立准备Host持有，本路径仍保留它。最后同VM UID/RV的
// Finish CAS与并发Start竞争，不能把Unconfirmed抹成取消。
//
// 这个入口在真实协调循环的第一个执行器内，早于所有领域适配器分流，避免
// 原计划尚未开始却落到“不能启动已取消Host持有”而永久无法闭合。这里只做
// WAL生命周期收口，预算/准入/Host初始持有、高水位和SDK未知栅栏一律不动。
// 已派发、Rejected、已存在Cleanup或跨原PG实例都不能使用此捷径。
func (e *Executor) finishUnstartedAbort(ctx context.Context, s *journal.Snapshot) (bool, error) {
	op := s.State().Active
	if op == nil || !op.AbortRequested || op.Cleanup != nil {
		return false, nil
	}
	for _, step := range op.Steps {
		if step.Phase != journal.Planned {
			return false, nil
		}
	}
	if !s.Dispatchable() || op.Request.Key.Instance != e.instance {
		return true, fmt.Errorf("unstarted cancellation changed original live VM/PG binding")
	}
	pending, closeIntent := e.client.PendingAdmission()
	if e.client.Pending() != nil || pending != nil || closeIntent != nil {
		// 本地未知与全Planned原文矛盾时保留两者，不能靠关闭外层意图
		// 或重新创建空SDK绕过。原未知仍须专门对账，不发送猜测Cancel。
		return true, fmt.Errorf("unstarted cancellation contradicts retained original SDK intent")
	}
	if e.resources != nil && e.resources.Pending() != nil {
		return true, fmt.Errorf("unstarted cancellation contradicts retained original root intent")
	}
	_, err := e.store.Finish(ctx, s, e.actor, op.Request.Key)
	return true, err
}
