package owner

import (
	"context"
	"fmt"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/initialexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/prepareexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/runtimeselection"
)

// RuntimeSelectionReader 来自 Agent 的可信平台配置，不接受管理请求或 VM 普通注解。
// 每次读取仍受当前 Session 取消；不另外启动执行器。
type RuntimeSelectionReader func(context.Context) (*runtimeselection.Selection, error)

func (w *PGWorker) refreshRuntimeSelection(ctx context.Context) bool {
	if w.runtimeReader == nil {
		return true
	}
	if time.Now().Before(w.runtimeNext) {
		return w.runtimeStatus.State == "applied"
	}
	w.runtimeNext = time.Now().Add(2 * time.Second)
	selected, err := w.runtimeReader(ctx)
	if err != nil || selected == nil {
		w.runtimeStatus.State = "waiting_for_original_selection"
		return false
	}
	instance, _ := w.Binding()
	if selected.Validate() != nil || selected.Namespace != w.session.binding.Namespace || selected.Name != w.session.binding.Name || selected.Instance != instance || selected.Incarnation != w.session.binding.Incarnation {
		w.runtimeStatus.State = "invalid_instance_selection"
		return false
	}
	digest := selected.Digest()
	// 代次只前进。同代改内容、旧代重现均暂停新建议，不能覆盖已应用状态。
	if selected.Generation < w.runtimeStatus.DesiredGeneration || selected.Generation == w.runtimeStatus.DesiredGeneration && w.runtimeStatus.DesiredSHA256 != "" && w.runtimeStatus.DesiredSHA256 != digest {
		w.runtimeStatus.State = "selection_generation_conflict"
		return false
	}
	if w.suspensionOperation != "" && selected.SuspensionOperation != w.suspensionOperation {
		w.runtimeStatus.State = "suspension_selection_changed"
		return false
	}
	w.runtimeStatus.DesiredGeneration, w.runtimeStatus.DesiredSHA256 = selected.Generation, digest
	if w.qualification != nil && *w.qualification != selected.Qualification {
		w.runtimeStatus.State = "original_qualification_changed"
		return false
	}
	if w.runtimeStatus.AppliedGeneration == selected.Generation && w.runtimeStatus.AppliedSHA256 == digest {
		w.runtimeStatus.State = "applied"
		return true
	}
	if w.automatic != nil && w.automatic.pending != nil {
		w.runtimeStatus.State = "waiting_for_original_operation"
		return false
	}
	snapshot, err := w.session.store.Load(ctx)
	if err != nil {
		w.runtimeStatus.State = "waiting_for_original_journal"
		return false
	}
	state := snapshot.State()
	if !snapshot.Dispatchable() || state.Owner != w.session.Actor() || state.PGInstance == nil || *state.PGInstance != instance {
		w.runtimeStatus.State = "waiting_for_original_owner"
		return false
	}
	if state.Active != nil || w.client.Pending() != nil || w.resourceOwner == nil || w.resourceOwner.client.Pending() != nil {
		w.runtimeStatus.State = "waiting_for_original_operation"
		return false
	}
	if w.safety == nil || w.productionCore == nil {
		w.runtimeStatus.State = "production_domains_unavailable"
		return false
	}
	q, err := w.safety.claims.Qualification(ctx, selected.Qualification)
	identity, _ := w.resourceOwner.client.Binding()
	if err != nil || q.Identity != identity || q.Incarnation != w.session.binding.Incarnation {
		w.runtimeStatus.State = "waiting_for_original_qualification"
		return false
	}
	if selected.Automatic != nil {
		for _, target := range selected.Automatic.Targets {
			if _, err := q.Target(target.VMMemoryBytes); err != nil {
				w.runtimeStatus.State = "unqualified_policy_target"
				return false
			}
		}
	}
	if selected.SuspensionOperation != "" {
		// Suppress new suggestions once the original qualification and journal
		// are authenticated. Earlier resize steps remain on their existing path.
		w.suspensionOperation = selected.SuspensionOperation
	}
	if w.qualification == nil {
		// 仅尚未产生服务/伸缩意图的新实例可在原 Owner 内首次绑定资格。
		// 已有服务须由 Agent 在构造时传回原来源，不能临时换来源恢复。
		if state.HighWater != nil || state.InitialHeld != nil || state.BudgetHeld != nil || state.CapacityHeld != nil || state.ServiceClosure != nil {
			w.runtimeStatus.State = "existing_service_requires_original_qualification"
			return false
		}
		source := selected.Qualification
		prepare, err := prepareexec.New(w.session.store, w.client, w.resourceOwner.client, w.safety.claims, w.safety.runner, w.productionCore, w.session.Actor(), source, w.renewPreparedAdmissionLease)
		if err != nil {
			w.runtimeStatus.State = "production_domains_unavailable"
			return false
		}
		initial, err := initialexec.New(w.session.store, w.client, w.resourceOwner.client, w.safety.claims, w.safety.runner, w.session.Actor(), &source)
		if err != nil {
			w.runtimeStatus.State = "production_domains_unavailable"
			return false
		}
		w.prepare, w.initial, w.qualification = prepare, initial, &source
	}
	var automatic *automaticMemory
	if selected.Automatic != nil {
		automatic, err = w.newAutomaticMemory(*selected.Automatic, w.productionCore)
		if err != nil {
			w.runtimeStatus.State = "invalid_automatic_policy"
			return false
		}
	}
	// 这里只换可丢弃的采样窗口。SDK、Journal、原资格、待决命令均不重建。
	w.automatic = automatic
	w.runtimeStatus.AppliedGeneration, w.runtimeStatus.AppliedSHA256 = selected.Generation, digest
	w.runtimeStatus.State = "applied"
	return true
}

func (w *PGWorker) initializeRuntimeSelection(reader RuntimeSelectionReader) error {
	if reader == nil {
		return nil
	}
	if w.runtimeReader != nil {
		return fmt.Errorf("runtime selection loop cannot be reused")
	}
	w.runtimeReader = reader
	w.runtimeStatus = &runtimeselection.Status{State: "waiting_for_original_selection"}
	return nil
}

func (w *PGWorker) advanceSelectedSuspension(ctx context.Context) (bool, pgexec.Progress, error) {
	if w.suspensionOperation == "" {
		return false, pgexec.Idle, nil
	}
	snapshot, err := w.session.store.Load(ctx)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	state := snapshot.State()
	pendingAutomatic := w.automatic != nil && w.automatic.pending != nil
	if state.Active != nil || pendingAutomatic {
		return false, pgexec.Idle, nil
	}
	progress, err := w.suspendCompute(ctx, snapshot, w.suspensionOperation)
	if err != nil {
		return true, progress, err
	}
	// Suppress admission lease refresh and further actions on a closed boot.
	return true, progress, ErrServiceSafety
}
