package owner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// NewRecoveryControlEndpoint不发现或伪造PG boot。零boot只在本阶段的Root管理
// 选择中有效；正式PG端点建立后取真实非零boot，旧管理决定不能跨阶段复用。
func (w *ResourceWorker) NewRecoveryControlEndpoint() *ControlEndpoint {
	id, epoch := w.client.Binding()
	instance := control.Instance{VMUID: id.VMUID, ComputeInstanceID: id.ComputeInstanceID, PGBootID: 0}
	return &ControlEndpoint{worker: nil, resource: w, session: w.session, instance: instance, epoch: epoch, calls: make(chan controlCall), done: make(chan struct{}), started: atomic.Bool{}}
}

// RunResourceRecovery只处理原Root恢复，不推进首次开放或任何业务步骤。普通轮询
// 只读；实际约束确认且没有本地修复未知后返回，调用者才可初始化正式PGWorker。
// HTTP/Run/Session三方取消必须同步join原调用，不另起后台写者或清原WAL。
// 错误管理决定只回复；资源未知/网络错误保持owner及端点，坏来源/身份则退出。
func (e *ControlEndpoint) RunResourceRecovery(ctx context.Context, interval time.Duration) error {
	if e.worker != nil || e.resource == nil || interval <= 0 || interval > time.Minute {
		return fmt.Errorf("invalid resource recovery loop")
	}
	if !e.resource.running.CompareAndSwap(false, true) {
		return fmt.Errorf("resource execution loop already running")
	}
	defer e.resource.running.Store(false)
	if !e.started.CompareAndSwap(false, true) {
		return fmt.Errorf("control endpoint cannot be reused")
	}
	defer close(e.done)
	callCtx, cancel := e.session.coupledContext(ctx)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := callCtx.Err(); err != nil {
			return err
		}
		view, err := e.resource.Inspect(callCtx)
		if err == nil {
			_, err = view.ConfirmedLimits()
			if err == nil && e.resource.client.PendingHeldRepair() == nil {
				// 最后再次确认租约/VM，不能把较早读回缓存成越过失租的许可。
				return e.session.Check(callCtx)
			}
		}
		var network *resourceguard.ClientError
		if err != nil && !errors.Is(err, resourceguard.ErrUnknown) && !errors.As(err, &network) {
			return err
		}
		select {
		case <-callCtx.Done():
			return callCtx.Err()
		case <-ticker.C:
		case call := <-e.calls:
			commandCtx, cancelCommand := context.WithCancel(callCtx)
			stopCaller := context.AfterFunc(call.ctx, cancelCommand)
			if call.ctx.Err() != nil {
				cancelCommand()
			}
			value, commandErr := e.resource.executeResourceControl(commandCtx, call.command)
			stopCaller()
			cancelCommand()
			call.result <- controlReply{value: value, err: commandErr}
		}
	}
}

// executeResourceControl同时供启动阶段和正式PG阶段调用，统一原来源/当前owner
// 校验；仅由持有执行槽的串行循环进入。Root修复确认只结清独立修复栅栏，
// 从不写外层GuestReady/预算/VM/Host状态或允许任意目标/PG操作。
func (w *ResourceWorker) executeResourceControl(ctx context.Context, command control.CoordinationCommand) (CoordinationResult, error) {
	result := CoordinationResult{ProtocolVersion: 1, Instance: command.Instance, OwnerEpoch: w.session.Actor().Epoch, Progress: pgexec.AwaitingFact, Disposition: "", State: nil, ResourceState: nil, HeldSourceSHA256: "", ResourceEvidence: nil}
	id, epoch := w.client.Binding()
	if command.Validate() != nil || !command.Action.ResourceOnly() || command.ExpectedOwnerEpoch != epoch || command.Instance.VMUID != id.VMUID || command.Instance.ComputeInstanceID != id.ComputeInstanceID {
		return result, ErrControlUnavailable
	}
	if err := w.session.Check(ctx); err != nil {
		return result, err
	}
	if command.Action == control.CoordinationResourceState {
		view, err := w.Inspect(ctx)
		if err != nil {
			return result, err
		}
		result.ResourceState = &view
		// 原Original未知依然可查，但不生成一个可以越过其未知的修复摘要。
		result.HeldSourceSHA256, _ = resourceguard.HeldSourceSHA256(view)
		result.Progress = pgexec.Idle
	} else {
		var evidence resourceguard.Evidence
		var err error
		if command.Action == control.CoordinationRepairHeld {
			evidence, err = w.RepairHeld(ctx, command.Selector)
		} else {
			evidence, err = w.ObserveHeldRepair(ctx, command.Selector)
		}
		if err != nil {
			return result, err
		}
		result.ResourceEvidence, result.HeldSourceSHA256, result.Progress = &evidence, command.Selector, pgexec.Reconciled
	}
	snapshot, err := w.session.store.Load(ctx)
	if err != nil {
		return result, err
	}
	state := snapshot.State()
	if !snapshot.Dispatchable() || state.Owner != w.session.Actor() || state.ResourceIdentity == nil || *state.ResourceIdentity != id {
		return result, ErrLeadershipLost
	}
	result.State = &state
	return result, nil
}
