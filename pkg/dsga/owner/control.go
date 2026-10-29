package owner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

var ErrControlUnavailable = errors.New("coordination owner or loop unavailable; inspect original journal")

// CoordinationResult是一次管理选择的执行/接受结果，不是整个resize已完成。
// State取调用后的原VM持久日志；没有PG/Guest/VM/Host回执时不补造终态。
// 客户端丢回复必须先读此日志，尤其不能因HTTP错误重新生成Begin身份。
type CoordinationResult struct {
	ProtocolVersion uint32                      `json:"protocol_version"`
	Instance        control.CoordinationBinding `json:"instance"`
	OwnerEpoch      control.Uint64              `json:"owner_epoch"`
	Progress        pgexec.Progress             `json:"progress"`
	Disposition     journal.Disposition         `json:"disposition,omitempty"`
	State           *journal.Model              `json:"state,omitempty"`
	// 资源状态/来源摘要只用于选择原Root持有；实际修复仍由Root在
	// 文件锁内重新认证floor/出生/原日志和逐维单调条件。Evidence不证明
	// GuestReady、完整预算、VM或Host完成，不改变外层业务步骤。
	ResourceState    *resourceguard.View     `json:"resource_state,omitempty"`
	HeldSourceSHA256 string                  `json:"held_source_sha256,omitempty"`
	ResourceEvidence *resourceguard.Evidence `json:"resource_evidence,omitempty"`
}

type controlCall struct {
	ctx     context.Context
	command control.CoordinationCommand
	result  chan controlReply
}

type controlReply struct {
	value CoordinationResult
	err   error
}

// ControlEndpoint是当前PG或启动Root恢复唯一串行循环的入口。请求通道无缓冲，不另建
// 后台执行队列；HTTP结束只取消该次调用，已持久的原意图/未知效果继续由WAL保存。
// 端点不跨Session复用，旧owner退出后其Done关闭，旧注册不可投递到新owner。
type ControlEndpoint struct {
	worker *PGWorker
	// 启动恢复没有PG客户端；完整VM/Compute及0 boot是独立的只Root绑定。
	// 两种端点共用投递/取消边界，但从不同时运行或复用一次端点跨阶段。
	resource *ResourceWorker
	session  *Session
	instance control.Instance
	epoch    control.Uint64
	calls    chan controlCall
	done     chan struct{}
	started  atomic.Bool
}

func (worker *PGWorker) NewControlEndpoint() *ControlEndpoint {
	instance, epoch := worker.Binding()
	return &ControlEndpoint{worker: worker, resource: nil, session: worker.session, instance: instance, epoch: epoch, calls: make(chan controlCall), done: make(chan struct{}), started: atomic.Bool{}}
}

func (e *ControlEndpoint) Binding() (control.Instance, control.Uint64) { return e.instance, e.epoch }
func (e *ControlEndpoint) Done() <-chan struct{}                       { return e.done }

func (e *ControlEndpoint) Submit(ctx context.Context, command control.CoordinationCommand) (CoordinationResult, error) {
	var empty CoordinationResult
	if err := command.Validate(); err != nil {
		return empty, err
	}
	instance, epoch := e.Binding()
	if control.Instance(command.Instance) != instance || command.ExpectedOwnerEpoch != epoch {
		return empty, ErrControlUnavailable
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	call := controlCall{ctx: ctx, command: command.Clone(), result: make(chan controlReply, 1)}
	select {
	case <-ctx.Done():
		return empty, ctx.Err()
	case <-e.done:
		return empty, ErrControlUnavailable
	case <-e.session.Context().Done():
		return empty, ErrLeadershipLost
	case e.calls <- call:
	}
	select {
	case reply := <-call.result:
		return reply.value, reply.err
	case <-ctx.Done():
		return empty, ctx.Err()
	case <-e.done:
		return empty, ErrControlUnavailable
	case <-e.session.Context().Done():
		return empty, ErrLeadershipLost
	}
}

// executeControl只能由Run所在goroutine调用，不能从HTTP处理器直接调用各SDK。
// 先检查调用ctx、真实Lease/journal及完整实例，再使用已有领域接口；不持本地
// 注册表/HTTP锁跨网络。错误返回不会清Pending，也不把坏请求变成owner退出。
func (worker *PGWorker) executeControl(ctx context.Context, command control.CoordinationCommand) (CoordinationResult, error) {
	instance, epoch := worker.Binding()
	result := CoordinationResult{ProtocolVersion: 1, Instance: control.CoordinationBinding(instance), OwnerEpoch: epoch, Progress: pgexec.AwaitingFact, Disposition: "", State: nil, ResourceState: nil, HeldSourceSHA256: "", ResourceEvidence: nil}
	if err := command.Validate(); err != nil {
		return result, err
	}
	if control.Instance(command.Instance) != instance || command.ExpectedOwnerEpoch != epoch {
		return result, ErrControlUnavailable
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	if err := callCtx.Err(); err != nil {
		return result, err
	}
	if err := worker.session.Check(callCtx); err != nil {
		return result, err
	}
	// 持久关闭是独立服务栅栏：只允许查询、原Root修复/观察和明确选择原
	// 关闭重放。不能从管理Begin、旧首次开放或取消恢复接口偷偷重开服务。
	safetySnapshot, safetyErr := worker.session.store.Load(callCtx)
	if safetyErr != nil {
		return result, safetyErr
	}
	if safetySnapshot.State().ServiceClosure != nil && command.Action != control.CoordinationState && !command.Action.ResourceOnly() && command.Action != control.CoordinationReplayServiceClosure && command.Action != control.CoordinationContinueServiceClosure {
		return result, ErrServiceSafety
	}
	var err error
	switch command.Action {
	case control.CoordinationState:
		result.Progress = pgexec.Idle
	case control.CoordinationResourceState, control.CoordinationRepairHeld, control.CoordinationObserveHeld:
		if worker.resourceOwner == nil {
			return result, fmt.Errorf("original resource owner is unavailable")
		}
		return worker.resourceOwner.executeResourceControl(callCtx, command)
	case control.CoordinationBegin, control.CoordinationCancel:
		var snapshot *journal.Snapshot
		snapshot, err = worker.session.store.Load(callCtx)
		if err == nil {
			if command.Action == control.CoordinationBegin {
				// 墙钟只限制新的Begin；幂等历史查找仍由Model先处理，不能
				// 因管理请求重试时期限已过而丢失原接受结果。
				millis := time.Now().UnixMilli()
				if millis <= 0 {
					return result, fmt.Errorf("invalid coordination wall clock")
				}
				snapshot, result.Disposition, err = worker.session.store.Begin(callCtx, snapshot, worker.session.Actor(), *command.Request, uint64(millis))
			} else {
				snapshot, err = worker.session.store.RequestAbort(callCtx, snapshot, worker.session.Actor(), *command.Key)
			}
			if err == nil {
				result.Progress = pgexec.Idle
				if command.Action == control.CoordinationCancel || result.Disposition == journal.Accepted {
					result.Progress = pgexec.Recorded
				} else if result.Disposition == journal.Replayed {
					result.Progress = pgexec.Replayed
				}
				state := snapshot.State()
				result.State = &state
			}
		}
	case control.CoordinationReplayPG:
		result.Progress, err = worker.ReplayOriginal(callCtx, *command.Key)
	case control.CoordinationReplayQuiesce:
		result.Progress, err = worker.ReplayQuiesceOriginal(callCtx, *command.Key)
	case control.CoordinationReplayAdmission:
		result.Progress, err = worker.ReplayAdmissionOriginal(callCtx, *command.Key)
	case control.CoordinationReplayResource:
		result.Progress, err = worker.ReplayResourceOriginal(callCtx, *command.Key)
	case control.CoordinationReplayPrepare:
		result.Progress, err = worker.ReplayPrepareOriginal(callCtx, *command.Key)
	case control.CoordinationReplayInitial:
		result.Progress, err = worker.ReplayInitialAdmission(callCtx, command.Selector)
	case control.CoordinationReplayServiceClosure:
		_, result.Progress, err = worker.advanceServiceSafety(callCtx, command.Selector)
	case control.CoordinationContinueServiceClosure:
		result.Progress, err = worker.continueServiceClosure(callCtx, command.Selector)
	case control.CoordinationReplayVM:
		result.Progress, err = worker.ReplayVMOriginal(callCtx, *command.Key)
	case control.CoordinationReplayAbort:
		result.Progress, err = worker.ReplayCommittedAbortOriginal(callCtx, *command.Key)
	case control.CoordinationRecoverUnchanged:
		result.Progress, err = worker.RecoverUnchangedAdmission(callCtx, *command.Key)
	case control.CoordinationRecoverCapacity:
		result.Progress, err = worker.RecoverCapacityAdmission(callCtx, *command.Key)
	default:
		return result, fmt.Errorf("unsupported coordination action")
	}
	if err != nil {
		return result, err
	}
	if result.State == nil {
		snapshot, readErr := worker.session.store.Load(callCtx)
		if readErr != nil {
			return result, readErr
		}
		if snapshot.State().Owner != worker.session.Actor() {
			return result, ErrLeadershipLost
		}
		state := snapshot.State()
		result.State = &state
	}
	return result, nil
}
