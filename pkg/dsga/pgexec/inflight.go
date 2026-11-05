package pgexec

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// resizeFlight只有一次原始Resize网络调用；写前登记、数组授权和落账仍由原writer执行。
// 不持久化第二份队列，不创建预算/Owner，也不以本地任务存在替代完整服务审计。
type resizeFlight struct {
	request  control.Request
	actor    journal.Owner
	step     journal.Step
	fence    control.FenceResponse
	ctx      context.Context
	cancel   context.CancelFunc
	finished bool
	result   control.ControlResponse
	err      error
}

type resizeFlightController struct {
	mu     sync.Mutex
	parent context.Context
	cancel context.CancelFunc
	job    *resizeFlight
	// 同一个原job的RAM引用，仅保原派发/fence供丢回复后只读观察；不是新任务/持久队列。
	retained *resizeFlight
}

// EnableInFlight仅由同Session的Run父ctx显式启用，默认保持原同步流程。
// 当前仅Grow使用；Shrink仍走原严格同步链。Owner完整fresh审计接线前不得生产启用。
// 返回的取消函数不清WAL/SDK Pending；Run结束/失主必须调用或取消父ctx。
func (e *Executor) EnableInFlight(parent context.Context) (context.CancelFunc, error) {
	if parent == nil || parent.Err() != nil {
		return nil, fmt.Errorf("PG in-flight requires a live same-session Run context")
	}
	e.flight.mu.Lock()
	defer e.flight.mu.Unlock()
	if e.flight.parent != nil {
		return nil, fmt.Errorf("PG in-flight Run context is already bound")
	}
	e.flight.parent, e.flight.cancel = context.WithCancel(parent)
	return e.flight.cancel, nil
}

// InFlightEnabled只说明本执行器配置过显式模式，不是准入/续租许可。
func (e *Executor) InFlightEnabled() bool {
	e.flight.mu.Lock()
	defer e.flight.mu.Unlock()
	return e.flight.parent != nil
}

// MatchInFlight只核本地任务与当前不可变派发。成功回执待writer消费仍属同一任务；
// 错误/取消/期限耗尽不能授予续接。调用方还须独立完成全部当前预算/Guest/Runner审计。
func (e *Executor) MatchInFlight(s *journal.Snapshot, request control.Request) bool {
	e.flight.mu.Lock()
	defer e.flight.mu.Unlock()
	j := e.flight.job
	if j == nil || e.flight.parent.Err() != nil || j.ctx.Err() != nil || j.finished && j.err != nil || j.request != request {
		return false
	}
	if s == nil || s.State().Active == nil || s.State().Active.AbortRequested || s.State().Active.Cleanup != nil || s.State().ServiceClosure != nil {
		return false
	}
	_, ok := e.flightStep(s, j)
	return ok
}

func (e *Executor) flightStep(s *journal.Snapshot, j *resizeFlight) (journal.Step, bool) {
	if s == nil || !s.Dispatchable() {
		return journal.Step{}, false
	}
	m := s.State()
	if m.Owner != e.actor || m.Owner != j.actor || m.VMUID != e.instance.VMUID || m.Active == nil || m.Active.Origin != j.actor || m.Active.Request != j.request || j.request.Key.Instance != e.instance {
		return journal.Step{}, false
	}
	if m.PGInstance != nil && *m.PGInstance != e.instance {
		return journal.Step{}, false
	}
	for _, step := range m.Active.Steps {
		if step.Action != journal.ResizePG {
			continue
		}
		intent, err := control.DecodeRequest(step.Command)
		if err != nil || intent != j.request || step.Phase != journal.Unconfirmed && step.Phase != journal.Settled || step.TargetID != j.step.TargetID || step.Ordinal != j.step.Ordinal || step.CommandSHA256 != j.step.CommandSHA256 || !bytes.Equal(step.Command, j.step.Command) || step.DispatchedBy == nil || *step.DispatchedBy != j.actor {
			return journal.Step{}, false
		}
		return step, true
	}
	return journal.Step{}, false
}

func (e *Executor) cancelResizeFlight() {
	e.flight.mu.Lock()
	defer e.flight.mu.Unlock()
	if e.flight.job != nil {
		e.flight.job.cancel()
	}
}

func (e *Executor) hasResizeFlight() bool {
	e.flight.mu.Lock()
	defer e.flight.mu.Unlock()
	return e.flight.job != nil
}

// startResizeFlight只能在原Start/数组授权均成功之后调用。goroutine仅调用同一Client.Resize。
func (e *Executor) startResizeFlight(intent control.Request, step journal.Step, fence control.FenceResponse) (bool, error) {
	e.flight.mu.Lock()
	defer e.flight.mu.Unlock()
	if e.flight.parent == nil || intent.Kind != control.Grow {
		return false, nil
	}
	if err := e.flight.parent.Err(); err != nil {
		return true, err
	}
	if e.flight.job != nil {
		return true, &compute.CallError{MayHaveExecuted: true, Code: "original_resize_inflight"}
	}
	// 使用原请求绝对期限与原HTTP客户端限制，不延长请求或服务lease。
	// schema的uint64期限可超过MaxInt64，不能回绕为过去；仍受Run父ctx和原HTTP Timeout约束。
	if uint64(intent.DeadlineUnixMillis) <= uint64(time.Now().UnixMilli()) {
		return true, context.DeadlineExceeded
	}
	ctx, cancel := context.WithCancel(e.flight.parent)
	if uint64(intent.DeadlineUnixMillis) <= uint64(math.MaxInt64) {
		cancel()
		ctx, cancel = context.WithDeadline(e.flight.parent, time.UnixMilli(int64(intent.DeadlineUnixMillis)))
	}
	if err := ctx.Err(); err != nil {
		cancel()
		return true, err
	}
	step.Command = append([]byte(nil), step.Command...)
	j := &resizeFlight{request: intent, actor: e.actor, step: step, fence: fence, ctx: ctx, cancel: cancel}
	e.flight.job = j
	e.flight.retained = j
	go func() {
		var fact control.ControlResponse
		var err error
		defer func() {
			if recover() != nil {
				err = &compute.CallError{MayHaveExecuted: true, Code: "original_resize_inflight_panic"}
			}
			e.flight.mu.Lock()
			j.result, j.err, j.finished = fact, err, true
			e.flight.mu.Unlock()
		}()
		fact, err = e.client.Resize(ctx, intent)
	}()
	return true, nil
}

// advanceResizeFlight在writer内消费回复；存储核验/record绝不在网络goroutine运行。
// 活任务允许原Status观察；其NotFound不是NoEffect。已观察committed仍等待唯一HTTP结束。
func (e *Executor) advanceResizeFlight(ctx context.Context, s *journal.Snapshot) (bool, Progress, error) {
	e.flight.mu.Lock()
	j, parent := e.flight.job, e.flight.parent
	if parent == nil {
		e.flight.mu.Unlock()
		return false, AwaitingFact, nil
	}
	if err := parent.Err(); err != nil {
		if j != nil {
			j.cancel()
		}
		e.flight.mu.Unlock()
		return true, AwaitingFact, err
	}
	if j == nil {
		e.flight.mu.Unlock()
		return false, AwaitingFact, nil
	}
	step, same := e.flightStep(s, j)
	if !same || s.State().ServiceClosure != nil {
		j.cancel()
		e.flight.mu.Unlock()
		return true, AwaitingFact, &compute.CallError{MayHaveExecuted: true, Code: "original_resize_dispatch_changed"}
	}
	if !j.finished {
		e.flight.mu.Unlock()
		return step.Phase == journal.Settled, AwaitingFact, nil
	}
	e.flight.job = nil
	fact, err := j.result, j.err
	j.cancel()
	e.flight.mu.Unlock()
	if err != nil {
		return true, AwaitingFact, err
	}
	if step.Phase == journal.Settled {
		// Status可先记录，但迟到回复不能更改其原生终态或制造第二次派发。
		var saved executionAudit
		if step.Observation == nil || control.UnmarshalObject(step.Observation.Data, &saved) != nil || saved.ProtocolVersion != 1 || saved.Response.ValidateResize(j.request, e.instance) != nil || fact.ValidateResize(j.request, e.instance) != nil || fact.Observed.IssuerFloorEpoch != e.actor.Epoch || fact.Observed.CapacityGeneration < j.fence.Observed.CapacityGeneration || saved.Response.Receipt.Result == nil || fact.Receipt.Result == nil || saved.Response.Receipt.Result.Operation.Phase != fact.Receipt.Result.Operation.Phase {
			return true, AwaitingFact, &compute.CallError{MayHaveExecuted: true, Code: "original_resize_terminal_differs"}
		}
		return true, Reconciled, nil
	}
	// 单步调用的ctx也必须受原Run/Session出生ctx约束；不能在锁内检查后
	// 用独立Background把迟到回执写入WAL。已经发出的CAS仍按原Unknown处理。
	recordCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(parent, cancel)
	defer func() { stop(); cancel() }()
	if parent.Err() != nil {
		return true, AwaitingFact, parent.Err()
	}
	progress, err := e.record(recordCtx, s, j.request, step, j.fence, fact, true)
	return true, progress, err
}

// ObserveDispatchedInFlight只在来源审计拒绝后对账本执行器原Grow派发。
// 它不授予服务/预算许可，也不经过普通Advance：绝无Start、授权、新Resize、Replay、Cancel或Fence。
// 原Run/Session父ctx结束则拒绝；原HTTP child期限/丢回复不取消同owner下原Key只读对账。
// snapshot须来自调用方本轮Store读；内部再次核同UID/RV，最终仍由原Record CAS核actor/step。
func (e *Executor) ObserveDispatchedInFlight(ctx context.Context, snapshot *journal.Snapshot) (Progress, error) {
	if ctx == nil {
		return AwaitingFact, fmt.Errorf("original in-flight observation requires context")
	}
	if err := ctx.Err(); err != nil {
		return AwaitingFact, err
	}
	e.flight.mu.Lock()
	parent, original, job := e.flight.parent, e.flight.retained, e.flight.job
	if parent == nil || original == nil {
		e.flight.mu.Unlock()
		return AwaitingFact, fmt.Errorf("original in-flight observation has no captured dispatch")
	}
	if err := parent.Err(); err != nil {
		e.flight.mu.Unlock()
		return AwaitingFact, err
	}
	step, matches := e.flightStep(snapshot, original)
	if !matches || snapshot.State().ServiceClosure != nil || snapshot.State().Active.AbortRequested || snapshot.State().Active.Cleanup != nil || job != nil && job != original {
		if job != nil {
			job.cancel()
		}
		e.flight.mu.Unlock()
		return AwaitingFact, fmt.Errorf("original in-flight observation lost retained dispatch")
	}
	finished := job != nil && job.finished
	e.flight.mu.Unlock()
	// 同时受调用ctx和原Run/Session取消约束；不能用新Background绕开父出生ctx。
	observeCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(parent, cancel)
	defer func() { stop(); cancel() }()
	if parent.Err() != nil {
		return AwaitingFact, parent.Err()
	}
	current, err := e.store.Load(observeCtx)
	if err != nil {
		return AwaitingFact, err
	}
	latest, ok := e.flightStep(current, original)
	if !ok || current.ResourceVersion() != snapshot.ResourceVersion() || current.State().ServiceClosure != nil || current.State().Active.AbortRequested || current.State().Active.Cleanup != nil {
		if !ok {
			e.cancelResizeFlight()
		}
		return AwaitingFact, fmt.Errorf("original in-flight observation lost current actor or snapshot")
	}
	if observeCtx.Err() != nil || parent.Err() != nil {
		return AwaitingFact, context.Canceled
	}
	if finished { // 只有原writer可消费回执；已由Status落账时仅原终态一致性核验。
		_, progress, err := e.advanceResizeFlight(observeCtx, current)
		return progress, err
	}
	if step.Phase == journal.Settled || latest.Phase == journal.Settled {
		return AwaitingFact, nil
	}
	if latest.Phase != journal.Unconfirmed {
		return AwaitingFact, fmt.Errorf("original in-flight observation is not durable MAY")
	}
	if pending := e.client.Pending(); pending != nil && *pending != original.request {
		return AwaitingFact, &compute.CallError{MayHaveExecuted: true, Code: "original_resize_pending_changed"}
	}
	fact, err := e.client.Status(observeCtx, control.Query{ProtocolVersion: 1, Key: original.request.Key})
	if err != nil {
		return AwaitingFact, err
	}
	// captured原fence只核原boot/floor基线；原record还核完整请求、当前floor及WAL CAS。
	return e.record(observeCtx, current, original.request, latest, original.fence, fact, false)
}
