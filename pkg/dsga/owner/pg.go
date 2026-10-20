package owner

import (
	"context"
	"errors"
	"fmt"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// PGWorker在一个已经确认的owner Session内长期拥有同一个客户端/原意图栅栏。
// 凭据配置来自可信控制面；每次取凭据还检查真实Lease/journal，不能因重新发现
// 或HTTP失败清空Pending。它不替代预算/Host/Guest/VM领域适配器。
type PGWorker struct {
	session  *Session
	client   *compute.Client
	executor *pgexec.Executor
}

func (session *Session) coupledContext(ctx context.Context) (context.Context, context.CancelFunc) {
	callCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(session.ctx, cancel)
	if session.ctx.Err() != nil {
		cancel()
	}
	return callCtx, func() { stop(); cancel() }
}

// NewPGWorker从已确认的持久活动意图恢复Pending。epoch只能采用Session已提交
// 的代次；boot/端点仍采用可信配置而非按PG回复静默改绑。新owner可观察/取消旧
// epoch，但不能把原意图改为新epoch后执行。nil Token或跨Store/VM直接拒绝。
func (session *Session) NewPGWorker(ctx context.Context, config compute.Config) (*PGWorker, error) {
	if err := session.Check(ctx); err != nil {
		return nil, err
	}
	if config.Token == nil || config.Instance.VMUID != string(session.binding.VMUID) ||
		(config.IssuerEpoch != 0 && config.IssuerEpoch != session.actor.Epoch) {
		return nil, fmt.Errorf("PG worker configuration differs from confirmed owner")
	}
	callCtx, cancel := session.coupledContext(ctx)
	defer cancel()
	snapshot, err := session.store.Load(callCtx)
	if err != nil {
		return nil, err
	}
	var pending *control.Request
	if active := snapshot.State().Active; active != nil {
		if active.Request.Key.Instance != config.Instance {
			return nil, fmt.Errorf("PG worker cannot silently replace an old boot")
		}
		for _, step := range active.Steps {
			if step.Action == journal.ResizePG && step.Phase != journal.Planned {
				request := active.Request
				pending = &request
				break
			}
		}
	}
	if config.Pending != nil && (pending == nil || *pending != *config.Pending) {
		return nil, fmt.Errorf("PG worker supplied pending intent differs from journal")
	}
	config.Pending = pending
	config.IssuerEpoch = session.actor.Epoch
	token := config.Token
	credentialWait := config.Timeout
	config.Token = func(ctx context.Context) (string, error) {
		// Client.Timeout只覆盖Do，不覆盖取凭据。此处单独限制包括授权检查
		// 和凭据服务的等待；凭据提供器必须遵守ctx，失败不输出其私密错误。
		tokenCtx, cancel := context.WithTimeout(ctx, credentialWait)
		defer cancel()
		if err := session.Check(tokenCtx); err != nil {
			return "", err
		}
		return token(tokenCtx)
	}
	client, err := compute.New(callCtx, config)
	if err != nil {
		return nil, err
	}
	executor, err := pgexec.New(session.store, client, session.actor)
	if err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	return &PGWorker{session: session, client: client, executor: executor}, nil
}

func (worker *PGWorker) CloseIdleConnections() { worker.client.CloseIdleConnections() }

func (worker *PGWorker) Advance(ctx context.Context) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	// 外层关闭/更换意图不能自动清本地SDK未知栅栏。必须在原意图尚处活动时
	// 显式对账/重放，或由后续受控跨owner关闭协议核验；不能把旧未知当新空闲，
	// 更不能先Start新PG步骤后才发现客户端拒绝，从而制造另一条虚假Unconfirmed。
	if pending := worker.client.Pending(); pending != nil {
		snapshot, err := worker.session.store.Load(callCtx)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		if snapshot.State().Active == nil || snapshot.State().Active.Request != *pending {
			return pgexec.AwaitingFact, fmt.Errorf("PG worker original SDK intent still needs explicit reconciliation")
		}
	}
	return worker.executor.Advance(callCtx)
}

func (worker *PGWorker) ReplayOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	snapshot, err := worker.session.store.Load(callCtx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if snapshot.State().Active == nil || snapshot.State().Active.Request.Key != key {
		return pgexec.AwaitingFact, fmt.Errorf("PG worker replay signal differs from active intent")
	}
	return worker.executor.ReplayOriginal(callCtx)
}

type Tick struct {
	Progress pgexec.Progress
	Err      error
}

type PGLoopOptions struct {
	PollInterval time.Duration
	// Wake只是重读持久状态的提示，不携带可覆盖原请求的新参数。
	Wake <-chan struct{}
	// Replay只能来自已认证协调入口的显式选择。Key仅用于匹配，实际命令
	// 始终从原journal取回；普通错误/定时轮询绝不向此通道自动投递。
	Replay <-chan control.Key
	// Observe供状态/指标使用，必须有界返回，不打印凭据或误把Recorded当VM完成。
	Observe func(Tick)
}

// Run实际执行持久PG协调循环，长期保留同一Client。网络/写前或写后结果未知时
// 退避后重读原状态，不重建连接对象或自动重发POST；权限/不变量错误则停止。
// Idle/ExternalStep仅等待其他领域/新意图，不跳过预算/VM步骤，也不关闭整个操作。
func (worker *PGWorker) Run(ctx context.Context, options PGLoopOptions) error {
	if options.PollInterval <= 0 || options.PollInterval > time.Minute {
		return fmt.Errorf("invalid PG loop polling interval")
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	defer worker.CloseIdleConnections()
	ticker := time.NewTicker(options.PollInterval)
	defer ticker.Stop()
	var replay *control.Key
	wake, replays := options.Wake, options.Replay
	for {
		if callCtx.Err() != nil {
			return callCtx.Err()
		}
		var progress pgexec.Progress
		var err error
		if replay != nil {
			progress, err = worker.ReplayOriginal(callCtx, *replay)
			replay = nil
		} else {
			progress, err = worker.Advance(callCtx)
		}
		if options.Observe != nil {
			options.Observe(Tick{Progress: progress, Err: err})
		}
		if callCtx.Err() != nil {
			return callCtx.Err()
		}
		if err != nil {
			var call *compute.CallError
			var unknown *journal.UnconfirmedWrite
			if !errors.As(err, &call) && !errors.As(err, &unknown) {
				return err
			}
		}
		select {
		case <-callCtx.Done():
			return callCtx.Err()
		case <-ticker.C:
		case _, open := <-wake:
			if !open {
				wake = nil
			}
		case key, open := <-replays:
			if open {
				replay = &key
			} else {
				replays = nil
			}
		}
	}
}
