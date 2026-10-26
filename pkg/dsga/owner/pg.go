package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/admissionexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/quiesceexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// PGWorker在一个已经确认的owner Session内长期拥有同一个客户端/原意图栅栏。
// 凭据配置来自可信控制面；每次取凭据还检查真实Lease/journal，不能因重新发现
// 或HTTP失败清空Pending。它不替代预算/Host/Guest/VM领域适配器。
type PGWorker struct {
	session   *Session
	client    *compute.Client
	executor  *pgexec.Executor
	admission *admissionexec.Executor
	resources *resourceexec.Executor
	quiesce   *quiesceexec.Executor
	finalize  *budgetexec.Executor
	vm        *vmexec.Executor
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
func (session *Session) NewPGWorker(ctx context.Context, config compute.Config, resources ...*ResourceWorker) (*PGWorker, error) {
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
	bootstrap := snapshot.VirtualMachine().Spec.DSGABootstrap
	// HWM尚空也已有创建时固定的Compute身份；不能仅因签发器/配置返回
	// 另一个合法CIID，就先向错误实例协商或fence，再等Begin时才拒绝。
	if bootstrap == nil || !bootstrap.Ready || bootstrap.ComputeInstanceID != config.Instance.ComputeInstanceID {
		return nil, fmt.Errorf("PG worker Compute binding differs from ready bootstrap")
	}
	initial := false
	model := snapshot.State()
	if model.ResourceIdentity != nil && len(resources) == 0 {
		return nil, fmt.Errorf("bound resource domain cannot be omitted from PG worker")
	}
	var resource *ResourceWorker
	if len(resources) > 1 {
		return nil, fmt.Errorf("one original resource worker required")
	}
	if len(resources) == 1 {
		resource = resources[0]
		if resource == nil || resource.session != session {
			return nil, fmt.Errorf("resource worker belongs to another owner")
		}
	}
	// 首次发现也有独立持久身份；不能等第一个Resize才依赖HWM。旧日志缺
	// 新字段时仍只取原HWM，不收养当前boot。显式传入boot同样不能绕过绑定，
	// 身份不符在凭据/能力HTTP之前拒绝，避免先对错误实例产生认证或fence。
	known := model.PGInstance
	if known == nil && model.HighWater != nil {
		copy := model.HighWater.Instance
		known = &copy
	}
	if known != nil {
		if known.VMUID != config.Instance.VMUID || known.ComputeInstanceID != config.Instance.ComputeInstanceID ||
			(config.Instance.PGBootID != 0 && config.Instance != *known) {
			return nil, fmt.Errorf("PG worker retained instance binding differs")
		}
		config.Instance = *known
	} else if config.Instance.PGBootID == 0 {
		if model.Active != nil || len(model.History) != 0 || model.ExpiredResults != 0 || config.Pending != nil || config.AdmissionPending != nil || config.AdmissionClose != nil {
			return nil, fmt.Errorf("PG worker initial discovery requires pristine journal")
		}
		initial = true
	}
	var pending *control.Request
	var admissionPending *control.AdmissionRequest
	if active := snapshot.State().Active; active != nil {
		if active.Request.Key.Instance != config.Instance {
			return nil, fmt.Errorf("PG worker cannot silently replace an old boot")
		}
		for _, step := range active.Steps {
			if step.Action == journal.ResizePG && step.Phase != journal.Planned {
				request := active.Request
				pending = &request
			}
			if (step.Action == journal.ResumeAdmission || step.Action == journal.Quiesce) && step.Phase == journal.Unconfirmed {
				var original control.AdmissionRequest
				// Quiesce关闭与最终Resume开放共用原生独立准入代次；恢复必须由
				// 原步骤确定方向，不能只恢复Open而丢掉写前/写后未知的关闭命令。
				wantOpen := step.Action == journal.ResumeAdmission
				var decodeErr error
				if step.Action == journal.Quiesce {
					var saved budget.QuiesceCommand
					decodeErr = json.Unmarshal(step.Command, &saved)
					if decodeErr != nil {
						// 旧仅关闭格式只能恢复原只读查询。执行器明确拒绝把它补成
						// 带来源的新证书或进行写重放，不因升级丢掉旧关闭未知。
						decodeErr = json.Unmarshal(step.Command, &original)
					} else {
						original = saved.Close
					}
					if decodeErr == nil && saved.ProtocolVersion != 0 && saved.Request != active.Request {
						decodeErr = fmt.Errorf("original quiesce request changed")
					}
				} else {
					decodeErr = json.Unmarshal(step.Command, &original)
				}
				if decodeErr != nil || original.Open != wantOpen || original.Instance != active.Request.Key.Instance || original.BudgetCommitmentID != active.Request.BudgetCommitmentID || step.DispatchedBy == nil || original.IssuerEpoch != step.DispatchedBy.Epoch || original.IssuerEpoch > session.actor.Epoch || step.TargetID != original.Instance.ComputeInstanceID {
					return nil, fmt.Errorf("original admission command differs from journal")
				}
				admissionPending = &original
			}
		}
	}
	if config.Pending != nil && (pending == nil || *pending != *config.Pending) {
		return nil, fmt.Errorf("PG worker supplied pending intent differs from journal")
	}
	config.Pending = pending
	if config.AdmissionClose != nil || (config.AdmissionPending != nil && (admissionPending == nil || *config.AdmissionPending != *admissionPending)) {
		return nil, fmt.Errorf("PG worker supplied admission differs from journal")
	}
	config.AdmissionPending = admissionPending
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
	var client *compute.Client
	if initial {
		client, err = compute.DiscoverInitial(callCtx, config)
	} else {
		client, err = compute.New(callCtx, config)
	}
	if err != nil {
		return nil, err
	}
	if known == nil {
		// 到此只有只读能力协商，没有PG写入。必须先固定真实boot、取得原
		// VM UID/RV CAS确认才返回可用执行器；写回复丢失也关闭本地客户端，
		// 保留服务器可能已经提交的原绑定。下次重建只读它，不能重新发现。
		instance, _ := client.Binding()
		if _, err := session.store.BindPG(callCtx, snapshot, session.actor, instance); err != nil {
			client.CloseIdleConnections()
			return nil, err
		}
	}
	var executor *pgexec.Executor
	if resource != nil {
		executor, err = pgexec.New(session.store, client, session.actor, resource.client)
	} else {
		executor, err = pgexec.New(session.store, client, session.actor)
	}
	if err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	var admissionResources *resourceguard.Client
	if resource != nil {
		admissionResources = resource.client
	}
	// 最终开放也必须复用同一已认证root客户端，不能只在GuestReady/Quiesce
	// 核验后就把旧预算当永久许可；nil仅保留旧未知命令的只读恢复。
	admission, err := admissionexec.New(session.store, client, session.actor, admissionResources)
	if err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	var guest *resourceexec.Executor
	var quiesce *quiesceexec.Executor
	if resource != nil {
		guest, err = resourceexec.New(session.store, client, resource.client, session.actor)
		if err != nil {
			client.CloseIdleConnections()
			return nil, err
		}
		quiesce, err = quiesceexec.New(session.store, client, resource.client, session.actor)
		if err != nil {
			client.CloseIdleConnections()
			return nil, err
		}
	}
	return &PGWorker{session: session, client: client, executor: executor, admission: admission, resources: guest, quiesce: quiesce}, nil
}

// NewProductionPGWorker只供具备可信Kube只读客户端的实际Agent创建。完整预算
// 结算与最终准入共用同一原root及持久对象读者；缺读者不降级为旧无来源路径。
// 不创建缺失承诺，不自动Begin或更换已有PG/Guest/owner绑定。
func (session *Session) NewProductionPGWorker(ctx context.Context, config compute.Config, resources *ResourceWorker, core coreclient.CoreV1Interface) (*PGWorker, error) {
	if resources == nil || core == nil {
		return nil, fmt.Errorf("production worker requires original root and reservation reader")
	}
	worker, err := session.NewPGWorker(ctx, config, resources)
	if err != nil {
		return nil, err
	}
	claims, err := budgetclaims.New(core, session.binding)
	var runner *vmguard.Reader
	if err == nil {
		runner, err = vmguard.NewReader(core, session.binding, string(session.leaseUID))
	}
	if err == nil {
		worker.vm, err = vmexec.New(session.store, worker.client, resources.client, claims, runner, session.actor)
	}
	if err == nil {
		worker.finalize, err = budgetexec.New(session.store, worker.client, resources.client, session.actor, claims, runner)
	}
	if err == nil {
		worker.admission, err = admissionexec.NewWithRunner(session.store, worker.client, session.actor, claims, resources.client, runner)
	}
	if err != nil {
		worker.CloseIdleConnections()
		return nil, err
	}
	return worker, nil
}

func (worker *PGWorker) CloseIdleConnections() { worker.client.CloseIdleConnections() }

// Binding只暴露实际协商/恢复后固定的PG实例，用于后续计划完整原意图；
// 它不是准入/预算或VM许可，也不允许调用方改写Client绑定。
func (worker *PGWorker) Binding() (control.Instance, control.Uint64) { return worker.client.Binding() }

func (worker *PGWorker) Advance(ctx context.Context) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	// 外层关闭/更换意图不能自动清本地SDK未知栅栏。必须在原意图尚处活动时
	// 完整原终态对账/授权显式重放，或由后续受控跨owner关闭协议核验；不能把旧未知当新空闲，
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
	progress, err := worker.executor.Advance(callCtx)
	if err == nil && progress == pgexec.ExternalStep && worker.quiesce != nil {
		progress, err = worker.quiesce.Advance(callCtx)
	}
	if err == nil && progress == pgexec.ExternalStep && worker.resources != nil {
		progress, err = worker.resources.Advance(callCtx)
	}
	if err == nil && progress == pgexec.ExternalStep && worker.vm != nil {
		progress, err = worker.vm.Advance(callCtx)
	}
	if err == nil && progress == pgexec.ExternalStep && worker.finalize != nil {
		progress, err = worker.finalize.Advance(callCtx)
	}
	if err == nil && progress == pgexec.ExternalStep {
		return worker.admission.Advance(callCtx)
	}
	return progress, err
}

// ReplayQuiesceOriginal与资源/Resize/最终开放的显式重放分域；选择Key不是新
// 命令输入，真正发送的关闭参数始终来自原VM账本，不能换epoch/revision。
func (worker *PGWorker) ReplayQuiesceOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	if worker.quiesce == nil {
		return pgexec.AwaitingFact, fmt.Errorf("original quiesce resource domain is missing")
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	return worker.quiesce.ReplayOriginal(callCtx, key)
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

// 准入重放与PG resize重放分开，不能将同一Key提示误解为另一个领域命令。
// 仅当前Session且原完整日志匹配时交实际准入执行器复核预算，不自动生成新请求。
func (worker *PGWorker) ReplayAdmissionOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	return worker.admission.ReplayOriginal(callCtx, key)
}

// Guest资源原意图重放由独立受信入口选择，不能将PG/准入重放提示复用为
// 放宽硬约束。执行器仍从原账本取完整命令并复核原预算、PG、Guest和Runner。
func (worker *PGWorker) ReplayResourceOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	if worker.resources == nil {
		return pgexec.AwaitingFact, fmt.Errorf("original resource executor is unavailable")
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	return worker.resources.ReplayOriginal(callCtx, key)
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
	// 独立的受信显式准入重放入口；nil时只读对账，错误/轮询不自动投递。
	AdmissionReplay <-chan control.Key
	// 与PG/准入分开的显式原资源重放；普通轮询只Observe。
	ResourceReplay <-chan control.Key
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
	var admissionReplay *control.Key
	var resourceReplay *control.Key
	wake, replays, admissionReplays, resourceReplays := options.Wake, options.Replay, options.AdmissionReplay, options.ResourceReplay
	for {
		if callCtx.Err() != nil {
			return callCtx.Err()
		}
		var progress pgexec.Progress
		var err error
		if replay != nil {
			progress, err = worker.ReplayOriginal(callCtx, *replay)
			replay = nil
		} else if admissionReplay != nil {
			progress, err = worker.ReplayAdmissionOriginal(callCtx, *admissionReplay)
			admissionReplay = nil
		} else if resourceReplay != nil {
			progress, err = worker.ReplayResourceOriginal(callCtx, *resourceReplay)
			resourceReplay = nil
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
			var resourceCall *resourceguard.ClientError
			var unknown *journal.UnconfirmedWrite
			if !errors.As(err, &call) && !errors.As(err, &resourceCall) && !errors.As(err, &unknown) && !errors.Is(err, resourceguard.ErrUnknown) {
				return err
			}
		}
		select {
		case <-callCtx.Done():
			return callCtx.Err()
		case <-ticker.C:
		case key, open := <-admissionReplays:
			if !open {
				admissionReplays = nil
			} else {
				copy := key
				admissionReplay = &copy
			}
		case key, open := <-resourceReplays:
			if !open {
				resourceReplays = nil
			} else {
				copy := key
				resourceReplay = &copy
			}
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
