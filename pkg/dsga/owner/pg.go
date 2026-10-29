package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/admissionexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/initialexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/prepareexec"
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
	host      *hostexec.Consumer
	prepare   *prepareexec.Executor
	initial   *initialexec.Executor
	// 管理恢复复用创建本worker时的原资源owner/SDK，不另建空客户端以
	// 绕过GuestReady或修复未知。生命周期仍由Agent同一Session持有。
	resourceOwner *ResourceWorker
	automatic     *automaticMemory
	qualification *budget.QualificationSource
	safety        *serviceSafety
	// Run期间独占SDK/控制入口，避免重复启动两个轮询goroutine竞争Pending。
	// Run返回后可由旧的测试/嵌入调用重新进入；ControlEndpoint本身不可复用。
	running atomic.Bool
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
		if resource.running.Load() {
			return nil, fmt.Errorf("resource recovery or PG loop is already running")
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
		if model.Active != nil || len(model.History) != 0 || model.ExpiredResults != 0 || config.Pending != nil || config.AdmissionPending != nil || config.AdmissionClose != nil || config.AdmissionContinuation != nil {
			return nil, fmt.Errorf("PG worker initial discovery requires pristine journal")
		}
		initial = true
	}
	var pending *control.Request
	var admissionPending *control.AdmissionRequest
	if held := model.InitialHeld; held != nil && held.Admission != nil && held.Admission.Phase == journal.Unconfirmed && (model.ServiceClosure == nil || model.ServiceClosure.Pending()) {
		// 首次开放没有Resize Active，仍可能已经执行但未获回复。必须先从
		// 原永久预算及独立WAL恢复同一SDK pending；不能把空Active当空闲，
		// 不能换budget/epoch或借能力发现悄悄重发首次开放。
		initial, restoreErr := model.InitialBudgetFor()
		var command budget.InitialAdmissionCommand
		if restoreErr != nil || json.Unmarshal(held.Admission.Command, &command) != nil || command.ValidateFor(initial) != nil || command.Request.Instance != config.Instance || command.Request.IssuerEpoch != held.Admission.Origin.Epoch || command.Request.IssuerEpoch > session.actor.Epoch {
			return nil, fmt.Errorf("original first admission command differs from permanent initial budget")
		}
		original := command.Request
		admissionPending = &original
	}
	if active := snapshot.State().Active; active != nil {
		if active.Request.Key.Instance != config.Instance {
			return nil, fmt.Errorf("PG worker cannot silently replace an old boot")
		}
		for _, step := range active.Steps {
			if step.Action == journal.ResizePG && step.Phase != journal.Planned {
				request := active.Request
				pending = &request
			}
			if (step.Action == journal.ResumeAdmission || step.Action == journal.Quiesce) && step.Phase == journal.Unconfirmed && (model.ServiceClosure == nil || model.ServiceClosure.Pending()) {
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
		if cleanup := active.Cleanup; cleanup != nil && cleanup.Phase == journal.Unconfirmed && (model.ServiceClosure == nil || model.ServiceClosure.Pending()) {
			// Cleanup是独立恢复开放，不能只从原Resume步骤恢复后漏掉它的
			// 写前/写后未知。先核原完整预算、原派发者与命令，再恢复同一SDK
			// pending；旧owner仅能查询，不得改epoch重新发送。独立关闭已确认
			// 后保留原Cleanup未知供审计，但不能把它重新恢复成可开放的SDK栅栏。
			original, restoreErr := originalAbortAdmission(model, *cleanup, config.Instance)
			if restoreErr != nil {
				return nil, restoreErr
			}
			if original != nil {
				if admissionPending != nil && *admissionPending != *original {
					return nil, fmt.Errorf("original abort conflicts with another unknown admission")
				}
				originalAdmission := *original
				admissionPending = &originalAdmission
			}
		}
	}
	if config.Pending != nil && (pending == nil || *pending != *config.Pending) {
		return nil, fmt.Errorf("PG worker supplied pending intent differs from journal")
	}
	config.Pending = pending
	var admissionClose *control.AdmissionRequest
	var admissionContinuation *control.AdmissionContinuationRequest
	if c := model.ServiceClosure; c != nil && c.Pending() {
		original := c.ProtectedAdmission()
		if admissionPending != nil && *admissionPending != original {
			return nil, fmt.Errorf("service closure conflicts with original admission unknown")
		}
		admissionPending = &original
		if c.Original != nil {
			closeRequest := c.Request
			admissionClose = &closeRequest
		}
		if c.Continuation != nil {
			// v11恢复最初未知、最近关闭及完整直接前驱。只恢复最后关闭
			// 会丢失选择摘要及旧owner影响；重建SDK不能借此退回旧接口。
			continuation := c.Continuation.Request
			closeRequest := continuation.Close
			admissionClose, admissionContinuation = &closeRequest, &continuation
		}
	}
	if (config.AdmissionClose != nil && (admissionClose == nil || *config.AdmissionClose != *admissionClose)) ||
		(config.AdmissionContinuation != nil && (admissionContinuation == nil || *config.AdmissionContinuation != *admissionContinuation)) ||
		(config.AdmissionPending != nil && (admissionPending == nil || *config.AdmissionPending != *admissionPending)) {
		return nil, fmt.Errorf("PG worker supplied admission differs from journal")
	}
	config.AdmissionPending = admissionPending
	config.AdmissionClose, config.AdmissionContinuation = admissionClose, admissionContinuation
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
	return &PGWorker{session: session, client: client, executor: executor, admission: admission, resources: guest, quiesce: quiesce, finalize: nil, vm: nil, host: nil, prepare: nil, initial: nil, resourceOwner: resource, automatic: nil, qualification: nil, safety: nil, running: atomic.Bool{}}, nil
}

// NewProductionPGWorker只供具备可信Kube客户端的实际Agent创建。完整预算
// 结算与最终准入共用同一原root及持久对象读者；缺读者不降级为旧无来源路径。
// 可选资格仅取可信配置，提供时接预算域的Outside创建；无配置维持ExternalStep，
// 不造默认资格。不创建Host/Node承诺，不自动Begin或更换已有身份。
func (session *Session) NewProductionPGWorker(ctx context.Context, config compute.Config, resources *ResourceWorker, core coreclient.CoreV1Interface, qualification ...budget.QualificationSource) (*PGWorker, error) {
	if resources == nil || core == nil || len(qualification) > 1 {
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
		worker.host, err = hostexec.NewConsumer(session.store, claims, session.actor)
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
	if err == nil && len(qualification) == 1 {
		worker.prepare, err = prepareexec.New(session.store, worker.client, resources.client, claims, runner, core, session.actor, qualification[0])
		if err == nil {
			// 固定创建Prepare时的原资格；自动策略不能传入另一份来源，
			// 避免建议集合和真正生产预算使用不同认证规格。
			source := qualification[0]
			worker.qualification = &source
		}
	}
	if err == nil {
		var source *budget.QualificationSource
		if len(qualification) == 1 {
			source = &qualification[0]
		}
		worker.initial, err = initialexec.New(session.store, worker.client, resources.client, claims, runner, session.actor, source)
	}
	if err != nil {
		worker.CloseIdleConnections()
		return nil, err
	}
	worker.safety = &serviceSafety{claims: claims, runner: runner}
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
	if handled, progress, err := worker.advanceServiceSafety(callCtx, ""); handled {
		return progress, err
	}
	if worker.initial != nil {
		progress, err := worker.initial.Advance(callCtx)
		if err != nil {
			// 首次setter成功后来源也可能在完成审计前漂移。仍在同一
			// Session/串行轮次尝试安全关闭，不等外层返回错误后换owner，
			// 不将原首次未知记为成功或重新派发一次开放。
			if handled, safetyProgress, safetyErr := worker.advanceServiceSafety(callCtx, ""); handled {
				return safetyProgress, safetyErr
			}
		}
		if err != nil || progress != pgexec.ExternalStep {
			return progress, err
		}
	}
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
	if err == nil && progress == pgexec.ExternalStep && worker.prepare != nil {
		progress, err = worker.prepare.Advance(callCtx)
	}
	// ReserveHost由独立Host分配者持有，本Agent只读两端原来源并保存完整
	// 原命令/回执；不能用手填Completed越过节点账本或给Agent增加写权限。
	if err == nil && progress == pgexec.ExternalStep && worker.host != nil {
		progress, err = worker.host.Advance(callCtx)
	}
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
		if err != nil {
			// Finalize只有外层CAS效果；若来源在本轮预审之后或写前
			// CAS期间失效，立即按完整资源前驱的独立边界关闭旧准入。
			// CAS已完成且来源仍健康时保留原错误，不能伪造关闭或成功。
			if handled, safetyProgress, safetyErr := worker.advanceServiceSafety(callCtx, ""); handled {
				return safetyProgress, safetyErr
			}
		}
	}
	if err == nil && progress == pgexec.ExternalStep {
		progress, err = worker.admission.Advance(callCtx)
		if err != nil {
			// 最终开放可能已送达、但写后来源审计失败。保持同一Session
			// 和串行轮次补做独立关闭，不让错误返回/换owner留下服务窗口。
			if handled, safetyProgress, safetyErr := worker.advanceServiceSafety(callCtx, ""); handled {
				return safetyProgress, safetyErr
			}
		}
	}
	if err != nil {
		// VM/Guest写后来源可能在原执行器审计前失效。仍在同一串行轮次
		// 复核独立关闭；合法原命令未知由检查器保留，不改写为NoEffect。
		if handled, safetyProgress, safetyErr := worker.advanceServiceSafety(callCtx, ""); handled {
			return safetyProgress, safetyErr
		}
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

// 预算来源创建的显式原重放与PG/限额/VM重放分开，不接受新预算参数。首次
// Start丢回复、原Create未发送时，普通循环只查询；只有可信选择可进入这里。
func (worker *PGWorker) ReplayPrepareOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	if worker.prepare == nil {
		return pgexec.AwaitingFact, fmt.Errorf("trusted prepare producer is unavailable")
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	return worker.prepare.ReplayOriginal(callCtx, key)
}

// 首次开放原重放独立于Resize Key与最终开放；selector仅指已持久命令摘要。
// 当前Session检查及执行器原派发者检查都通过后，才可能原样发送同一setter。
func (worker *PGWorker) ReplayInitialAdmission(ctx context.Context, selector string) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	if worker.initial == nil {
		return pgexec.AwaitingFact, fmt.Errorf("initial producer is unavailable")
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	return worker.initial.ReplayOriginal(callCtx, selector)
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

// 原VM重放使用独立受信选择，与PG/Root重放分开；Session检查和完整原
// not_sent/预算复核先于持久UID/RV选择。普通轮询永远不调用此入口。
func (worker *PGWorker) ReplayVMOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	if worker.vm == nil {
		return pgexec.AwaitingFact, fmt.Errorf("original VM executor is unavailable")
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	return worker.vm.ReplayOriginal(callCtx, key)
}

// ReplayCommittedAbortOriginal只供已认证协调器明确选择原Cleanup开放命令；
// 普通轮询绝不自动重发，选择Key不携带可覆盖预算/epoch的新参数。
func (worker *PGWorker) ReplayCommittedAbortOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	return worker.admission.ReplayCommittedAbortOriginal(callCtx, key)
}

// RecoverCapacityAdmission是可信服务策略的独立恢复选择；只有原Key，没有
// 用户可改写的预算/限额。普通轮询不会自动选择覆盖外部门禁，已有Cleanup
// 则只能查询/显式原重放，不能通过本入口新建替代命令。
func (worker *PGWorker) RecoverCapacityAdmission(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if err := worker.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	return worker.admission.RecoverCapacityAdmission(callCtx, key)
}

type Tick struct {
	Progress pgexec.Progress
	Err      error
}

type PGLoopOptions struct {
	PollInterval time.Duration
	// Control只接受当前worker的受信管理选择，执行仍在本Run串行goroutine。
	// 与旧领域通道并存，普通轮询不会生成管理命令或自动重放。
	Control *ControlEndpoint
	// Wake只是重读持久状态的提示，不携带可覆盖原请求的新参数。
	Wake <-chan struct{}
	// Replay只能来自已认证协调入口的显式选择。Key仅用于匹配，实际命令
	// 始终从原journal取回；普通错误/定时轮询绝不向此通道自动投递。
	Replay <-chan control.Key
	// 独立的受信显式准入重放入口；nil时只读对账，错误/轮询不自动投递。
	AdmissionReplay <-chan control.Key
	// 与PG/准入分开的显式原资源重放；普通轮询只Observe。
	ResourceReplay <-chan control.Key
	// 仅可信原预算恢复选择；nil与普通轮询都不会自动重试Outside创建。
	PrepareReplay <-chan control.Key
	// 受信协调入口明确选择原首次开放命令摘要；nil时普通循环只读恢复。
	InitialAdmissionReplay <-chan string
	// VMReplay只能来自受信显式协调入口，不以HTTP错误、超时或not_sent
	// 自动投递。选择仅指原Key，完整命令和原回执始终从持久来源读取。
	VMReplay <-chan control.Key
	// AbortReplay只选择原Cleanup的恢复开放，不重放旧PG/VM/资源动作。
	// nil或关闭时普通循环只读查询；未知/超时不会向此通道自动投递。
	AbortReplay <-chan control.Key
	// UnchangedAdmissionRecovery是可信策略确认仍应服务后的独立恢复选择。
	// 它可在旧预算下新建开放Cleanup，与重放原命令不同；nil时普通取消
	// 不覆盖任何当前关闭意图，不由错误/超时向此通道自动投递。
	UnchangedAdmissionRecovery <-chan control.Key
	// CapacityAdmissionRecovery对部分效果取消独立确认当前仍应服务。选择
	// 只能来自可信策略；参数由原预算导出，错误/超时不向该通道自动投递。
	CapacityAdmissionRecovery <-chan control.Key
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
	if !worker.running.CompareAndSwap(false, true) {
		return fmt.Errorf("PG worker loop already running")
	}
	defer worker.running.Store(false)
	if worker.resourceOwner != nil {
		if !worker.resourceOwner.running.CompareAndSwap(false, true) {
			return fmt.Errorf("resource execution loop already running")
		}
		defer worker.resourceOwner.running.Store(false)
	}
	var controls <-chan controlCall
	if options.Control != nil {
		if options.Control.worker != worker {
			return fmt.Errorf("control endpoint belongs to another PG worker")
		}
		if !options.Control.started.CompareAndSwap(false, true) {
			return fmt.Errorf("control endpoint cannot be reused")
		}
		controls = options.Control.calls
		defer close(options.Control.done)
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	defer worker.CloseIdleConnections()
	leaseReady := false
	var nextLeaseRenew time.Time
	var leaseRenewInterval time.Duration
	ensureLease := func() error {
		if worker.safety == nil || leaseReady {
			return nil
		}
		snapshot, err := worker.session.store.Load(callCtx)
		if err != nil {
			return err
		}
		m := snapshot.State()
		// 完全未派发、无历史/永久预算的原取消只有外层CAS，不得为关闭
		// 本地空意图额外取得凭据或触碰PG。该特例不能进入任何新派发；
		// 下一轮一旦离开此精确边界，仍先完成下面的强制原生协商。
		localAbort := m.Active != nil && m.Active.AbortRequested && m.Active.Cleanup == nil && m.InitialHeld == nil && m.BudgetHeld == nil && m.CapacityHeld == nil && len(m.History) == 0 && m.ExpiredResults == 0
		if localAbort {
			for _, step := range m.Active.Steps {
				if step.Phase != journal.Planned {
					localAbort = false
					break
				}
			}
		}
		if localAbort {
			return nil
		}
		// 正式长驻服务必须协商内核失联关闭；测试用的单步领域调用不能
		// 代替此运行门槛。旧网关/扩展明确拒绝，没有legacy心跳兜底。
		fact, err := worker.observeAdmissionLease(callCtx)
		if err != nil {
			return err
		}
		if fact.Expired {
			return ErrServiceSafety
		}
		if options.PollInterval > time.Duration(fact.TimeoutMS)*time.Millisecond/3 {
			return fmt.Errorf("PG polling interval exceeds native admission lease renewal bound")
		}
		leaseReady = true
		leaseRenewInterval = time.Duration(fact.TimeoutMS) * time.Millisecond / 3
		return nil
	}
	ticker := time.NewTicker(options.PollInterval)
	defer ticker.Stop()
	var replay *control.Key
	var admissionReplay *control.Key
	var resourceReplay *control.Key
	var prepareReplay *control.Key
	var initialReplay *string
	var vmReplay *control.Key
	var abortReplay *control.Key
	var unchangedAdmission *control.Key
	var capacityAdmission *control.Key
	var pendingControl *controlCall
	wake, replays, admissionReplays, resourceReplays := options.Wake, options.Replay, options.AdmissionReplay, options.ResourceReplay
	vmReplays := options.VMReplay
	prepareReplays := options.PrepareReplay
	initialReplays := options.InitialAdmissionReplay
	abortReplays := options.AbortReplay
	unchangedAdmissions := options.UnchangedAdmissionRecovery
	capacityAdmissions := options.CapacityAdmissionRecovery
	for {
		if callCtx.Err() != nil {
			return callCtx.Err()
		}
		if err := ensureLease(); err != nil {
			return err
		}
		if pendingControl != nil {
			// 调用方可能已在等待队列中取消；executeControl在任何WAL/SDK
			// 写前核ctx。单容量回复通道允许HTTP先离开，循环不会阻塞发送。
			// 管理错误只回复本次调用，不能用坏Key/旧epoch终止健康owner。
			// 管理调用同时受Run、Session和HTTP调用者约束。不能只采用
			// HTTP ctx：Agent循环先停止而调用者仍在线时，旧管理读/写也必须
			// 被取消并join，不能一直执行到HTTP期限后才归还owner。
			controlCtx, cancelControl := context.WithCancel(callCtx)
			stopCaller := context.AfterFunc(pendingControl.ctx, cancelControl)
			if pendingControl.ctx.Err() != nil {
				cancelControl()
			}
			value, controlErr := worker.executeControl(controlCtx, pendingControl.command)
			stopCaller()
			cancelControl()
			pendingControl.result <- controlReply{value: value, err: controlErr}
			pendingControl = nil
			continue
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
		} else if prepareReplay != nil {
			progress, err = worker.ReplayPrepareOriginal(callCtx, *prepareReplay)
			prepareReplay = nil
		} else if initialReplay != nil {
			progress, err = worker.ReplayInitialAdmission(callCtx, *initialReplay)
			initialReplay = nil
		} else if vmReplay != nil {
			progress, err = worker.ReplayVMOriginal(callCtx, *vmReplay)
			vmReplay = nil
		} else if abortReplay != nil {
			progress, err = worker.ReplayCommittedAbortOriginal(callCtx, *abortReplay)
			abortReplay = nil
		} else if unchangedAdmission != nil {
			progress, err = worker.RecoverUnchangedAdmission(callCtx, *unchangedAdmission)
			unchangedAdmission = nil
		} else if capacityAdmission != nil {
			progress, err = worker.RecoverCapacityAdmission(callCtx, *capacityAdmission)
			capacityAdmission = nil
		} else {
			if worker.automatic != nil && worker.automatic.pending != nil {
				// Begin投递未知先读原WAL/旧RV屏障；未闭合前不采样，也不
				// 让后续物理步骤掩盖本地尚未确认的投递状态。
				progress, err = worker.advanceAutomatic(callCtx)
			} else {
				progress, err = worker.Advance(callCtx)
			}
			if err == nil && progress == pgexec.Idle && worker.automatic != nil {
				progress, err = worker.advanceAutomatic(callCtx)
			}
		}
		// 每轮已有领域来源审计，期限还充足时不重复完整网络审计；否则
		// 限流会扩大策略采样间隔、反复重置缩容窗口。首次及每个期限的
		// 三分之一执行独立完整续租，失败不更新next，更不发盲心跳。
		if err == nil && leaseReady && !time.Now().Before(nextLeaseRenew) {
			err = worker.renewAdmissionLease(callCtx)
			if err == nil {
				nextLeaseRenew = time.Now().Add(leaseRenewInterval)
			}
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
			// Runner异步执行/发布与Agent轮询没有同步时序。原VM仍待读回、
			// prepared明确未发送都属于持有原意图的等待，不能退出owner后
			// 改epoch，也不能为消除错误重发QMP。只有显式原重放/取消入口
			// 能改变not_sent的推进选择；其他身份/协议/来源错误仍照常退出。
			// 原预算无容量变化的取消，可能仍在等待可信策略确认是否恢复
			// 服务。保持owner并等待独立通道，不把拒绝自动开放当作致命
			// 错误退出，也不根据此等待状态自行生成恢复选择。
			if !errors.As(err, &call) && !errors.As(err, &resourceCall) && !errors.As(err, &unknown) && !errors.Is(err, resourceguard.ErrUnknown) && !errors.Is(err, vmguard.ErrUnknown) && !errors.Is(err, vmguard.ErrNotSent) && !errors.Is(err, admissionexec.ErrRecoverySelectionRequired) && !errors.Is(err, ErrPolicyObservation) && !errors.Is(err, ErrServiceSafety) {
				return err
			}
		}
		select {
		case <-callCtx.Done():
			return callCtx.Err()
		case call := <-controls:
			pendingControl = &call
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
		case key, open := <-prepareReplays:
			if !open {
				prepareReplays = nil
			} else {
				selectedKey := key
				prepareReplay = &selectedKey
			}
		case selector, open := <-initialReplays:
			if !open {
				initialReplays = nil
			} else {
				selected := selector
				initialReplay = &selected
			}
		case key, open := <-vmReplays:
			if !open {
				vmReplays = nil
			} else {
				selectedKey := key
				vmReplay = &selectedKey
			}
		case key, open := <-abortReplays:
			if !open {
				abortReplays = nil
			} else {
				selectedKey := key
				abortReplay = &selectedKey
			}
		case key, open := <-unchangedAdmissions:
			if !open {
				unchangedAdmissions = nil
			} else {
				selectedKey := key
				unchangedAdmission = &selectedKey
			}
		case key, open := <-capacityAdmissions:
			if !open {
				capacityAdmissions = nil
			} else {
				selectedKey := key
				capacityAdmission = &selectedKey
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
