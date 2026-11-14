package agent

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/credential"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/runtimeselection"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/neondatabase/autoscaling/pkg/util"
)

// DSGAConfig是受信Agent配置，不从VM普通注解/HTTP请求接受URL、CA或token路径。
// nil保留原官方静态流程；看到受管标记后即使缺配置也只能失败，不能回落legacy。
type DSGAConfig struct {
	// 可信平台专用命名空间内逐 VM UID 选择，须先安装独立写入准入保护。
	RuntimeSelectionNamespace string `json:"runtimeSelectionNamespace,omitempty"`
	GatewayURL                string `json:"gatewayURL"`
	ManagementCAFile          string `json:"managementCAFile"`
	ProjectedTokenFile        string `json:"projectedTokenFile"`
	RequestTimeoutSeconds     uint   `json:"requestTimeoutSeconds"`
	// 凭据/授权等待与PG执行有不同期限：PG协议允许等待60秒，不能把凭据
	// 的30秒上限复用于执行而固定制造未知结果。90..120秒覆盖网关总期限。
	ComputeTimeoutSeconds uint `json:"computeTimeoutSeconds"`
	PollIntervalSeconds   uint `json:"pollIntervalSeconds"`
	// 可选的受信本地管理UDS；专用目录仅Agent服务UID/root可访问。调用者
	// 仍须选择完整实例和当前owner，不能把此路径挂给Guest或普通租户。
	ControlSocketPath string `json:"controlSocketPath,omitempty"`
	// 独立平台资格发布器只能挂此资源读取UDS，不共享完整管理端点目录。
	ResourceReadSocketPath string `json:"resourceReadSocketPath,omitempty"`
	// 仅可信配置可按VM UID选择原平台资格。跨Pod/boot不会自动收养新资格；
	// 未配置时不生产Prepare。值没有平台默认数，也不从VM普通注解读取。
	// 首次服务同样使用该原资格，必须包含真实启动VM的独立认证规格；
	// 只有未来变配目标的资格不能按比例外推成启动预算或提前开放准入。
	PrepareQualifications map[string]budget.QualificationSource `json:"prepareQualifications,omitempty"`
	// 明确按VM UID选择的自动策略；只读采样和新Begin进入原owner串行
	// 循环。未配置维持手动入口，不由旧LFC/公共metrics猜测执行权。
	AutomaticMemory map[string]policy.Config `json:"automaticMemory,omitempty"`
}

type DSGARunnerState struct {
	AutomaticObservationVersion uint32                    `json:"automaticObservationVersion,omitempty"`
	RuntimeSelection            *runtimeselection.Status  `json:"runtimeSelection,omitempty"`
	AutomaticDecisions          []owner.AutomaticDecision `json:"automaticDecisions,omitempty"`
	SuspensionDrain             *control.PreparedResponse `json:"suspensionDrain,omitempty"`
	ObservedAt                  time.Time                 `json:"observedAt,omitzero"`
	Phase                       string                    `json:"phase"`
	IssuerEpoch                 control.Uint64            `json:"issuerEpoch"`
	Instance                    control.Instance          `json:"instance"`
}

func (r *Runner) dsgaState(ctx context.Context, state DSGARunnerState) {
	if r.lock.TryLock(ctx) == nil {
		r.dsga = state
		r.lock.Unlock()
	}
}

// runDSGA在任何legacy执行器/monitor任务启动前直接GET固定VM UID。GET失败、
// 同名重建、受管标记缺半边/损坏一律失败关闭；网络不持全局/Runner/status锁。
// 返回false仅代表明确读取到同一未受管VM，不以未知状态猜测旧流程可安全执行。
func (r *Runner) runDSGA(ctx context.Context, logger *zap.Logger, updated util.CondChannelReceiver) (bool, error) {
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	vm, err := r.global.vmClient.NeonvmV1().VirtualMachines(r.vmName.Namespace).Get(readCtx, r.vmName.Name, metav1.GetOptions{})
	cancel()
	if err != nil {
		return true, fmt.Errorf("VM mode could not be confirmed before starting executor")
	}
	if vm == nil || vm.UID != r.vmUID || vm.Name != r.vmName.Name || vm.Namespace != r.vmName.Namespace || vm.DeletionTimestamp != nil {
		return true, fmt.Errorf("VM identity changed before starting executor")
	}
	if vm.Spec.DSGABootstrap == nil && !control.Managed(vm.Annotations) {
		return false, nil
	}
	r.status.update(r.global, func(stat podStatus) podStatus {
		stat.dsga = true
		now := time.Now()
		stat.dsgaLastSuccessfulCheck = &now
		return stat
	})
	r.dsgaState(ctx, DSGARunnerState{Phase: "validating", IssuerEpoch: 0, Instance: control.Instance{VMUID: string(r.vmUID), ComputeInstanceID: "", PGBootID: 0}})
	if held, err := vm.DSGABootstrapHeld(); err != nil || held {
		return true, fmt.Errorf("VM bootstrap is held or invalid; legacy fallback forbidden")
	}
	if control.ValidateManagedTransition(vm.Annotations, vm.Annotations, false) != nil {
		return true, fmt.Errorf("DSGA managed marker is malformed; legacy fallback forbidden")
	}
	config := r.global.config.DSGA
	if config == nil || r.global.envArgs.K8sPodUID == "" {
		return true, fmt.Errorf("DSGA requires explicit trusted configuration and Agent Pod UID")
	}
	pod, err := r.initialDSGAPod(ctx, vm, r.dsgaPodUID)
	if err != nil {
		return true, err
	}
	originalPodUID := string(pod.UID)
	store, err := journal.NewStore(r.global.vmClient.NeonvmV1().VirtualMachines(r.vmName.Namespace), journal.Binding{Namespace: r.vmName.Namespace, Name: r.vmName.Name, VMUID: r.vmUID, Incarnation: vm.Annotations[control.ModeAnnotation][2:]})
	if err != nil {
		return true, err
	}
	options := owner.DefaultOptions()
	options.PodUID = r.global.envArgs.K8sPodUID
	// 直接嵌入旧Runner的调用保持原New语义；正式MainRunner在启动前强制
	// 注入独立选主客户端，不能把业务资格审计的限流等待传到续租循环。
	var elector *owner.Elector
	if r.global.dsgaElectionClient == nil {
		elector, err = owner.New(r.global.kubeClient.CoordinationV1(), store, options)
	} else {
		elector, err = owner.NewWithElectionClient(r.global.kubeClient.CoordinationV1(), r.global.dsgaElectionClient, store, options)
	}
	if err != nil {
		return true, err
	}
	r.dsgaState(ctx, DSGARunnerState{Phase: "electing", IssuerEpoch: 0, Instance: control.Instance{VMUID: string(r.vmUID), ComputeInstanceID: "", PGBootID: 0}})
	err = elector.Run(ctx, func(ownerCtx context.Context, session *owner.Session) (callbackErr error) {
		// 只记录固定阶段/类型码。Owner外层错误有意隐藏cause正文，但不能因此
		// 丢失首个生产失败位置；这里不记录URL、请求体、令牌或任意Error文本。
		stage := "initial_binding"
		defer func() {
			if callbackErr != nil {
				logDSGAOwnerCallbackFailure(logger, stage, callbackErr)
			}
		}()
		if err := r.awaitInitialDSGABinding(ownerCtx, session, originalPodUID, dsgaInitialStartupTimeout, time.Duration(config.PollIntervalSeconds)*time.Second); err != nil {
			return err
		}
		stage = "credentials_new"
		credentials, err := credential.New(session, credential.Config{BaseURL: config.GatewayURL, CAFile: config.ManagementCAFile, TokenFile: config.ProjectedTokenFile, Timeout: time.Duration(config.RequestTimeoutSeconds) * time.Second})
		if err != nil {
			return err
		}
		defer credentials.CloseIdleConnections()
		stage = "credentials_identity"
		identity, err := credentials.Identity(ownerCtx)
		if err != nil {
			return err
		}
		// 资源身份先于PG客户端恢复，Root floor不随SQL断连/PG boot变化清空。
		// 必须同一已确认Session，缺root来源/未知旧Guest时失败关闭，不走legacy。
		stage = "resource_recovery_new"
		resources, err := session.NewResourceRecoveryWorker(ownerCtx, resourceguard.ClientConfig{Context: nil, BaseURL: config.GatewayURL, Identity: resourceguard.Identity{VMUID: identity.VMUID, ComputeInstanceID: identity.ComputeInstanceID, GuestBootID: "", CgroupDevice: 0, CgroupInode: 0, SystemCgroupDevice: 0, SystemCgroupInode: 0}, IssuerEpoch: 0, Timeout: time.Duration(config.ComputeTimeoutSeconds) * time.Second, Transport: credentials.ManagementTransport(), Token: credentials.Token, ArrayAuthorityToken: credentials.ArrayAuthorityToken, Pending: nil})
		if err != nil {
			return err
		}
		defer resources.CloseIdleConnections()
		// PG客户端尚未发现/建立时也保留同一Session及原Root SDK的显式恢复
		// 入口。首次约束漂移只读轮询，不发PG业务或自动修复/开放；已有
		// 完整原PG/GuestReady未知则交正式恢复入口处理原重放/取消，避免
		// Root未收到时无限等Observe。0 boot端点不与正式端点并存。
		stage = "resource_recovery_wait"
		if err := r.awaitDSGAResources(ownerCtx, config, resources); err != nil {
			return err
		}
		var qualifications []budget.QualificationSource
		if source, ok := config.PrepareQualifications[string(r.vmUID)]; ok {
			qualifications = append(qualifications, source)
		}
		var runtimeRead owner.RuntimeSelectionReader
		var runtimeInitial *runtimeselection.Selection
		if len(qualifications) == 0 && config.RuntimeSelectionNamespace != "" {
			stage = "runtime_selection"
			reader, readErr := runtimeselection.NewReader(r.global.kubeClient.CoreV1(), store.Binding(), config.RuntimeSelectionNamespace)
			if readErr != nil {
				return readErr
			}
			selected, readErr := reader.Read(ownerCtx)
			if readErr != nil {
				return readErr
			}
			if selected != nil {
				runtimeInitial = selected
				qualifications = append(qualifications, selected.Qualification)
			}
			runtimeRead = reader.Read
		}
		stage = "pg_worker_new"
		worker, err := session.NewProductionPGWorker(ownerCtx, compute.Config{BaseURL: config.GatewayURL, Instance: control.Instance{VMUID: identity.VMUID, ComputeInstanceID: identity.ComputeInstanceID, PGBootID: 0}, IssuerEpoch: session.Actor().Epoch, Timeout: time.Duration(config.ComputeTimeoutSeconds) * time.Second, Transport: credentials.ManagementTransport(), Token: credentials.Token, Pending: nil, AdmissionPending: nil, AdmissionClose: nil}, resources, r.global.kubeClient.CoreV1(), qualifications...)
		if err != nil {
			return err
		}
		defer worker.CloseIdleConnections()
		// 构造后已取得真实 PG boot；冷启动/换 Owner 也必须核这一完整身份，
		// 不能仅凭相同 VM/资格引用让另一个 PG boot 沿用旧选择。
		if runtimeInitial != nil {
			instance, _ := worker.Binding()
			if runtimeInitial.Instance != instance {
				return fmt.Errorf("runtime selection belongs to another original PG instance")
			}
		}
		if automatic, ok := config.AutomaticMemory[string(r.vmUID)]; ok {
			stage = "automatic_policy"
			if err := worker.ConfigureAutomaticMemory(automatic, r.global.kubeClient.CoreV1()); err != nil {
				return err
			}
		}
		instance, epoch := worker.Binding()
		phase := "running"
		if len(qualifications) == 0 {
			phase = "awaiting-qualification"
		}
		r.dsgaState(ownerCtx, DSGARunnerState{Phase: phase, IssuerEpoch: epoch, Instance: instance})
		logger.Info("DSGA owner PG reconciliation started", zap.String("phase", phase))
		var endpoint *owner.ControlEndpoint
		if config.ControlSocketPath != "" || config.ResourceReadSocketPath != "" {
			stage = "control_register"
			endpoint = worker.NewControlEndpoint()
			unregister, err := r.global.dsgaControls.Register(endpoint)
			if err != nil {
				return err
			}
			defer unregister()
		}
		// 同一worker推进PG、最终预算结算和带原持有来源的恢复准入，先持久原门禁意图，
		// 丢回复后只读对账。预算生产者/Guest/VM尚未确认的步骤仍保持ExternalStep；
		// 显式配置自动策略时仅空闲轮次生成持久Begin；不伪造外部回执或
		// 重发未知PG/Root/VM POST，也不另开legacy执行器绕过领域。
		stage = "pg_run"
		var diagnostic dsgaTickDiagnostic
		return worker.Run(ownerCtx, owner.PGLoopOptions{RuntimeSelection: runtimeRead, EnableInFlightGrow: true, PollInterval: time.Duration(config.PollIntervalSeconds) * time.Second, Control: endpoint, Wake: updated.Recv(), Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(tick owner.Tick) {
			diagnostic.observe(logger, tick)
			if tick.RuntimeSelection != nil || len(tick.AutomaticDecisions) > 0 || tick.SuspensionDrain != nil {
				if tick.RuntimeSelection != nil && tick.RuntimeSelection.AppliedGeneration > 0 {
					phase = "running"
				}
				r.dsgaState(ownerCtx, DSGARunnerState{AutomaticObservationVersion: 1, Phase: phase, IssuerEpoch: epoch, Instance: instance, RuntimeSelection: tick.RuntimeSelection, AutomaticDecisions: tick.AutomaticDecisions, SuspensionDrain: tick.SuspensionDrain, ObservedAt: time.Now().UTC()})
			}
			if tick.Err == nil {
				r.status.update(r.global, func(stat podStatus) podStatus { now := time.Now(); stat.dsgaLastSuccessfulCheck = &now; return stat })
			}
		}})
	})
	return true, err
}

func (r *Runner) awaitDSGAResources(ctx context.Context, config *DSGAConfig, resources *owner.ResourceWorker) error {
	// 已绑定原PG且有完整GuestReady写前未知时，直接交同一SDK给正式恢复
	// 循环。它先Observe，写入仍须可信显式选择和全套领域检查；新owner只
	// 能查询/取消旧epoch。这里不把NotFound当成功，也不清原Pending。
	if pending, err := resources.PrepareRetainedPGRecovery(ctx); err != nil || pending {
		return err
	}
	endpoint := resources.NewRecoveryControlEndpoint()
	instance, epoch := endpoint.Binding()
	r.dsgaState(ctx, DSGARunnerState{Phase: "recovering-resources", IssuerEpoch: epoch, Instance: instance})
	if config.ControlSocketPath != "" || config.ResourceReadSocketPath != "" {
		unregister, err := r.global.dsgaControls.Register(endpoint)
		if err != nil {
			return err
		}
		defer unregister()
	}
	return endpoint.RunResourceRecovery(ctx, time.Duration(config.PollIntervalSeconds)*time.Second)
}

// logDSGAOwnerCallbackFailure不改变错误/重试/选主语义；仅本地诊断允许固定码。
func logDSGAOwnerCallbackFailure(logger *zap.Logger, stage string, err error) {
	switch stage {
	case "initial_binding", "credentials_new", "credentials_identity", "resource_recovery_new", "resource_recovery_wait", "pg_worker_new", "automatic_policy", "control_register", "pg_run":
	default:
		stage = "unclassified"
	}
	code := dsgaOwnerFailureCode(err)
	logger.Warn("DSGA owner callback stopped", zap.String("stage", stage), zap.String("code", code))
}

func dsgaOwnerFailureCode(err error) string {
	// 有界解包避免任意错误链的循环；不把外部Error正文拼进可记录值。
	queue := []error{err}
	for remaining := 16; remaining > 0 && len(queue) != 0; remaining-- {
		e := queue[0]
		queue = queue[1:]
		switch e {
		case nil:
			continue
		case context.Canceled:
			return "context_canceled"
		case context.DeadlineExceeded:
			return "context_deadline"
		case owner.ErrLeadershipLost:
			return "leadership_lost"
		case resourceguard.ErrUnknown:
			return "resource_unknown"
		case vmexec.ErrSnapshotChanged:
			return "vm_snapshot_changed"
		case vmguard.ErrUnknown:
			return "vm_unknown"
		}
		switch typed := e.(type) {
		case *credential.Error:
			if typed == nil {
				return "credential_other"
			}
			switch typed.Code {
			case "invalid_configuration", "owner_unavailable", "owner_unconfirmed", "issuer_unavailable", "issuer_response_rejected", "invalid_grant", "invalid_projected_token", "owner_binding_changed", "compute_instance_changed", "guest_tls_identity_rejected":
				return "credential_" + typed.Code
			default:
				return "credential_other"
			}
		case *resourceguard.ClientError:
			if typed == nil {
				return "resource_other"
			}
			switch typed.Code {
			case "invalid_configuration", "credential_unavailable", "owner_unavailable", "context_cancelled", "transport_unconfirmed", "response_unconfirmed", "guest_discovery_unconfirmed", "resource_state_unconfirmed", "guest_identity_changed", "postmaster_birth_unconfirmed", "fence_unconfirmed", "array_original_binding_changed", "array_binding_unavailable", "array_action_unavailable", "original_pending", "invalid_array_body", "array_credential_unavailable", "array_transport_unconfirmed", "array_response_unconfirmed":
				return "resource_" + typed.Code
			default:
				return "resource_other"
			}
		}
		switch wrapped := e.(type) {
		case interface{ Unwrap() []error }:
			for _, inner := range wrapped.Unwrap() {
				if len(queue) == 16 {
					break
				}
				queue = append(queue, inner)
			}
		case interface{ Unwrap() error }:
			queue = append(queue, wrapped.Unwrap())
		}
	}
	return "unclassified"
}

// dsgaTickDiagnostic只观察原Run串行回调，不改变错误、进度、WAL或重放。
// 同一固定码/进度连续出现只记一次，恢复后重新计；不格式化外部Error正文。
type dsgaTickDiagnostic struct {
	code, progress, idleStage, idleType string
	automaticSequence                   uint64
}

func (d *dsgaTickDiagnostic) observe(logger *zap.Logger, tick owner.Tick) {
	// 同原key的提交观测转移有新sequence；轮询重读不重复打日志。正文只有
	// 固定来源身份、数值/时间/固定码，不含凭据、SQL或任意底层错误。
	for _, decision := range tick.AutomaticDecisions {
		if decision.Sequence > d.automaticSequence {
			logger.Info("DSGA automatic memory decision", zap.Any("decision", decision))
			d.automaticSequence = decision.Sequence
		}
	}
	// 固定叶与最终Err独立：原关闭WAL成功的Recorded/nil同样必须保留触发首因。
	if tick.IdleService != nil {
		stage, kind := tick.IdleService.Stage(), tick.IdleService.Type()
		if stage != d.idleStage || kind != d.idleType {
			logger.Warn("DSGA idle service source audit failed", zap.String("stage", "idle_service"), zap.String("source_stage", stage), zap.String("source_type", kind))
		}
		d.idleStage, d.idleType = stage, kind
	} else {
		d.idleStage, d.idleType = "", ""
	}
	if tick.Err == nil {
		if d.code != "" {
			logger.Info("DSGA reconciliation observation recovered", zap.String("previous_code", d.code))
		}
		d.code, d.progress = "", ""
		return
	}
	code := dsgaOwnerFailureCode(tick.Err)
	progress := string(tick.Progress)
	switch progress {
	case "idle", "external_step", "awaiting_fact", "recorded", "replayed", "reconciled":
	default:
		progress = "unclassified"
	}
	if d.code == code && d.progress == progress {
		return
	}
	logger.Warn("DSGA reconciliation awaiting confirmed facts", zap.String("stage", "pg_reconcile"), zap.String("progress", progress), zap.String("code", code))
	d.code, d.progress = code, progress
}
