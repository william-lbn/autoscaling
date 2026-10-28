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
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/util"
)

// DSGAConfig是受信Agent配置，不从VM普通注解/HTTP请求接受URL、CA或token路径。
// nil保留原官方静态流程；看到受管标记后即使缺配置也只能失败，不能回落legacy。
type DSGAConfig struct {
	GatewayURL            string `json:"gatewayURL"`
	ManagementCAFile      string `json:"managementCAFile"`
	ProjectedTokenFile    string `json:"projectedTokenFile"`
	RequestTimeoutSeconds uint   `json:"requestTimeoutSeconds"`
	// 凭据/授权等待与PG执行有不同期限：PG协议允许等待60秒，不能把凭据
	// 的30秒上限复用于执行而固定制造未知结果。90..120秒覆盖网关总期限。
	ComputeTimeoutSeconds uint `json:"computeTimeoutSeconds"`
	PollIntervalSeconds   uint `json:"pollIntervalSeconds"`
	// 可选的受信本地管理UDS；专用目录仅Agent服务UID/root可访问。调用者
	// 仍须选择完整实例和当前owner，不能把此路径挂给Guest或普通租户。
	ControlSocketPath string `json:"controlSocketPath,omitempty"`
	// 仅可信配置可按VM UID选择原平台资格。跨Pod/boot不会自动收养新资格；
	// 未配置时不生产Prepare。值没有平台默认数，也不从VM普通注解读取。
	// 首次服务同样使用该原资格，必须包含真实启动VM的独立认证规格；
	// 只有未来变配目标的资格不能按比例外推成启动预算或提前开放准入。
	PrepareQualifications map[string]budget.QualificationSource `json:"prepareQualifications,omitempty"`
}

type DSGARunnerState struct {
	Phase       string           `json:"phase"`
	IssuerEpoch control.Uint64   `json:"issuerEpoch"`
	Instance    control.Instance `json:"instance"`
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
	if vm.Status.Node != r.global.envArgs.K8sNodeName || vm.Status.PodName != r.podName.Name || vm.Status.PodIP != r.podIP {
		return true, fmt.Errorf("DSGA VM runner or node binding changed")
	}
	store, err := journal.NewStore(r.global.vmClient.NeonvmV1().VirtualMachines(r.vmName.Namespace), journal.Binding{Namespace: r.vmName.Namespace, Name: r.vmName.Name, VMUID: r.vmUID, Incarnation: vm.Annotations[control.ModeAnnotation][2:]})
	if err != nil {
		return true, err
	}
	options := owner.DefaultOptions()
	options.PodUID = r.global.envArgs.K8sPodUID
	elector, err := owner.New(r.global.kubeClient.CoordinationV1(), store, options)
	if err != nil {
		return true, err
	}
	r.dsgaState(ctx, DSGARunnerState{Phase: "electing", IssuerEpoch: 0, Instance: control.Instance{VMUID: string(r.vmUID), ComputeInstanceID: "", PGBootID: 0}})
	err = elector.Run(ctx, func(ownerCtx context.Context, session *owner.Session) error {
		credentials, err := credential.New(session, credential.Config{BaseURL: config.GatewayURL, CAFile: config.ManagementCAFile, TokenFile: config.ProjectedTokenFile, Timeout: time.Duration(config.RequestTimeoutSeconds) * time.Second})
		if err != nil {
			return err
		}
		defer credentials.CloseIdleConnections()
		identity, err := credentials.Identity(ownerCtx)
		if err != nil {
			return err
		}
		// 资源身份先于PG客户端恢复，Root floor不随SQL断连/PG boot变化清空。
		// 必须同一已确认Session，缺root来源/未知旧Guest时失败关闭，不走legacy。
		resources, err := session.NewResourceRecoveryWorker(ownerCtx, resourceguard.ClientConfig{Context: nil, BaseURL: config.GatewayURL, Identity: resourceguard.Identity{VMUID: identity.VMUID, ComputeInstanceID: identity.ComputeInstanceID, GuestBootID: "", CgroupDevice: 0, CgroupInode: 0, SystemCgroupDevice: 0, SystemCgroupInode: 0}, IssuerEpoch: 0, Timeout: time.Duration(config.ComputeTimeoutSeconds) * time.Second, Transport: credentials.ManagementTransport(), Token: credentials.Token, Pending: nil})
		if err != nil {
			return err
		}
		defer resources.CloseIdleConnections()
		// PG客户端尚未发现/建立时也保留同一Session及原Root SDK的显式恢复
		// 入口。首次约束漂移只读轮询，不发PG业务或自动修复/开放；已有
		// 完整原PG/GuestReady未知则交正式恢复入口处理原重放/取消，避免
		// Root未收到时无限等Observe。0 boot端点不与正式端点并存。
		if err := r.awaitDSGAResources(ownerCtx, config, resources); err != nil {
			return err
		}
		var qualifications []budget.QualificationSource
		if source, ok := config.PrepareQualifications[string(r.vmUID)]; ok {
			qualifications = append(qualifications, source)
		}
		worker, err := session.NewProductionPGWorker(ownerCtx, compute.Config{BaseURL: config.GatewayURL, Instance: control.Instance{VMUID: identity.VMUID, ComputeInstanceID: identity.ComputeInstanceID, PGBootID: 0}, IssuerEpoch: session.Actor().Epoch, Timeout: time.Duration(config.ComputeTimeoutSeconds) * time.Second, Transport: credentials.ManagementTransport(), Token: credentials.Token, Pending: nil, AdmissionPending: nil, AdmissionClose: nil}, resources, r.global.kubeClient.CoreV1(), qualifications...)
		if err != nil {
			return err
		}
		defer worker.CloseIdleConnections()
		instance, epoch := worker.Binding()
		r.dsgaState(ownerCtx, DSGARunnerState{Phase: "running", IssuerEpoch: epoch, Instance: instance})
		logger.Info("DSGA owner PG reconciliation started")
		var endpoint *owner.ControlEndpoint
		if config.ControlSocketPath != "" {
			endpoint = worker.NewControlEndpoint()
			unregister, err := r.global.dsgaControls.Register(endpoint)
			if err != nil {
				return err
			}
			defer unregister()
		}
		// 同一worker推进PG、最终预算结算和带原持有来源的恢复准入，先持久原门禁意图，
		// 丢回复后只读对账。预算生产者/Guest/VM尚未确认的步骤仍保持ExternalStep；
		// 不自动Begin、伪造外部回执或重发未知POST，也不另开legacy执行器绕过领域。
		return worker.Run(ownerCtx, owner.PGLoopOptions{PollInterval: time.Duration(config.PollIntervalSeconds) * time.Second, Control: endpoint, Wake: updated.Recv(), Replay: nil, AdmissionReplay: nil, ResourceReplay: nil, PrepareReplay: nil, InitialAdmissionReplay: nil, VMReplay: nil, AbortReplay: nil, UnchangedAdmissionRecovery: nil, CapacityAdmissionRecovery: nil, Observe: func(tick owner.Tick) {
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
	if config.ControlSocketPath != "" {
		unregister, err := r.global.dsgaControls.Register(endpoint)
		if err != nil {
			return err
		}
		defer unregister()
	}
	return endpoint.RunResourceRecovery(ctx, time.Duration(config.PollIntervalSeconds)*time.Second)
}
