package agent

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/credential"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
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
	if !control.Managed(vm.Annotations) {
		return false, nil
	}
	r.status.update(r.global, func(stat podStatus) podStatus {
		stat.dsga = true
		now := time.Now()
		stat.dsgaLastSuccessfulCheck = &now
		return stat
	})
	r.dsgaState(ctx, DSGARunnerState{Phase: "validating", IssuerEpoch: 0, Instance: control.Instance{VMUID: string(r.vmUID), ComputeInstanceID: "", PGBootID: 0}})
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
		worker, err := session.NewPGWorker(ownerCtx, compute.Config{BaseURL: config.GatewayURL, Instance: control.Instance{VMUID: identity.VMUID, ComputeInstanceID: identity.ComputeInstanceID, PGBootID: 0}, IssuerEpoch: session.Actor().Epoch, Timeout: time.Duration(config.ComputeTimeoutSeconds) * time.Second, Transport: credentials.ManagementTransport(), Token: credentials.Token, Pending: nil})
		if err != nil {
			return err
		}
		defer worker.CloseIdleConnections()
		instance, epoch := worker.Binding()
		r.dsgaState(ownerCtx, DSGARunnerState{Phase: "running", IssuerEpoch: epoch, Instance: instance})
		logger.Info("DSGA owner PG reconciliation started")
		// 仅执行原journal中已获外层领域确认的PG步骤；其他领域保持ExternalStep，
		// 不伪造预算/Guest/VM回执，不自动Begin或隐式重放未知POST。后续完整领域
		// 规划器接入同一日志，不能重开另一套legacy执行器填补缺失领域。
		return worker.Run(ownerCtx, owner.PGLoopOptions{PollInterval: time.Duration(config.PollIntervalSeconds) * time.Second, Wake: updated.Recv(), Replay: nil, Observe: func(tick owner.Tick) {
			if tick.Err == nil {
				r.status.update(r.global, func(stat podStatus) podStatus { now := time.Now(); stat.dsgaLastSuccessfulCheck = &now; return stat })
			}
		}})
	})
	return true, err
}
