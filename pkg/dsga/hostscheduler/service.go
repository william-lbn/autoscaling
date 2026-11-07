package hostscheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

const (
	hostLeaseDuration = 15 * time.Second
	hostRenewDeadline = 10 * time.Second
	hostRetryPeriod   = 2 * time.Second

	// HostOperationTimeout是原节点owner一次完整多RPC事务的有限预算。
	// 事件/Filter共用此界限，不能误用单个Kubernetes CRUD的1秒配置。
	HostOperationTimeout = 10 * time.Second
)

// InitialOwnerObservationBudget覆盖client-go首次观察旧Lease的完整窗口及一次
// 最大jitter重试，向上取整到秒。只供DSGA首次启动预算叠加，不改变后续选主语义。
func InitialOwnerObservationBudget() time.Duration {
	maximumRetry := hostRetryPeriod + time.Duration(float64(hostRetryPeriod)*leaderelection.JitterFactor)
	budget := hostLeaseDuration + maximumRetry
	return (budget + time.Second - 1) / time.Second * time.Second
}

type domain struct {
	config NodeConfig
	store  *hostguard.Store
	work   *domainWork
}

// Service创建后配置/客户端/holder不变。网络请求不持PluginState.mu；所有
// 副本均可增加容量持有，只有节点Lease holder可fence/发布。没有本地“可用
// 内存”缓存可绕过Node CAS，响应未知不得返回调度成功或在Unreserve中清账。
type Service struct {
	core     coreclient.CoreV1Interface
	coord    coordclient.CoordinationV1Interface
	election coordclient.CoordinationV1Interface
	vm       vmclient.Interface
	holder   string
	domains  map[string]domain
	started  atomic.Bool
	// 只缓存一条纯Model解析；所有Kubernetes读取和原授权/CAS仍每次新鲜执行。
	models journal.ModelInspector
}

// New保留既有嵌入调用合同；正式Scheduler使用NewWithElectionClient隔离续租。
func New(coreClient coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, vm vmclient.Interface, c Config, schedulerName string) (*Service, error) {
	return NewWithElectionClient(coreClient, coord, coord, vm, c, schedulerName)
}

// NewWithElectionClient固定工作与选主客户端。Store里的普通Lease校验仍走coord，
// 只有client-go选主/续租走election，避免大量Pod对账占满续租限流桶。
func NewWithElectionClient(coreClient coreclient.CoreV1Interface, coord, election coordclient.CoordinationV1Interface, vm vmclient.Interface, c Config, schedulerName string) (*Service, error) {
	if coreClient == nil || coord == nil || election == nil || vm == nil {
		return nil, fmt.Errorf("host scheduler requires fixed Kubernetes clients")
	}
	if err := c.Validate(schedulerName); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	s := &Service{core: coreClient, coord: coord, election: election, vm: vm, holder: "host-" + hex.EncodeToString(nonce[:]), domains: make(map[string]domain), started: atomic.Bool{}}
	for _, n := range slices.Clone(c.Nodes) {
		if n.KubernetesBudget != nil {
			policy := *n.KubernetesBudget
			n.KubernetesBudget = &policy
		}
		store, err := hostguard.NewStore(coreClient, coord, n.Binding, s.holder)
		if err != nil {
			return nil, err
		}
		s.domains[n.Binding.NodeName] = domain{config: n, store: store, work: newDomainWork()}
	}
	return s, nil
}

func (s *Service) HasNode(name string) bool {
	_, ok := s.domains[name]
	return ok
}

func checkProfile(snapshot *hostguard.Snapshot, n NodeConfig) error {
	if n.KubernetesBudget != nil {
		if err := n.KubernetesBudget.Inspect(snapshot.Node(), n.Profile, n.SystemBytes); err != nil {
			return err
		}
	}
	m := snapshot.State()
	if m.ProtocolVersion < 3 || m.Profile == nil || *m.Profile != n.Profile || m.OutsideBytes != n.SystemBytes || m.Binding != n.Binding {
		return fmt.Errorf("original complete Host domain differs from fixed startup configuration")
	}
	return nil
}

// CheckPlacement在Filter只读重核原Pod/VM、完整Node账本/profile和新鲜Lease。
// 不调用protectPod或任何CAS；不能以此结果替代最终Reserve。
func (s *Service) CheckPlacement(ctx context.Context, node string, pod *core.Pod, target control.Uint64) error {
	d, ok := s.domains[node]
	if !ok {
		return fmt.Errorf("node is not an explicitly configured Host domain")
	}
	_, p, vm, err := s.allocation(ctx, d.config, pod, target)
	if err != nil {
		return err
	}
	snapshot, err := d.store.Load(ctx)
	if err != nil {
		return err
	}
	if err := checkProfile(snapshot, d.config); err != nil {
		return err
	}
	key, err := s.placementKey(ctx, p, vm)
	if err != nil {
		return err
	}
	return d.store.CheckPlacement(ctx, snapshot, p, key)
}

// Key只来自再次读取且UID/RV未变的原VM Active。试算与最终Reserve共用，
// 普通Pod/关闭请求不能伪造Key；读回未知依旧拒绝，不清任何持有。
func (s *Service) placementKey(ctx context.Context, p hostguard.Placement, vm *vmv1.VirtualMachine) (*control.Key, error) {
	if !p.Managed || vm == nil {
		return nil, nil
	}
	latest, err := s.vm.NeonvmV1().VirtualMachines(p.Namespace).Get(ctx, p.VMName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if latest.UID != vm.UID || latest.ResourceVersion != vm.ResourceVersion {
		return nil, fmt.Errorf("managed placement original VM changed before Node CAS")
	}
	b := journal.Binding{Namespace: p.Namespace, Name: p.VMName, VMUID: latest.UID, Incarnation: p.Incarnation}
	current, err := s.models.Inspect(latest, b)
	if err != nil {
		return nil, err
	}
	m := current.State()
	if m.Active != nil && m.ServiceClosure == nil && !m.Active.AbortRequested && m.Active.Origin == m.Owner && m.Active.Request.Key.IssuerEpoch == m.Owner.Epoch {
		key := m.Active.Request.Key
		return &key, nil
	}
	return nil, nil
}

// Reserve是Framework.Reserve、事件恢复、旧VM批准写共同使用的唯一容量入口。
// 并发冲突/丢回复交回队列重试，重试重新GET完整原对象；不在这里盲重放写入。
func (s *Service) Reserve(ctx context.Context, node string, pod *core.Pod, target control.Uint64) (*core.Pod, hostguard.Placement, error) {
	var none hostguard.Placement
	d, ok := s.domains[node]
	if !ok {
		return nil, none, fmt.Errorf("node is not an explicitly configured Host domain")
	}
	// 已绑定普通Pod未知终态只保留/补全完整占账，不发新调度许可或释放。
	if target == 0 && closedAssignedPod(pod) {
		return s.observeRetainedOrdinary(ctx, d, pod)
	}
	canonical, p, vm, err := s.allocation(ctx, d.config, pod, target)
	if err != nil {
		return nil, none, err
	}
	// 未初始化/不匹配的分配域不得先给Pod加保护；否则首次启动失败会留下
	// 无账本可退休的孤立finalizer，包括Scheduler自己的Pod。
	snapshot, err := d.store.Load(ctx)
	if err != nil {
		return nil, none, err
	}
	if err := checkProfile(snapshot, d.config); err != nil {
		return nil, none, err
	}
	canonical, err = s.protectPod(ctx, canonical)
	if err != nil {
		return nil, none, err
	}
	key, err := s.placementKey(ctx, p, vm)
	if err != nil {
		return nil, none, err
	}
	var held hostguard.Placement
	if key != nil {
		_, held, err = d.store.HoldPlacementFor(ctx, snapshot, p, *key)
	} else {
		_, held, err = d.store.HoldPlacement(ctx, snapshot, p)
	}
	if err != nil {
		return nil, none, err
	}
	return canonical, held, nil
}

// Run在真实plugin入口启动，每节点独立选主。全局Scheduler leader更换不改变
// 节点账本；节点owner更换只抬floor，保留所有原Pod与未知在途持有。停止后不
// 主动释放Lease或容量，新owner必须等待原Lease并再次fence。默认配置nil无循环。
func (s *Service) Run(ctx context.Context, logger *zap.Logger) error {
	if logger == nil || !s.started.CompareAndSwap(false, true) {
		return fmt.Errorf("host scheduler loop requires logger and exactly one start")
	}
	for _, d := range s.domains {
		go s.elect(ctx, logger.With(zap.String("node", d.config.Binding.NodeName)), d)
	}
	return nil
}

func (s *Service) elect(ctx context.Context, logger *zap.Logger, d domain) {
	for ctx.Err() == nil {
		lock, err := hostguard.NewLeaseLock(s.election, d.config.Binding, s.holder)
		if err != nil {
			logger.Error("Host election configuration rejected", zap.Error(err))
			return
		}
		elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock: lock, LeaseDuration: hostLeaseDuration, RenewDeadline: hostRenewDeadline, RetryPeriod: hostRetryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(owned context.Context) { s.ownedLoop(owned, logger, d) },
				OnStoppedLeading: func() { logger.Info("Host node owner stopped; durable holds retained") },
			},
			ReleaseOnCancel: false, Name: "dsga-host-" + d.config.Binding.NodeName,
		})
		if err != nil {
			logger.Error("Host election setup rejected", zap.Error(err))
			return
		}
		elector.Run(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Service) ownedLoop(ctx context.Context, logger *zap.Logger, d domain) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		callCtx, cancel := context.WithTimeout(ctx, HostOperationTimeout)
		err := s.reconcile(callCtx, d)
		cancel()
		if err != nil && ctx.Err() == nil {
			logger.Warn("Host reconciliation retained original capacity", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) bootstrapAssigned(ctx context.Context, node string) ([]core.Pod, error) {
	// 仅显式首次引导要求一次取得完整有界快照；不能分批提交初始容量。
	// 运行期改用有界发现队列，历史已结清Pod不占活动账本256条上限。
	var pods []core.Pod
	cursor := ""
	for {
		list, err := s.assignedPage(ctx, node, cursor)
		if err != nil {
			return nil, err
		}
		pods = append(pods, list.Items...)
		if len(pods) > hostguard.MaxPlacements {
			return nil, fmt.Errorf("assigned Pod set exceeds bounded Host ledger")
		}
		if list.Continue == "" {
			return pods, nil
		}
		cursor = list.Continue
	}
}

func (s *Service) reconcile(ctx context.Context, d domain) error {
	if d.work == nil || !d.work.running.CompareAndSwap(false, true) {
		return fmt.Errorf("original Node reconciliation is already running or unconfigured")
	}
	defer d.work.running.Store(false)
	snapshot, err := d.store.Load(ctx)
	if errors.Is(err, hostguard.ErrUninitialized) {
		if d.config.InitializeAtResourceVersion == "" {
			return fmt.Errorf("explicit original Node bootstrap resourceVersion is required")
		}
		// 策略读取与随后初始化使用同一原 RV；期间 Node 变化由原 CAS 拒绝。
		if d.config.KubernetesBudget != nil {
			node, readErr := s.core.Nodes().Get(ctx, d.config.Binding.NodeName, metav1.GetOptions{})
			if readErr != nil {
				return readErr
			}
			if node.ResourceVersion != d.config.InitializeAtResourceVersion {
				return fmt.Errorf("original Kubernetes budget bootstrap Node version changed")
			}
			if err := d.config.KubernetesBudget.Inspect(node, d.config.Profile, d.config.SystemBytes); err != nil {
				return err
			}
		}
		pods, listErr := s.bootstrapAssigned(ctx, d.config.Binding.NodeName)
		if listErr != nil {
			return listErr
		}
		rows := make([]hostguard.Placement, 0, len(pods))
		for i := range pods {
			_, p, _, err := s.allocation(ctx, d.config, &pods[i], 0)
			if err != nil {
				return err
			}
			rows = append(rows, p)
		}
		snapshot, err = d.store.InitializeAllocations(ctx, d.config.InitializeAtResourceVersion, d.config.SystemBytes, d.config.Profile, rows)
	}
	if err != nil {
		return err
	}
	if err := checkProfile(snapshot, d.config); err != nil {
		return err
	}
	snapshot, err = d.store.Fence(ctx, snapshot)
	if err != nil {
		return err
	}
	return s.reconcileWork(ctx, d, snapshot)
}

func (s *Service) publish(ctx context.Context, d domain, pod *core.Pod, p hostguard.Placement) error {
	b := journal.Binding{Namespace: p.Namespace, Name: p.VMName, VMUID: types.UID(p.VMUID), Incarnation: p.Incarnation}
	store, err := journal.NewStore(s.vm.NeonvmV1().VirtualMachines(p.Namespace), b)
	if err != nil {
		return err
	}
	snapshot, err := store.Load(ctx)
	if err != nil {
		return err
	}
	m := snapshot.State()
	sealing := false
	if m.Active != nil && m.ServiceClosure != nil {
		// 关闭仍禁止普通Host发布；仅严格Completed Prepare早期分支
		// 可进入独立只封不放入口，不把未知或404补成Host成功。
		if _, err := m.PreparedHostClosure(); err != nil {
			return nil
		}
		sealing = true
	}
	if m.Active == nil && m.BudgetHeld == nil && m.CapacityHeld == nil && m.InitialHeld == nil || m.Active != nil && m.Active.AbortRequested && !sealing && !hostexec.CancelledShrinkNeedsAccounting(m) {
		return nil
	}
	// 等Prepare真实完成后再请求发布，不制造替代预算。Runner已发布的原
	// Lease UID作为固定来源；启动初期尚无Lease绑定时等待其第一次原fence。
	prepared := m.Active == nil
	if m.Active != nil {
		for _, step := range m.Active.Steps {
			if step.Action == journal.PrepareBudget && step.Phase == journal.Settled && step.Observation != nil && step.Observation.Result == journal.Completed {
				prepared = true
			}
		}
	}
	if !prepared {
		return nil
	}
	if m.Active == nil && m.InitialHeld == nil {
		closed, err := hostguard.SettlementFor(m)
		if err != nil {
			return err
		}
		if d.work.skipSettledScan(p, closed, time.Now()) {
			return nil
		}
	}
	_, _, view, err := vmguard.ReadPublished(ctx, s.core, b, nil)
	if err != nil {
		return err
	}
	if view.State == nil || view.State.LeaseUID == "" || view.Identity.RunnerPodUID != string(pod.UID) || view.Identity.NodeUID != string(d.config.Binding.NodeUID) {
		return fmt.Errorf("original Runner publication lacks fixed Pod/Node/VM Lease binding")
	}
	publisher, err := hostexec.New(s.core, s.coord, store, d.store, types.UID(view.State.LeaseUID), d.config.Profile.VMOverheadBytes)
	if err != nil {
		return err
	}
	if sealing {
		_, err = publisher.SealClosedHostRequest(ctx)
		return err
	}
	if m.Active == nil {
		// 只有成功原操作闭合后才进入明确结清；普通轮询不会因Lease/
		// Pod事件清峰值。Publisher独立重读所有事实和原Node快照。
		_, err = publisher.SettleCurrentWithCollection(ctx, d.work.collectionFor(string(pod.UID)))
		if err == hostexec.ErrCollectionPending { //nolint:errorlint // 只有裸调度信号可静默；Join内真实API错误必须继续报告。
			// 本轮只到达分页/写入上限；保留进度，下轮继续，不记
			// 完整扫描缓存。若同时含真实API错误则仍交原告警路径。
			return nil
		}
		if err == nil && p.Settled != nil {
			closed, closedErr := hostguard.SettlementFor(m)
			if closedErr == nil && *p.Settled == closed {
				d.work.rememberSettledScan(p, closed, time.Now())
			}
		}
		return err
	}
	if m.Active.AbortRequested {
		_, err = publisher.AccountCancelledShrink(ctx)
		return err
	}
	_, err = publisher.Reconcile(ctx)
	return err
}

// settleBeforeRetainedPlacement只由已选主Node循环使用。公开Reserve仍只做
// 单调容量记账，不能凭新实际数值或无Key重抬带水位的旧行。真正Settle仍
// 在Publisher中完整重读原VM/Runner/预算/Node/Lease并使用同UID/RV CAS。
func (s *Service) settleBeforeRetainedPlacement(ctx context.Context, d domain, pod *core.Pod, row hostguard.Placement) error {
	vm, err := s.vm.NeonvmV1().VirtualMachines(row.Namespace).Get(ctx, row.VMName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	binding := journal.Binding{Namespace: row.Namespace, Name: row.VMName, VMUID: types.UID(row.VMUID), Incarnation: row.Incarnation}
	current, err := s.models.Inspect(vm, binding)
	if err != nil {
		return err
	}
	m := current.State()
	if m.Active != nil || m.ServiceClosure != nil || m.BudgetHeld == nil && m.CapacityHeld == nil && m.InitialHeld == nil {
		return nil
	}
	return s.publish(ctx, d, pod, row)
}
