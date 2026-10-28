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
	"k8s.io/apimachinery/pkg/types"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"

	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

type domain struct {
	config NodeConfig
	store  *hostguard.Store
	work   *domainWork
}

// Service创建后配置/客户端/holder不变。网络请求不持PluginState.mu；所有
// 副本均可增加容量持有，只有节点Lease holder可fence/发布。没有本地“可用
// 内存”缓存可绕过Node CAS，响应未知不得返回调度成功或在Unreserve中清账。
type Service struct {
	core    coreclient.CoreV1Interface
	coord   coordclient.CoordinationV1Interface
	vm      vmclient.Interface
	holder  string
	domains map[string]domain
	started atomic.Bool
}

func New(coreClient coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, vm vmclient.Interface, c Config, schedulerName string) (*Service, error) {
	if coreClient == nil || coord == nil || vm == nil {
		return nil, fmt.Errorf("host scheduler requires fixed Kubernetes clients")
	}
	if err := c.Validate(schedulerName); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	s := &Service{core: coreClient, coord: coord, vm: vm, holder: "host-" + hex.EncodeToString(nonce[:]), domains: make(map[string]domain), started: atomic.Bool{}}
	for _, n := range slices.Clone(c.Nodes) {
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
	m := snapshot.State()
	if m.ProtocolVersion < 3 || m.Profile == nil || *m.Profile != n.Profile || m.OutsideBytes != n.SystemBytes || m.Binding != n.Binding {
		return fmt.Errorf("original complete Host domain differs from fixed startup configuration")
	}
	return nil
}

// Reserve是Framework.Reserve、事件恢复、旧VM批准写共同使用的唯一容量入口。
// 并发冲突/丢回复交回队列重试，重试重新GET完整原对象；不在这里盲重放写入。
func (s *Service) Reserve(ctx context.Context, node string, pod *core.Pod, target control.Uint64) (*core.Pod, hostguard.Placement, error) {
	var none hostguard.Placement
	d, ok := s.domains[node]
	if !ok {
		return nil, none, fmt.Errorf("node is not an explicitly configured Host domain")
	}
	canonical, p, _, err := s.allocation(ctx, d.config, pod, target)
	if err != nil {
		return nil, none, err
	}
	canonical, err = s.protectPod(ctx, canonical)
	if err != nil {
		return nil, none, err
	}
	snapshot, err := d.store.Load(ctx)
	if err != nil {
		return nil, none, err
	}
	if err := checkProfile(snapshot, d.config); err != nil {
		return nil, none, err
	}
	_, held, err := d.store.HoldPlacement(ctx, snapshot, p)
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
		lock, err := hostguard.NewLeaseLock(s.coord, d.config.Binding, s.holder)
		if err != nil {
			logger.Error("Host election configuration rejected", zap.Error(err))
			return
		}
		elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock: lock, LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second,
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
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
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
	if m.Active == nil && m.BudgetHeld == nil && m.CapacityHeld == nil && m.InitialHeld == nil || m.Active != nil && m.Active.AbortRequested {
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
	_, err = publisher.Reconcile(ctx)
	return err
}
