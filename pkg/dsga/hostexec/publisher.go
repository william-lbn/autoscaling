// Package hostexec由Host分配者发布真实Node持有对应的原Host来源。
// 它不接收调用者自报的Prepared/Runner/Node或额度，不写PG/QMP/VM spec，
// 也不把CM创建成功当分配完成。Scheduler全授予/恢复及分配域资格仍由启动层落实。
package hostexec

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

type Publisher struct {
	core     coreclient.CoreV1Interface
	coord    coordclient.CoordinationV1Interface
	vm       *journal.Store
	node     *hostguard.Store
	runner   *vmguard.Reader
	claims   *budgetclaims.Reader
	overhead control.Uint64
	leaseUID types.UID
}

// New的VM/Node客户端、原Lease UID和Host开销只能来自可信启动配置。
// overhead必须是目标平台已认证的上界；此生产者不根据RSS猜测，也不认证任意
// 数字。Node域必须已显式初始化并覆盖全部其他成本，不能在失败时自动创建。
func New(coreClient coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, vm *journal.Store, node *hostguard.Store, vmLeaseUID types.UID, overhead control.Uint64) (*Publisher, error) {
	if coreClient == nil || coord == nil || vm == nil || node == nil || overhead == 0 {
		return nil, fmt.Errorf("host publisher requires fixed clients/original domains/qualified overhead")
	}
	runner, err := vmguard.NewReader(coreClient, vm.Binding(), string(vmLeaseUID))
	if err != nil {
		return nil, err
	}
	claims, err := budgetclaims.New(coreClient, vm.Binding())
	if err != nil {
		return nil, err
	}
	return &Publisher{core: coreClient, coord: coord, vm: vm, node: node, runner: runner, claims: claims, overhead: overhead, leaseUID: vmLeaseUID}, nil
}

func (p *Publisher) currentOwner(ctx context.Context, s *journal.Snapshot) error {
	b := p.vm.Binding()
	l, err := p.coord.Leases(b.Namespace).Get(ctx, journal.LeaseName(b), metav1.GetOptions{})
	if err != nil {
		return err
	}
	if journal.ValidateLease(l, b, journal.LeaseName(b), p.leaseUID) != nil || *l.Spec.HolderIdentity != s.State().Owner.HolderID || l.Spec.RenewTime.After(time.Now().Add(5*time.Second)) || !time.Now().Before(l.Spec.RenewTime.Add(time.Duration(*l.Spec.LeaseDurationSeconds)*time.Second)) {
		return fmt.Errorf("original VM owner Lease changed or expired before Host write")
	}
	return nil
}

// prepared只取真实原VM同一次GET中的完整Completed前驱，不使用Pod传播注解。
// 只要原Prepare已完成即可为Grow/Shrink持有Host；Shrink无独立ReserveHost步骤，
// 也必须在最终VM动作之前发布。取消/新owner不从旧意图生成新的容量写入。
func (p *Publisher) prepared(s *journal.Snapshot) (budget.Prepared, error) {
	var out budget.Prepared
	m := s.State()
	if !s.Dispatchable() || m.Active == nil || m.Active.AbortRequested || m.ResourceIdentity == nil || m.PGInstance == nil || *m.PGInstance != m.Active.Request.Key.Instance || m.Owner != m.Active.Origin || m.Active.Request.Key.IssuerEpoch != m.Owner.Epoch {
		return out, fmt.Errorf("host publisher lacks current original dispatchable operation")
	}
	for _, step := range m.Active.Steps {
		if step.Action != journal.PrepareBudget {
			continue
		}
		if step.Phase != journal.Settled || step.Observation == nil || step.Observation.Result != journal.Completed || step.TargetID != m.PGInstance.ComputeInstanceID || json.Unmarshal(step.Observation.Data, &out) != nil || out.ValidateFor(m.Active.Request, *m.ResourceIdentity) != nil || out.OutsideClaim == nil {
			return out, fmt.Errorf("host publisher lacks complete original prepared source")
		}
		return out, nil
	}
	return out, fmt.Errorf("host publisher has no original Prepare step")
}

func (p *Publisher) unchanged(ctx context.Context, original *journal.Snapshot) error {
	s, err := p.vm.Load(ctx)
	if err != nil {
		return err
	}
	if s.ResourceVersion() != original.ResourceVersion() || !s.Dispatchable() {
		return fmt.Errorf("original VM intent changed across Host verification")
	}
	return p.currentOwner(ctx, s)
}

// before独立读取原Runner Birth/State的实际QMP观察，并核真实原Pod/Node。
// 它不把“无pending”自报当完成；Runner源对象本身须受固定UID与保护规则约束。
// 最后仍有网络窗口，所以这里只生成容量承诺，不生成QMP或VM缩容许可。
func (p *Publisher) before(ctx context.Context, s *journal.Snapshot, prepared budget.Prepared) (hostguard.Plan, error) {
	var none hostguard.Plan
	birth, _, publication, err := p.runner.Inspect(ctx, nil, s.State().Owner)
	if err != nil {
		return none, err
	}
	b, vm := p.node.Binding(), s.VirtualMachine()
	id := birth.Identity
	target := prepared.Request.TargetVMMemoryBytes
	if id.VMUID != string(p.vm.Binding().VMUID) || id.Incarnation != p.vm.Binding().Incarnation || id.NodeUID != string(b.NodeUID) || publication.State.Pending != nil || publication.Actual.RequestedBytes != prepared.VMBeforeMemoryBytes || publication.Actual.ActualBytes != prepared.VMBeforeMemoryBytes || target < id.MinimumBytes || target > id.MaximumBytes || (target-id.MinimumBytes)%id.BlockBytes != 0 || vm.Status.PodName != prepared.RunnerPodName || vm.Status.RestartCount != prepared.RunnerRestartCount || vm.Status.Node != b.NodeName {
		return none, fmt.Errorf("original Runner/Node/actual memory differs from prepared Host plan")
	}
	pod, err := p.core.Pods(p.vm.Binding().Namespace).Get(ctx, prepared.RunnerPodName, metav1.GetOptions{})
	if err != nil {
		return none, err
	}
	if pod == nil {
		return none, fmt.Errorf("original Runner Pod unavailable")
	}
	owner := metav1.GetControllerOf(pod)
	if pod.UID != types.UID(id.RunnerPodUID) || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Spec.NodeName != b.NodeName || pod.Status.Phase != core.PodRunning || pod.Status.PodIP != vm.Status.PodIP || owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != vm.Name || owner.UID != vm.UID {
		return none, fmt.Errorf("original live Runner Pod changed before Host reservation")
	}
	return hostguard.Plan{Namespace: vm.Namespace, VMName: vm.Name, Incarnation: p.vm.Binding().Incarnation, RunnerPodUID: id.RunnerPodUID, Request: prepared.Request, BeforeBytes: prepared.VMBeforeMemoryBytes, HostOverheadBytes: p.overhead}, nil
}

func validClaim(cm *core.ConfigMap, binding journal.Binding, expected budget.HostReservation) bool {
	if cm == nil || cm.Namespace != binding.Namespace || cm.UID == "" || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || cm.Immutable == nil || !*cm.Immutable || !slices.Contains(cm.Finalizers, budgetclaims.ProtectionFinalizer) || cm.Labels[budgetclaims.ClaimLabel] != budgetclaims.HostKind || len(cm.Data) != 1 || len(cm.BinaryData) != 0 || len(cm.Data[budgetclaims.ClaimKey]) > journal.MaxObservationBytes || len(cm.OwnerReferences) != 1 {
		return false
	}
	owner := metav1.GetControllerOf(cm)
	if owner == nil || owner.APIVersion != "vm.neon.tech/v1" || owner.Kind != "VirtualMachine" || owner.Name != binding.Name || owner.UID != binding.VMUID || owner.BlockOwnerDeletion == nil || !*owner.BlockOwnerDeletion {
		return false
	}
	var got budget.HostReservation
	return json.Unmarshal([]byte(cm.Data[budgetclaims.ClaimKey]), &got) == nil && got == expected
}

// Reconcile先Node容量CAS、再原不可变CM、最后Node固定CM UID。最终发布CAS之前
// 所有消费者均拒绝，因此只允许恢复尚未发布的同一完整CM；发布后缺原UID绝不
// 创建替代。任一步未知都保留容量，下次从实际原Node/VM重读，绝不自动释放。
func (p *Publisher) Reconcile(ctx context.Context) (budget.ObjectRef, error) {
	var none budget.ObjectRef
	s, err := p.vm.Load(ctx)
	if err != nil {
		return none, err
	}
	prepared, err := p.prepared(s)
	if err != nil {
		return none, err
	}
	if err := p.claims.CheckOutside(ctx, prepared); err != nil {
		return none, err
	}
	n, err := p.node.Load(ctx)
	if err != nil {
		return none, err
	}
	// 已发布来源先只读恢复；VM可能已完成动作，不能为了查询原持有要求
	// 实际值仍等于before，也不能因原请求期限/Host Lease过期清掉容量。
	for _, h := range n.State().Holds {
		if h.Plan.Request == prepared.Request && h.Plan.Namespace == p.vm.Binding().Namespace && h.Plan.VMName == p.vm.Binding().Name && h.Plan.Incarnation == p.vm.Binding().Incarnation && h.Plan.BeforeBytes == prepared.VMBeforeMemoryBytes && h.Plan.HostOverheadBytes == p.overhead && h.Claim != nil {
			if _, err := p.claims.Read(ctx, s, prepared, *h.Claim); err != nil {
				return none, err
			}
			if err := p.unchanged(ctx, s); err != nil {
				return none, err
			}
			return *h.Claim, nil
		}
	}
	if prepared.Request.DeadlineUnixMillis <= control.Uint64(time.Now().UnixMilli()) {
		return none, fmt.Errorf("original Host request deadline expired before new publication")
	}
	plan, err := p.before(ctx, s, prepared)
	if err != nil {
		return none, err
	}
	if err := p.unchanged(ctx, s); err != nil {
		return none, err
	}
	n, err = p.node.Fence(ctx, n)
	if err != nil {
		return none, err
	}
	n, held, err := p.node.Reserve(ctx, n, plan)
	if err != nil {
		return none, err
	}
	bytes, err := plan.Bytes()
	if err != nil {
		return none, err
	}
	h := budget.HostReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: prepared.Request, Identity: prepared.Identity, Incarnation: plan.Incarnation, SourceID: held.SourceID, RunnerPodName: prepared.RunnerPodName, RunnerPodUID: plan.RunnerPodUID, NodeName: n.State().Binding.NodeName, NodeUID: string(n.State().Binding.NodeUID), ReservedMemoryBytes: bytes, HostOverheadBytes: p.overhead, HoldUntilExplicitSettlement: true}
	name, err := budgetclaims.HostClaimName(p.vm.Binding(), prepared.Request)
	if err != nil {
		return none, err
	}
	cm, err := p.core.ConfigMaps(plan.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// 尚未发布的CREATE可使用相同完整内容恢复；这里从未发任何VM动作。
		// 在CM写入之前再核原VM/Runner，失联保留容量但不创建自报来源。
		if _, err := p.before(ctx, s, prepared); err != nil {
			return none, err
		}
		if err := p.unchanged(ctx, s); err != nil {
			return none, err
		}
		raw, marshalErr := json.Marshal(h)
		if marshalErr != nil {
			return none, marshalErr
		}
		controller, immutable := true, true
		expected := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: plan.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: plan.VMName, UID: p.vm.Binding().VMUID, Controller: &controller, BlockOwnerDeletion: &controller}}, Finalizers: []string{budgetclaims.ProtectionFinalizer}, Labels: map[string]string{budgetclaims.ClaimLabel: budgetclaims.HostKind}}, Immutable: &immutable, Data: map[string]string{budgetclaims.ClaimKey: string(raw)}}
		// 只用实际GET结果确认CREATE；无论回复成功/丢失都不相信返回body。
		_, _ = p.core.ConfigMaps(plan.Namespace).Create(ctx, expected, metav1.CreateOptions{})
		cm, err = p.core.ConfigMaps(plan.Namespace).Get(ctx, name, metav1.GetOptions{})
	}
	if err != nil {
		return none, err
	}
	if cm == nil || cm.Name != name || !validClaim(cm, p.vm.Binding(), h) {
		return none, fmt.Errorf("original Host immutable source differs from held complete plan")
	}
	ref := budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)}
	if err := p.unchanged(ctx, s); err != nil {
		return none, err
	}
	if _, err := p.node.BindClaim(ctx, n, plan, ref); err != nil {
		return none, err
	}
	// 两端GET在最终CAS之后再核；部分来源丢失不返回票据，也不回滚已经持有
	// 的容量。实际VM派发仍由Agent/最终Runner各自读取原Node与CM后执行。
	if _, err := p.claims.Read(ctx, s, prepared, ref); err != nil {
		return none, err
	}
	if err := p.unchanged(ctx, s); err != nil {
		return none, err
	}
	return ref, nil
}
