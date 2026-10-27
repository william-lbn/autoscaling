package hostguard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/util/patch"
)

const (
	LedgerAnnotation    = "internal.autoscaling.neon.tech/dsga-host-ledger-v1"
	OriginAnnotation    = "internal.autoscaling.neon.tech/dsga-host-origin-v1"
	LeaseAnnotation     = "internal.autoscaling.neon.tech/dsga-host-lease-v1"
	ProtectionFinalizer = "autoscaling.neon.tech/dsga-host-ledger-protection"
)

var ErrUninitialized = errors.New("original Host allocator is not initialized")

var (
	ErrUnpublished = errors.New("original Host capacity is held but claim UID is not yet published")
	ErrSettled     = errors.New("original Host peak is settled; only current budget reads are permitted")
)

type Store struct {
	core    coreclient.CoreV1Interface
	coord   coordclient.CoordinationV1Interface
	binding Binding
	holder  string
}

// Store只接受启动流程固定的可信客户端、Node/Lease UID和本进程holder。
// 不能按远程请求/同名对象临时换绑；Lease选主与Scheduler授予接线由上层完成。
func NewStore(core coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, b Binding, holder string) (*Store, error) {
	if core == nil || coord == nil || b.Validate() != nil || control.ValidateID(holder, "Host allocator holder") != nil {
		return nil, fmt.Errorf("host allocator requires original Node/Lease/client/holder")
	}
	return &Store{core: core, coord: coord, binding: b, holder: holder}, nil
}

func LeaseMarker(b Binding) string { return "1:" + string(b.NodeUID) + ":" + b.NodeName }

type Snapshot struct {
	node  *core.Node
	model Model
}

func (s *Snapshot) State() Model            { return s.model.Clone() }
func (s *Snapshot) ResourceVersion() string { return s.node.ResourceVersion }

// Node返回独立副本，供直接来源校验；不能把它当作后续写入的许可或最新读取。
func (s *Snapshot) Node() *core.Node { return s.node.DeepCopy() }

// Hold只读查询完整原持有，接管/Lease过期不让它消失。返回值不是Host源对象或
// VM执行票据；Allocator/Scheduler生产者还须独立核真实当前容量和原VM来源。
func (s *Snapshot) Hold(p Plan) (Hold, error) {
	for _, h := range s.model.Holds {
		if h.Plan.Request.Key == p.Request.Key {
			if h.Plan != p {
				return Hold{}, fmt.Errorf("original Host plan changed")
			}
			if h.Claim != nil {
				ref := *h.Claim
				h.Claim = &ref
			}
			return h, nil
		}
	}
	return Hold{}, fmt.Errorf("original Host hold not found")
}

func (s *Store) Binding() Binding { return s.binding }

// BindClaim是Host来源第一次生效的线性化点。调用者须先真实GET校验原CM的
// UID/完整body/保护；本CAS把该UID与完整容量计划一起固定。此前即使CM存在，
// 消费者也必须因Node尚无匹配Claim而拒绝。因此CREATE丢回复可恢复尚未发布
// 的原CM；一旦CAS固定UID，来源丢失绝不能用同名新对象替代。
func (s *Store) BindClaim(ctx context.Context, snapshot *Snapshot, p Plan, ref budget.ObjectRef) (*Snapshot, error) {
	if snapshot == nil || snapshot.model.Binding != s.binding || ref.Validate() != nil {
		return nil, fmt.Errorf("host publication requires original Node/plan/claim UID")
	}
	actor, err := s.authorize(ctx, snapshot.node)
	if err != nil || actor != snapshot.model.LeaseFloor {
		return nil, fmt.Errorf("host publication owner is no longer original fenced holder")
	}
	h, err := snapshot.Hold(p)
	if err != nil {
		return nil, err
	}
	if h.Claim != nil {
		if *h.Claim != ref {
			return nil, fmt.Errorf("original published Host claim UID cannot be replaced")
		}
		return snapshot, nil
	}
	if snapshot.model.Sequence == control.Uint64(math.MaxUint64) {
		return nil, fmt.Errorf("host ledger publication sequence exhausted")
	}
	m := snapshot.model.Clone()
	for i := range m.Holds {
		if m.Holds[i].Plan == p {
			m.Holds[i].Claim = &ref
		}
	}
	if m.ProtocolVersion < 2 {
		m.ProtocolVersion = 2
	}
	m.Sequence++
	return s.write(ctx, snapshot.node, m, false)
}

// InspectClaim只检查本次真实Node读取中的持有，不认证调用者传入的对象。
// Reader必须用自己的固定可信客户端GET原Node/CM。模型v1仅保留旧账本读取，
// 无Claim的持有和CM自报SourceID均不构成发布。Lease过期不自动取消原持有。
func InspectClaim(n *core.Node, p Plan, source string, ref budget.ObjectRef) error {
	return inspectClaim(n, p, source, ref, nil)
}

// InspectRetainedClaim只允许原VM当前永久预算继续消费已结清的目标持有，绝不
// 返回执行授权。调用者必须从自己的原VM GET派生expected，并重核来源；旧
// 目标、另一原预算/CM、被淘汰历史仍拒绝，不能把只读接口交给Runner派发路径。
func InspectRetainedClaim(n *core.Node, p Plan, source string, ref budget.ObjectRef, expected Settlement) error {
	if expected.Validate() != nil || expected.Request != p.Request || expected.HostClaim != ref {
		return fmt.Errorf("retained Host reader changed original current budget")
	}
	return inspectClaim(n, p, source, ref, &expected)
}

func inspectClaim(n *core.Node, p Plan, source string, ref budget.ObjectRef, expected *Settlement) error {
	if n == nil || ref.Validate() != nil {
		return fmt.Errorf("original Host Node/claim unavailable")
	}
	var m Model
	raw := n.Annotations[LedgerAnnotation]
	if len(raw) > MaxRawBytes || json.Unmarshal([]byte(raw), &m) != nil {
		return fmt.Errorf("original Host published ledger unavailable")
	}
	snapshot, err := inspect(n, m.Binding)
	if err != nil || n.DeletionTimestamp != nil {
		return fmt.Errorf("original Host ledger source is not live/protected")
	}
	h, err := snapshot.Hold(p)
	if err != nil || h.SourceID != source {
		return fmt.Errorf("original Host claim is not the complete published Node hold")
	}
	used, usedErr := m.UsedBytes()
	total, capErr := capacity(n)
	if usedErr != nil || capErr != nil || used > total {
		return fmt.Errorf("original Host held domain exceeds current Node capacity")
	}
	if h.Claim == nil {
		return ErrUnpublished
	}
	if m.ProtocolVersion < 2 || *h.Claim != ref {
		return fmt.Errorf("original Host claim UID differs from published Node hold")
	}
	if closed, settled := m.settledPlacement(p); settled {
		if expected == nil {
			return ErrSettled
		}
		if *expected != *closed {
			return fmt.Errorf("retained Host budget differs from current settlement")
		}
	}
	return nil
}

func (s *Store) node(ctx context.Context) (*core.Node, error) {
	n, err := s.core.Nodes().Get(ctx, s.binding.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if n == nil || n.Name != s.binding.NodeName || n.UID != s.binding.NodeUID || n.ResourceVersion == "" {
		return nil, fmt.Errorf("original Host Node UID/name changed")
	}
	return n, nil
}

func inspect(n *core.Node, binding Binding) (*Snapshot, error) {
	if n == nil || n.Name != binding.NodeName || n.UID != binding.NodeUID || n.ResourceVersion == "" {
		return nil, fmt.Errorf("original Host Node response identity changed")
	}
	raw, hasLedger := n.Annotations[LedgerAnnotation]
	origin, hasOrigin := n.Annotations[OriginAnnotation]
	protected := slices.Contains(n.Finalizers, ProtectionFinalizer)
	if !hasLedger && !hasOrigin && !protected {
		return nil, ErrUninitialized
	}
	if len(raw) > MaxRawBytes || !protected {
		return nil, fmt.Errorf("original Host ledger missing, malformed or unprotected; reset forbidden")
	}
	var m Model
	if json.Unmarshal([]byte(raw), &m) != nil || m.Binding != binding || origin != "1:"+m.Origin {
		return nil, fmt.Errorf("original Host ledger identity/content changed; reset forbidden")
	}
	return &Snapshot{node: n.DeepCopy(), model: m}, nil
}

func (s *Store) Load(ctx context.Context) (*Snapshot, error) {
	n, err := s.node(ctx)
	if err != nil {
		return nil, err
	}
	return inspect(n, s.binding)
}

// Lease只限制新写资格；不因过期清原floor/持有。原UID与Node ownerRef固定，
// transitions+1给持久floor；同epoch换holder和同名新Lease不自动收养。
func (s *Store) actor(ctx context.Context) (journal.Owner, error) {
	return s.leaseActor(ctx, true)
}

// 多写者只能做单调增加的Pod容量记账，仍须核原Lease UID、当前新鲜holder及
// Node CAS。只有requireSelf=true的节点owner能发布Host源/抬floor/结算，
// 不能因为共享容量写入角色而把另一副本的Lease身份当作自己的执行权。
func (s *Store) leaseActor(ctx context.Context, requireSelf bool) (journal.Owner, error) {
	var none journal.Owner
	l, err := s.coord.Leases(s.binding.Namespace).Get(ctx, s.binding.LeaseName, metav1.GetOptions{})
	if err != nil {
		return none, err
	}
	if l == nil || l.UID != s.binding.LeaseUID || l.Name != s.binding.LeaseName || l.Namespace != s.binding.Namespace || l.ResourceVersion == "" || l.DeletionTimestamp != nil || l.Annotations[LeaseAnnotation] != LeaseMarker(s.binding) || len(l.OwnerReferences) != 1 {
		return none, fmt.Errorf("original Host Lease identity/binding changed")
	}
	o := l.OwnerReferences[0]
	v := l.Spec
	if o.APIVersion != "v1" || o.Kind != "Node" || o.Name != s.binding.NodeName || o.UID != s.binding.NodeUID || o.Controller == nil || !*o.Controller || v.HolderIdentity == nil || control.ValidateID(*v.HolderIdentity, "Host holder") != nil || requireSelf && *v.HolderIdentity != s.holder || v.LeaseTransitions == nil || *v.LeaseTransitions < 0 || v.LeaseDurationSeconds == nil || *v.LeaseDurationSeconds < 1 || *v.LeaseDurationSeconds > 86400 || v.RenewTime == nil || v.AcquireTime == nil || v.Strategy != nil || v.PreferredHolder != nil {
		return none, fmt.Errorf("current Host allocator is not original Lease holder")
	}
	now := time.Now()
	if v.RenewTime.After(now.Add(5*time.Second)) || !now.Before(v.RenewTime.Add(time.Duration(*v.LeaseDurationSeconds)*time.Second)) {
		return none, fmt.Errorf("host allocator Lease expired or timestamp is in future")
	}
	return journal.Owner{HolderID: *v.HolderIdentity, Epoch: control.Uint64(uint64(*v.LeaseTransitions) + 1)}, nil
}

func capacity(n *core.Node) (control.Uint64, error) {
	v, exact := n.Status.Allocatable.Memory().AsInt64()
	if !exact || v <= 0 {
		return 0, fmt.Errorf("original Node has no exact positive allocatable memory")
	}
	return control.Uint64(v), nil
}

// authorize采用Lease→当前Node UID/RV→Lease读取；不在网络间持本地锁。
// 真正线性化点是最后Node UID/RV CAS，不能将快照或时间戳当跨对象写许可。
func (s *Store) authorize(ctx context.Context, original *core.Node) (journal.Owner, error) {
	first, err := s.actor(ctx)
	if err != nil {
		return first, err
	}
	n, err := s.node(ctx)
	if err != nil {
		return first, err
	}
	last, err := s.actor(ctx)
	if err != nil {
		return last, err
	}
	if first != last || original == nil || n.UID != original.UID || n.ResourceVersion != original.ResourceVersion || n.DeletionTimestamp != nil {
		return last, fmt.Errorf("host owner/Node changed across original verification")
	}
	return last, nil
}

type UnconfirmedWrite struct{ Err error }

func (e *UnconfirmedWrite) Error() string {
	return "Host ledger write is unconfirmed: " + e.Err.Error()
}
func (e *UnconfirmedWrite) Unwrap() error { return e.Err }

func (s *Store) write(ctx context.Context, n *core.Node, m Model, initialize bool) (*Snapshot, error) {
	if m.Validate() != nil || m.Binding != s.binding {
		return nil, fmt.Errorf("invalid original Host ledger update")
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	annotations := maps.Clone(n.Annotations)
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[LedgerAnnotation], annotations[OriginAnnotation] = string(raw), "1:"+m.Origin
	annotationBytes := 0
	for k, v := range annotations {
		annotationBytes += len(k) + len(v)
	}
	if len(raw) > MaxRawBytes || annotationBytes > 256*1024 {
		return nil, fmt.Errorf("host ledger exceeds bounded Node annotation capacity")
	}
	ops := []patch.Operation{
		{Op: patch.OpTest, Path: "/metadata/uid", Value: s.binding.NodeUID},
		{Op: patch.OpTest, Path: "/metadata/resourceVersion", Value: n.ResourceVersion},
	}
	if initialize {
		finalizers := append(slices.Clone(n.Finalizers), ProtectionFinalizer)
		ops = append(ops, patch.Operation{Op: patch.OpAdd, Path: "/metadata/annotations", Value: annotations}, patch.Operation{Op: patch.OpAdd, Path: "/metadata/finalizers", Value: finalizers})
	} else {
		ops = append(ops, patch.Operation{Op: patch.OpTest, Path: "/metadata/annotations/" + patch.PathEscape(OriginAnnotation), Value: "1:" + m.Origin}, patch.Operation{Op: patch.OpReplace, Path: "/metadata/annotations/" + patch.PathEscape(LedgerAnnotation), Value: string(raw)})
	}
	payload, err := json.Marshal(ops)
	if err != nil {
		return nil, err
	}
	result, err := s.core.Nodes().Patch(ctx, n.Name, types.JSONPatchType, payload, metav1.PatchOptions{})
	if err != nil {
		return nil, &UnconfirmedWrite{Err: err}
	}
	got, err := inspect(result, s.binding)
	if err != nil {
		return nil, &UnconfirmedWrite{Err: err}
	}
	if result.Annotations[LedgerAnnotation] != string(raw) {
		return nil, &UnconfirmedWrite{Err: fmt.Errorf("host ledger response differs from complete original intent")}
	}
	return got, nil
}

// Initialize是明确的分配域引导，不由Load/Reserve失败调用。域外保留必须由
// 可信Scheduler/平台另行确定；本方法只原子冻结给定值，不给它认证资格。
func (s *Store) Initialize(ctx context.Context, expectedVersion string, outside control.Uint64) (*Snapshot, error) {
	return s.initialize(ctx, expectedVersion, outside, nil, nil)
}

// InitializeAllocations只供显式引导：expectedVersion来自所有者固定的原Node
// 快照，不在重试中换成当前RV。域内全部已运行Pod必须先按原API来源恢复入参，
// 同一CAS写完整v3容量域。旧v1/v2的Outside含义不同，不做隐式在线改义升级。
func (s *Store) InitializeAllocations(ctx context.Context, expectedVersion string, outside control.Uint64, profile Profile, placements []Placement) (*Snapshot, error) {
	if profile.Validate() != nil || placements == nil || outside == 0 {
		return nil, fmt.Errorf("host allocation bootstrap requires complete original profile/Pod snapshot")
	}
	rows := slices.Clone(placements)
	return s.initialize(ctx, expectedVersion, outside, &profile, &rows)
}

func (s *Store) initialize(ctx context.Context, expectedVersion string, outside control.Uint64, profile *Profile, placements *[]Placement) (*Snapshot, error) {
	n, err := s.node(ctx)
	if err != nil {
		return nil, err
	}
	_, inspectionErr := inspect(n, s.binding)
	if !errors.Is(inspectionErr, ErrUninitialized) || n.ResourceVersion != expectedVersion {
		return nil, fmt.Errorf("host allocator initialization requires original uninitialized Node snapshot")
	}
	actor, err := s.authorize(ctx, n)
	if err != nil {
		return nil, err
	}
	total, err := capacity(n)
	if err != nil || outside >= total {
		return nil, fmt.Errorf("host outside allocation leaves no Node capacity")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	m := Model{ProtocolVersion: 1, Binding: s.binding, Origin: hex.EncodeToString(nonce[:]), LeaseFloor: actor, Sequence: 1, OutsideBytes: outside, Holds: []Hold{}, Profile: nil, Placements: nil}
	if profile != nil {
		m.ProtocolVersion, m.Profile, m.Placements = 3, profile, placements
	}
	used, err := m.UsedBytes()
	if m.Validate() != nil || err != nil || used > total {
		return nil, fmt.Errorf("complete initial Host allocation exceeds original Node capacity or is invalid")
	}
	return s.write(ctx, n, m, true)
}

// HoldPlacement是原Node容量的多写者CAS。全局Scheduler leader与Host源的
// 节点lease holder可以是不同副本；只有“增加/保留容量”可由任一可信Scheduler
// 写者执行，发布和释放仍要求原holder。网络间无本地锁，任何未知保持原记账。
func (s *Store) HoldPlacement(ctx context.Context, snapshot *Snapshot, p Placement) (*Snapshot, Placement, error) {
	var none Placement
	if snapshot == nil || snapshot.model.Binding != s.binding || snapshot.model.ProtocolVersion < 3 {
		return nil, none, fmt.Errorf("pod allocation requires original initialized v3 Node snapshot")
	}
	if snapshot.model.ProtocolVersion >= 5 {
		// v5允许已停止Pod的行收敛；所有容量写者必须重读不可复用的
		// 原Pod UID/终态，不能把旧队列项在释放后重新加回。旧v4读者
		// 不接受v5账本，不能依赖其尚未实现的关闭语义。
		pod, err := s.core.Pods(p.Namespace).Get(ctx, p.PodName, metav1.GetOptions{})
		if err != nil {
			return nil, none, err
		}
		if string(pod.UID) != p.PodUID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed || pod.Spec.NodeName != "" && pod.Spec.NodeName != s.binding.NodeName || !slices.Contains(pod.Finalizers, PodProtectionFinalizer) {
			return nil, none, fmt.Errorf("original Pod is closed, replaced or lacks allocation protection")
		}
	}
	first, err := s.leaseActor(ctx, false)
	if err != nil {
		return nil, none, err
	}
	n, err := s.node(ctx)
	if err != nil {
		return nil, none, err
	}
	last, err := s.leaseActor(ctx, false)
	if err != nil || first != last || n.ResourceVersion != snapshot.ResourceVersion() || n.DeletionTimestamp != nil || last.Epoch < snapshot.model.LeaseFloor.Epoch || last.Epoch == snapshot.model.LeaseFloor.Epoch && last != snapshot.model.LeaseFloor {
		return nil, none, fmt.Errorf("original allocation Node/Lease changed or floor is stale")
	}
	total, err := capacity(n)
	if err != nil {
		return nil, none, err
	}
	m, err := snapshot.model.reservePlacement(p, total)
	if err != nil {
		return nil, none, err
	}
	if m.Sequence != snapshot.model.Sequence {
		snapshot, err = s.write(ctx, n, m, false)
		if err != nil {
			return nil, none, err
		}
	}
	held, err := snapshot.model.Placement(p.PodUID)
	return snapshot, held, err
}

// Settle是明确的节点owner结清CAS，不由超时/删除事件/Lease过期触发。生产者
// 必须在本Node快照之后验证原VM无未知/Active及实际Runner和当前预算，再使用
// 同一RV提交；并发新授予改变RV会拒绝。未知回复只读恢复，目标容量与完整
// 当前来源继续保留；返回旧Hold列表仅供已结清源的后续安全清理。
func (s *Store) Settle(ctx context.Context, snapshot *Snapshot, podUID string, closed Settlement) (*Snapshot, []Hold, error) {
	if snapshot == nil || snapshot.model.Binding != s.binding {
		return nil, nil, fmt.Errorf("host settlement requires original Node snapshot")
	}
	actor, err := s.authorize(ctx, snapshot.node)
	if err != nil {
		return nil, nil, err
	}
	next, retired, err := snapshot.model.settle(actor, podUID, closed)
	if err != nil {
		return nil, nil, err
	}
	if next.Sequence == snapshot.model.Sequence {
		return snapshot, retired, nil
	}
	result, err := s.write(ctx, snapshot.node, next, false)
	if err != nil {
		return nil, nil, err
	}
	return result, retired, nil
}

// Fence只抬单调floor，不清旧Key/持有、不改域外预算；新holder必须先确认此
// CAS，再申请新容量。迟到旧owner写受原RV/floor拒绝，不能凭Lease更换清账。
func (s *Store) Fence(ctx context.Context, snapshot *Snapshot) (*Snapshot, error) {
	if snapshot == nil || snapshot.model.Binding != s.binding {
		return nil, fmt.Errorf("host fence snapshot belongs to another Node/domain")
	}
	actor, err := s.authorize(ctx, snapshot.node)
	if err != nil {
		return nil, err
	}
	m := snapshot.model.Clone()
	if actor.Epoch < m.LeaseFloor.Epoch || actor.Epoch == m.LeaseFloor.Epoch && actor != m.LeaseFloor {
		return nil, fmt.Errorf("stale or conflicting Host allocator floor")
	}
	if actor == m.LeaseFloor {
		return snapshot, nil
	}
	if m.Sequence == control.Uint64(math.MaxUint64) {
		return nil, fmt.Errorf("host ledger sequence exhausted")
	}
	m.LeaseFloor, m.Sequence = actor, m.Sequence+1
	return s.write(ctx, snapshot.node, m, false)
}

func (s *Store) Reserve(ctx context.Context, snapshot *Snapshot, p Plan) (*Snapshot, Hold, error) {
	var none Hold
	if snapshot == nil || snapshot.model.Binding != s.binding {
		return nil, none, fmt.Errorf("host reserve snapshot belongs to another Node/domain")
	}
	actor, err := s.authorize(ctx, snapshot.node)
	if err != nil {
		return nil, none, err
	}
	total, err := capacity(snapshot.node)
	if err != nil {
		return nil, none, err
	}
	next, held, err := snapshot.model.reserve(actor, p, total)
	if err != nil {
		return nil, none, err
	}
	if next.Sequence == snapshot.model.Sequence {
		return snapshot, held, nil
	}
	result, err := s.write(ctx, snapshot.node, next, false)
	if err != nil {
		return nil, none, err
	}
	return result, held, nil
}
