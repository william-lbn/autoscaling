// Package prepareexec实现可信PrepareBudget生产路径。只保留外部承诺并落账，
// 不改PG/Guest限额，不发QMP、不开放准入、不释放Host。资格必须由独立平台
// 签发者事先建立；本包绝不把测试值、当前RSS或请求自报数字变成平台认证。
package prepareexec

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

type Executor struct {
	store     *journal.Store
	pg        *compute.Client
	resources *resourceguard.Client
	claims    *budgetclaims.Reader
	runner    *vmguard.Reader
	core      coreclient.CoreV1Interface
	actor     journal.Owner
	source    budget.QualificationSource
}

// 资格选择来自可信Agent配置；旧构造路径不默认补它。此域需要创建Outside的
// 单独授权，仍无Node/Host来源/资格创建、修改或释放权限。真实RBAC/准入规则
// 必须将资格签发、Outside生产、Host分配与普通请求者分开。
func New(store *journal.Store, pg *compute.Client, resources *resourceguard.Client, claims *budgetclaims.Reader, runner *vmguard.Reader, client coreclient.CoreV1Interface, actor journal.Owner, source budget.QualificationSource) (*Executor, error) {
	if store == nil || pg == nil || resources == nil || claims == nil || runner == nil || client == nil || actor.Validate() != nil || source.Validate() != nil {
		return nil, fmt.Errorf("prepare producer requires trusted original domains and qualification")
	}
	instance, epoch := pg.Binding()
	id, resourceEpoch := resources.Binding()
	if epoch != actor.Epoch || resourceEpoch != epoch || instance.VMUID != string(store.Binding().VMUID) || id.VMUID != instance.VMUID || id.ComputeInstanceID != instance.ComputeInstanceID {
		return nil, fmt.Errorf("prepare producer domains differ")
	}
	return &Executor{store: store, pg: pg, resources: resources, claims: claims, runner: runner, core: client, actor: actor, source: source}, nil
}

// command先持久完整原承诺、资格UID/摘要及当时Root来源，后才创建Outside。
// 首次Create的服务端UID只能在真实回复/原保护对象读回后得到；Completed同时
// 将其存入Prepared。发布后所有消费者固定该UID，不能同名收养替代来源。
type command struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Qualification   budget.QualificationSource `json:"qualification"`
	Prepared        budget.Prepared            `json:"prepared"`
	RootSHA256      string                     `json:"root_sha256"`
	OutsideName     string                     `json:"outside_name"`
}

func outsideName(c command) (string, error) {
	// 名称内容寻址包含完整原命令；不含自己，避免循环摘要。不同原资格/
	// Root/成本不能碰巧共用来源。重读相同命令所得名称必须逐字节一致。
	c.OutsideName = ""
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return "dsga-o-" + vmguard.SourceDigest(raw)[:56], nil
}

func (c command) outside(incarnation string) budget.OutsideReservation {
	source := c.Qualification
	p := c.Prepared
	return budget.OutsideReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: p.Request, Identity: p.Identity, Incarnation: incarnation, ResidualProfileID: "qualified-" + source.SHA256, SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true, Qualification: &source}
}

// inspect包围Start/Create/Record重新取认证PG/Root/Runner事实。各域网络请求不
// 共持内存锁；最终VM UID/RV CAS使并发取消/换主失去落账权。真实内核约束与
// 原生floor仍由各自执行器持久化，本函数不把一次观察当永久许可。
func (e *Executor) inspect(ctx context.Context, s *journal.Snapshot) (command, error) {
	var c command
	if err := e.checkSnapshot(ctx, s); err != nil {
		return c, err
	}
	m, vm := s.State(), s.VirtualMachine()
	instance, _ := e.pg.Binding()
	id, _ := e.resources.Binding()
	if !s.Dispatchable() || m.Owner != e.actor || m.PGInstance == nil || *m.PGInstance != instance || m.ResourceIdentity == nil || *m.ResourceIdentity != id || m.Active == nil || m.Active.Request.Key.Instance != instance || e.pg.Pending() != nil || e.resources.Pending() != nil {
		return c, fmt.Errorf("prepare producer lacks original dispatchable VM/PG/Root")
	}
	q, err := e.claims.Qualification(ctx, e.source)
	if err != nil {
		return c, err
	}
	if q.Identity != id || q.RunnerPodName != vm.Status.PodName || q.NodeName != vm.Status.Node || q.RunnerRestartCount != vm.Status.RestartCount {
		return c, fmt.Errorf("platform qualification differs from current original Guest/Runner")
	}
	birth, birthRef, publication, err := e.runner.Inspect(ctx, &q.RunnerBirth, e.actor)
	if err != nil {
		return c, err
	}
	rawIdentity, err := json.Marshal(birth.Identity)
	if err != nil {
		return c, err
	}
	if birthRef != q.RunnerBirth || vmguard.SourceDigest(rawIdentity) != q.RunnerIdentitySHA256 || birth.Identity.RunnerPodUID != q.RunnerPodUID || birth.Identity.NodeUID != q.NodeUID || publication.State.Pending != nil || publication.Actual.RequestedBytes != publication.Actual.ActualBytes || vm.Status.MemorySize == nil {
		return c, fmt.Errorf("prepare requires original settled Runner actual capacity")
	}
	before, exact := vm.Status.MemorySize.AsInt64()
	if !exact || before <= 0 || control.Uint64(before) != publication.Actual.ActualBytes {
		return c, fmt.Errorf("VM status and native Runner actual capacity differ")
	}
	request := m.Active.Request
	if request.TargetVMMemoryBytes < birth.Identity.MinimumBytes || request.TargetVMMemoryBytes > birth.Identity.MaximumBytes || (request.TargetVMMemoryBytes-birth.Identity.MinimumBytes)%birth.Identity.BlockBytes != 0 {
		return c, fmt.Errorf("requested VM size violates original native geometry")
	}
	fence, err := e.pg.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return c, err
	}
	shared, err := e.pg.SharedCost(ctx)
	if err != nil {
		return c, err
	}
	if fence.Disposition != control.Fenced || !reflect.DeepEqual(fence.Observed, shared.Observed) {
		return c, fmt.Errorf("prepare native cost and current issuer fence differ")
	}
	view, err := e.resources.InspectState(ctx)
	if err != nil {
		return c, err
	}
	held, err := view.ConfirmedLimits()
	if err != nil {
		return c, err
	}
	process, err := e.resources.InspectProcess(ctx)
	if err != nil {
		return c, err
	}
	root, err := budget.RootAggregateFromView(view)
	if err != nil {
		return c, err
	}
	bootstrapRaw, err := json.Marshal(view.Bootstrap)
	if err != nil {
		return c, err
	}
	if view.Identity != id || view.IssuerFloorEpoch != e.actor.Epoch || view.Birth == nil || process != *view.Birth || vmguard.SourceDigest(bootstrapRaw) != q.RootBootstrapSHA256 {
		return c, fmt.Errorf("qualified original root/bootstrap/Postmaster birth changed")
	}
	target, err := q.Target(request.TargetVMMemoryBytes)
	if err != nil {
		return c, err
	}
	// N、每进程AS和系统组额度在bootstrap后固定；所有hstate须原样列全。当前没有安全
	// 降normal生产协议，不能靠一次排空/采样直接压低memory.max。Prepare仅
	// 保留目标，grow尚小时也绝不写入更大的硬限额。
	if target.Limits.MemoryMaxBytes < held.MemoryMaxBytes || target.Limits.AddressSpaceMaxBytes != held.AddressSpaceMaxBytes || target.Limits.PidsMaxTasks != held.PidsMaxTasks || target.Limits.SystemMemoryMaxBytes != held.SystemMemoryMaxBytes || len(target.Limits.HugePools) != len(held.HugePools) {
		return c, fmt.Errorf("qualified target changes fixed limits or requires unsupported normal-cap decrease")
	}
	for i := range held.HugePools {
		if target.Limits.HugePools[i].PageSizeKB != held.HugePools[i].PageSizeKB {
			return c, fmt.Errorf("qualified target omits or replaces an original HugeTLB hstate")
		}
	}
	upper := shared.CurrentMainAndArraysUpperBytes
	if request.Kind == control.Grow {
		upper, err = budget.GrowSharedUpper(shared, request.TargetBuffers)
		if err != nil {
			return c, err
		}
	}
	system, transition, safety := target.Bounds(e.source)
	p := budget.Prepared{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Identity: id, RunnerPodName: q.RunnerPodName, RunnerRestartCount: q.RunnerRestartCount, VMBeforeMemoryBytes: control.Uint64(before), GuestUsableMemoryBytes: target.GuestUsableMemoryBytes, Shared: shared, TargetSharedUpperBytes: upper, Limits: target.Limits, SystemOutsidePG: system, Transition: transition, Safety: safety, OutsideClaim: nil}
	if err := p.ValidateFor(request, id); err != nil {
		return c, err
	}
	digest, err := budget.RootDigest(root)
	if err != nil {
		return c, err
	}
	c = command{ProtocolVersion: 1, Qualification: e.source, Prepared: p, RootSHA256: digest, OutsideName: ""}
	c.OutsideName, err = outsideName(c)
	return c, err
}

// checkSnapshot在任何新来源读取/创建前重新GET原VM。独立关闭冻结原操作，
// 不能拿关闭前的Snapshot或已覆盖的原操作读回继续Prepare。跨对象不持本地
// 锁，RV改变只保留原Unknown；这里不能撤销已经在网络中的Outside CREATE，
// 所以CREATE晚到后仍要再检查，最终Record还必须通过同UID/RV原子条件写。
func (e *Executor) checkSnapshot(ctx context.Context, s *journal.Snapshot) error {
	if s == nil || !s.Dispatchable() || s.State().Owner != e.actor || s.State().Active == nil {
		return fmt.Errorf("prepare producer requires original active VM snapshot")
	}
	if s.State().ServiceClosure != nil {
		return fmt.Errorf("prepare producer blocked by retained service closure")
	}
	current, err := e.store.Load(ctx)
	if err != nil {
		return err
	}
	m := current.State()
	if m.ServiceClosure != nil {
		return fmt.Errorf("prepare producer blocked by retained service closure")
	}
	if current.ResourceVersion() != s.ResourceVersion() || !current.Dispatchable() || m.Owner != e.actor || m.Active == nil {
		return fmt.Errorf("prepare producer original VM UID/RV or operation changed")
	}
	return nil
}

func (e *Executor) Advance(ctx context.Context) (pgexec.Progress, error) { return e.advance(ctx, nil) }

// ReplayOriginal只选择原Key，不接受另一份额度或资格。普通轮询/404/超时不会
// 调用此入口；只有原派发者仍持相同Session才可创建原命令确定的同一对象。
func (e *Executor) ReplayOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	if err := key.Validate(); err != nil {
		return pgexec.AwaitingFact, err
	}
	return e.advance(ctx, &key)
}

func (e *Executor) advance(ctx context.Context, replay *control.Key) (pgexec.Progress, error) {
	s, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != e.actor {
		return pgexec.AwaitingFact, fmt.Errorf("prepare producer owner changed")
	}
	if m.ServiceClosure != nil {
		// 普通查询和显式原重放均不能越过独立关闭再创建Outside。原
		// Prepare的Planned/Unknown逐字保留，不按来源缺失造NoEffect。
		return pgexec.AwaitingFact, fmt.Errorf("prepare producer blocked by retained service closure")
	}
	if m.Active == nil {
		return pgexec.Idle, nil
	}
	op := m.Active
	step := op.Steps[0]
	if step.Action != journal.PrepareBudget || step.Phase == journal.Settled {
		return pgexec.ExternalStep, nil
	}
	planned := step.Phase == journal.Planned
	if planned && (op.Origin != e.actor || op.AbortRequested) && replay == nil {
		return pgexec.ExternalStep, nil
	}
	if replay != nil && (planned || *replay != op.Request.Key || op.Origin != e.actor || op.AbortRequested || step.DispatchedBy == nil || *step.DispatchedBy != e.actor) {
		return pgexec.ExternalStep, fmt.Errorf("original prepare is not eligible for dispatch/replay")
	}
	c, err := e.inspect(ctx, s)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if planned {
		raw, err := json.Marshal(c)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		var dispatch *journal.Dispatch
		s, dispatch, err = e.store.Start(ctx, s, e.actor, op.Request.Key, journal.PrepareBudget, op.Request.Key.Instance.ComputeInstanceID, raw)
		if err != nil {
			// 没有确认票据，绝不继续Create；普通下一轮只查原命令。
			return pgexec.AwaitingFact, err
		}
		step = dispatch.Step
	} else {
		var original command
		if control.UnmarshalObject(step.Command, &original) != nil || !reflect.DeepEqual(original, c) || step.TargetID != op.Request.Key.Instance.ComputeInstanceID {
			return pgexec.AwaitingFact, fmt.Errorf("original prepare source or current facts changed")
		}
		c = original
	}
	if planned {
		// Start后再取事实；若期间发生漂移，保留原Unconfirmed，不创建另一份
		// 承诺或将它改成NoEffect。下一轮仍查询完整原命令，不自动重发。
		latest, err := e.inspect(ctx, s)
		if err != nil {
			return pgexec.AwaitingFact, fmt.Errorf("%w: prepare facts unavailable after original WAL: %w", resourceguard.ErrUnknown, err)
		}
		if !reflect.DeepEqual(latest, c) {
			return pgexec.AwaitingFact, fmt.Errorf("%w: prepare facts changed after original WAL", resourceguard.ErrUnknown)
		}
	}
	ref, err := e.outside(ctx, c, planned || replay != nil)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	latest, err := e.inspect(ctx, s)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if !reflect.DeepEqual(latest, c) {
		return pgexec.AwaitingFact, fmt.Errorf("prepare sources changed before recording")
	}
	p := c.Prepared
	p.OutsideClaim = &ref
	if err := e.claims.CheckOutside(ctx, p); err != nil {
		return pgexec.AwaitingFact, err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	// WAL的SourceID必须等于原派发TargetID（Compute实例）；平台资格身份
	// 保存在完整原命令/Outside中，不能占用执行目标字段导致回执串域。
	_, err = e.store.Record(ctx, s, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: journal.PrepareBudget, Result: journal.Completed, SourceID: step.TargetID, Data: raw})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}

func (e *Executor) outside(ctx context.Context, c command, create bool) (budget.ObjectRef, error) {
	var ref budget.ObjectRef
	cm, err := e.core.ConfigMaps(e.store.Binding().Namespace).Get(ctx, c.OutsideName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && create {
		out := c.outside(e.store.Binding().Incarnation)
		raw, marshalErr := json.Marshal(out)
		if marshalErr != nil {
			return ref, marshalErr
		}
		binding := e.store.Binding()
		protected := true
		cm, err = e.core.ConfigMaps(binding.Namespace).Create(ctx, &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: c.OutsideName, Namespace: binding.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: binding.Name, UID: binding.VMUID, Controller: &protected, BlockOwnerDeletion: &protected}}, Finalizers: []string{budgetclaims.ProtectionFinalizer}, Labels: map[string]string{budgetclaims.ClaimLabel: budgetclaims.OutsideKind}}, Immutable: &protected, Data: map[string]string{budgetclaims.ClaimKey: string(raw)}}, metav1.CreateOptions{})
		if err != nil {
			// 包括AlreadyExists和丢回复均不在本次透明重试；下次只GET。
			// 原保护finalizer和释放协议禁止未知期间删后重建该名称。
			return ref, fmt.Errorf("%w: original outside creation not confirmed: %w", resourceguard.ErrUnknown, err)
		}
	}
	if err != nil {
		return ref, fmt.Errorf("%w: original outside source not confirmed: %w", resourceguard.ErrUnknown, err)
	}
	if cm == nil || cm.Name != c.OutsideName {
		return ref, fmt.Errorf("original outside creation returned another object")
	}
	ref = budget.ObjectRef{Name: cm.Name, UID: string(cm.UID)}
	out, err := e.claims.ReadOutside(ctx, ref)
	if err != nil {
		return ref, err
	}
	if !reflect.DeepEqual(out, c.outside(e.store.Binding().Incarnation)) {
		return ref, fmt.Errorf("original outside complete body differs from saved prepare")
	}
	return ref, nil
}
