// Package prepareexec实现可信PrepareBudget生产路径。只保留外部承诺并落账，
// 不改PG/Guest限额，不发QMP、不开放准入、不释放Host。资格必须由独立平台
// 签发者事先建立；本包绝不把测试值、当前RSS或请求自报数字变成平台认证。
package prepareexec

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
type command budget.PrepareCommand

func outsideName(c command) (string, error) {
	return budget.PrepareCommand(c).CanonicalOutsideName()
}

func (c *command) UnmarshalJSON(data []byte) error {
	var value budget.PrepareCommand
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*c = command(value)
	return nil
}

func decodeOriginalPrepareCommand(raw []byte) (command, error) {
	var original command
	err := json.Unmarshal(raw, &original)
	return original, err
}

func (c command) outside(incarnation string) budget.OutsideReservation {
	source := c.Qualification
	p := c.Prepared
	return budget.OutsideReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: p.Request, Identity: p.Identity, Incarnation: incarnation, ResidualProfileID: "qualified-" + source.SHA256, SystemOutsidePG: p.SystemOutsidePG, Transition: p.Transition, Safety: p.Safety, HoldUntilExplicitSettlement: true, Qualification: &source}
}

// inspect包围Start/Create/Record重新取认证PG/Root/Runner事实。各域网络请求不
// 共持内存锁；最终VM UID/RV CAS使并发取消/换主失去落账权。真实内核约束与
// 原生floor仍由各自执行器持久化，本函数不把一次观察当永久许可。
func (e *Executor) inspect(ctx context.Context, s *journal.Snapshot) (c command, err error) {
	diagnosticStage := uint16(0)
	defer func() {
		if err != nil {
			log.Printf("DSGA_PREPARE_FAILURE scope=%d stage=%d", 1, diagnosticStage)
		}
	}()
	diagnosticStage = 86
	if err := e.checkSnapshot(ctx, s); err != nil {
		diagnosticStage = 87
		return c, err
	}
	diagnosticStage = 89
	m, vm := s.State(), s.VirtualMachine()
	diagnosticStage = 90
	instance, _ := e.pg.Binding()
	diagnosticStage = 91
	id, _ := e.resources.Binding()
	diagnosticStage = 92
	if !s.Dispatchable() || m.Owner != e.actor || m.PGInstance == nil || *m.PGInstance != instance || m.ResourceIdentity == nil || *m.ResourceIdentity != id || m.Active == nil || m.Active.Request.Key.Instance != instance || e.pg.Pending() != nil || e.resources.Pending() != nil {
		diagnosticStage = 93
		return c, fmt.Errorf("prepare producer lacks original dispatchable VM/PG/Root")
	}
	diagnosticStage = 95
	q, err := e.claims.Qualification(ctx, e.source)
	diagnosticStage = 96
	if err != nil {
		diagnosticStage = 97
		return c, err
	}
	diagnosticStage = 99
	if q.Identity != id || q.RunnerPodName != vm.Status.PodName || q.NodeName != vm.Status.Node || q.RunnerRestartCount != vm.Status.RestartCount {
		diagnosticStage = 100
		return c, fmt.Errorf("platform qualification differs from current original Guest/Runner")
	}
	diagnosticStage = 102
	birth, birthRef, publication, err := e.runner.Inspect(ctx, &q.RunnerBirth, e.actor)
	diagnosticStage = 103
	if err != nil {
		diagnosticStage = 104
		return c, err
	}
	diagnosticStage = 106
	rawIdentity, err := json.Marshal(birth.Identity)
	diagnosticStage = 107
	if err != nil {
		diagnosticStage = 108
		return c, err
	}
	diagnosticStage = 110
	if birthRef != q.RunnerBirth || vmguard.SourceDigest(rawIdentity) != q.RunnerIdentitySHA256 || birth.Identity.RunnerPodUID != q.RunnerPodUID || birth.Identity.NodeUID != q.NodeUID || publication.State.Pending != nil || publication.Actual.RequestedBytes != publication.Actual.ActualBytes || vm.Status.MemorySize == nil {
		diagnosticStage = 111
		return c, fmt.Errorf("prepare requires original settled Runner actual capacity")
	}
	diagnosticStage = 113
	before, exact := vm.Status.MemorySize.AsInt64()
	diagnosticStage = 114
	if !exact || before <= 0 || control.Uint64(before) != publication.Actual.ActualBytes {
		diagnosticStage = 115
		return c, fmt.Errorf("VM status and native Runner actual capacity differ")
	}
	request := m.Active.Request
	diagnosticStage = 118
	if request.TargetVMMemoryBytes < birth.Identity.MinimumBytes || request.TargetVMMemoryBytes > birth.Identity.MaximumBytes || (request.TargetVMMemoryBytes-birth.Identity.MinimumBytes)%birth.Identity.BlockBytes != 0 {
		diagnosticStage = 119
		return c, fmt.Errorf("requested VM size violates original native geometry")
	}
	diagnosticStage = 121
	fence, err := e.pg.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: e.actor.Epoch})
	diagnosticStage = 122
	if err != nil {
		diagnosticStage = 123
		return c, err
	}
	diagnosticStage = 125
	shared, err := e.pg.SharedCost(ctx)
	diagnosticStage = 126
	if err != nil {
		diagnosticStage = 127
		return c, err
	}
	diagnosticStage = 129
	if fence.Disposition != control.Fenced || !reflect.DeepEqual(fence.Observed, shared.Observed) {
		diagnosticStage = 130
		return c, fmt.Errorf("prepare native cost and current issuer fence differ")
	}
	diagnosticStage = 132
	view, err := e.resources.InspectState(ctx)
	diagnosticStage = 133
	if err != nil {
		diagnosticStage = 134
		return c, err
	}
	diagnosticStage = 136
	held, err := view.ConfirmedLimits()
	diagnosticStage = 137
	if err != nil {
		diagnosticStage = 138
		return c, err
	}
	diagnosticStage = 140
	process, err := e.resources.InspectProcess(ctx)
	diagnosticStage = 141
	if err != nil {
		diagnosticStage = 142
		return c, err
	}
	diagnosticStage = 144
	root, err := budget.RootAggregateFromView(view)
	diagnosticStage = 145
	if err != nil {
		diagnosticStage = 146
		return c, err
	}
	diagnosticStage = 148
	bootstrapRaw, err := json.Marshal(view.Bootstrap)
	diagnosticStage = 149
	if err != nil {
		diagnosticStage = 150
		return c, err
	}
	diagnosticStage = 152
	if view.Identity != id || view.IssuerFloorEpoch != e.actor.Epoch || view.Birth == nil || process != *view.Birth || vmguard.SourceDigest(bootstrapRaw) != q.RootBootstrapSHA256 {
		diagnosticStage = 153
		return c, fmt.Errorf("qualified original root/bootstrap/Postmaster birth changed")
	}
	diagnosticStage = 155
	target, err := q.Target(request.TargetVMMemoryBytes)
	diagnosticStage = 156
	if err != nil {
		diagnosticStage = 157
		return c, err
	}
	// 旧profile仍不允许降低normal；显式新接口只在此持久选择，实际授权由Root
	// 消费完整PG事实和真实预算。每进程AS/pids/System及全部hstate保持原样；
	// Prepare只保留目标，不能靠排空/采样在这里写任何硬限额。
	diagnosticStage = 162
	if target.Limits.MemoryMaxBytes < held.MemoryMaxBytes && !e.resources.NormalQuotaEnabled() || target.Limits.AddressSpaceMaxBytes != held.AddressSpaceMaxBytes || target.Limits.PidsMaxTasks != held.PidsMaxTasks || target.Limits.SystemMemoryMaxBytes != held.SystemMemoryMaxBytes || len(target.Limits.HugePools) != len(held.HugePools) {
		diagnosticStage = 163
		return c, fmt.Errorf("qualified target changes fixed limits or requires unsupported normal-cap decrease")
	}
	diagnosticStage = 165
	for i := range held.HugePools {
		diagnosticStage = 166
		if target.Limits.HugePools[i].PageSizeKB != held.HugePools[i].PageSizeKB {
			diagnosticStage = 167
			return c, fmt.Errorf("qualified target omits or replaces an original HugeTLB hstate")
		}
	}
	upper := shared.CurrentMainAndArraysUpperBytes
	diagnosticStage = 171
	if request.Kind == control.Grow {
		diagnosticStage = 172
		upper, err = budget.GrowSharedUpper(shared, request.TargetBuffers)
		diagnosticStage = 173
		if err != nil {
			diagnosticStage = 174
			return c, err
		}
	}
	diagnosticStage = 177
	system, transition, safety := target.Bounds(e.source)
	diagnosticStage = 178
	p := budget.Prepared{ProtocolVersion: 1, VMShrinkPermit: false, Request: request, Identity: id, RunnerPodName: q.RunnerPodName, RunnerRestartCount: q.RunnerRestartCount, VMBeforeMemoryBytes: control.Uint64(before), GuestUsableMemoryBytes: target.GuestUsableMemoryBytes, Shared: shared, TargetSharedUpperBytes: upper, Limits: target.Limits, SystemOutsidePG: system, Transition: transition, Safety: safety, OutsideClaim: nil}
	// 新profile的预算ID/数组权限合同跨相同N阶段仍须保持，不能因本轮N恰好
	// 相等退回旧Apply；首次Grow也要由Root新协议核原PG事实和预算移交。
	diagnosticStage = 181
	if e.resources.NormalQuotaEnabled() {
		p.NormalQuotaProtocolVersion = 1
		originalFence := fence
		p.NormalQuotaFence = &originalFence
	}
	diagnosticStage = 186
	if err := p.ValidateFor(request, id); err != nil {
		diagnosticStage = 187
		return c, err
	}
	diagnosticStage = 189
	digest, err := budget.RootDigest(root)
	diagnosticStage = 190
	if err != nil {
		diagnosticStage = 191
		return c, err
	}
	c = command{ProtocolVersion: 1, Qualification: e.source, Prepared: p, RootSHA256: digest, OutsideName: ""}
	c.OutsideName, err = outsideName(c)
	if err == nil && c.Prepared.NormalQuotaProtocolVersion == 1 {
		err = budget.PrepareCommand(c).Validate()
	}
	diagnosticStage = 195
	return c, err
}

// checkSnapshot在任何新来源读取/创建前重新GET原VM。独立关闭冻结原操作，
// 不能拿关闭前的Snapshot或已覆盖的原操作读回继续Prepare。跨对象不持本地
// 锁，RV改变只保留原Unknown；这里不能撤销已经在网络中的Outside CREATE，
// 所以CREATE晚到后仍要再检查，最终Record还必须通过同UID/RV原子条件写。
func (e *Executor) checkSnapshot(ctx context.Context, s *journal.Snapshot) (err error) {
	diagnosticStage := uint16(0)
	defer func() {
		if err != nil {
			log.Printf("DSGA_PREPARE_FAILURE scope=%d stage=%d", 3, diagnosticStage)
		}
	}()
	diagnosticStage = 203
	if s == nil || !s.Dispatchable() || s.State().Owner != e.actor || s.State().Active == nil {
		diagnosticStage = 204
		return fmt.Errorf("prepare producer requires original active VM snapshot")
	}
	diagnosticStage = 206
	if s.State().ServiceClosure != nil {
		diagnosticStage = 207
		return fmt.Errorf("prepare producer blocked by retained service closure")
	}
	diagnosticStage = 209
	current, err := e.store.Load(ctx)
	diagnosticStage = 210
	if err != nil {
		diagnosticStage = 211
		return err
	}
	diagnosticStage = 213
	m := current.State()
	diagnosticStage = 214
	if m.ServiceClosure != nil {
		diagnosticStage = 215
		return fmt.Errorf("prepare producer blocked by retained service closure")
	}
	diagnosticStage = 217
	if current.ResourceVersion() != s.ResourceVersion() || !current.Dispatchable() || m.Owner != e.actor || m.Active == nil {
		diagnosticStage = 218
		return fmt.Errorf("prepare producer original VM UID/RV or operation changed")
	}
	diagnosticStage = 220
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

func (e *Executor) advance(ctx context.Context, replay *control.Key) (progress pgexec.Progress, err error) {
	diagnosticStage := uint16(0)
	defer func() {
		if err != nil {
			log.Printf("DSGA_PREPARE_FAILURE scope=%d stage=%d", 2, diagnosticStage)
		}
	}()
	diagnosticStage = 235
	s, err := e.store.Load(ctx)
	diagnosticStage = 236
	if err != nil {
		diagnosticStage = 237
		return pgexec.AwaitingFact, err
	}
	diagnosticStage = 239
	m := s.State()
	diagnosticStage = 240
	if m.Owner != e.actor {
		diagnosticStage = 241
		return pgexec.AwaitingFact, fmt.Errorf("prepare producer owner changed")
	}
	diagnosticStage = 243
	if m.ServiceClosure != nil {
		// 普通查询和显式原重放均不能越过独立关闭再创建Outside。原
		// Prepare的Planned/Unknown逐字保留，不按来源缺失造NoEffect。
		diagnosticStage = 246
		return pgexec.AwaitingFact, fmt.Errorf("prepare producer blocked by retained service closure")
	}
	diagnosticStage = 248
	if m.Active == nil {
		diagnosticStage = 249
		return pgexec.Idle, nil
	}
	op := m.Active
	step := op.Steps[0]
	diagnosticStage = 253
	if step.Action != journal.PrepareBudget || step.Phase == journal.Settled {
		diagnosticStage = 254
		return pgexec.ExternalStep, nil
	}
	planned := step.Phase == journal.Planned
	diagnosticStage = 257
	if planned && (op.Origin != e.actor || op.AbortRequested) && replay == nil {
		diagnosticStage = 258
		return pgexec.ExternalStep, nil
	}
	diagnosticStage = 260
	if replay != nil && (planned || *replay != op.Request.Key || op.Origin != e.actor || op.AbortRequested || step.DispatchedBy == nil || *step.DispatchedBy != e.actor) {
		diagnosticStage = 261
		return pgexec.ExternalStep, fmt.Errorf("original prepare is not eligible for dispatch/replay")
	}
	diagnosticStage = 263
	c, err := e.inspect(ctx, s)
	diagnosticStage = 264
	if err != nil {
		diagnosticStage = 265
		return pgexec.AwaitingFact, err
	}
	diagnosticStage = 267
	if planned {
		diagnosticStage = 268
		raw, err := json.Marshal(c)
		diagnosticStage = 269
		if err != nil {
			diagnosticStage = 270
			return pgexec.AwaitingFact, err
		}
		var dispatch *journal.Dispatch
		diagnosticStage = 273
		s, dispatch, err = e.store.Start(ctx, s, e.actor, op.Request.Key, journal.PrepareBudget, op.Request.Key.Instance.ComputeInstanceID, raw)
		diagnosticStage = 274
		if err != nil {
			// 没有确认票据，绝不继续Create；普通下一轮只查原命令。
			diagnosticStage = 276
			return pgexec.AwaitingFact, err
		}
		step = dispatch.Step
	} else {
		original, decodeErr := decodeOriginalPrepareCommand(step.Command)
		diagnosticStage = 281
		if decodeErr != nil || !reflect.DeepEqual(original, c) || step.TargetID != op.Request.Key.Instance.ComputeInstanceID {
			diagnosticStage = 282
			return pgexec.AwaitingFact, fmt.Errorf("original prepare source or current facts changed")
		}
		c = original
	}
	diagnosticStage = 286
	if planned {
		// Start后再取事实；若期间发生漂移，保留原Unconfirmed，不创建另一份
		// 承诺或将它改成NoEffect。下一轮仍查询完整原命令，不自动重发。
		diagnosticStage = 289
		latest, err := e.inspect(ctx, s)
		diagnosticStage = 290
		if err != nil {
			diagnosticStage = 291
			return pgexec.AwaitingFact, fmt.Errorf("%w: prepare facts unavailable after original WAL: %w", resourceguard.ErrUnknown, err)
		}
		diagnosticStage = 293
		if !reflect.DeepEqual(latest, c) {
			diagnosticStage = 294
			return pgexec.AwaitingFact, fmt.Errorf("%w: prepare facts changed after original WAL", resourceguard.ErrUnknown)
		}
	}
	diagnosticStage = 297
	ref, err := e.outside(ctx, c, planned || replay != nil)
	diagnosticStage = 298
	if err != nil {
		diagnosticStage = 299
		return pgexec.AwaitingFact, err
	}
	diagnosticStage = 301
	latest, err := e.inspect(ctx, s)
	diagnosticStage = 302
	if err != nil {
		diagnosticStage = 303
		return pgexec.AwaitingFact, err
	}
	diagnosticStage = 305
	if !reflect.DeepEqual(latest, c) {
		diagnosticStage = 306
		return pgexec.AwaitingFact, fmt.Errorf("prepare sources changed before recording")
	}
	p := c.Prepared
	p.OutsideClaim = &ref
	diagnosticStage = 310
	if err := e.claims.CheckOutside(ctx, p); err != nil {
		diagnosticStage = 311
		return pgexec.AwaitingFact, err
	}
	diagnosticStage = 313
	raw, err := json.Marshal(p)
	diagnosticStage = 314
	if err != nil {
		diagnosticStage = 315
		return pgexec.AwaitingFact, err
	}
	// WAL的SourceID必须等于原派发TargetID（Compute实例）；平台资格身份
	// 保存在完整原命令/Outside中，不能占用执行目标字段导致回执串域。
	diagnosticStage = 319
	_, err = e.store.Record(ctx, s, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: journal.PrepareBudget, Result: journal.Completed, SourceID: step.TargetID, Data: raw})
	diagnosticStage = 320
	if err != nil {
		diagnosticStage = 321
		return pgexec.AwaitingFact, err
	}
	diagnosticStage = 323
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
