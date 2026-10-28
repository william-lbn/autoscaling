package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// ResourceWorker长期拥有原Guest资源客户端，与PG客户端的boot/SQL生命周期分离。
// 先冻结完整资源身份到同一VM账本，再推进root floor；不因初次RPC失败重新发现，
// 不创建另一份资源日志，也不把当前上界读回当作完整预算或VM许可。
type ResourceWorker struct {
	session *Session
	client  *resourceguard.Client
	// 启动恢复和正式PG循环共用原Root SDK，只允许一个循环持有执行槽。
	// 槽不跨网络持锁；原子位仅拒绝重复启动，实际取消/join仍由各循环完成。
	running atomic.Bool
}

func (session *Session) NewResourceWorker(ctx context.Context, config resourceguard.ClientConfig) (*ResourceWorker, error) {
	return session.newResourceWorker(ctx, config, false)
}

// NewResourceRecoveryWorker供Agent在PG发现前建立仅Root恢复的持有者。首次原
// Bootstrap已确认、没有孤立运行命令且有完整可修复来源时，允许把漂移中的原
// Guest身份先写入同一VM账本，然后fence；绑定不代表资源确认或准入开放。调用者
// 须先完成RunResourceRecovery，或由PrepareRetainedPGRecovery核完整原PG/
// GuestReady后交接同一Pending；本构造本身不授权任何业务或新资源写。
// 缺/坏日志、未知Bootstrap、孤立Original一律拒绝，不以恢复名义生成新来源。
func (session *Session) NewResourceRecoveryWorker(ctx context.Context, config resourceguard.ClientConfig) (*ResourceWorker, error) {
	return session.newResourceWorker(ctx, config, true)
}

func (session *Session) newResourceWorker(ctx context.Context, config resourceguard.ClientConfig, recovery bool) (*ResourceWorker, error) {
	if err := session.Check(ctx); err != nil {
		return nil, err
	}
	if config.Token == nil || config.Identity.VMUID != string(session.binding.VMUID) || (config.IssuerEpoch != 0 && config.IssuerEpoch != session.actor.Epoch) {
		return nil, fmt.Errorf("resource worker differs from confirmed owner")
	}
	callCtx, cancel := session.coupledContext(ctx)
	defer cancel()
	snapshot, err := session.store.Load(callCtx)
	if err != nil {
		return nil, err
	}
	model := snapshot.State()
	bootstrap := snapshot.VirtualMachine().Spec.DSGABootstrap
	if !snapshot.Dispatchable() || bootstrap == nil || !bootstrap.Ready || bootstrap.ComputeInstanceID != config.Identity.ComputeInstanceID {
		return nil, fmt.Errorf("resource worker original bootstrap differs")
	}
	config.Context = session.ctx
	config.IssuerEpoch = session.actor.Epoch
	token := config.Token
	config.Token = func(ctx context.Context) (string, error) {
		if err := session.Check(ctx); err != nil {
			return "", err
		}
		return token(ctx)
	}
	initial := model.ResourceIdentity == nil
	if initial {
		if model.HighWater != nil || model.Active != nil || len(model.History) != 0 || model.ExpiredResults != 0 || config.Pending != nil {
			return nil, fmt.Errorf("resource worker cannot adopt old unbound operations")
		}
		id, err := resourceguard.DiscoverIdentity(callCtx, config)
		if err != nil {
			return nil, err
		}
		config.Identity = id
	} else {
		// 只允许恢复原完整Guest；调用者可只提供VM/CI以使用既有绑定，不能传另一
		// boot/cgroup替换它。PG新boot与外层历史淘汰都不影响此字段。
		if (config.Identity.GuestBootID != "" || config.Identity.CgroupDevice != 0 || config.Identity.CgroupInode != 0) && config.Identity != *model.ResourceIdentity {
			return nil, fmt.Errorf("resource worker cannot replace original Guest")
		}
		if model.ResourceIdentity.VMUID != config.Identity.VMUID || model.ResourceIdentity.ComputeInstanceID != config.Identity.ComputeInstanceID {
			return nil, fmt.Errorf("resource worker Compute identity changed")
		}
		config.Identity = *model.ResourceIdentity
	}
	pending, err := retainedResourceCommand(model, config.Identity, session.actor.Epoch)
	if err != nil {
		return nil, err
	}
	if config.Pending != nil && (pending == nil || !reflect.DeepEqual(*pending, *config.Pending)) {
		return nil, fmt.Errorf("supplied resource pending differs from complete journal")
	}
	config.Pending = pending
	client, err := resourceguard.NewClient(config)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			client.CloseIdleConnections()
		}
	}()
	view, err := client.InspectState(callCtx)
	if err != nil {
		return nil, err
	}
	if initial {
		// 发现初始Guest之前必须没有任何原操作；root也必须没有运行期原命令。
		// 普通构造要求实际约束确认；专用恢复构造只固化已确认Bootstrap的
		// 完整来源/身份，随后仍须实际恢复门槛。不收养孤立效果，不开放准入。
		if view.Original != nil {
			return nil, fmt.Errorf("unbound resource domain retains original effects")
		}
		if recovery {
			if _, err := resourceguard.HeldSourceSHA256(view); err != nil {
				return nil, err
			}
		} else {
			if _, err := view.ConfirmedLimits(); err != nil {
				return nil, err
			}
		}
		if _, err := session.store.BindResource(callCtx, snapshot, session.actor, config.Identity); err != nil {
			return nil, err
		}
	}
	// 完整Guest绑定已写前持久化。Fence失败/回复丢失都保留它，下一owner只能
	// 按此原身份接管，不借新发现覆盖旧Guest、Original或Held。
	if err := client.Fence(callCtx); err != nil {
		return nil, err
	}
	view, err = client.InspectState(callCtx)
	if err != nil {
		return nil, err
	}
	if view.IssuerFloorEpoch != session.actor.Epoch {
		return nil, fmt.Errorf("resource issuer floor was superseded")
	}
	success = true
	return &ResourceWorker{session: session, client: client, running: atomic.Bool{}}, nil
}

// retainedResourceCommand只从完整外层GuestReady写前票据恢复原资源命令。
// 资源revision独立于PG Key，epoch/PID/限额不能由当前发现或新owner改写。
// 构造SDK和启动期只读恢复共用此校验，避免一个入口接受另一个入口拒绝的来源。
func retainedResourceCommand(model journal.Model, id resourceguard.Identity, epoch control.Uint64) (*resourceguard.Command, error) {
	if model.Active == nil {
		return nil, nil
	}
	for _, step := range model.Active.Steps {
		if step.Action != journal.GuestReady || step.Phase != journal.Unconfirmed {
			continue
		}
		var original resourceguard.Command
		if json.Unmarshal(step.Command, &original) != nil || original.Validate() != nil || original.Identity != id || original.BudgetCommitmentID != model.Active.Request.BudgetCommitmentID || step.TargetID != original.Identity.ComputeInstanceID || step.DispatchedBy == nil || original.IssuerEpoch != step.DispatchedBy.Epoch || original.IssuerEpoch > epoch {
			return nil, fmt.Errorf("retained resource command differs from journal")
		}
		return &original, nil
	}
	return nil, nil
}

// observeRetainedOriginal解除“外层已持久GuestReady，Root仍待原Observe”的
// 启动环依赖。仅串行Root恢复循环调用；普通轮询允许查询完整原命令，但
// 绝不Apply/Replay/换epoch或记录外层GuestReady成功。Root Observe可按真实
// 读回补其自身完成落账；正式PG阶段仍须独立核PG/VM/预算后才Record外层。
//
// SDK Pending必须与当前UID/RV WAL逐字段一致，不能从Root自报Original收养
// 孤立命令。缺原来源/NotFound/部分效果继续未知；HeldRepair另有显式栅栏，
// 本查询不能替代其Observe或清它。调用前后重核原owner/绑定/活动请求。
func (w *ResourceWorker) observeRetainedOriginal(ctx context.Context) error {
	pending := w.client.Pending()
	if pending == nil {
		return nil
	}
	if w.client.PendingHeldRepair() != nil {
		return resourceguard.ErrUnknown
	}
	if err := w.session.Check(ctx); err != nil {
		return err
	}
	id, epoch := w.client.Binding()
	before, err := w.session.store.Load(ctx)
	if err != nil {
		return err
	}
	model := before.State()
	if !before.Dispatchable() || model.Owner != w.session.Actor() || model.ResourceIdentity == nil || *model.ResourceIdentity != id {
		return fmt.Errorf("retained resource observation changed original owner or Guest")
	}
	original, err := retainedResourceCommand(model, id, epoch)
	if err != nil || original == nil || !reflect.DeepEqual(*pending, *original) {
		return fmt.Errorf("resource SDK pending has no matching original GuestReady dispatch")
	}
	if _, err := w.client.Observe(ctx, *original); err != nil {
		return err
	}
	after, err := w.session.store.Load(ctx)
	if err != nil {
		return err
	}
	current := after.State()
	retained, err := retainedResourceCommand(current, id, epoch)
	if err != nil || !after.Dispatchable() || current.Owner != model.Owner || current.ResourceIdentity == nil || *current.ResourceIdentity != id || current.Active == nil || current.Active.Request != model.Active.Request || retained == nil || !reflect.DeepEqual(*retained, *original) {
		return fmt.Errorf("original GuestReady changed during resource observation")
	}
	return w.session.Check(ctx)
}

func (w *ResourceWorker) CloseIdleConnections() { w.client.CloseIdleConnections() }

// PrepareRetainedPGRecovery只选择启动顺序，不授予写权限。有完整原
// PG绑定和GuestReady写前命令时，必须把同一个Pending SDK交给正式PGWorker，
// 才能到达已有的显式原重放/取消入口；一直等Observe会使“Root尚未收到”窗口
// 永久卡在PGWorker建立之前。首次绑定或无完整原PG身份仍走只Root恢复gate。
//
// 先只读Observe原命令；成功时仍由只Root gate核实际约束，未知时将同一
// Pending交正式执行器，不清未知、不发送Root/PG写、不改epoch/出生/预算。
// 正式执行器仍核原Prepare、PG、VM、Guest及Session；旧owner只能查/取消。
func (w *ResourceWorker) PrepareRetainedPGRecovery(ctx context.Context) (bool, error) {
	if w.running.Load() {
		return false, fmt.Errorf("resource recovery or PG loop is already running")
	}
	pending := w.client.Pending()
	if pending == nil {
		return false, nil
	}
	if err := w.session.Check(ctx); err != nil {
		return false, err
	}
	snapshot, err := w.session.store.Load(ctx)
	if err != nil {
		return false, err
	}
	id, epoch := w.client.Binding()
	model := snapshot.State()
	original, err := retainedResourceCommand(model, id, epoch)
	if err != nil || !snapshot.Dispatchable() || model.Owner != w.session.Actor() || model.ResourceIdentity == nil || *model.ResourceIdentity != id || original == nil || !reflect.DeepEqual(*original, *pending) || model.Active == nil || model.PGInstance == nil || *model.PGInstance != model.Active.Request.Key.Instance || model.PGInstance.Validate() != nil || w.client.PendingHeldRepair() != nil {
		return false, fmt.Errorf("pending GuestReady lacks complete original PG recovery binding")
	}
	if err := w.observeRetainedOriginal(ctx); err == nil {
		return false, nil
	} else {
		var network *resourceguard.ClientError
		if !errors.Is(err, resourceguard.ErrUnknown) && !errors.As(err, &network) {
			return false, err
		}
	}
	return true, w.session.Check(ctx)
}

// Inspect不清未知原命令，也不要求原PID还活着；日志和当前约束用于独立资源域
// 恢复。具体原命令完成仍须原Observe，不能从这个metadata读回推导NoEffect。
func (w *ResourceWorker) Inspect(ctx context.Context) (resourceguard.View, error) {
	if err := w.session.Check(ctx); err != nil {
		return resourceguard.View{}, err
	}
	callCtx, cancel := w.session.coupledContext(ctx)
	defer cancel()
	view, err := w.client.InspectState(callCtx)
	if err != nil {
		return view, err
	}
	if view.IssuerFloorEpoch != w.session.actor.Epoch {
		return resourceguard.View{}, fmt.Errorf("resource issuer floor differs from owner")
	}
	return view, nil
}

// RepairHeld只供可信恢复入口显式选择原Root持有摘要。参数不能携带新限额、
// PID或boot；普通协调轮询不调用。修复只恢复到已承诺上界，既不新建预算，
// 也不把原未知GuestReady/PG/VM步骤落成完成。Root日志继续是此原约束的
// 写前持久事实；外层检查当前Lease/VM/资源绑定，Root文件锁内最终核floor。
// 丢回复后先ObserveHeldRepair，显式重试仍由Root逐维拒绝任何实际降限。
func (w *ResourceWorker) RepairHeld(ctx context.Context, selector string) (resourceguard.Evidence, error) {
	if err := w.session.Check(ctx); err != nil {
		return resourceguard.Evidence{}, err
	}
	callCtx, cancel := w.session.coupledContext(ctx)
	defer cancel()
	s, err := w.session.store.Load(callCtx)
	if err != nil {
		return resourceguard.Evidence{}, err
	}
	id, epoch := w.client.Binding()
	if !s.Dispatchable() || s.State().Owner != w.session.actor || s.State().ResourceIdentity == nil || *s.State().ResourceIdentity != id || epoch != w.session.actor.Epoch {
		return resourceguard.Evidence{}, fmt.Errorf("held repair original VM/Guest/owner changed")
	}
	view, err := w.Inspect(callCtx)
	if err != nil {
		return resourceguard.Evidence{}, err
	}
	digest, err := resourceguard.HeldSourceSHA256(view)
	if err != nil || digest != selector {
		return resourceguard.Evidence{}, fmt.Errorf("held repair must select the complete original root source")
	}
	if _, err := w.client.RepairHeld(callCtx, view, selector); err != nil {
		return resourceguard.Evidence{}, err
	}
	// 原效果读回与外层活跃执行权再次认证；不把POST成功缓存成后续
	// 请求许可。任何后继动作仍由各自领域重新认证完整预算及来源。
	return w.ObserveHeldRepair(callCtx, selector)
}

func (w *ResourceWorker) ObserveHeldRepair(ctx context.Context, selector string) (resourceguard.Evidence, error) {
	if err := w.session.Check(ctx); err != nil {
		return resourceguard.Evidence{}, err
	}
	callCtx, cancel := w.session.coupledContext(ctx)
	defer cancel()
	evidence, err := w.client.ObserveHeldRepair(callCtx, selector)
	if err != nil {
		return resourceguard.Evidence{}, err
	}
	s, err := w.session.store.Load(callCtx)
	if err != nil {
		return resourceguard.Evidence{}, err
	}
	if !s.Dispatchable() || s.State().Owner != w.session.actor || s.State().ResourceIdentity == nil || *s.State().ResourceIdentity != evidence.Identity {
		return resourceguard.Evidence{}, fmt.Errorf("held repair observation original owner/Guest changed")
	}
	if err := w.session.Check(callCtx); err != nil {
		return resourceguard.Evidence{}, err
	}
	return evidence, nil
}
