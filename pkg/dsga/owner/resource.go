package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"

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
// 必须先完成RunResourceRecovery，再构造PGWorker，不能从本方法直接开始业务。
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
	var pending *resourceguard.Command
	if model.Active != nil {
		for _, step := range model.Active.Steps {
			if step.Action != journal.GuestReady || step.Phase != journal.Unconfirmed {
				continue
			}
			var original resourceguard.Command
			if json.Unmarshal(step.Command, &original) != nil || original.Validate() != nil || original.Identity != config.Identity || original.BudgetCommitmentID != model.Active.Request.BudgetCommitmentID || step.TargetID != original.Identity.ComputeInstanceID || step.DispatchedBy == nil || original.IssuerEpoch != step.DispatchedBy.Epoch || original.IssuerEpoch > session.actor.Epoch {
				return nil, fmt.Errorf("retained resource command differs from journal")
			}
			pending = &original
		}
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

func (w *ResourceWorker) CloseIdleConnections() { w.client.CloseIdleConnections() }

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
