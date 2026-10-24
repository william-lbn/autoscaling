package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// ResourceWorker长期拥有原Guest资源客户端，与PG客户端的boot/SQL生命周期分离。
// 先冻结完整资源身份到同一VM账本，再推进root floor；不因初次RPC失败重新发现，
// 不创建另一份资源日志，也不把当前上界读回当作完整预算或VM许可。
type ResourceWorker struct {
	session *Session
	client  *resourceguard.Client
}

func (session *Session) NewResourceWorker(ctx context.Context, config resourceguard.ClientConfig) (*ResourceWorker, error) {
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
		// 不能把孤立root效果收养为干净起点。这里仅核验初始约束，并不开放准入。
		if view.Original != nil {
			return nil, fmt.Errorf("unbound resource domain retains original effects")
		}
		if _, err := view.ConfirmedLimits(); err != nil {
			return nil, err
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
	return &ResourceWorker{session: session, client: client}, nil
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
