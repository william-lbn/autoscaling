package vmexec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// ReplayOriginal只由受信协调入口的明确选择调用；轮询和网络失败不调用。
// 它仅保存同VM条件选择，由Runner自己拉取；不发送QMP、不改原命令/期限/
// epoch、不把原not_sent结算成Completed。重复调用先复用已持久的同一选择。
func (e *Executor) ReplayOriginal(ctx context.Context, key control.Key) (pgexec.Progress, error) {
	s, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	state := s.State()
	op := state.Active
	if key.Validate() != nil || state.Owner != e.actor || op == nil || op.Origin != e.actor || op.AbortRequested || op.Request.Key != key || !s.Dispatchable() || state.PGInstance == nil || *state.PGInstance != e.instance || state.ResourceIdentity == nil || *state.ResourceIdentity != e.id {
		return pgexec.AwaitingFact, fmt.Errorf("explicit VM replay requires same active original owner/PG/Guest")
	}
	prior, step := steps(op)
	var c vmguard.Command
	if step == nil || step.Action != journal.ResizeVM || step.Phase != journal.Unconfirmed || step.DispatchedBy == nil || *step.DispatchedBy != e.actor || json.Unmarshal(step.Command, &c) != nil || c.Validate() != nil || c.Request != op.Request || step.TargetID != c.Identity.RunnerPodUID {
		return pgexec.AwaitingFact, fmt.Errorf("explicit VM replay requires unchanged original typed command")
	}
	// 必须先读取原执行域。may_have_sent、pending、已完成或原before漂移均
	// 不能因外部来源恢复而被重放；它们继续走原只读对账。
	proof, err := e.runner.UnsentOriginal(ctx, c, step.CommandSHA256, e.actor)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if _, valid := op.Request.RemainingWaitMillis(uint64(time.Now().UnixMilli())); !valid {
		return pgexec.AwaitingFact, fmt.Errorf("explicit VM replay cannot extend original deadline")
	}
	p, err := e.prepared(op, prior)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	observed, err := e.claims.Read(ctx, s, p, c.HostClaim)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	expected, err := e.command(p, prior, proof.Birth, c.BindingClaim, c.HostClaim, observed.Host)
	if err != nil || expected != c || !hostHeld(observed.Host, c) {
		return pgexec.AwaitingFact, fmt.Errorf("explicit VM replay original Host/Prepare/Guest changed")
	}
	if _, err := e.native(ctx, p, prior, false); err != nil {
		return pgexec.AwaitingFact, err
	}
	choice := journal.VMReplaySelection{ProtocolVersion: 1, Key: key, Owner: e.actor, Sources: proof.Sources, Nonce: ""}
	if raw := s.VirtualMachine().Annotations[journal.VMReplayAnnotation]; raw != "" {
		original, err := journal.ParseVMReplaySelection(raw)
		if err != nil {
			return pgexec.AwaitingFact, fmt.Errorf("original VM replay selection is malformed")
		}
		if original.Key == key && original.Owner == e.actor && original.Sources == proof.Sources && original.Nonce != proof.Receipt.Intent.ReplayNonce {
			// Select回复丢失亦只复用已提交的原选择；下面仍重新核当前来源。
			choice = original
		}
	}
	if choice.Nonce == "" {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return pgexec.AwaitingFact, err
		}
		choice.Nonce = hex.EncodeToString(nonce[:])
		if choice.Validate() != nil || choice.Nonce == proof.Receipt.Intent.ReplayNonce {
			return pgexec.AwaitingFact, fmt.Errorf("explicit VM replay requires a fresh nonzero nonce")
		}
	}
	if err := e.claims.Recheck(ctx, s, p, c.HostClaim, observed); err != nil {
		return pgexec.AwaitingFact, err
	}
	latest, err := e.runner.UnsentOriginal(ctx, c, step.CommandSHA256, e.actor)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if latest.Sources != proof.Sources {
		return pgexec.AwaitingFact, fmt.Errorf("original not-sent receipt changed during explicit selection")
	}
	if err := e.sameSnapshot(ctx, s); err != nil {
		return pgexec.AwaitingFact, err
	}
	_, err = e.store.SelectVMReplay(ctx, s, e.actor, choice)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}
