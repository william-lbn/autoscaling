package resourceexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// CancelledAudit仅证明当前GuestReady命令从未进入Root；原资源上界保持。
// Prepared/VM/PG和实际完整预算仍由后续Cleanup独立认证，不把本回执升级
// 成整次resize取消完成。保留原命令与Root完整屏障供跨owner/丢回复恢复。
type CancelledAudit struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Query           bool                       `json:"query"`
	Cancellation    resourceguard.Cancellation `json:"cancellation"`
}

func (v *CancelledAudit) UnmarshalJSON(raw []byte) error {
	type plain CancelledAudit
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	*v = CancelledAudit(p)
	return nil
}

func (e *Executor) cancelUnreceived(ctx context.Context, before *journal.Snapshot, command resourceguard.Command, view resourceguard.View) (pgexec.Progress, error) {
	op := before.State().Active
	if op == nil || !op.AbortRequested {
		return pgexec.AwaitingFact, fmt.Errorf("resource cancellation requires retained abort selection")
	}
	step, _ := guestStep(op)
	if step == nil || step.Phase != journal.Unconfirmed {
		return pgexec.AwaitingFact, fmt.Errorf("resource cancellation lacks original pending dispatch")
	}
	var receipt resourceguard.Cancellation
	var err error
	query := view.Cancelled != nil && reflect.DeepEqual(*view.Cancelled, command)
	if query {
		receipt, err = e.resources.ObserveCancellation(ctx, command)
	} else {
		// 数字/NotFound只决定“尝试哪个领域接口”，不能直接生成NoEffect。
		// 最终Root仍串行核完整原日志/出生、确认旧Held并fsync防迟到屏障。
		receipt, err = e.resources.CancelOriginal(ctx, command)
	}
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := receipt.ValidateFor(command, e.actor.Epoch); err != nil {
		return pgexec.AwaitingFact, err
	}
	after, err := e.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	current := after.State()
	if !after.Dispatchable() || current.Owner != e.actor || current.Active == nil || current.Active.Request != op.Request || !current.Active.AbortRequested || current.ResourceIdentity == nil || *current.ResourceIdentity != e.id {
		return pgexec.AwaitingFact, fmt.Errorf("resource cancellation original owner or operation changed")
	}
	now, _ := guestStep(current.Active)
	if now == nil || now.Phase != journal.Unconfirmed || now.CommandSHA256 != step.CommandSHA256 || !bytes.Equal(now.Command, step.Command) {
		return pgexec.AwaitingFact, fmt.Errorf("resource cancellation original dispatch changed")
	}
	data, err := json.Marshal(CancelledAudit{ProtocolVersion: 1, Query: query, Cancellation: receipt})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	_, err = e.store.Record(ctx, after, e.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: journal.GuestReady, Result: journal.NoEffect, SourceID: step.TargetID, Data: data})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}
