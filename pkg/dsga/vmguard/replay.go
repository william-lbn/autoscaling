package vmguard

import (
	"context"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// UnsentOriginal仅供受信显式重放入口查询：原不可变not_sent、原owner、当前
// 无pending且真实before未变。它不将not_sent提升为完成，不修改任何来源。
func (r *Reader) UnsentOriginal(ctx context.Context, c Command, journalSHA string, actor journal.Owner) (Confirmed, error) {
	var out Confirmed
	if c.Validate() != nil || !digestValid(journalSHA) || c.Request.Key.IssuerEpoch != actor.Epoch {
		return out, fmt.Errorf("invalid original not-sent query")
	}
	birth, _, p, err := r.Inspect(ctx, &c.BindingClaim, actor)
	if err != nil {
		return out, err
	}
	if p.State.LastPublished == nil || p.State.Last == nil {
		return out, ErrUnknown
	}
	sources := budget.RunnerSources{BirthClaim: c.BindingClaim, StateClaim: birth.StateClaim, ReceiptClaim: *p.State.LastPublished, CommandSHA256: p.State.Last.Intent.CommandSHA256, JournalSHA256: journalSHA}
	return r.confirmKind(ctx, sources, c, actor, notSent)
}

// replaySelection只读取同VM的持久选择。旧Key提示可保留作为审计，但不能
// 授权新Key；当前选择必须精确绑定原不可变not_sent UID和本次原命令。
// 可变发布滞后不能授权重复尝试，engine还在文件锁内核对本地LastPublished。
func (r *Runner) replaySelection(ctx context.Context, s *journal.Snapshot, c Command, outer string, pub Publication) (string, budget.ObjectRef, error) {
	var none budget.ObjectRef
	raw := s.VirtualMachine().Annotations[journal.VMReplayAnnotation]
	if raw == "" {
		return "", none, nil
	}
	choice, err := journal.ParseVMReplaySelection(raw)
	if err != nil {
		return "", none, fmt.Errorf("malformed explicit VM replay selection")
	}
	if choice.Key != c.Request.Key {
		return "", none, nil
	}
	hash, err := c.digest()
	if err != nil || choice.Owner != s.State().Owner || choice.Sources.BirthClaim != r.engine.binding || choice.Sources.StateClaim != r.birth.StateClaim || choice.Sources.CommandSHA256 != hash || choice.Sources.JournalSHA256 != outer {
		return "", none, fmt.Errorf("explicit VM replay changed original owner/command/source")
	}
	if pub.State == nil || pub.State.Last == nil || pub.State.LastPublished == nil || *pub.State.LastPublished != choice.Sources.ReceiptClaim {
		// 原提示已消费后不得对后来的not_sent重用nonce。这里只取消提示的
		// 新写资格，原pending只读恢复始终由engine优先处理。
		return "", none, nil
	}
	result, err := ReadReceipt(ctx, r.core, r.store.Binding(), choice.Sources.ReceiptClaim, c, outer)
	if err != nil {
		return "", none, err
	}
	if !result.NotSent() || result.Intent.Owner != choice.Owner || choice.Nonce == result.Intent.ReplayNonce {
		return "", none, fmt.Errorf("explicit replay lacks original not-sent receipt and fresh nonce")
	}
	return choice.Nonce, choice.Sources.ReceiptClaim, nil
}
