package vmguard

import (
	"context"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// Cancellation记录Runner真实认证的原VM取消快照，不是调用者自报NoEffect。
// VMSequence属于VM WAL，与Runner本地Sequence是不同域，不能互相比数字。
// 该证明必须在原flock内、确认原命令从未越过may_have_sent屏障后持久保存。
// 取消之后保留原高水位与完整命令，拒绝任何迟到原重放；不释放外部预算。
type Cancellation struct {
	ProtocolVersion   uint32         `json:"protocol_version"`
	Owner             journal.Owner  `json:"owner"`
	VMSequence        control.Uint64 `json:"vm_sequence"`
	VMResourceVersion string         `json:"vm_resource_version"`
}

type cancellationAuthority struct {
	Origin  journal.Owner
	Witness Cancellation
}

func (c Cancellation) validate() error {
	if c.ProtocolVersion != 1 || c.Owner.Validate() != nil || c.VMSequence == 0 || control.ValidateID(c.VMResourceVersion, "original VM resource version") != nil {
		return fmt.Errorf("invalid original VM cancellation source")
	}
	return nil
}

func (c *Cancellation) UnmarshalJSON(data []byte) error {
	type plain Cancellation
	var decoded plain
	if err := strictDecode(data, &decoded); err != nil {
		return err
	}
	next := Cancellation(decoded)
	if err := next.validate(); err != nil {
		return err
	}
	*c = next
	return nil
}

func validateCancellation(r record, result receipt) error {
	if result.Kind != cancelledBeforeSend {
		if result.Cancellation != nil {
			return fmt.Errorf("ordinary Runner result cannot carry a cancellation witness")
		}
		return nil
	}
	c := result.Cancellation
	if r.ProtocolVersion != 2 || c == nil || c.validate() != nil || result.Intent.Stage != prepared || result.Actual != result.Intent.Before ||
		c.Owner.Epoch < result.Intent.Owner.Epoch || c.Owner.Epoch > r.Floor.Epoch || (c.Owner.Epoch == r.Floor.Epoch && c.Owner != r.Floor) || (c.Owner.Epoch == result.Intent.Owner.Epoch && c.Owner != result.Intent.Owner) {
		return fmt.Errorf("Runner cancellation has no exact durable unsent original/fenced owner")
	}
	return nil
}

// cancelBeforeSend与正常发送使用同一原文件锁。它只在真实取消标志存在时处理
// prepared/not_sent或尚未接收的原命令；may_have_sent必须继续只读对账，哪怕
// 当前内存等于before也不能消除晚到QMP。两次原Kube认证包围原QEMU读回，
// 任一身份/floor/取消改变便不写终态。文件保存失败保留unknown，不返回票据。
func (e *engine) cancelBeforeSend(ctx context.Context, r *record, s *lockedState, a authorization) (bool, error) {
	if a.Abort == nil || a.Command == nil {
		return false, nil
	}
	if a.Abort.Witness.validate() != nil || a.Abort.Witness.Owner != a.Owner || a.Abort.Origin.Validate() != nil || a.Abort.Origin.Epoch != a.Command.Request.Key.IssuerEpoch || a.Abort.Origin.Epoch > a.Owner.Epoch || (a.Abort.Origin.Epoch == a.Owner.Epoch && a.Abort.Origin != a.Owner) {
		return true, fmt.Errorf("Runner cancellation changed original origin or current owner")
	}
	if r.Pending != nil && (r.Pending.Stage == mayHaveSent || r.Pending.Command != *a.Command || r.Pending.JournalSHA256 != a.JournalSHA256) {
		return false, nil
	}
	c := *a.Command
	hash, err := c.digest()
	if err != nil || c.Identity != e.id || c.BindingClaim != e.binding || !digestValid(a.JournalSHA256) {
		return true, fmt.Errorf("Runner cancellation changed original command or birth")
	}
	key := c.Request.Key
	original := intent{Command: c, CommandSHA256: hash, JournalSHA256: a.JournalSHA256, Owner: a.Abort.Origin, Stage: prepared, ReplayNonce: "", Before: Observation{RequestedBytes: c.BeforeBytes, ActualBytes: c.BeforeBytes}}
	if r.Pending != nil {
		original = *r.Pending
	} else if r.HighWater != nil {
		order := compareKey(key, *r.HighWater)
		if key.Instance != r.HighWater.Instance || order < 0 {
			return true, fmt.Errorf("Runner cancellation is below original high water or changed PG boot")
		}
		if order == 0 {
			if key != *r.HighWater || r.Last == nil || r.Last.Intent.Command != c || r.Last.Intent.CommandSHA256 != hash || r.Last.Intent.JournalSHA256 != a.JournalSHA256 {
				return true, fmt.Errorf("Runner cancellation changed retained full original command")
			}
			if r.Last.Kind == cancelledBeforeSend {
				return true, nil
			}
			if r.Last.Kind == completed {
				return false, nil
			}
			original = r.Last.Intent
		} else if r.Last != nil && r.LastPublished == nil {
			return true, ErrUnknown
		}
	}
	if original.Owner != a.Abort.Origin || original.Stage != prepared || original.CommandSHA256 != hash {
		return true, fmt.Errorf("Runner cancellation differs from original dispatch owner or unsent stage")
	}
	actual, err := e.native.Read(ctx)
	if err != nil {
		return true, err
	}
	if actual.validate(e.id) != nil || actual != original.Before {
		return true, ErrUnknown
	}
	latest, err := e.read(ctx)
	if err != nil {
		return true, err
	}
	if latest.Abort == nil || latest.Command == nil || *latest.Command != c || latest.Owner != a.Owner || latest.LeaseUID != a.LeaseUID || latest.JournalSHA256 != a.JournalSHA256 || latest.Abort.Origin != original.Owner || latest.Abort.Witness.validate() != nil || latest.Abort.Witness.Owner != latest.Owner || latest.Abort.Witness.VMSequence < a.Abort.Witness.VMSequence || ctx.Err() != nil {
		return true, fmt.Errorf("Runner original cancellation changed during physical baseline readback")
	}
	witness := latest.Abort.Witness
	r.ProtocolVersion = 2
	r.HighWater = &key
	r.Last = &receipt{Intent: original, Kind: cancelledBeforeSend, Actual: actual, Cancellation: &witness}
	r.LastPublished, r.Pending = nil, nil
	return true, saveNext(s, r)
}
