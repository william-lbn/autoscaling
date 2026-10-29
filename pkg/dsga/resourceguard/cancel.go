package resourceguard

import (
	"context"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// CancelRequest用当前owner选择外层已经写前持久的完整原命令。Command中的
// 旧epoch/出生/目标一字不改；IssuerEpoch只授权本次取消，不授权重放旧命令。
// 取消仅处理Root根本未接收的窗口，已有部分效果一律保留未知。
type CancelRequest struct {
	ProtocolVersion uint32         `json:"protocol_version"`
	IssuerEpoch     control.Uint64 `json:"issuer_epoch"`
	Command         Command        `json:"command"`
}

func (r CancelRequest) Validate() error {
	if r.ProtocolVersion != 1 || r.IssuerEpoch == 0 || r.Command.Validate() != nil || r.Command.IssuerEpoch > r.IssuerEpoch {
		return fmt.Errorf("invalid original resource cancellation")
	}
	return nil
}

func (r *CancelRequest) UnmarshalJSON(raw []byte) error {
	type plain CancelRequest
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	next := CancelRequest(p)
	if err := next.Validate(); err != nil {
		return err
	}
	*r = next
	return nil
}

// Cancellation证明Root在同一文件锁内核对“从未收到该revision”、确认原
// Held/Bootstrap并fsync禁止迟到Apply的屏障。它只证明此资源命令NoEffect，
// 不是PG/VM取消或Host退还许可。网络接收方必须核完整原命令和当前floor。
type Cancellation struct {
	ProtocolVersion uint32  `json:"protocol_version"`
	VMShrinkPermit  bool    `json:"vm_shrink_permit"`
	Command         Command `json:"command"`
	State           View    `json:"state"`
}

func (c Cancellation) ValidateFor(command Command, epoch control.Uint64) error {
	if c.ProtocolVersion != 1 || c.VMShrinkPermit || command.Validate() != nil || !reflect.DeepEqual(c.Command, command) || c.State.Identity != command.Identity || c.State.IssuerFloorEpoch != epoch || command.IssuerEpoch > epoch || c.State.Cancelled == nil || !reflect.DeepEqual(*c.State.Cancelled, command) || c.State.Birth == nil || !c.State.Birth.matches(command) || c.State.Original != nil && c.State.Original.Revision >= command.Revision {
		return fmt.Errorf("resource cancellation lacks complete original barrier")
	}
	_, err := c.State.ConfirmedLimits()
	return err
}

func (c *Cancellation) UnmarshalJSON(raw []byte) error {
	type plain Cancellation
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	next := Cancellation(p)
	if err := next.ValidateFor(next.Command, next.State.IssuerFloorEpoch); err != nil {
		return err
	}
	*c = next
	return nil
}

func (r *record) nextRevision() (control.Uint64, error) {
	return nextResourceRevision(r.Original, r.Cancelled)
}

func nextResourceRevision(original, cancelled *Command) (control.Uint64, error) {
	previous := control.Uint64(0)
	for _, c := range []*Command{original, cancelled} {
		if c != nil && c.Revision > previous {
			previous = c.Revision
		}
	}
	if previous == control.Uint64(math.MaxUint64) {
		return 0, fmt.Errorf("resource revision exhausted")
	}
	return previous + 1, nil
}

// CancelOriginal与Apply/Replay/Fence使用相同的文件锁。先验证完整原来源、
// floor、出生与当前原约束，再fsync取消屏障，最后才返回NoEffect证据。
// 若Apply先取得锁，它必先持久Original；这里拒绝取消，即使实际数值恰好
// 仍等于旧值。若取消先落盘，Apply的连续revision检查拒绝迟到命令。
// 没有回滚资源写、重建日志、改绑PG或替代任何未确认效果的分支。
func (g *Guard) CancelOriginal(ctx context.Context, request CancelRequest) (Cancellation, error) {
	var result Cancellation
	if err := request.Validate(); err != nil {
		return result, err
	}
	err := g.withRecord(ctx, func(r *record, state *lockedState) error {
		command := request.Command
		if request.IssuerEpoch != r.Floor || command.Identity != r.Identity || r.Bootstrap == nil || !r.Bootstrap.Applied || r.Birth == nil || !r.Birth.matches(command) || command.PidsMaxTasks != r.taskLimit() || command.SystemMemoryMaxBytes != r.systemLimit() || command.AddressSpaceMaxBytes != r.addressSpaceLimit() {
			return fmt.Errorf("resource cancellation original source or floor differs")
		}
		if err := g.kernel.VerifyProcess(*r.Birth); err != nil {
			return err
		}
		same := r.Cancelled != nil && reflect.DeepEqual(*r.Cancelled, command)
		if r.Original != nil && (!r.Applied || r.Original.Revision >= command.Revision) {
			return fmt.Errorf("%w: resource original was already received", ErrUnknown)
		}
		if !same {
			next, err := r.nextRevision()
			if err != nil || command.Revision != next {
				return fmt.Errorf("resource cancellation revision is not consecutive")
			}
		}
		limits := r.Bootstrap.Original.Limits
		if r.Held != nil {
			limits = r.Held.limits()
		}
		actual, err := g.kernel.ReadBounds(r.Identity, limits)
		if err != nil {
			return err
		}
		view := r.view(actual)
		if _, err := view.ConfirmedLimits(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !same {
			cancelled := cloneCommand(command)
			r.Cancelled = &cancelled
			// 格式升级与屏障同一次原子rename/fsync，不能让旧程序在回滚后
			// 把新字段当作可忽略metadata，从而失去防迟到执行的硬约束。
			r.Version = 2
			if err := state.save(r); err != nil {
				return fmt.Errorf("%w: resource cancellation persistence: %w", ErrUnknown, err)
			}
			view.Cancelled = r.Cancelled
		}
		result = Cancellation{ProtocolVersion: 1, VMShrinkPermit: false, Command: cloneCommand(command), State: view}
		return result.ValidateFor(command, request.IssuerEpoch)
	})
	return result, err
}
