package resourceguard

import (
	"context"
	"reflect"
)

// CancelOriginal要求SDK已经持有同一完整外层原命令。取消是独立显式/持久
// Abort选择的领域动作，不是Observe失败后的通用重试。HTTP丢回复仍保留
// Pending；只能读同一Root取消屏障或重试相同取消，不重发Apply。
func (c *Client) CancelOriginal(ctx context.Context, command Command) (Cancellation, error) {
	command = cloneCommand(command)
	if command.Validate() != nil || command.Identity != c.config.Identity || command.IssuerEpoch > c.config.IssuerEpoch {
		return Cancellation{}, &ClientError{MayHaveExecuted: false, Code: "invalid_resource_cancellation"}
	}
	c.mu.Lock()
	if c.busy || c.repair != nil || c.pending == nil || !reflect.DeepEqual(*c.pending, command) {
		c.mu.Unlock()
		return Cancellation{}, &ClientError{MayHaveExecuted: false, Code: "original_pending"}
	}
	c.busy = true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.busy = false; c.mu.Unlock() }()
	var result Cancellation
	request := CancelRequest{ProtocolVersion: 1, IssuerEpoch: c.config.IssuerEpoch, Command: command}
	if err := c.call(ctx, false, "cancel-original", request, &result); err != nil {
		return Cancellation{}, err
	}
	if err := result.ValidateFor(command, c.config.IssuerEpoch); err != nil {
		return Cancellation{}, &ClientError{MayHaveExecuted: true, Code: "resource_cancellation_unconfirmed"}
	}
	c.clearCancelledOriginal(command)
	return result, nil
}

// ObserveCancellation只读取原State；取消回复丢失后不再发POST。完整屏障、
// 原出生、当前floor和实际旧限额缺一不可；普通NotFound不能清SDK Pending。
func (c *Client) ObserveCancellation(ctx context.Context, command Command) (Cancellation, error) {
	command = cloneCommand(command)
	view, err := c.InspectState(ctx)
	if err != nil {
		return Cancellation{}, err
	}
	result := Cancellation{ProtocolVersion: 1, VMShrinkPermit: false, Command: command, State: view}
	if err := result.ValidateFor(command, c.config.IssuerEpoch); err != nil {
		return Cancellation{}, ErrUnknown
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.busy || c.repair != nil || c.pending != nil && !reflect.DeepEqual(*c.pending, command) {
		return Cancellation{}, ErrUnknown
	}
	c.pending = nil
	return result, nil
}

func (c *Client) clearCancelledOriginal(command Command) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending != nil && reflect.DeepEqual(*c.pending, command) {
		c.pending = nil
	}
}
