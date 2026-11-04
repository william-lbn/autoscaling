package vmguard

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// 私有标记仅由真实Start捕获后的原initial runCtx创建；不进入ready或持久化来源。
// q指针固定同一Birth构造的QMP；普通请求不能从公开接口构造此标记。
type initialQMPIOKey struct{}

func withInitialQMPIO(ctx context.Context, q *nativeQMP) context.Context {
	return context.WithValue(ctx, initialQMPIOKey{}, q)
}

func initialQMPIOEnabled(ctx context.Context, q *nativeQMP) bool {
	v, ok := ctx.Value(initialQMPIOKey{}).(*nativeQMP)
	return ok && q != nil && v == q
}

// 每个完整command只在唯一Write前设置一次窗口；Write和全部reply/event共享，
// 不按frame、partial write或重试续期。原outer ctx和2秒配置均不延长。
// 这里不连接、不授权、不写WAL，也不重放命令；失败不降级为NoEffect。
func initialQMPCommand(ctx context.Context, q *nativeQMP, c *qmpConnection, name string, args any) (json.RawMessage, error) {
	if !initialQMPIOEnabled(ctx, q) {
		return c.execute(name, args)
	}
	if c == nil || c.conn == nil || q.timeout < 100*time.Millisecond || q.timeout > 10*time.Second {
		return nil, fmt.Errorf("invalid initial QMP operation binding")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(q.timeout)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if !time.Now().Before(deadline) {
		return nil, context.DeadlineExceeded
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("initial QMP command %s deadline unconfirmed: %w", name, err)
	}
	// setter期间可能取消；仍未Write，必须先拒绝，不依赖异步Close调度。
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := c.execute(name, args)
	if err != nil {
		return nil, fmt.Errorf("initial QMP command %s unconfirmed: %w", name, err)
	}
	return raw, nil
}
