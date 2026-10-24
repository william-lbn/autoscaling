package resourceguard

import (
	"context"
	"reflect"
	"testing"
)

// 真实签名/TLS/客户端/Guard及Mac文件WAL；kernel仍是明确夹具。
// 验证新增外层WAL恢复入口不会变成普通重试或覆盖另一个未知命令的通道。
func TestResourceRecordedDeliveryKeepsUnknownAndRequiresExplicitChoice(t *testing.T) {
	h, key, _, kernel, command := resourceHTTPFixture(t)
	client := resourceClientFixture(t, h, command, resourceToken(t, h, key, nil), nil)
	ctx := context.Background()
	if _, err := client.DeliverOriginal(ctx, command); err == nil {
		t.Fatal("explicit delivery without retained original accepted")
	}
	if err := client.RetainOriginal(command); err != nil {
		t.Fatal(err)
	}
	other := cloneCommand(command)
	other.BudgetCommitmentID = "different-original"
	if err := client.RetainOriginal(other); err == nil || !reflect.DeepEqual(*client.Pending(), command) {
		t.Fatal("another original replaced pending")
	}
	if _, err := client.Apply(ctx, command); err == nil || kernel.writes != 0 {
		t.Fatal("ordinary apply bypassed retained write-ahead uncertainty")
	}
	if _, err := client.Observe(ctx, command); err == nil || client.Pending() == nil || kernel.writes != 0 {
		t.Fatal("not-found inferred NoEffect or dispatched")
	}
	kernel.fail = true
	if _, err := client.DeliverOriginal(ctx, command); err == nil || client.Pending() == nil || kernel.writes != 1 {
		t.Fatal("partial explicit first delivery lost original")
	}
	// root已有部分效果：再次明确选择首次派发也不能跨过root未知栅栏；
	// 只有选择其原未知命令重放才可继续同步内核写，绝不透明fallback。
	if _, err := client.DeliverOriginal(ctx, command); err == nil || kernel.writes != 1 {
		t.Fatal("root unknown was replayed through normal apply")
	}
	kernel.mu.Lock()
	kernel.fail = false
	kernel.mu.Unlock()
	if _, err := client.ReplayOriginal(ctx, command); err != nil || client.Pending() != nil || kernel.writes != 2 {
		t.Fatal("explicit root original replay failed", err)
	}
}
