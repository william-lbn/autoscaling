//go:build !linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
)

// 非Linux没有原PIDFD/seqpacket/cgroup与只读Source链，固定CLI必须明确拒绝。
func RunNormalSystemRoleBroker(context.Context) error {
	return errors.Join(ErrUnknown, fmt.Errorf("fixed System role broker requires Linux Guest/root"))
}
func RunNormalSystemRoleClient(context.Context, string) (int, error) {
	return 0, errors.Join(ErrUnknown, fmt.Errorf("fixed System role client requires Linux Guest/root"))
}
