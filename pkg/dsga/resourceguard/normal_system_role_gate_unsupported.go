//go:build !linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
)

// 固定Root Gate只支持已验证的Linux原进程/命名空间/ptrace合同。
// 非Linux只恢复公共CLI编译，绝不提供替代执行或补签来源。
func RunNormalSystemRoleExecGate(context.Context) error {
	return errors.Join(ErrUnknown, fmt.Errorf("fixed System role exec Gate requires Linux Guest"))
}
