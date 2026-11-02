//go:build linux

package resourceguard

import (
	"errors"
	"fmt"
	"log"
	"syscall"
)

// normalQuotaFailure仅向既有Root日志写固定阶段和errno，不输出原错误、请求或来源。
// 返回同一个原错误对象；不会改变Unknown、HTTP判定、持有状态或恢复选择。
func normalQuotaFailure(stage uint16, err error) error {
	if err != nil {
		log.Print(normalQuotaFailureLine(stage, err))
	}
	return err
}

func normalQuotaFailureLine(stage uint16, err error) string {
	var errno syscall.Errno
	_ = errors.As(err, &errno)
	return fmt.Sprintf("DSGA_NORMAL_RPC_FAILURE stage=%d errno=%d", stage, uint64(errno))
}
