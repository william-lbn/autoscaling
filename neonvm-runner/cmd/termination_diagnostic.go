package main

import (
	"fmt"
	"os"
	"syscall"

	"github.com/neondatabase/autoscaling/pkg/api/runnerdiagnostic"
)

// 终止消息只写有限枚举，不复制日志。失败写入不改变原启动失败、关闭WAL或
// 退出语义；Controller仍能从可信容器终态记录unknown。固定Kubelet挂载文件，
// 不创建替代路径、不跟随链接；非阻塞打开避免异常FIFO拖延原失败退出。
func writeRunnerFailure(cause error) {
	_ = writeRunnerFailureAt(runnerdiagnostic.TerminationPath, cause)
}

func writeRunnerFailureAt(path string, cause error) error {
	raw, err := runnerdiagnostic.Encode(runnerdiagnostic.FromError(cause))
	if err != nil || len(raw) > runnerdiagnostic.MaxBytes {
		return fmt.Errorf("invalid fixed runner diagnostic")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("termination message requires original regular file")
	}
	if err := file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.Write(append(raw, '\n')); err != nil {
		return err
	}
	return file.Sync()
}
