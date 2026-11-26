//go:build !linux

package vmguard

import (
	"context"
	"fmt"
	"os/exec"
)

type cpuProcessObserver struct{}

func (r *Runner) captureCPUProcess(_ *exec.Cmd) {}
func (r *Runner) closeCPUProcess()              {}
func (r *Runner) WithOriginalCPUProcess(func(int, uint64) error) error {
	return fmt.Errorf("original CPU process source requires Linux")
}
func (r *Runner) OriginalCPUContainer(context.Context) (string, string, error) {
	return "", "", fmt.Errorf("original CPU container source requires Linux")
}
