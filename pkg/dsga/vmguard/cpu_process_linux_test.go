//go:build linux

package vmguard

import (
	"errors"
	"os/exec"
	"testing"
)

func TestCPUObserverUsesOriginalStartBeforeWaitAndRejectsDeadProcess(t *testing.T) {
	runner := &Runner{cpuProcess: &cpuProcessObserver{}}
	if runner.WithOriginalCPUProcess(func(int, uint64) error { return nil }) == nil {
		t.Fatal("missing original Start was accepted")
	}
	command := exec.Command("/bin/sleep", "30")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
		runner.closeCPUProcess()
	}()
	runner.captureCPUProcess(command)
	want := errors.New("synthetic membership mismatch")
	if err := runner.WithOriginalCPUProcess(func(int, uint64) error { return want }); !errors.Is(err, want) {
		t.Fatal("membership check was ignored", err)
	}
	if err := runner.WithOriginalCPUProcess(func(pid int, start uint64) error {
		if pid != command.Process.Pid || start == 0 {
			t.Fatal("observer borrowed another PID or start")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	if runner.WithOriginalCPUProcess(func(int, uint64) error { return nil }) == nil {
		t.Fatal("reaped original process was treated as live")
	}
	runner.closeCPUProcess()
	replacement := exec.Command("/bin/sleep", "30")
	if err := replacement.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Process.Kill(); _ = replacement.Wait() }()
	runner.captureCPUProcess(replacement)
	if runner.WithOriginalCPUProcess(func(int, uint64) error { return nil }) == nil {
		t.Fatal("original observer silently rebound to replacement child")
	}
}
