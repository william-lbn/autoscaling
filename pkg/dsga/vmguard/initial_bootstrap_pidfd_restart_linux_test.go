//go:build linux

package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func TestInitialBootPIDFDOriginalStates(t *testing.T) {
	fd, e := unix.PidfdOpen(os.Getpid(), 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(fd)
	t.Run("live-original", func(t *testing.T) {
		o, e := initialBootPIDFDAliveRestart(fd)
		if e != nil || o.N != 0 || o.Revents != 0 || o.Attempts < 1 || o.Attempts > 4 {
			t.Fatalf("%+v %v", o, e)
		}
	})
	t.Run("missing-original", func(t *testing.T) {
		o, e := initialBootPIDFDAliveRestart(-1)
		if e == nil || o.PollCalled || o.Attempts != 0 {
			t.Fatalf("%+v %v", o, e)
		}
	})
	t.Run("invalid-descriptor-still-rejected", func(t *testing.T) {
		bad, e := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 10000)
		if e != nil {
			t.Fatal(e)
		}
		unix.Close(bad)
		o, e := initialBootPIDFDAliveRestart(bad)
		if e == nil || o.Revents != unix.POLLNVAL || o.Attempts != 1 {
			t.Fatalf("%+v %v", o, e)
		}
	})
	t.Run("confirmed-exit-still-rejected", func(t *testing.T) {
		cmd := exec.Command("/bin/sh", "-c", "read ignored; exit 0")
		in, e := cmd.StdinPipe()
		if e != nil {
			t.Fatal(e)
		}
		if e = cmd.Start(); e != nil {
			t.Fatal(e)
		}
		defer cmd.Process.Kill()
		child, e := unix.PidfdOpen(cmd.Process.Pid, 0)
		if e != nil {
			in.Close()
			cmd.Wait()
			t.Fatal(e)
		}
		defer unix.Close(child)
		in.Close()
		if e = cmd.Wait(); e != nil {
			t.Fatal(e)
		}
		o, e := initialBootPIDFDAliveRestart(child)
		if e == nil || o.Revents&unix.POLLIN == 0 || o.Attempts != 1 {
			t.Fatalf("%+v %v", o, e)
		}
	})
}

// 子进程内真实SIGURG中断真实Linux Poll；未观察EINTR就失败，不以模型序列冒机制。
func TestInitialBootPIDFDRestartRealSignal(t *testing.T) {
	if os.Getenv("DSGA_INITIAL_PIDFD_REAL_SIGNAL_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInitialBootPIDFDRestartRealSignal$", "-test.v", "-test.timeout=10s")
		cmd.Env = append(os.Environ(), "DSGA_INITIAL_PIDFD_REAL_SIGNAL_CHILD=1")
		out, e := cmd.CombinedOutput()
		fmt.Print(string(out))
		if e != nil {
			t.Fatalf("actual signal child: %v", e)
		}
		return
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	fd, e := unix.PidfdOpen(os.Getpid(), 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(fd)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGURG)
	defer signal.Stop(signals)
	stop := make(chan struct{})
	done := make(chan struct{})
	pid, tid := os.Getpid(), unix.Gettid()
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = unix.Tgkill(pid, tid, unix.SIGURG)
			time.Sleep(25 * time.Microsecond)
		}
	}()
	defer func() { close(stop); <-done }()
	deadline := time.Now().Add(5 * time.Second)
	calls, interrupted, exhausted := 0, 0, 0
	observedSuccess := false
	for calls < 2000000 && time.Now().Before(deadline) {
		calls++
		o, e := initialBootPIDFDAliveRestart(fd)
		if o.Attempts > 4 {
			t.Fatalf("unbounded attempts %+v", o)
		}
		interrupted += o.Interruptions
		if errors.Is(e, unix.EINTR) {
			if o.Attempts != 4 {
				t.Fatalf("premature exhaustion %+v", o)
			}
			exhausted++
			continue
		}
		if e != nil {
			t.Fatalf("unexpected original result %+v %v", o, e)
		}
		if o.N != 0 || o.Revents != 0 {
			t.Fatalf("accepted unsuccessful Poll %+v", o)
		}
		if o.Interruptions > 0 {
			observedSuccess = true
			break
		}
	}
	raw, _ := json.Marshal(map[string]any{"actual_calls": calls, "observed_EINTR_attempts": interrupted, "four_attempt_exhaustions": exhausted, "actual_success_after_EINTR": observedSuccess, "same_original_PIDFD": true, "actual_signal": "SIGURG", "synthetic_Poll_sequence": false})
	fmt.Printf("ACTUAL_INITIAL_PIDFD_RESTART_SIGNAL_JSON %s\n", raw)
	if !observedSuccess || interrupted == 0 {
		t.Fatal("actual EINTR followed by successful original Poll was not observed")
	}
}

// 原signal0和固定proc启动身份检查仍必须真实执行，不能只凭一次Poll签存活。
func TestInitialBootPIDFDOriginalProcessChecks(t *testing.T) {
	fd, e := unix.PidfdOpen(os.Getpid(), 0)
	if e != nil {
		t.Fatal(e)
	}
	proc, e := os.Open("/proc/self")
	if e != nil {
		unix.Close(fd)
		t.Fatal(e)
	}
	p := &initialBootProcess{pid: os.Getpid(), pidfd: fd, proc: proc}
	defer p.close()
	p.start, e = p.startTime()
	if e != nil {
		t.Fatal(e)
	}
	if e = p.alive(); e != nil {
		t.Fatal(e)
	}
	p.start++
	if e = p.alive(); e == nil {
		t.Fatal("changed original proc start identity accepted")
	}
}
