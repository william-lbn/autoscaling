//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
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

func TestArrayPIDFDListenerRestartOriginalStates(t *testing.T) {
	fd, e := unix.PidfdOpen(os.Getpid(), 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(fd)
	t.Run("live-original", func(t *testing.T) {
		o, e := arrayRegistryPIDFDAliveBounded(fd)
		if e != nil || o.N != 0 || o.Revents != 0 || o.Attempts < 1 || o.Attempts > 4 {
			t.Fatalf("%+v %v", o, e)
		}
	})
	t.Run("native birth original pin", func(t *testing.T) {
		dup, err := arrayRegistryDup(fd)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Close(dup)
		if err := arrayMatchPIDFD(fd, dup); err != nil {
			t.Fatal(err)
		}
		k := nativeBirthTraceKernel{nativeTraceOriginalControl: nativeTraceOriginalControl{pidfd: dup}}
		if err := k.alive(); err != nil {
			t.Fatal(err)
		}
		k.pidfd = -1
		if err := k.alive(); err == nil {
			t.Fatal("native birth adopted an absent pin")
		}
	})
	t.Run("missing-original", func(t *testing.T) {
		o, e := arrayRegistryPIDFDAliveBounded(-1)
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
		o, e := arrayRegistryPIDFDAliveBounded(bad)
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
		o, e := arrayRegistryPIDFDAliveBounded(child)
		if e == nil || o.Revents&unix.POLLIN == 0 || o.Attempts != 1 {
			t.Fatalf("%+v %v", o, e)
		}
	})
}

// 子进程内真实SIGURG中断真实Linux Poll；未观察EINTR就失败，不以模型序列冒机制。
func TestArrayPIDFDListenerRestartRealSignal(t *testing.T) {
	if os.Getenv("DSGA_PIDFD_REAL_SIGNAL_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestArrayPIDFDListenerRestartRealSignal$", "-test.v", "-test.timeout=10s")
		cmd.Env = append(os.Environ(), "DSGA_PIDFD_REAL_SIGNAL_CHILD=1")
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
		o, e := arrayRegistryPIDFDAliveBounded(fd)
		if o.Attempts > 4 {
			t.Fatalf("unbounded attempts %+v", o)
		}
		interrupted += o.Interruptions
		if e == unix.EINTR {
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
	fmt.Printf("ACTUAL_PIDFD_RESTART_SIGNAL_JSON %s\n", raw)
	if !observedSuccess || interrupted == 0 {
		t.Fatal("actual EINTR followed by successful original Poll was not observed")
	}
}
