//go:build linux

package resourceguard

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestNormalStockUdevSupervisorFixedStages(t *testing.T) {
	expected := [][]string{
		{"/neonvm/bin/udevd", "--children-max=1"},
		{"/neonvm/bin/udevadm", "trigger", "--type=subsystems", "--action=add"},
		{"/neonvm/bin/udevadm", "trigger", "--type=devices", "--action=add"},
		{"/neonvm/bin/udevadm", "settle", "--timeout=10"},
	}
	for i, want := range expected {
		got, e := normalOrdinaryStockUdevArgv(normalOrdinaryStockUdevStage(i + 1))
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("stage%d: %v %v", i+1, got, e)
		}
		got[0] = "/caller/cannot/replace"
		again, _ := normalOrdinaryStockUdevArgv(normalOrdinaryStockUdevStage(i + 1))
		if again[0] != want[0] {
			t.Fatal("caller mutated fixed command")
		}
	}
	for _, stage := range []normalOrdinaryStockUdevStage{0, 5, 255} {
		if _, e := normalOrdinaryStockUdevArgv(stage); !errors.Is(e, ErrUnknown) {
			t.Fatal("arbitrary execution stage allowed")
		}
	}
	if _, e := pinNormalOrdinaryStockUdevPrograms(nil); !errors.Is(e, ErrUnknown) {
		t.Fatal("nil System accepted")
	}
	if e := (*normalOrdinaryStockUdevPrograms)(nil).verify(); !errors.Is(e, ErrUnknown) {
		t.Fatal("nil pins accepted")
	}
}

func TestNormalStockUdevSupervisorBoundedConcurrentLog(t *testing.T) {
	b := &normalOrdinaryStockUdevLog{}
	original := bytes.Repeat([]byte{'a'}, 4096)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if n, e := b.Write(original); n != len(original) || e != nil {
				t.Errorf("blocked/truncated producer: %d %v", n, e)
			}
		}()
	}
	wg.Wait()
	raw, overflow := b.snapshot()
	if len(raw) != 32<<10 || !overflow || !bytes.Equal(raw, bytes.Repeat([]byte{'a'}, 32<<10)) {
		t.Fatal("unbounded or corrupted log")
	}
	raw[0] = 'z'
	again, _ := b.snapshot()
	if again[0] != 'a' {
		t.Fatal("mutable log snapshot")
	}
	for i := range original {
		original[i] = 'x'
	}
	if again[0] != 'a' {
		t.Fatal("retained caller buffer")
	}
}

func TestNormalStockUdevSupervisorOriginalWaitAndChildren(t *testing.T) {
	if os.Getenv("NEON_STOCK_WAIT_FIXTURE") == "1" {
		fmt.Println("ready")
		var b [1]byte
		_, _ = os.Stdin.Read(b[:])
		os.Exit(0)
	}

	// 仅本测试ELF的固定fixture，不运行Root stock程序，不授Source。
	fd := -1
	cmd := exec.Command("/proc/self/exe", "-test.run=^TestNormalStockUdevSupervisorOriginalWaitAndChildren$")
	cmd.Env = []string{"NEON_STOCK_WAIT_FIXTURE=1"}
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd}
	input, e := cmd.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	output, e := cmd.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	if fd < 0 {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("no original clone pidfd")
	}
	peer := os.NewFile(uintptr(fd), "original-test-clone")
	defer peer.Close()
	defer input.Close()
	p := &normalOrdinaryStockUdevProcess{cmd: cmd, pidfd: peer, done: make(chan error, 1)}
	ready, e := bufio.NewReader(output).ReadString('\n')
	if e != nil || ready != "ready\n" {
		_ = p.signal(unix.SIGKILL)
		_ = cmd.Wait()
		t.Fatalf("fixture not ready: %q %v", ready, e)
	}
	go func() { p.done <- cmd.Wait() }()
	defer func() {
		if !p.waited {
			_ = p.signal(unix.SIGKILL)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = p.wait(ctx)
		}
	}()
	children, e := normalOrdinaryStockUdevChildren()
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, pid := range children {
		found = found || pid == cmd.Process.Pid
	}
	if !found {
		t.Fatal("lost actual live direct child")
	}
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	e = p.wait(short)
	cancel()
	if !errors.Is(e, context.DeadlineExceeded) || p.waited {
		t.Fatal("timeout manufactured original Wait")
	}
	if e = input.Close(); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if e = p.wait(ctx); e != nil || !p.waited || p.witness.ExitCode != 0 {
		t.Fatalf("original Wait lost: %+v %v", p.witness, e)
	}
	if e = p.wait(ctx); !errors.Is(e, ErrUnknown) {
		t.Fatal("second Wait accepted")
	}
	children, e = normalOrdinaryStockUdevChildren()
	if e != nil {
		t.Fatal(e)
	}
	for _, pid := range children {
		if pid == cmd.Process.Pid {
			t.Fatal("reaped original child remains")
		}
	}
}

func TestNormalStockUdevSupervisorMissingWaitState(t *testing.T) {
	p := &normalOrdinaryStockUdevProcess{done: make(chan error, 1)}
	p.done <- nil
	if e := p.wait(context.Background()); !errors.Is(e, ErrUnknown) {
		t.Fatal("receipt without original process state accepted")
	}
}

func TestNormalStockUdevSupervisorKernelOverrideRejected(t *testing.T) {
	for _, raw := range []string{"", "quiet\x00x", "quiet\n", "udev.children-max=2", "rd.udev.children-max=2", "quiet udev.exec-delay=60", "x=\"udev.event-timeout=200\"", "other=udev.log-priority=debug", strings.Repeat("x", (64<<10)+1)} {
		if e := normalOrdinaryStockUdevCommandLine(raw); !errors.Is(e, ErrUnknown) {
			t.Fatalf("override accepted: %q", raw)
		}
	}
	for _, raw := range []string{"quiet", "console=ttyS0 panic=-1 neon.normal_split_metadata=1"} {
		if e := normalOrdinaryStockUdevCommandLine(raw); e != nil {
			t.Fatalf("fixed boot rejected: %v", e)
		}
	}
}
