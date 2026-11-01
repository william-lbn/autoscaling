//go:build linux

package resourceguard

import (
	"context"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"testing"
)

// 真实原pidfd各状态；不注入Poll替身，不将整数构造当Source正控。
func TestArrayPIDFDObservationOriginalObjects(t *testing.T) {
	fd, e := unix.PidfdOpen(os.Getpid(), 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(fd)
	t.Run("live-original", func(t *testing.T) {
		o, e := arrayRegistryPIDFDAliveObservation(fd)
		if e != nil || !o.FDPresent || !o.PollCalled || o.N != 0 || o.Revents != 0 || o.ErrnoKnown {
			t.Fatalf("%+v %v", o, e)
		}
		if e = arrayRegistryPIDFDAlive(fd); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("missing-original", func(t *testing.T) {
		o, e := arrayRegistryPIDFDAliveObservation(-1)
		if e == nil || e.Error() != "original pidfd missing" || o.FDPresent || o.PollCalled {
			t.Fatalf("%+v %v", o, e)
		}
	})
	t.Run("closed-descriptor-is-unknown-not-exit-proof", func(t *testing.T) {
		bad, e := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 10000)
		if e != nil {
			t.Fatal(e)
		}
		if e = unix.Close(bad); e != nil {
			t.Fatal(e)
		}
		o, e := arrayRegistryPIDFDAliveObservation(bad)
		if e == nil || !o.PollCalled || o.N != 1 || o.Revents != unix.POLLNVAL || o.ErrnoKnown {
			t.Fatalf("%+v %v", o, e)
		}
	})
	t.Run("original-confirmed-exit", func(t *testing.T) {
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
		o, e := arrayRegistryPIDFDAliveObservation(child)
		if e == nil || !o.PollCalled || o.N != 1 || o.Revents&unix.POLLIN == 0 || o.ErrnoKnown {
			t.Fatalf("%+v %v", o, e)
		}
		if arrayRegistryPIDFDAlive(child) == nil {
			t.Fatal("legacy wrapper accepted exited original")
		}
	})
}
func TestNormalRoleDiagnosticPeerPollSnapshotAndAbsent(t *testing.T) {
	// absent ctx必须连额外Fstat都不执行（bad fd仅作为不可用输入，不作为许可）。
	if x := normalSystemRoleDiagnosticPeerBaseline(nil, &normalSystemOriginalTask{pidfd: -1}); x.BaselineCaptured {
		t.Fatal(x)
	}
	fd, e := unix.PidfdOpen(os.Getpid(), 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(fd)
	d := &normalSystemRoleDiagnostic{}
	ctx := context.WithValue(context.Background(), normalSystemRoleDiagnosticKey{}, d)
	peer := &normalSystemOriginalTask{source: normalSystemOriginalTaskSource{PID: os.Getpid()}, pidfd: fd}
	baseline := normalSystemRoleDiagnosticPeerBaseline(ctx, peer)
	if !baseline.BaselineCaptured || !baseline.BeforeMonitor.StatOK || !baseline.BeforeMonitor.StatFSOK || baseline.BeforeMonitor.Inode == 0 {
		t.Fatal(baseline)
	}
	poll, e := arrayRegistryPIDFDAliveObservation(fd)
	if e != nil {
		t.Fatal(e)
	}
	// 这里只验私有观测的数据复制，不把手工记录当真实kill或Source正控。
	normalSystemRoleDiagnosticRecordPeerPoll(ctx, "udev-init", 123, baseline, poll)
	a := d.snapshot()
	if a.Count != 1 || a.Events[0].PeerPIDFD == nil {
		t.Fatal(a)
	}
	x := a.Events[0].PeerPIDFD
	if x.OriginalPeerPID != os.Getpid() || x.OriginalPoll.N != 0 || !x.AfterKillWait.StatOK || x.AfterKillWait.Device != x.BeforeMonitor.Device || x.AfterKillWait.Inode != x.BeforeMonitor.Inode {
		t.Fatal(x)
	}
	x.BeforeMonitor.Inode = 0
	if d.snapshot().Events[0].PeerPIDFD.BeforeMonitor.Inode == 0 {
		t.Fatal("snapshot mutated fixed private observation")
	}

}
