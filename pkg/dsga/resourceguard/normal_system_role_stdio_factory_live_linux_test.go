//go:build linux

package resourceguard

import (
	"context"
	"golang.org/x/sys/unix"
	"os/exec"
	"syscall"
	"testing"
)

// 只验证cheap移交状态，不从raw factory/本测试child建立任何FS/proof/Source。
func TestNormalSystemRoleStdioFreshFactoryLiveGate(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "read ignored")
	cmd.Env = []string{}
	pidfd := -1
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &pidfd}
	input, e := cmd.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer unix.Close(pidfd)
	waited := false
	defer func() {
		input.Close()
		if !waited {
			_ = unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0)
			_ = cmd.Wait()
		}
	}()
	makeFactory := func() *normalSystemRoleStdioFactory {
		fd, e := unix.FcntlInt(uintptr(pidfd), unix.F_DUPFD_CLOEXEC, 3)
		if e != nil {
			t.Fatal(e)
		}
		actor := &normalSystemOriginalTask{pidfd: fd, procfd: -1}
		t.Cleanup(actor.close)
		return &normalSystemRoleStdioFactory{authority: &normalSystemResourceAuthority{admission: &normalAuthorityAdmission{}}, filesystem: &normalFilesystemOriginSource{}, proof: &normalSystemRoleBrokerExecProof{}, actor: actor}
	}
	for _, name := range []string{"live-only-not-Source", "nil-context", "cancelled", "filesystem-closed", "proof-closed", "factory-closed", "actor-closed", "admission-failed", "commit-absent", "nil-filesystem", "nil-proof"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := makeFactory()
			requireCommit := false
			switch name {
			case "nil-context":
				ctx = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "filesystem-closed":
				f.filesystem.mu.Lock()
				f.filesystem.closed = true
				f.filesystem.mu.Unlock()
			case "proof-closed":
				f.proof.mu.Lock()
				f.proof.closed = true
				f.proof.mu.Unlock()
			case "factory-closed":
				f.closed.Store(true)
			case "actor-closed":
				f.actor.close()
			case "admission-failed":
				f.authority.admission.failed.Store(true)
			case "commit-absent":
				requireCommit = true
			case "nil-filesystem":
				f.filesystem = nil
			case "nil-proof":
				f.proof = nil
			}
			e := f.checkExternalSourcesLive(ctx, requireCommit)
			if name == "live-only-not-Source" {
				if e != nil {
					t.Fatal(e)
				}
			} else if e == nil {
				t.Fatal("closed/cancelled/failed gate accepted")
			}
		})
	}
	f := makeFactory()
	input.Close()
	_ = cmd.Wait()
	waited = true
	if e := f.checkExternalSourcesLive(context.Background(), false); e == nil {
		t.Fatal("exited original pidfd accepted")
	}
}
