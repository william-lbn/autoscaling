//go:build linux

package resourceguard

import (
	"context"
	"golang.org/x/sys/unix"
	"os/exec"
	"syscall"
	"testing"
)

// 该夹具只验证实例/引用比较，不是实际stock观察或Source。
func stockMemoryObservationRawTarget(t *testing.T) (*normalOrdinaryStockUdevPreparation, normalMemoryBrokerStockObservation) {
	t.Helper()
	source := stockMemoryObservationRawLifecycle(t)
	q := &normalOrdinaryUdevV2{g: source.g, filesystem: source.filesystem, proof: source.proof, authority: &normalSystemResourceAuthority{source: source.filesystem.system}, originalFS: normalFilesystemOriginReference{ProtocolVersion: 2}, kernel: normalKernelPrepV2HandoffReference{ProtocolVersion: 1}, memory: source.reference}
	p := &normalOrdinaryStockUdevPreparation{q: q}
	observation := normalMemoryBrokerStockObservation{source: source, guard: q.g, filesystem: q.filesystem, proof: q.proof, kernel: q.filesystem.kernelConsumer, filesystemReference: q.originalFS, kernelReference: q.kernel, memoryReference: q.memory, task: &normalSystemOriginalTask{pidfd: -1, procfd: -1}}
	return p, observation
}
func TestNormalOrdinaryStockPreparationObservationBinding(t *testing.T) {
	for _, name := range []string{"same-fields-only-not-Source", "nil-source", "nil-task", "wrong-source", "wrong-guard", "wrong-filesystem", "wrong-proof", "wrong-kernel", "source-other-guard", "source-other-filesystem", "source-other-proof", "source-other-kernel", "authority-other-system", "expected-filesystem", "expected-kernel", "expected-memory", "legacy-family"} {
		t.Run(name, func(t *testing.T) {
			p, o := stockMemoryObservationRawTarget(t)
			switch name {
			case "nil-source":
				o.source = nil
			case "nil-task":
				o.task = nil
			case "wrong-source":
				o.source = &normalMemoryBrokerConsumerSource{g: o.guard, filesystem: o.filesystem, proof: o.proof, kernel: o.kernel}
			case "wrong-guard":
				o.guard = &Guard{}
			case "wrong-filesystem":
				o.filesystem = &normalFilesystemOriginSource{g: o.guard, system: p.q.authority.source}
			case "wrong-proof":
				o.proof = &normalSystemRoleBrokerExecProof{system: p.q.authority.source}
			case "wrong-kernel":
				o.kernel = &normalKernelPrepV2BrokerConsumerSource{g: o.guard}
			case "source-other-guard":
				o.source.g = &Guard{}
			case "source-other-filesystem":
				o.source.filesystem = &normalFilesystemOriginSource{}
			case "source-other-proof":
				o.source.proof = &normalSystemRoleBrokerExecProof{}
			case "source-other-kernel":
				o.source.kernel = &normalKernelPrepV2BrokerConsumerSource{}
			case "authority-other-system":
				p.q.authority.source = &normalSystemLifecycleSource{}
			case "expected-filesystem":
				p.q.originalFS.IntentSHA256 = "different"
			case "expected-kernel":
				p.q.kernel.StateInode++
			case "expected-memory":
				p.q.memory.ProtocolVersion++
			case "legacy-family":
				o.memoryReference.InitialWALFamily = "legacy"
				p.q.memory = o.memoryReference // 相等引用仍必须独立拒legacy family。
			}
			e := o.matchStockTarget(p)
			if name == "same-fields-only-not-Source" {
				if e != nil {
					t.Fatal(e)
				}
			} else if e == nil {
				t.Fatal("different instance/reference accepted")
			}
		})
	}
}
func TestNormalOrdinaryStockPreparationObservationFailureClosesPin(t *testing.T) {
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
	defer func() { input.Close(); _ = unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0); _ = cmd.Wait() }()
	for _, name := range []string{"wrong-guard", "wrong-reference", "cancelled", "raw-matching-not-Source"} {
		t.Run(name, func(t *testing.T) {
			p, o := stockMemoryObservationRawTarget(t)
			fd, e := unix.FcntlInt(uintptr(pidfd), unix.F_DUPFD_CLOEXEC, 3)
			if e != nil {
				t.Fatal(e)
			}
			task := &normalSystemOriginalTask{pidfd: fd, procfd: -1}
			o.task = task
			ctx := context.Background()
			switch name {
			case "wrong-guard":
				o.guard = &Guard{}
			case "wrong-reference":
				o.memoryReference.ProtocolVersion++
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if e = p.consumeStockObservation(ctx, &o); e == nil {
				t.Fatal("raw tuple created Source")
			}
			if o != (normalMemoryBrokerStockObservation{}) {
				t.Fatal("failed observation remained reusable")
			}
			task.mu.Lock()
			closed := task.closed
			task.mu.Unlock()
			if !closed {
				t.Fatal("owned Task pin leaked")
			}
			if _, e = unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); e != unix.EBADF {
				t.Fatal("owned duplicate remained open", e)
			}
		})
	}
}
