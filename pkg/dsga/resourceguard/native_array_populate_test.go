//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

// 原PG/Source/FD为明确模型，仅验私有跨C/Go数据合同；不冒充真实helper或收费S。
func arrayPopulateFixture(t *testing.T) arrayPopulatePending {
	t.Helper()
	_, _, command := fixtureGuard(t)
	p := arrayPopulatePending{ProtocolVersion: 1, Scope: arrayPopulateScope, Nonce: strings.Repeat("01", 32), Parent: nativeBirthConsumer{PID: 444, StartTicks: 100, InstanceNonce: strings.Repeat("02", 32)}, StateDevice: 37, StateInode: 12, LockDevice: 37, LockInode: 13}
	p.Context = arrayPopulateContext{Original: ProcessIdentity{ProtocolVersion: 1, Identity: command.Identity, PostmasterPID: 5432, PostmasterStartTicks: 100, AddressSpaceMaxBytes: 2 << 30}, PGBootID: 333, ResourceFloor: 1, OriginalContextSHA256: strings.Repeat("ab", 32), HelperSHA256: strings.Repeat("cd", 32), MemlockUpperBytes: 1 << 20, AddressSpaceMaxBytes: 64 << 20, HelperReserveBytes: 65 << 20, ArraysMaxBytes: 128 << 20, SystemMaxBytes: 128 << 20, ArraysDevice: command.Identity.CgroupDevice, ArraysInode: command.Identity.CgroupInode + 1, SystemDevice: command.Identity.CgroupDevice, SystemInode: command.Identity.CgroupInode + 2}
	for i := range p.Context.Files {
		p.Context.Files[i] = arrayPopulateFile{Device: 11, Inode: control.Uint64(100 + i), FileBytes: 4 << 20, Offset: 64 << 10, Length: 64 << 10}
	}
	p.ParentGuestBootID = p.Context.Original.Identity.GuestBootID
	if e := p.validate(); e != nil {
		t.Fatal(e)
	}
	return p
}
func arrayPopulateReadyFixture(t *testing.T, p arrayPopulatePending) []byte {
	t.Helper()
	b := make([]byte, arrayPopulateStatusBytes)
	copy(b, "NAPS0001")
	binary.LittleEndian.PutUint32(b[8:12], 1)
	binary.LittleEndian.PutUint64(b[16:24], 80<<10)
	binary.LittleEndian.PutUint64(b[24:32], 555)
	var ranges [4][2]uint64
	for i, f := range p.Context.Files {
		ranges[i] = [2]uint64{0x7f1000000000 + uint64(i)*(1<<30), uint64(f.Length)}
		binary.LittleEndian.PutUint64(b[32+16*i:40+16*i], ranges[i][0])
		binary.LittleEndian.PutUint64(b[40+16*i:48+16*i], ranges[i][1])
	}
	nonce, e := p.wire()
	if e != nil {
		t.Fatal(e)
	}
	copy(b[96:128], nonce[144:176])
	binary.LittleEndian.PutUint32(b[128:132], uint32(len(arrayPopulateFilter(ranges))))
	return b
}
func TestNativeArrayPopulateOriginalPlanWire(t *testing.T) {
	p := arrayPopulateFixture(t)
	b, e := p.wire()
	if e != nil || len(b) != 368 || string(b[:8]) != "NAPOP001" || binary.LittleEndian.Uint64(b[16:24]) != 444 || binary.LittleEndian.Uint64(b[32:40]) != 1<<20 || binary.LittleEndian.Uint64(b[40:48]) != 64<<20 {
		t.Fatal("canonical plan differs", e)
	}
	for _, v := range b[96:112] {
		if v != 0 {
			t.Fatal("reserved wire bytes nonzero")
		}
	}
	for name, alter := range map[string]func(*arrayPopulatePending){
		"parent-instance": func(p *arrayPopulatePending) { p.Parent.InstanceNonce = "" }, "zero-nonce": func(p *arrayPopulatePending) { p.Nonce = strings.Repeat("0", 64) }, "pgboot": func(p *arrayPopulatePending) { p.Context.PGBootID = 0 }, "context-sha": func(p *arrayPopulatePending) { p.Context.OriginalContextSHA256 = "request" }, "zero-lock": func(p *arrayPopulatePending) { p.LockInode = 0 }, "other-lock-filesystem": func(p *arrayPopulatePending) { p.LockDevice++ }, "arrays-is-parent": func(p *arrayPopulatePending) { p.Context.ArraysInode = p.Context.Original.Identity.CgroupInode }, "system-is-arrays": func(p *arrayPopulatePending) { p.Context.SystemInode = p.Context.ArraysInode }, "oversize-memlock": func(p *arrayPopulatePending) { p.Context.MemlockUpperBytes = 2 << 20 }, "small-AS": func(p *arrayPopulatePending) {
			p.Context.AddressSpaceMaxBytes = 1 << 20
			p.Context.Files[0].Length = 2 << 20
		}, "unaligned-range": func(p *arrayPopulatePending) { p.Context.Files[0].Offset++ }, "beyond-file": func(p *arrayPopulatePending) { p.Context.Files[0].Length = 8 << 20 }, "same-inode": func(p *arrayPopulatePending) { p.Context.Files[1] = p.Context.Files[0] }, "zero-total": func(p *arrayPopulatePending) {
			for i := range p.Context.Files {
				p.Context.Files[i].Length = 0
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			x := p
			alter(&x)
			if _, e := x.wire(); e == nil {
				t.Fatal("invalid original plan accepted")
			}
		})
	}
}
func TestNativeArrayPopulatePreparedResponse(t *testing.T) {
	p := arrayPopulateFixture(t)
	raw := arrayPopulateReadyFixture(t, p)
	o, e := decodeArrayPopulateStatus(raw, p, nil)
	if e != nil || o.LockedBytes != 80<<10 {
		t.Fatal(e)
	}
	for name, alter := range map[string]func([]byte) []byte{
		"truncated": func(b []byte) []byte { return b[:255] }, "extra": func(b []byte) []byte { return append(b, 0) }, "nonce": func(b []byte) []byte { b[96] ^= 1; return b }, "reserved": func(b []byte) []byte { b[200] = 1; return b }, "wrong-range": func(b []byte) []byte { binary.LittleEndian.PutUint64(b[40:48], 4096); return b }, "overlap": func(b []byte) []byte { copy(b[48:56], b[32:40]); return b }, "no-lock": func(b []byte) []byte { binary.LittleEndian.PutUint64(b[16:24], 0); return b }, "excess-lock": func(b []byte) []byte { binary.LittleEndian.PutUint64(b[16:24], 2<<20); return b }, "parent-pid": func(b []byte) []byte { binary.LittleEndian.PutUint64(b[24:32], 444); return b }, "pg-pid": func(b []byte) []byte { binary.LittleEndian.PutUint64(b[24:32], 5432); return b }, "filter-count": func(b []byte) []byte { binary.LittleEndian.PutUint32(b[128:132], 1); return b }, "unknown-phase": func(b []byte) []byte { binary.LittleEndian.PutUint32(b[8:12], 9); return b },
	} {
		t.Run(name, func(t *testing.T) {
			b := alter(append([]byte(nil), raw...))
			if _, e := decodeArrayPopulateStatus(b, p, nil); e == nil {
				t.Fatal("malformed response accepted")
			}
		})
	}
	b := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint32(b[8:12], 2)
	binary.LittleEndian.PutUint32(b[132:136], 2)
	if _, e := decodeArrayPopulateStatus(b, p, &o); e != nil {
		t.Fatal(e)
	}
	binary.LittleEndian.PutUint64(b[32:40], o.Ranges[0][0]+4096)
	if _, e := decodeArrayPopulateStatus(b, p, &o); e == nil {
		t.Fatal("prepared mapping drift accepted")
	}
}

// 解释执行生成的cBPF以验证安全性质，不把它当kernel安装证明；实际安装只能S读回。
func arrayPopulateEvaluate(t *testing.T, p []unix.SockFilter, nr uint32, args [6]uint64, arch uint32) uint32 {
	t.Helper()
	var frame [64]byte
	binary.LittleEndian.PutUint32(frame[:4], nr)
	binary.LittleEndian.PutUint32(frame[4:8], arch)
	for i, a := range args {
		binary.LittleEndian.PutUint64(frame[16+8*i:24+8*i], a)
	}
	var a uint32
	for pc, steps := 0, 0; steps < 1000; steps++ {
		if pc >= len(p) {
			t.Fatal("filter ran beyond program")
		}
		x := p[pc]
		pc++
		switch x.Code {
		case unix.BPF_LD | unix.BPF_W | unix.BPF_ABS:
			if x.K > 60 {
				t.Fatal("bad load")
			}
			a = binary.LittleEndian.Uint32(frame[x.K : x.K+4])
		case unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K:
			if a == x.K {
				pc += int(x.Jt)
			} else {
				pc += int(x.Jf)
			}
		case unix.BPF_JMP | unix.BPF_JSET | unix.BPF_K:
			if a&x.K != 0 {
				pc += int(x.Jt)
			} else {
				pc += int(x.Jf)
			}
		case unix.BPF_RET | unix.BPF_K:
			return x.K
		default:
			t.Fatal("unknown filter operation")
		}
	}
	t.Fatal("filter did not terminate")
	return 0
}
func TestNativeArrayPopulateExactFilterProperties(t *testing.T) {
	pending := arrayPopulateFixture(t)
	o, e := decodeArrayPopulateStatus(arrayPopulateReadyFixture(t, pending), pending, nil)
	if e != nil {
		t.Fatal(e)
	}
	p := arrayPopulateFilter(o.Ranges)
	allow := uint32(0x7fff0000)
	deny := uint32(0x50001)
	for i, r := range o.Ranges {
		if got := arrayPopulateEvaluate(t, p, unix.SYS_MADVISE, [6]uint64{r[0], r[1], 23}, 0xc000003e); got != allow {
			t.Fatal(i, got)
		}
	}
	for _, nr := range []uint32{unix.SYS_MMAP, unix.SYS_BRK, unix.SYS_MPROTECT, unix.SYS_MUNMAP, unix.SYS_MLOCK, unix.SYS_CLONE, unix.SYS_FORK, unix.SYS_EXECVE, unix.SYS_FUTEX, unix.SYS_SOCKET, unix.SYS_IOCTL, unix.SYS_IO_URING_SETUP, unix.SYS_MBIND, unix.SYS_MIGRATE_PAGES, unix.SYS_MOVE_PAGES, unix.SYS_GETPID, unix.SYS_FLOCK, unix.SYS_CLOSE} {
		if got := arrayPopulateEvaluate(t, p, nr, [6]uint64{}, 0xc000003e); got != deny {
			t.Fatalf("syscall %d accepted %x", nr, got)
		}
	}
	cases := map[string]struct {
		nr   uint32
		args [6]uint64
	}{"other-advice": {unix.SYS_MADVISE, [6]uint64{o.Ranges[0][0], o.Ranges[0][1], 9}}, "short-span": {unix.SYS_MADVISE, [6]uint64{o.Ranges[0][0], 4096, 23}}, "other-address": {unix.SYS_MADVISE, [6]uint64{o.Ranges[0][0] + 4096, o.Ranges[0][1], 23}}, "high-address": {unix.SYS_MADVISE, [6]uint64{o.Ranges[0][0] ^ (1 << 40), o.Ranges[0][1], 23}}, "high-length": {unix.SYS_MADVISE, [6]uint64{o.Ranges[0][0], o.Ranges[0][1] + (1 << 32), 23}}, "other-fd": {unix.SYS_WRITE, [6]uint64{4, 0, 256}}, "unbounded-write": {unix.SYS_WRITE, [6]uint64{9, 0, 257}}, "read-not-trunc": {unix.SYS_RECVFROM, [6]uint64{9, 0, 1, 0}}, "poll-many": {unix.SYS_POLL, [6]uint64{0, 2, 0}}}
	for n, c := range cases {
		t.Run(n, func(t *testing.T) {
			if got := arrayPopulateEvaluate(t, p, c.nr, c.args, 0xc000003e); got != deny {
				t.Fatalf("accepted %x", got)
			}
		})
	}
	// 指针所指内容并非cBPF证明；固定C/static buffer/parentfd来源另验，不能误称全指针验证。
	if arrayPopulateEvaluate(t, p, unix.SYS_POLL, [6]uint64{0xdeadbeef, 1, 0}, 0xc000003e) != allow {
		t.Fatal("documented pointer-independent poll differs")
	}
	if arrayPopulateEvaluate(t, p, unix.SYS_WRITE, [6]uint64{9, 0xdeadbeef, 256}, 0xc000003e) != allow {
		t.Fatal("fixed FD/count contract differs")
	}
	if arrayPopulateEvaluate(t, p, unix.SYS_EXIT, [6]uint64{}, 0) != 0x80000000 || arrayPopulateEvaluate(t, p, unix.SYS_EXIT|0x40000000, [6]uint64{}, 0xc000003e) != 0x80000000 {
		t.Fatal("arch/x32 escape")
	}
}
func TestNativeArrayPopulateAppendOnlyHistory(t *testing.T) {
	p := arrayPopulateFixture(t)
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	s, e := lockState(context.Background(), dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	arrayPopulateBindFixtureState(t, s, &p)
	if e := rejectOutstandingArrayPopulateLocked(s); e != nil {
		t.Fatal(e)
	}
	raw, e := arrayPopulateAppend(s, p.Nonce, "pending", p)
	if e != nil {
		t.Fatal(e)
	}
	if e := rejectOutstandingArrayPopulateLocked(s); !errors.Is(e, ErrUnknown) {
		t.Fatal("pending automatically adopted", e)
	}
	if _, e := arrayPopulateAppend(s, p.Nonce, "pending", p); e == nil {
		t.Fatal("pending overwritten")
	}
	child := arrayPopulateChild{ProtocolVersion: 1, PendingSHA256: physicalDigest(raw), PID: 555, StartTicks: 777, PIDFDDevice: 22, PIDFDInode: 88}
	raw, e = arrayPopulateAppend(s, p.Nonce, "child", child)
	if e != nil {
		t.Fatal(e)
	}
	prepared := arrayPopulatePrepared{ProtocolVersion: 1, TracerTID: 555, ChildSHA256: physicalDigest(raw), StatusSHA256: strings.Repeat("ab", 32), FilterSHA256: strings.Repeat("cd", 32), VmSizeBytes: 2 << 20, LockedBytes: 80 << 10}
	raw, e = arrayPopulateAppend(s, p.Nonce, "prepared", prepared)
	if e != nil {
		t.Fatal(e)
	}
	if e := rejectOutstandingArrayPopulateLocked(s); !errors.Is(e, ErrUnknown) {
		t.Fatal("prepared assumed released")
	}
	_, e = arrayPopulateAppend(s, p.Nonce, "complete", arrayPopulateComplete{ProtocolVersion: 1, PreparedSHA256: physicalDigest(raw), PID: 555, ExitObserved: true, Progress: [4]bool{true, true, true, true}})
	if e != nil {
		t.Fatal(e)
	}
	if e := rejectOutstandingArrayPopulateLocked(s); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "array-populate.bad.pending.json"), []byte("{"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := rejectOutstandingArrayPopulateLocked(s); !errors.Is(e, ErrUnknown) {
		t.Fatal("foreign family ignored", e)
	}
}
func TestNativeArrayPopulateOFDLastClose(t *testing.T) {
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	s, e := lockState(context.Background(), dir)
	if e != nil {
		t.Fatal(e)
	}
	dup, e := unix.Dup(int(s.lock.Fd()))
	if e != nil {
		t.Fatal(e)
	}
	s.close()
	other, e := unix.Open(filepath.Join(dir, "lock"), unix.O_RDWR|unix.O_CLOEXEC, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(other)
	if e := unix.Flock(other, unix.LOCK_EX|unix.LOCK_NB); e != unix.EWOULDBLOCK {
		t.Fatal("same OFD duplicate did not retain lock", e)
	}
	unix.Close(dup)
	if e := unix.Flock(other, unix.LOCK_EX|unix.LOCK_NB); e != nil {
		t.Fatal("last-close lock not released", e)
	}
}

// smaps Locked是PSS，不要求与RSS相等；这里验证所有LOAD页的lo/resident覆盖，
// 缺页、延迟锁、目标误锁及锁到未知映射都必须拒绝。明确模型，不是kernel执行证明。
func TestNativeArrayPopulateWholeImageLocked(t *testing.T) {
	image := []arrayPopulateImageSpan{{0x400000, 0x401000}, {0x403000, 0x415000}}
	targets := [4][2]uint64{{0x70000000, 4096}}
	const head = "00400000-00401000 r-xp 00000000 00:01 3 /helper\nRss: 4 kB\nLocked: 2 kB\nVmFlags: rd ex mr me lo\n"
	const data = "00403000-00415000 rw-p 00000000 00:00 0\nRss: 72 kB\nLocked: 72 kB\nVmFlags: rd wr mr mw lo\n"
	const target = "70000000-70001000 rw-s 00000000 00:01 4 /array\nRss: 0 kB\nLocked: 0 kB\nVmFlags: rd wr sh mr mw\n"
	good := head + data + target
	if err := arrayPopulateVerifyLockedSmaps(good, image, 76<<10, targets); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"missing-load":         data + target,
		"missing-lo":           strings.Replace(good, "VmFlags: rd ex mr me lo", "VmFlags: rd ex mr me", 1),
		"on-fault":             strings.Replace(good, "VmFlags: rd ex mr me lo", "VmFlags: rd ex mr me lo lf", 1),
		"missing-resident":     strings.Replace(good, "Rss: 72 kB", "Rss: 68 kB", 1),
		"target-locked":        strings.Replace(good, "Rss: 0 kB\nLocked: 0 kB\nVmFlags: rd wr sh mr mw", "Rss: 4 kB\nLocked: 4 kB\nVmFlags: rd wr sh mr mw lo", 1),
		"duplicate-rss":        strings.Replace(good, "Rss: 4 kB", "Rss: 4 kB\nRss: 4 kB", 1),
		"duplicate-flags":      strings.Replace(good, "VmFlags: rd ex mr me lo", "VmFlags: rd ex mr me lo lo", 1),
		"excess-locked":        strings.Replace(good, "Locked: 72 kB", "Locked: 76 kB", 1),
		"locked-outside-image": good + "80000000-80001000 rw-p 00000000 00:00 0\nRss: 4 kB\nLocked: 4 kB\nVmFlags: rd wr lo\n",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if err := arrayPopulateVerifyLockedSmaps(raw, image, 76<<10, targets); err == nil {
				t.Fatal("invalid image lock accepted")
			}
		})
	}
	if err := arrayPopulateVerifyLockedSmaps(good, image, 80<<10, targets); err == nil {
		t.Fatal("self-reported locked total trusted")
	}
}

func TestNativeArrayPopulateKernelFilterComparison(t *testing.T) {
	ranges := [4][2]uint64{{0x7f0000000000, 4096}}
	program := arrayPopulateFilter(ranges)
	for _, mode := range []string{"valid", "wrong-length", "wrong-instruction", "short-read", "extra-program", "unknown-terminal"} {
		t.Run(mode, func(t *testing.T) {
			read := func(offset uint32, dst []unix.SockFilter) (int, error) {
				if offset == 1 {
					if mode == "extra-program" {
						return 1, nil
					}
					if mode == "unknown-terminal" {
						return 0, unix.EPERM
					}
					return 0, unix.ENOENT
				}
				if len(dst) == 0 {
					if mode == "wrong-length" {
						return len(program) - 1, nil
					}
					return len(program), nil
				}
				copy(dst, program)
				if mode == "wrong-instruction" {
					dst[len(dst)-1].K ^= 1
				}
				if mode == "short-read" {
					return len(dst) - 1, nil
				}
				return len(dst), nil
			}
			err := arrayPopulateCompareInstalled(program, read)
			if (err == nil) != (mode == "valid") {
				t.Fatal("kernel chain comparison accepted wrong source", mode, err)
			}
		})
	}
}
func TestNativeArrayPopulateExplicitReserveAndCaps(t *testing.T) {
	p := arrayPopulateFixture(t)
	for name, alter := range map[string]func(*arrayPopulateContext){
		"zero-A":                       func(c *arrayPopulateContext) { c.ArraysMaxBytes = 0 },
		"infinite-A":                   func(c *arrayPopulateContext) { c.ArraysMaxBytes = 1 << 63 },
		"zero-System":                  func(c *arrayPopulateContext) { c.SystemMaxBytes = 0 },
		"missing-helper-reserve":       func(c *arrayPopulateContext) { c.HelperReserveBytes = 0 },
		"reserve-exceeds-A":            func(c *arrayPopulateContext) { c.HelperReserveBytes = c.ArraysMaxBytes + 4096 },
		"array-plus-reserve-exceeds-A": func(c *arrayPopulateContext) { c.ArraysMaxBytes = c.HelperReserveBytes },
	} {
		t.Run(name, func(t *testing.T) {
			c := p.Context
			alter(&c)
			if err := c.validate(); err == nil {
				t.Fatal("unbound cap/reserve accepted")
			}
		})
	}
}

func TestNativeArrayPopulateOriginalTerminalWait(t *testing.T) {
	frame := func(pid, uid, cause, status uint32) [128]byte {
		var b [128]byte
		binary.LittleEndian.PutUint32(b[0:4], uint32(unix.SIGCHLD))
		binary.LittleEndian.PutUint32(b[8:12], cause)
		binary.LittleEndian.PutUint32(b[16:20], pid)
		binary.LittleEndian.PutUint32(b[20:24], uid)
		binary.LittleEndian.PutUint32(b[24:28], status)
		return b
	}
	for _, uid := range []uint32{0, 65534} {
		if status, e := arrayPopulateDecodeExit(frame(77, uid, 1, 0), 77); e != nil || !status.Exited() {
			t.Fatal("valid source UID rejected", uid, e)
		}
	}
	for name, f := range map[string][128]byte{"wrong-pid": frame(78, 65534, 1, 0), "wrong-uid": frame(77, 1000, 1, 0), "ptrace-stop": frame(77, 65534, 4, uint32(unix.SIGTRAP)), "not-exit": [128]byte{}} {
		t.Run(name, func(t *testing.T) {
			if _, e := arrayPopulateDecodeExit(f, 77); e == nil {
				t.Fatal("nonterminal original wait accepted")
			}
		})
	}
}
