//go:build linux

package resourceguard

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"golang.org/x/sys/unix"
)

func stockMonitorPacket(t *testing.T) ([]byte, []byte, *unix.SockaddrNetlink) {
	t.Helper()
	p := map[string]string{"ACTION": "add", "DEVPATH": "/devices/test/port0", "SUBSYSTEM": "virtio-ports", "SEQNUM": "22", "DEVNAME": "/dev/vport0p1", "MAJOR": "246", "MINOR": "1", "TAGS": ":seat:systemd:"}
	b, e := normalOrdinaryUdevV2PublicPacket(p)
	if e != nil {
		t.Fatal(e)
	}
	return b, unix.UnixCredentials(&unix.Ucred{Pid: 88, Uid: 0, Gid: 0}), &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Pid: 9001, Groups: 2}
}
func TestNormalStockUdevMonitorProcessedOriginalFraming(t *testing.T) {
	b, o, a := stockMonitorPacket(t)
	g, v, e := decodeNormalOrdinaryStockUdevMonitorEvent(b, o, 0, a)
	if e != nil || g != 2 || v.Sequence != 22 || v.SenderPID != 88 || v.SenderPort != 9001 {
		t.Fatalf("original stock: %d %#v %v", g, v, e)
	}
	b[0] = 'X'
	if v.Raw[0] != 'l' {
		t.Fatal("borrowed raw bytes")
	}
	for _, name := range []string{"magic", "prefix", "offset", "length", "hash", "bloom", "truncated", "duplicate-property", "unknown-group", "kernel-pid", "no-credentials", "nonroot", "duplicate-credential", "overflow-cmsg"} {
		t.Run(name, func(t *testing.T) {
			b, o, a := stockMonitorPacket(t)
			flags := 0
			switch name {
			case "magic":
				b[8] ^= 1
			case "prefix":
				b[0] = 'X'
			case "offset":
				binary.NativeEndian.PutUint32(b[16:20], 41)
			case "length":
				binary.NativeEndian.PutUint32(b[20:24], 1)
			case "hash":
				b[24] ^= 1
			case "bloom":
				b[36] ^= 1
			case "truncated":
				flags = unix.MSG_TRUNC
			case "duplicate-property":
				b = append(b, []byte("SEQNUM=22\x00")...)
				binary.NativeEndian.PutUint32(b[20:24], uint32(len(b)-40))
			case "unknown-group":
				a.Groups = 3
			case "kernel-pid":
				o = unix.UnixCredentials(&unix.Ucred{})
			case "no-credentials":
				o = nil
			case "nonroot":
				o = unix.UnixCredentials(&unix.Ucred{Pid: 88, Uid: 60004, Gid: 60004})
			case "duplicate-credential":
				o = append(o, o...)
			case "overflow-cmsg":
				flags = unix.MSG_CTRUNC
			}
			if _, _, e := decodeNormalOrdinaryStockUdevMonitorEvent(b, o, flags, a); !errors.Is(e, ErrUnknown) {
				t.Fatalf("accepted %s: %v", name, e)
			}
		})
	}
}
func TestNormalStockUdevMonitorKernelOriginalCredentials(t *testing.T) {
	b := []byte("add@/devices/system/memory/memory0\x00ACTION=add\x00DEVPATH=/devices/system/memory/memory0\x00SUBSYSTEM=memory\x00SEQNUM=12\x00")
	g, v, e := decodeNormalOrdinaryStockUdevMonitorEvent(b, unix.UnixCredentials(&unix.Ucred{}), 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 1})
	if e != nil || g != 1 || v.Sequence != 12 || v.SenderPID != 0 {
		t.Fatalf("kernel: %#v %v", v, e)
	}
	if _, _, e := decodeNormalOrdinaryStockUdevMonitorEvent(b, unix.UnixCredentials(&unix.Ucred{Pid: 88}), 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 1}); !errors.Is(e, ErrUnknown) {
		t.Fatal("userspace kernel claim")
	}
}
func stockMonitorMatchFixture() (map[uint64]normalOrdinaryStockUdevMonitorEvent, map[uint64]normalOrdinaryStockUdevMonitorEvent, []normalOrdinaryStockUdevMonitorTarget) {
	k := normalOrdinaryStockUdevMonitorEvent{Sequence: 22, Raw: []byte("original-kernel"), Properties: map[string]string{"ACTION": "add", "DEVPATH": "/devices/test/port0", "SUBSYSTEM": "virtio-ports", "SEQNUM": "22", "DEVNAME": "vport0p1", "MAJOR": "246", "MINOR": "1"}}
	p := normalOrdinaryStockUdevMonitorCopy(k)
	p.Properties["DEVNAME"] = "/dev/vport0p1"
	p.Properties["USEC_INITIALIZED"] = "123"
	p.Raw = []byte("actual-stock")
	p.SenderPID = 88
	p.SenderPort = 9001
	return map[uint64]normalOrdinaryStockUdevMonitorEvent{22: k}, map[uint64]normalOrdinaryStockUdevMonitorEvent{22: p}, []normalOrdinaryStockUdevMonitorTarget{{Devpath: k.Properties["DEVPATH"], Subsystem: "virtio-ports", KernelProperties: map[string]string{"DEVNAME": "vport0p1", "MAJOR": "246", "MINOR": "1"}}}
}
func TestNormalStockUdevMonitorOriginalSequenceMatchAndCopy(t *testing.T) {
	k, p, targets := stockMonitorMatchFixture()
	got, e := normalOrdinaryStockUdevMonitorMatch(k, p, targets)
	if e != nil {
		t.Fatal(e)
	}
	got.KernelEvents[0].Properties["DEVPATH"] = "changed"
	got.KernelEvents[0].Raw[0] = 'X'
	v := got.ProcessedBySequence[22]
	v.Properties["DEVNAME"] = "changed"
	v.Raw[0] = 'X'
	if k[22].Properties["DEVPATH"] == "changed" || p[22].Properties["DEVNAME"] == "changed" || k[22].Raw[0] == 'X' || p[22].Raw[0] == 'X' {
		t.Fatal("receipt aliases original evidence")
	}
	for _, name := range []string{"missing-processed", "unknown-sequence", "wrong-action", "wrong-path", "wrong-subsystem", "wrong-devnum", "missing-original-key", "extra-original-key", "unknown-target", "duplicate-target", "target-selfreported-seq", "wrong-target-property", "empty", "sequence-field"} {
		t.Run(name, func(t *testing.T) {
			k, p, target := stockMonitorMatchFixture()
			switch name {
			case "missing-processed":
				delete(p, 22)
			case "unknown-sequence":
				p[23] = p[22]
				delete(p, 22)
			case "wrong-action":
				p[22].Properties["ACTION"] = "change"
			case "wrong-path":
				p[22].Properties["DEVPATH"] = "/devices/other"
			case "wrong-subsystem":
				p[22].Properties["SUBSYSTEM"] = "block"
			case "wrong-devnum":
				p[22].Properties["MINOR"] = "2"
			case "missing-original-key":
				delete(p[22].Properties, "MAJOR")
			case "extra-original-key":
				k[22].Properties["EMPTY_UNKNOWN"] = ""
			case "unknown-target":
				target[0].Devpath = "/devices/other"
			case "duplicate-target":
				target = append(target, target[0])
			case "target-selfreported-seq":
				target[0].KernelProperties["SEQNUM"] = "22"
			case "wrong-target-property":
				target[0].KernelProperties["MINOR"] = "2"
			case "empty":
				k = nil
				p = nil
			case "sequence-field":
				v := p[22]
				v.Sequence = 23
				p[22] = v
			}
			if _, e := normalOrdinaryStockUdevMonitorMatch(k, p, target); !errors.Is(e, ErrUnknown) {
				t.Fatalf("accepted %s: %v", name, e)
			}
		})
	}
}
func TestNormalStockUdevMonitorFinitePropertyBounds(t *testing.T) {
	var raw []byte
	for i := 0; i < 126; i++ {
		raw = append(raw, []byte(fmt.Sprintf("X%03d=1\x00", i))...)
	}
	if p, e := normalOrdinaryStockUdevMonitorProperties(raw); e != nil || len(p) != 126 {
		t.Fatalf("126: %v", e)
	}
	for _, b := range [][]byte{append(bytes.Clone(raw), []byte("X127=1\x00")...), []byte("x=1\x00"), []byte("A=bad\n\x00"), []byte("A=1"), []byte("A=1\x00A=1\x00"), bytes.Repeat([]byte{'X'}, 4097)} {
		if _, e := normalOrdinaryStockUdevMonitorProperties(b); !errors.Is(e, ErrUnknown) {
			t.Fatal("unbounded/unsupported accepted")
		}
	}
}
func TestNormalStockUdevMonitorDuplicateAndLedgerBound(t *testing.T) {
	b, o, a := stockMonitorPacket(t)
	m := &normalOrdinaryStockUdevMonitor{kernel: map[uint64]normalOrdinaryStockUdevMonitorEvent{}, processed: map[uint64]normalOrdinaryStockUdevMonitorEvent{}}
	if e := m.consume(b, o, 0, a); e != nil {
		t.Fatal(e)
	}
	if e := m.consume(b, o, 0, a); !errors.Is(e, ErrUnknown) {
		t.Fatal("duplicate")
	}
	m.processed = map[uint64]normalOrdinaryStockUdevMonitorEvent{}
	m.rawBytes = 32 << 20
	if e := m.consume(b, o, 0, a); !errors.Is(e, ErrUnknown) {
		t.Fatal("raw byte overflow")
	}
	m.rawBytes = 0
	for i := uint64(0); i < 8192; i++ {
		m.kernel[i] = normalOrdinaryStockUdevMonitorEvent{}
	}
	if e := m.consume(b, o, 0, a); !errors.Is(e, ErrUnknown) {
		t.Fatal("record overflow")
	}
}
