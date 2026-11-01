//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func ordinaryDeviceCheckGoodStatus() []byte {
	return []byte("Name:\tTEST_ONLY\nUid:\t60005 60005 60005 60005\nGid:\t60005 60005 60005 60005\nGroups:\t\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000000\nCapEff:\t0000000000000000\nCapBnd:\t0000000000000000\nCapAmb:\t0000000000000000\nNoNewPrivs:\t1\n")
}

func TestNormalOrdinaryDeviceCheckRejectsIdentityAndCapabilities(t *testing.T) {
	good := ordinaryDeviceCheckGoodStatus()
	if err := normalOrdinaryDeviceCheckStatus(good); err != nil {
		t.Fatal(err)
	}
	bad := map[string][]byte{
		"root":             []byte(strings.ReplaceAll(string(good), "60005", "0")),
		"suid-root":        []byte(strings.Replace(string(good), "60005 60005 60005 60005", "60005 0 0 0", 1)),
		"fsuid":            []byte(strings.Replace(string(good), "60005 60005 60005 60005", "60005 60005 60005 0", 1)),
		"fsgid":            []byte(strings.Replace(string(good), "Gid:\t60005 60005 60005 60005", "Gid:\t60005 60005 60005 0", 1)),
		"supplementary-pg": []byte(strings.Replace(string(good), "Groups:\t\n", "Groups:\t1000\n", 1)),
		"nnp":              []byte(strings.Replace(string(good), "NoNewPrivs:\t1", "NoNewPrivs:\t0", 1)),
		"missing":          []byte(strings.Replace(string(good), "CapAmb:\t0000000000000000\n", "", 1)),
		"duplicate":        append(append([]byte{}, good...), []byte("CapEff:\t0000000000000000\n")...),
		"nul":              append(append([]byte{}, good...), 0),
	}
	for _, key := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
		bad[key] = []byte(strings.Replace(string(good), key+":\t0000000000000000", key+":\t0000000000000001", 1))
	}
	for name, raw := range bad {
		t.Run(name, func(t *testing.T) {
			if normalOrdinaryDeviceCheckStatus(raw) == nil {
				t.Fatal("uninstalled identity admitted")
			}
		})
	}
	// 实际测试Root不因静态表里有60005而取得执行客户端资格。
	if os.Getuid() == 0 && normalOrdinaryDeviceCheckCaller() == nil {
		t.Fatal("real Root became ordinary init")
	}
}

func TestNormalOrdinaryDeviceCheckFixedActionFive(t *testing.T) {
	request := normalOrdinaryActionFrame(5, nil, false)
	action, body, e := decodeNormalOrdinaryActionFrame(request, "udev-init")
	if e != nil || action != 5 || len(body) != 0 {
		t.Fatal(e)
	}
	for _, role := range []string{"udev", "acpi", "daemon", "compute", "resource", ""} {
		t.Run("wrong-role-"+role, func(t *testing.T) {
			if _, _, e := decodeNormalOrdinaryActionFrame(request, role); e == nil {
				t.Fatal("action5 crossed role endpoint")
			}
		})
	}
	for _, raw := range [][]byte{request[:15], append(append([]byte{}, request...), 1), normalOrdinaryActionFrame(4, nil, false), normalOrdinaryActionFrame(5, nil, true)} {
		if _, _, e := decodeNormalOrdinaryActionFrame(raw, "udev-init"); e == nil {
			t.Fatal("nonexact action5 admitted")
		}
	}
	want := [4]normalOrdinaryActionEndpoint{{"acpi", "/run/neon-dsga/ordinary-acpi.sock", 60006}, {"daemon", "/run/neon-dsga/ordinary-tls.sock", 60009}, {"udev", "/run/neon-dsga/ordinary-udev.sock", 60004}, {"udev-init", normalOrdinaryDeviceCheckSocket, 60005}}
	if len(normalOrdinaryActionEndpoints) != len(want) {
		t.Fatal("fixed listeners differ")
	}
	for i, e := range want {
		if normalOrdinaryActionEndpoints[i] != e {
			t.Fatal("fixed listener changed", i)
		}
	}
}

func TestNormalOrdinaryDeviceCheckStrictReply(t *testing.T) {
	good := normalOrdinaryActionFrame(5, nil, true)
	if e := normalOrdinaryDeviceCheckReply(good); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 16; i++ {
		bad := append([]byte{}, good...)
		bad[i] ^= 1
		if normalOrdinaryDeviceCheckReply(bad) == nil {
			t.Fatal("corrupt reply accepted", i)
		}
	}
	for _, bad := range [][]byte{nil, good[:15], append(append([]byte{}, good...), 0), normalOrdinaryActionFrame(5, nil, false)} {
		if normalOrdinaryDeviceCheckReply(bad) == nil {
			t.Fatal("partial/wrong reply accepted")
		}
	}
	// 旧四动作字节没有因5新增而改变。
	for a := uint32(1); a <= 4; a++ {
		raw := normalOrdinaryActionFrame(a, []byte("old"), false)
		if string(raw[:8]) != "NDSGOA02" || binary.BigEndian.Uint32(raw[8:12]) != 2 || binary.BigEndian.Uint32(raw[12:16]) != a || string(raw[16:]) != "old" {
			t.Fatal("old wire drift")
		}
	}
}

func TestNormalOrdinaryDeviceCheckUnknownWithoutOriginalRootPeer(t *testing.T) {
	call, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f, e := os.Open(os.DevNull)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if normalOrdinaryDeviceCheckPeer(call, int(f.Fd())) == nil {
		t.Fatal("ordinary FD became original pidfd")
	}
	if normalOrdinaryDeviceCheckPeer(call, -1) == nil {
		t.Fatal("missing pidfd accepted")
	}
	if !errors.Is(normalOrdinaryDeviceCheckConnected(call, nil), ErrUnknown) {
		t.Fatal("nil connection admitted")
	}
	expired, done := context.WithCancel(context.Background())
	done()
	if RunNormalSystemOrdinaryV2DeviceCheck(expired) == nil {
		t.Fatal("canceled call admitted")
	}
}

func TestNormalOrdinaryDeviceCheckCannotCreateUdevSourceFromIdentity(t *testing.T) {
	guard := &Guard{}
	action := &normalOrdinaryActionsV2{g: guard, authority: &normalSystemResourceAuthority{}, filesystem: &normalFilesystemOriginSource{}, proof: &normalSystemRoleBrokerExecProof{}}
	if q, e := action.originalUdevSourceLocked(); e == nil || q != nil {
		t.Fatal("absent producer admitted")
	}
	normalOrdinaryUdevV2Slots.Store(guard, &normalOrdinaryUdevV2Slot{})
	defer normalOrdinaryUdevV2Slots.Delete(guard)
	if q, e := action.originalUdevSourceLocked(); e == nil || q != nil {
		t.Fatal("failed/tombstone producer admitted")
	}
	other := &normalOrdinaryUdevV2{g: &Guard{}, authority: action.authority, filesystem: action.filesystem, proof: action.proof}
	normalOrdinaryUdevV2Slots.Store(guard, &normalOrdinaryUdevV2Slot{source: other})
	if q, e := action.originalUdevSourceLocked(); e == nil || q != nil {
		t.Fatal("foreign Guard producer admitted")
	}
}

func TestNormalOrdinaryDeviceCheckReceivedRightsAreClosed(t *testing.T) {
	pair, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(pair[0])
	defer unix.Close(pair[1])
	if e = unix.SetsockoptInt(pair[1], unix.SOL_SOCKET, unix.SO_PASSCRED, 1); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(os.DevNull)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if e = unix.Sendmsg(pair[0], normalOrdinaryActionFrame(5, nil, true), unix.UnixRights(int(f.Fd())), nil, 0); e != nil {
		t.Fatal(e)
	}
	data := make([]byte, 17)
	oob := make([]byte, 256)
	_, on, flags, _, e := unix.Recvmsg(pair[1], data, oob, unix.MSG_CMSG_CLOEXEC)
	if e != nil {
		t.Fatal(e)
	}
	messages, e := unix.ParseSocketControlMessage(oob[:on])
	if e != nil {
		t.Fatal(e)
	}
	var delivered []int
	for _, m := range messages {
		if m.Header.Level == unix.SOL_SOCKET && m.Header.Type == unix.SCM_RIGHTS {
			ds, e := unix.ParseUnixRights(&m)
			if e != nil {
				t.Fatal(e)
			}
			delivered = append(delivered, ds...)
		}
	}
	if len(delivered) != 1 {
		t.Fatal(delivered)
	}
	if flags, e := unix.FcntlInt(uintptr(delivered[0]), unix.F_GETFD, 0); e != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatal("received rights not CLOEXEC", e)
	}
	if _, e = normalOrdinaryPacketCredentials(oob[:on], flags); e == nil {
		t.Fatal("rights ACK admitted")
	}
	for _, fd := range delivered {
		if _, e = unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(e, unix.EBADF) {
			t.Fatal("delivered FD leaked", e)
		}
	}
}
