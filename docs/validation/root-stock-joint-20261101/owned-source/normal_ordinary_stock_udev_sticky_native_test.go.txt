//go:build linux && dsga_native_stock_udev_sticky

package resourceguard

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// 独立无PG Guest机制S，仅在本测试新建普通tmpfs目录；不是原生产/run/udev资产。
func TestNativeStockUdevStickyDirectoryRights(t *testing.T) {
	if os.Getenv("DSGA_STOCK_STICKY_NATIVE") != "isolated18c-sticky-v1" {
		t.Fatal("isolated Guest marker required")
	}
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil {
		t.Fatal(e)
	}
	release, e := kernelText("/proc/sys/kernel/osrelease")
	if e != nil || release != ref.KernelRelease {
		t.Fatalf("exact release: %v", e)
	}
	notes, e := physicalKernelNotes()
	if e != nil || physicalDigest(notes) != ref.KernelNotesSHA256 {
		t.Fatalf("exact notes: %v", e)
	}
	if e = normalKernelPrepV2R2Caller(); e != nil {
		t.Fatal(e)
	}
	root, e := os.MkdirTemp("/run", "stock-sticky-own-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(root)
	dir := filepath.Join(root, "udev")
	rules := filepath.Join(dir, "rules.d")
	if e = os.Chmod(root, 0755); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(dir, 0755); e != nil {
		t.Fatal(e)
	}
	if e = os.Chown(dir, 0, 60004); e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(dir, os.ModeSticky|0771); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(rules, 0755); e != nil {
		t.Fatal(e)
	}
	if e = os.Chown(rules, 0, 0); e != nil {
		t.Fatal(e)
	}
	before, e := os.Stat(rules)
	if e != nil {
		t.Fatal(e)
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(exe, "-test.v", "-test.run=^TestNativeStockUdevStickyWorker$")
	cmd.Env = []string{"PATH=/neonvm/bin:/bin", "DSGA_STOCK_STICKY_WORKER=" + dir}
	raw, e := cmd.CombinedOutput()
	t.Logf("original dedicated worker:\n%s", raw)
	if e != nil {
		t.Fatalf("original Wait: %v", e)
	}
	after, e := os.Stat(rules)
	if e != nil || !os.SameFile(before, after) {
		t.Fatalf("original rules object replaced: %v", e)
	}
	for _, p := range []string{dir, rules} {
		var st unix.Stat_t
		if e = unix.Stat(p, &st); e != nil {
			t.Fatal(e)
		}
		wantGID, wantMode := uint32(60004), uint32(01771)
		if p == rules {
			wantGID = 0
			wantMode = 0755
		}
		if st.Uid != 0 || st.Gid != wantGID || uint32(st.Mode)&07777 != wantMode {
			t.Fatalf("original metadata drift %s: %#v", p, st)
		}
		t.Logf("original object %s uid=%d gid=%d mode=%#o", p, st.Uid, st.Gid, uint32(st.Mode)&07777)
	}
	if e = normalKernelPrepV2R2Caller(); e != nil {
		t.Fatalf("parent Root changed: %v", e)
	}
}

func TestNativeStockUdevStickyWorker(t *testing.T) {
	dir := os.Getenv("DSGA_STOCK_STICKY_WORKER")
	if dir == "" {
		t.Skip("only own independent test worker")
	}
	if filepath.Clean(dir) != dir || !strings.HasPrefix(dir, "/run/stock-sticky-own-") || filepath.Base(dir) != "udev" {
		t.Fatal("own temp path required")
	}
	all := func(trap, a, b, c uintptr) {
		t.Helper()
		if _, _, e := syscall.AllThreadsSyscall(trap, a, b, c); e != 0 {
			t.Fatal(e)
		}
	}
	lastRaw, e := kernelText("/proc/sys/kernel/cap_last_cap")
	if e != nil {
		t.Fatal(e)
	}
	last, e := strconv.Atoi(lastRaw)
	if e != nil || last < 0 || last > 63 {
		t.Fatal("capability bound")
	}
	for cap := 0; cap <= last; cap++ {
		if _, _, e := syscall.AllThreadsSyscall6(unix.SYS_PRCTL, unix.PR_CAPBSET_DROP, uintptr(cap), 0, 0, 0, 0); e != 0 {
			t.Fatal(e)
		}
	}
	all(unix.SYS_SETGROUPS, 0, 0, 0)
	all(unix.SYS_SETRESGID, 60004, 60004, 60004)
	all(unix.SYS_SETRESUID, 60004, 60004, 60004)
	header := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	data := [2]unix.CapUserData{}
	all(unix.SYS_CAPSET, uintptr(unsafe.Pointer(&header)), uintptr(unsafe.Pointer(&data[0])), 0)
	runtime.KeepAlive(&header)
	runtime.KeepAlive(&data)
	all(unix.SYS_PRCTL, unix.PR_SET_NO_NEW_PRIVS, 1, 0)
	// 实读所有当前M；不凭setuid/NNP调用成功自报全cap已清。
	threads, e := os.ReadDir("/proc/self/task")
	if e != nil {
		t.Fatal(e)
	}
	for _, thread := range threads {
		p := filepath.Join("/proc/self/task", thread.Name(), "status")
		b, e := os.ReadFile(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		fields := map[string]string{}
		for _, line := range strings.Split(string(b), "\n") {
			k, v, ok := strings.Cut(line, ":")
			if ok {
				fields[k] = strings.TrimSpace(v)
			}
		}
		for _, key := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
			if fields[key] != "0000000000000000" {
				t.Fatalf("%s %s=%q", thread.Name(), key, fields[key])
			}
		}
		if strings.Join(strings.Fields(fields["Uid"]), " ") != "60004 60004 60004 60004" || strings.Join(strings.Fields(fields["Gid"]), " ") != "60004 60004 60004 60004" || fields["Groups"] != "" || fields["NoNewPrivs"] != "1" {
			t.Fatalf("actual credential/NNP mismatch: %#v", fields)
		}
	}
	t.Logf("actual uid/gid60004 all5capsets0 NNP1 supplementary0 threads=%d", len(threads))
	fd, e := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(fd)
	if e = unix.Bind(fd, &unix.SockaddrUnix{Name: filepath.Join(dir, "control")}); e != nil {
		t.Fatalf("control create: %v", e)
	}
	if e = os.WriteFile(filepath.Join(dir, "queue"), []byte("own queue\n"), 0644); e != nil {
		t.Fatalf("queue create: %v", e)
	}
	if e = os.Mkdir(filepath.Join(dir, "data"), 0755); e != nil {
		t.Fatalf("own DB directory create: %v", e)
	}
	rules := filepath.Join(dir, "rules.d")
	deny := func(name string, e error) {
		t.Helper()
		if !os.IsPermission(e) && !normalStockUdevStickyPermission(e) {
			t.Fatalf("%s required EPERM/EACCES, got %v", name, e)
		}
		t.Logf("denied %s: %v", name, e)
	}
	deny("rename original rules", unix.Renameat(unix.AT_FDCWD, rules, unix.AT_FDCWD, filepath.Join(dir, "moved-rules")))
	deny("remove original rules", os.Remove(rules))
	replacement := filepath.Join(dir, "replacement")
	if e = os.Mkdir(replacement, 0755); e != nil {
		t.Fatal(e)
	}
	deny("replace original rules", unix.Renameat(unix.AT_FDCWD, replacement, unix.AT_FDCWD, rules))
	deny("write original rules", os.WriteFile(filepath.Join(rules, "injected.rules"), []byte("own test only\n"), 0600))
	deny("chmod Root parent", os.Chmod(dir, 0777))
	deny("chmod original rules", os.Chmod(rules, 0777))
	if _, e = os.Lstat(filepath.Join(dir, "control")); e != nil {
		t.Fatal(e)
	}
	t.Log(fmt.Sprintf("actual control/queue/DB positive and six sticky/DAC negative operations completed on own temp %s", dir))
}

func normalStockUdevStickyPermission(e error) bool { return e == syscall.EPERM || e == syscall.EACCES }
