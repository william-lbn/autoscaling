//go:build linux && dsga_native_array_populate

package resourceguard

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

func arrayNativeCheckGuest(t *testing.T) {
	t.Helper()
	if os.Getenv("DSGA_ARRAY_HELPER_NATIVE") != "isolated369-array-helper-v1" {
		t.Fatal("explicit isolated369 opt-in required")
	}
	release, err := kernelText("/proc/sys/kernel/osrelease")
	if err != nil || release != "6.12.26-william-normal-split-v1-e210f5e3ee94" {
		t.Fatal("accurate369 only", err)
	}
	cap, err := kernelText("/proc/sys/kernel/neonvm_pid_max_limit")
	if err != nil || cap != "512" {
		t.Fatal("early hard PID source differs", err)
	}
	line, err := kernelText("/proc/cmdline")
	if err != nil || !strings.Contains(line, "neon_array_populate_component=1") {
		t.Fatal("isolated Guest cmdline required")
	}
}
func arrayNativeFiles(t *testing.T) [4]*os.File {
	t.Helper()
	var files [4]*os.File
	for i := range files {
		fd, err := unix.MemfdCreate(fmt.Sprintf("native-array-%d", i), unix.MFD_CLOEXEC)
		if err != nil {
			t.Fatal(err)
		}
		if err = unix.Ftruncate(fd, 2<<20); err != nil {
			t.Fatal(err)
		}
		files[i] = os.NewFile(uintptr(fd), "original-native-shmem")
		t.Cleanup(func() { _ = files[i].Close() })
	}
	return files
}
func arrayNativeContext(t *testing.T, files [4]*os.File) arrayPopulateContext {
	t.Helper()
	_, _, command := fixtureGuard(t)
	var pg, a, system unix.Stat_t
	for path, st := range map[string]*unix.Stat_t{pgCgroup: &pg, arrayPopulateCgroup: &a, systemCgroup: &system} {
		if err := unix.Stat(path, st); err != nil {
			t.Fatal(err)
		}
	}
	boot, err := kernelText("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		t.Fatal(err)
	}
	ticks, err := postmasterTicks(string(raw), os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	// 原PG/Key/逻辑boot是明确fixture；不把这条helper机制认证成生产原出生。
	command.Identity.GuestBootID = boot
	command.Identity.CgroupDevice = control.Uint64(pg.Dev)
	command.Identity.CgroupInode = control.Uint64(pg.Ino)
	c := arrayPopulateContext{Original: ProcessIdentity{ProtocolVersion: 1, Identity: command.Identity, PostmasterPID: os.Getpid(), PostmasterStartTicks: control.Uint64(ticks), AddressSpaceMaxBytes: 2 << 30}, PGBootID: 11, ResourceFloor: 1, OriginalContextSHA256: strings.Repeat("ab", 32), HelperSHA256: os.Getenv("DSGA_ARRAY_HELPER_ELF_SHA256"), MemlockUpperBytes: 1 << 20, AddressSpaceMaxBytes: 64 << 20, HelperReserveBytes: 65 << 20, ArraysMaxBytes: 128 << 20, SystemMaxBytes: 512 << 20, ArraysDevice: control.Uint64(a.Dev), ArraysInode: control.Uint64(a.Ino), SystemDevice: control.Uint64(system.Dev), SystemInode: control.Uint64(system.Ino)}
	for i, f := range files {
		var st unix.Stat_t
		if err := unix.Fstat(int(f.Fd()), &st); err != nil {
			t.Fatal(err)
		}
		c.Files[i] = arrayPopulateFile{Device: control.Uint64(st.Dev), Inode: control.Uint64(st.Ino), FileBytes: control.Uint64(st.Size), Offset: 64 << 10, Length: 1 << 20}
	}
	if err = c.validate(); err != nil {
		t.Fatal(err)
	}
	return c
}
func arrayNativeResident(t *testing.T, files [4]*os.File, c arrayPopulateContext) [4]int {
	t.Helper()
	var counts [4]int
	for i, f := range files {
		data, err := unix.Mmap(int(f.Fd()), int64(c.Files[i].Offset), int(c.Files[i].Length), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			t.Fatal(err)
		}
		vec := make([]byte, len(data)/4096)
		_, _, eno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(&vec[0])))
		runtime.KeepAlive(data)
		runtime.KeepAlive(vec)
		if eno != 0 {
			t.Fatal(eno)
		}
		for _, b := range vec {
			if b&1 != 0 {
				counts[i]++
			}
		}
		if err = unix.Munmap(data); err != nil {
			t.Fatal(err)
		}
	}
	return counts
}
func arrayNativePFNs(t *testing.T, child arrayPopulateChild, idx int, c arrayPopulateContext) []map[string]uint64 {
	t.Helper()
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/maps", child.PID))
	if err != nil {
		t.Fatal(err)
	}
	var base uint64
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) >= 5 && f[4] == strconv.FormatUint(uint64(c.Files[idx].Inode), 10) && f[1] == "rw-s" {
			ends := strings.Split(f[0], "-")
			base, err = strconv.ParseUint(ends[0], 16, 64)
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if base == 0 {
		t.Fatal("real helper target VMA not found")
	}
	page, err := os.Open(fmt.Sprintf("/proc/%d/pagemap", child.PID))
	if err != nil {
		t.Fatal(err)
	}
	defer page.Close()
	owner, err := os.Open("/proc/kpagecgroup")
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	var out []map[string]uint64
	for _, v := range []uint64{base, base + uint64(c.Files[idx].Length) - 4096} {
		var raw [8]byte
		if _, err = page.ReadAt(raw[:], int64(v/4096*8)); err != nil {
			t.Fatal(err)
		}
		entry := binary.LittleEndian.Uint64(raw[:])
		if entry>>63 != 1 || entry&(1<<62) != 0 {
			t.Fatal("target page not ordinary present")
		}
		pfn := entry & ((uint64(1) << 55) - 1)
		if pfn == 0 {
			t.Fatal("actual root PFN visibility required")
		}
		if _, err = owner.ReadAt(raw[:], int64(pfn*8)); err != nil {
			t.Fatal(err)
		}
		charge := binary.LittleEndian.Uint64(raw[:])
		if charge != uint64(c.ArraysInode) {
			t.Fatalf("first touch charged elsewhere: PFN%d owner%d A%d", pfn, charge, c.ArraysInode)
		}
		out = append(out, map[string]uint64{"virtual": v, "PFN": pfn, "owner_inode": charge})
	}
	return out
}
func arrayNativeSave(t *testing.T, name string, v any) {
	t.Helper()
	root := os.Getenv("DSGA_ARRAY_HELPER_EVIDENCE")
	if root == "" {
		t.Fatal("evidence dir required")
	}
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, name+".json"), append(raw, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestNativeArrayPopulateKernelComponent(t *testing.T) {
	arrayNativeCheckGuest(t)
	if os.Getenv("DSGA_ARRAY_HELPER_RECOVER_PATH") != "" {
		arrayNativeRecoverRoot(t)
		return
	}
	if os.Getenv("DSGA_ARRAY_HELPER_PARENT") == "1" {
		arrayNativeRootParent(t)
		return
	}
	t.Run("root-death", func(t *testing.T) { arrayNativeRootDeath(t) })
	for _, mode := range []string{"valid", "cancel-after-first"} {
		t.Run(mode, func(t *testing.T) {
			files := arrayNativeFiles(t)
			c := arrayNativeContext(t, files)
			dir := filepath.Join("/state", mode)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			s, err := lockState(context.Background(), dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if before := arrayNativeResident(t, files, c); before != [4]int{} {
				t.Fatal("fixture shmem already populated", before)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var pfns [4][]map[string]uint64
			var imageOwners map[string]any
			result, runErr := runNativeArrayPopulate(ctx, s, c, files, func(result arrayPopulateResult, index, pidfd int) error {
				p := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
				n, err := unix.Poll(p, 0)
				if err != nil || n != 0 {
					t.Fatal("original producer not live at progress")
				}
				pfns[index] = arrayNativePFNs(t, result.Child, index, c)
				if index == 0 {
					imageOwners = arrayNativeImageOwners(t, result.Child, c)
				}
				if mode == "cancel-after-first" {
					cancel()
				}
				return nil
			})
			resident := arrayNativeResident(t, files, c)
			if mode == "valid" {
				if runErr != nil || !result.Completed || !result.ExitObserved || result.Progress != [4]bool{true, true, true, true} || resident != [4]int{256, 256, 256, 256} {
					t.Fatalf("helper valid failed: %+v %v %v", result, runErr, resident)
				}
				if err = rejectOutstandingArrayPopulateLocked(s); err != nil {
					t.Fatal(err)
				}
				t.Run("new-root-recovery", func(t *testing.T) {
					before := arrayPopulateSnapshot(t, s)
					s.close() // 原Root先释放同resource锁；新Root不能借旧进程内存见证。
					exe, e := os.Executable()
					if e != nil {
						t.Fatal(e)
					}
					cmd := exec.Command(exe, "-test.run=^TestNativeArrayPopulateKernelComponent$", "-test.timeout=10s")
					cmd.Env = append(os.Environ(), "DSGA_ARRAY_HELPER_RECOVER_PATH="+dir, "DSGA_ARRAY_HELPER_RECOVER_NONCE="+result.Nonce)
					if raw, e := cmd.CombinedOutput(); e != nil {
						t.Fatalf("new Root failed: %v %s", e, raw)
					}
					reopened, e := lockState(context.Background(), dir)
					if e != nil {
						t.Fatal(e)
					}
					defer reopened.close()
					if !reflect.DeepEqual(before, arrayPopulateSnapshot(t, reopened)) {
						t.Fatal("recovery changed original JSON/nonce")
					}
					if now := arrayNativeResident(t, files, c); now != resident {
						t.Fatal("recovery changed populated arrays", now)
					}
					if e := arrayPopulateGroups(c, 0); e != nil {
						t.Fatal("helper live after recovery", e)
					}
				})
			} else {
				if !errors.Is(runErr, context.Canceled) || !errors.Is(runErr, ErrUnknown) || result.Completed || !result.ExitObserved || result.Progress != [4]bool{true, false, false, false} || resident != [4]int{256, 0, 0, 0} {
					t.Fatalf("helper cancel failed: %+v %v %v", result, runErr, resident)
				}
				if !errors.Is(rejectOutstandingArrayPopulateLocked(s), ErrUnknown) {
					t.Fatal("partial pending automatically adopted")
				}
			}
			if result.AllSourcesClosed || result.NormalAuthorized {
				t.Fatal("component forged normal authorization")
			}
			detail := ""
			if runErr != nil {
				detail = runErr.Error()
			}
			arrayNativeSave(t, "array-helper-"+mode, map[string]any{"result": result, "error": detail, "resident_target_pages": resident, "actual_first_last_pfns": pfns, "actual_helper_image_owners": imageOwners, "arrays_inode": c.ArraysInode, "helper_reserve_bytes": c.HelperReserveBytes, "scope": "REAL369_GO_C_ATOMIC_A_COMPONENT_FIXTURE_PG_SOURCE_NO_NORMAL", "physical_free_proved": false})
		})
	}
}

type arrayNativeDeathReport struct{ Result arrayPopulateResult }

func arrayNativeRootParent(t *testing.T) {
	files := [4]*os.File{}
	for i := range files {
		files[i] = os.NewFile(uintptr(4+i), "inherited-original-shmem")
		defer files[i].Close()
	}
	c := arrayNativeContext(t, files)
	s, err := lockState(context.Background(), "/state/root-death")
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = runNativeArrayPopulate(ctx, s, c, files, func(result arrayPopulateResult, index, pidfd int) error {
		if index != 0 {
			return fmt.Errorf("test parent must stop at first range")
		}
		raw, e := json.Marshal(arrayNativeDeathReport{Result: result})
		if e != nil {
			return e
		}
		n, e := unix.SendmsgN(3, raw, unix.UnixRights(pidfd), nil, 0)
		if e != nil || n != len(raw) {
			return fmt.Errorf("original child pidfd report failed: %v", e)
		}
		// 明确native夹具：Root被外部原pidfd杀死前不返回，不可能发下一token。
		<-make(chan struct{})
		return nil
	})
	t.Fatalf("fixture Root returned before explicit death: %v", err)
}
func arrayNativeRootDeath(t *testing.T) {
	t.Helper()
	files := arrayNativeFiles(t)
	c := arrayNativeContext(t, files)
	if err := os.Mkdir("/state/root-death", 0700); err != nil {
		t.Fatal(err)
	}
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	parent := os.NewFile(uintptr(pair[0]), "grandparent-private-report")
	defer parent.Close()
	child := os.NewFile(uintptr(pair[1]), "root-private-report")
	defer child.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pidfd := -1
	cmd := exec.Command(executable, "-test.run=^TestNativeArrayPopulateKernelComponent$", "-test.v", "-test.timeout=40s")
	cmd.Env = append(os.Environ(), "DSGA_ARRAY_HELPER_PARENT=1")
	cmd.ExtraFiles = append([]*os.File{child}, files[:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &pidfd, Pdeathsig: syscall.SIGKILL}
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	child.Close()
	if pidfd < 0 {
		t.Fatal("original Root atomic pidfd missing")
	}
	defer unix.Close(pidfd)
	waited := false
	defer func() {
		if !waited {
			_ = unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0)
			_ = cmd.Wait()
		}
	}()
	poll := []unix.PollFd{{Fd: int32(pair[0]), Events: unix.POLLIN | unix.POLLHUP}}
	n, err := unix.Poll(poll, 15000)
	if err != nil || n == 0 {
		t.Fatalf("Root report missing: %v %s", err, output.String())
	}
	raw := make([]byte, 8192)
	oob := make([]byte, unix.CmsgSpace(4))
	n, on, flags, _, err := unix.Recvmsg(pair[0], raw, oob, 0)
	if err != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || n == 0 {
		t.Fatalf("Root report invalid: %v %s", err, output.String())
	}
	msgs, err := unix.ParseSocketControlMessage(oob[:on])
	if err != nil || len(msgs) != 1 {
		t.Fatal("original helper pidfd control missing", err)
	}
	fds, err := unix.ParseUnixRights(&msgs[0])
	if err != nil || len(fds) != 1 {
		t.Fatal("original helper pidfd count differs", err)
	}
	helperFD := fds[0]
	defer unix.Close(helperFD)
	var report arrayNativeDeathReport
	if err = json.Unmarshal(raw[:n], &report); err != nil {
		t.Fatal(err)
	}
	if report.Result.Progress != [4]bool{true, false, false, false} || report.Result.Child.PID <= 1 {
		t.Fatal("Root report progress differs")
	}
	firstPFNs := arrayNativePFNs(t, report.Result.Child, 0, c)
	before := arrayNativeResident(t, files, c)
	if before != [4]int{256, 0, 0, 0} {
		t.Fatal("fixture Root populated late range", before)
	}
	// 实际父Root仍持原flock时，新打开OFD不能取锁。KCMP同OFD已由prepared停点核实。
	competing, err := unix.Open("/state/root-death/lock", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(competing)
	if err = unix.Flock(competing, unix.LOCK_EX|unix.LOCK_NB); err != unix.EWOULDBLOCK {
		t.Fatal("original live OFD lock missing", err)
	}
	if err = unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	waited = true
	if err == nil || cmd.ProcessState == nil {
		t.Fatal("original Root death not observed", output.String())
	}
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("Root termination differs", status)
	}
	hp := []unix.PollFd{{Fd: int32(helperFD), Events: unix.POLLIN}}
	n, err = unix.Poll(hp, 3000)
	if err != nil || n == 0 || hp[0].Revents&unix.POLLIN == 0 {
		t.Fatal("original helper exit not observed after Root death", err)
	}
	after := arrayNativeResident(t, files, c)
	if after != [4]int{256, 0, 0, 0} {
		t.Fatal("late target charge after Root death", after)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := lockState(ctx, "/state/root-death")
	if err != nil {
		t.Fatal("original inherited OFD did not close", err)
	}
	defer s.close()
	if !errors.Is(rejectOutstandingArrayPopulateLocked(s), ErrUnknown) {
		t.Fatal("restarted Root adopted incomplete operation")
	}
	arrayNativeSave(t, "array-helper-root-death", map[string]any{"originalRootPID": cmd.Process.Pid, "originalHelperPID": report.Result.Child.PID, "originalNonce": report.Result.Nonce, "originalRootKilledAndWaited": true, "originalHelperPidfdExitObserved": true, "originalOFDCompetingOpenBlockedBeforeDeath": true, "pendingBlocksRestartedWriter": true, "residentBefore": before, "residentAfter": after, "firstLastArrayPFNs": firstPFNs, "scope": "REAL369_ROOT_DEATH_HELPER_COMPONENT_NOT_PG_OR_NORMAL", "physicalFreeProved": false, "allSourcesClosed": false})
}

// 实际kpagecgroup核验helper全部可写LOAD（含64K固定栈）的新匿名/COW收费。
// 只读LOAD保留真实owner，不把Root预读的旧cache改说成全部已计A。
func arrayNativeImageOwners(t *testing.T, child arrayPopulateChild, c arrayPopulateContext) map[string]any {
	t.Helper()
	elf, err := os.Open(arrayPopulateHelperPath)
	if err != nil {
		t.Fatal(err)
	}
	defer elf.Close()
	image, _, err := arrayPopulateLockedImage(elf)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/maps", child.PID))
	if err != nil {
		t.Fatal(err)
	}
	page, err := os.Open(fmt.Sprintf("/proc/%d/pagemap", child.PID))
	if err != nil {
		t.Fatal(err)
	}
	defer page.Close()
	owners, err := os.Open("/proc/kpagecgroup")
	if err != nil {
		t.Fatal(err)
	}
	defer owners.Close()
	writable := 0
	readonly := map[string]uint64{}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) < 5 {
			continue
		}
		ends := strings.Split(f[0], "-")
		if len(ends) != 2 {
			t.Fatal("maps header differs")
		}
		a, e1 := strconv.ParseUint(ends[0], 16, 64)
		b, e2 := strconv.ParseUint(ends[1], 16, 64)
		if e1 != nil || e2 != nil {
			t.Fatal("maps addresses invalid")
		}
		for _, span := range image {
			for v := max(a, span.Start); v < min(b, span.End); v += 4096 {
				var raw [8]byte
				if _, err = page.ReadAt(raw[:], int64(v/4096*8)); err != nil {
					t.Fatal(err)
				}
				entry := binary.LittleEndian.Uint64(raw[:])
				if entry>>63 != 1 {
					t.Fatal("locked image not present")
				}
				pfn := entry & ((uint64(1) << 55) - 1)
				if _, err = owners.ReadAt(raw[:], int64(pfn*8)); err != nil {
					t.Fatal(err)
				}
				owner := binary.LittleEndian.Uint64(raw[:])
				if f[1][1] == 'w' {
					writable++
					if owner != uint64(c.ArraysInode) {
						t.Fatalf("helper new writable LOAD page charged outside A: %x owner%d", v, owner)
					}
				} else {
					readonly[strconv.FormatUint(v, 16)] = owner
				}
			}
		}
	}
	if writable < 16 {
		t.Fatal("full fixed64K stack image was not observed")
	}
	var rootGroup unix.Stat_t
	if err := unix.Stat("/sys/fs/cgroup", &rootGroup); err != nil {
		t.Fatal(err)
	}
	return map[string]any{"actualRootCgroupInode": rootGroup.Ino, "originalSystemInode": c.SystemInode, "writableLOADPagesAllA": writable, "readonlyLOADOwnerInodes": readonly, "readonlyOwner0NotClassifiedAsK": true, "helperReserveIsConservativeNotChargeMigration": true}
}

// 真369 Guest已完成原helper的独立新Root；这里只做文件持久恢复，不启动helper。
func arrayNativeRecoverRoot(t *testing.T) {
	t.Helper()
	dir, nonce := os.Getenv("DSGA_ARRAY_HELPER_RECOVER_PATH"), os.Getenv("DSGA_ARRAY_HELPER_RECOVER_NONCE")
	s, err := lockState(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	before := arrayPopulateSnapshot(t, s)
	if !errors.Is(rejectOutstandingArrayPopulateLocked(s), ErrUnknown) {
		t.Fatal("new Root borrowed old durability witness")
	}
	if !reflect.DeepEqual(before, arrayPopulateSnapshot(t, s)) {
		t.Fatal("ordinary gate wrote disk")
	}
	members, err := kernelText(arrayPopulateCgroup + "/cgroup.procs")
	if err != nil || strings.TrimSpace(members) != "" {
		t.Fatal("helper unexpectedly live", err, members)
	}
	if err := recoverArrayPopulateCompletedLocked(s, nonce); err != nil {
		t.Fatal(err)
	}
	if err := rejectOutstandingArrayPopulateLocked(s); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, arrayPopulateSnapshot(t, s)) {
		t.Fatal("recovery changed original four files")
	}
	after, err := kernelText(arrayPopulateCgroup + "/cgroup.procs")
	if err != nil || strings.TrimSpace(after) != "" {
		t.Fatal("recovery spawned helper", err, after)
	}
	arrayNativeSave(t, "array-helper-new-root-recovery", map[string]any{"nonce": nonce, "ordinaryGateUnknown": true, "ordinaryGateZeroWrite": true, "explicitRecoverSucceeded": true, "originalFourJSONBytesUnchanged": true, "arraysLeafEmptyBeforeAfter": true, "noNewHelperOrPopulate": true, "diskFaultInjected": false, "scope": "REAL369_NEW_ROOT_EXPLICIT_DURABILITY_RECOVERY_COMPONENT_ONLY", "allSourcesClosed": false, "normalAuthorized": false, "physicalFreeProved": false})
}
