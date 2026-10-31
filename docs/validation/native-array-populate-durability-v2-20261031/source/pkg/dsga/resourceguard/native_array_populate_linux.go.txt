//go:build linux

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const arrayPopulateCgroup = pgCgroup + "/arrays"

type arrayPopulateChild struct {
	ProtocolVersion uint32         `json:"protocol_version"`
	PendingSHA256   string         `json:"pending_sha256"`
	PID             int            `json:"pid"`
	StartTicks      control.Uint64 `json:"start_ticks"`
	PIDFDDevice     control.Uint64 `json:"pidfd_device"`
	PIDFDInode      control.Uint64 `json:"pidfd_inode"`
}
type arrayPopulatePrepared struct {
	TracerTID       int            `json:"tracer_tid"`
	ProtocolVersion uint32         `json:"protocol_version"`
	ChildSHA256     string         `json:"child_sha256"`
	StatusSHA256    string         `json:"status_sha256"`
	FilterSHA256    string         `json:"filter_sha256"`
	VmSizeBytes     control.Uint64 `json:"vm_size_bytes"`
	LockedBytes     control.Uint64 `json:"locked_bytes"`
}
type arrayPopulateComplete struct {
	ProtocolVersion uint32  `json:"protocol_version"`
	PreparedSHA256  string  `json:"prepared_sha256"`
	PID             int     `json:"pid"`
	ExitObserved    bool    `json:"exit_observed"`
	Progress        [4]bool `json:"progress"`
}
type arrayPopulateResult struct {
	Nonce            string             `json:"nonce"`
	Child            arrayPopulateChild `json:"child"`
	Progress         [4]bool            `json:"progress"`
	LockedBytes      control.Uint64     `json:"helper_locked_bytes"`
	ExitObserved     bool               `json:"exit_observed"`
	Completed        bool               `json:"completed"`
	AllSourcesClosed bool               `json:"all_sources_closed"`
	NormalAuthorized bool               `json:"normal_authorized"`
}

// 仅测试/未来原Root观察用途；不向HTTP暴露，也不以callback返回值授预算。
// callback发生在真实populate进度后，可取消ctx；生产接线必须传nil。
type arrayPopulateProgressObserver func(arrayPopulateResult, int, int) error

func arrayPopulateName(nonce, phase string) string {
	return "array-populate." + nonce + "." + phase + ".json"
}

// 文件身份和ctime随原字节一起绑定进内存见证。同字节的新inode/重写文件
// 仍须显式恢复；不能仅因hash相同就沿用旧文件的fsync证明。
type arrayPopulateHistoryIdentity struct {
	Device            uint64
	Inode             uint64
	Bytes             int64
	ChangeSeconds     int64
	ChangeNanoseconds int64
}

func arrayPopulateHistoryStat(st unix.Stat_t) arrayPopulateHistoryIdentity {
	return arrayPopulateHistoryIdentity{st.Dev, st.Ino, st.Size, st.Ctim.Sec, st.Ctim.Nsec}
}
func arrayPopulateHistoryRaw(s *lockedState, name string) ([]byte, arrayPopulateHistoryIdentity, error) {
	var zero arrayPopulateHistoryIdentity
	fd, err := unix.Openat(int(s.dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, zero, errors.Join(ErrUnknown, err)
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	var before, after unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return nil, zero, errors.Join(ErrUnknown, err)
	}
	if err := privateRegular(fd, unix.S_IFREG); err != nil {
		return nil, zero, errors.Join(ErrUnknown, err)
	}
	raw, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil || len(raw) > 64*1024 {
		return nil, zero, errors.Join(ErrUnknown, fmt.Errorf("array history unreadable or exceeds bound"), err)
	}
	if err := unix.Fstat(fd, &after); err != nil {
		return nil, zero, errors.Join(ErrUnknown, err)
	}
	if arrayPopulateHistoryStat(before) != arrayPopulateHistoryStat(after) || privateRegular(fd, unix.S_IFREG) != nil {
		return nil, zero, errors.Join(ErrUnknown, fmt.Errorf("original array file changed while read"))
	}
	return raw, arrayPopulateHistoryStat(after), nil
}

// 完成记录只说明本次helper已同步退出；不认证PG当前容量、不产生normal许可。
// 磁盘四链可见不等于原fsync已经成功。仅本Root进程内的私有见证可让普通
// 门禁通过；Root重启后必须显式Recover，pending/partial从不重放或删除。
type arrayPopulateDurabilityKey struct {
	StateDevice uint64
	StateInode  uint64
	LockDevice  uint64
	LockInode   uint64
	Nonce       string
	ChainSHA256 [4]string
	Files       [4]arrayPopulateHistoryIdentity
}

// 仅内存持有，故exec/Root重启不会继承；不是可由JSON/marker重建的授权。
// key覆盖实际原目录/锁身份、nonce及四链原字节，不能跨目录或代次借用。
var arrayPopulateDurabilityWitnesses sync.Map

func arrayPopulateStateKey(s *lockedState, nonce string) (arrayPopulateDurabilityKey, error) {
	var key arrayPopulateDurabilityKey
	if s == nil || s.dir == nil || s.lock == nil {
		return key, errors.Join(ErrUnknown, fmt.Errorf("array operation requires original resource lock"))
	}
	if _, err := nativeBirthDigest(nonce); err != nil {
		return key, errors.Join(ErrUnknown, err)
	}
	var dir, lock unix.Stat_t
	if err := unix.Fstat(int(s.dir.Fd()), &dir); err != nil {
		return key, errors.Join(ErrUnknown, err)
	}
	if err := unix.Fstat(int(s.lock.Fd()), &lock); err != nil {
		return key, errors.Join(ErrUnknown, err)
	}
	if privateRegular(int(s.dir.Fd()), unix.S_IFDIR) != nil || privateRegular(int(s.lock.Fd()), unix.S_IFREG) != nil || dir.Dev != lock.Dev {
		return key, errors.Join(ErrUnknown, fmt.Errorf("original array state identity differs"))
	}
	key.StateDevice, key.StateInode = dir.Dev, dir.Ino
	key.LockDevice, key.LockInode, key.Nonce = lock.Dev, lock.Ino, nonce
	return key, nil
}

// 完整链只读验证：ExitObserved只能来自原helper终态记录，不能由恢复再运行helper补齐。
func arrayPopulateCompleteKeyLocked(s *lockedState, nonce string) (arrayPopulateDurabilityKey, error) {
	var zero arrayPopulateDurabilityKey
	key, err := arrayPopulateStateKey(s, nonce)
	if err != nil {
		return zero, err
	}
	pending, pendingIdentity, e := arrayPopulateHistoryRaw(s, arrayPopulateName(nonce, "pending"))
	if e != nil {
		return zero, e
	}
	var p arrayPopulatePending
	if control.UnmarshalObject(pending, &p) != nil || p.validate() != nil || p.Nonce != nonce || uint64(p.StateDevice) != key.StateDevice || uint64(p.StateInode) != key.StateInode || uint64(p.LockDevice) != key.LockDevice || uint64(p.LockInode) != key.LockInode {
		return zero, ErrUnknown
	}
	child, childIdentity, e := arrayPopulateHistoryRaw(s, arrayPopulateName(nonce, "child"))
	if e != nil {
		return zero, e
	}
	var c arrayPopulateChild
	if control.UnmarshalObject(child, &c) != nil || c.ProtocolVersion != 1 || c.PendingSHA256 != physicalDigest(pending) || c.PID <= 1 || c.StartTicks == 0 || c.PIDFDDevice == 0 || c.PIDFDInode == 0 {
		return zero, ErrUnknown
	}
	prepared, preparedIdentity, e := arrayPopulateHistoryRaw(s, arrayPopulateName(nonce, "prepared"))
	if e != nil {
		return zero, e
	}
	var q arrayPopulatePrepared
	if control.UnmarshalObject(prepared, &q) != nil || q.ProtocolVersion != 1 || q.TracerTID <= 0 || q.ChildSHA256 != physicalDigest(child) || !physicalDigestValid(q.StatusSHA256) || !physicalDigestValid(q.FilterSHA256) || q.VmSizeBytes > p.Context.AddressSpaceMaxBytes || q.LockedBytes == 0 || q.LockedBytes > p.Context.MemlockUpperBytes {
		return zero, ErrUnknown
	}
	complete, completeIdentity, e := arrayPopulateHistoryRaw(s, arrayPopulateName(nonce, "complete"))
	if e != nil {
		return zero, e
	}
	var z arrayPopulateComplete
	if control.UnmarshalObject(complete, &z) != nil || z.ProtocolVersion != 1 || z.PreparedSHA256 != physicalDigest(prepared) || z.PID != c.PID || !z.ExitObserved {
		return zero, ErrUnknown
	}
	for i, spec := range p.Context.Files {
		if z.Progress[i] != (spec.Length != 0) {
			return zero, ErrUnknown
		}
	}
	key.ChainSHA256 = [4]string{physicalDigest(pending), physicalDigest(child), physicalDigest(prepared), physicalDigest(complete)}
	key.Files = [4]arrayPopulateHistoryIdentity{pendingIdentity, childIdentity, preparedIdentity, completeIdentity}
	return key, nil
}

func rejectOutstandingArrayPopulateLocked(s *lockedState) error {
	if s == nil || s.dir == nil || s.lock == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("array operation requires original resource lock"))
	}
	fd, e := unix.Openat(int(s.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	f := os.NewFile(uintptr(fd), "array-operation-scan")
	names, e := f.Readdirnames(-1)
	f.Close()
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if len(names) > 4096 {
		return errors.Join(ErrUnknown, fmt.Errorf("array history exceeds bound"))
	}
	families := map[string]map[string]bool{}
	for _, name := range names {
		if !strings.HasPrefix(name, "array-populate.") {
			continue
		}
		parts := strings.Split(name, ".")
		if len(parts) != 4 || parts[0] != "array-populate" || parts[3] != "json" {
			return ErrUnknown
		}
		if _, e := nativeBirthDigest(parts[1]); e != nil {
			return errors.Join(ErrUnknown, e)
		}
		if parts[2] != "pending" && parts[2] != "child" && parts[2] != "prepared" && parts[2] != "complete" {
			return ErrUnknown
		}
		if families[parts[1]] == nil {
			families[parts[1]] = map[string]bool{}
		}
		families[parts[1]][parts[2]] = true
	}
	for nonce, files := range families {
		if len(files) != 4 {
			return errors.Join(ErrUnknown, fmt.Errorf("original array operation pending/partial; no automatic replay"))
		}
		key, err := arrayPopulateCompleteKeyLocked(s, nonce)
		if err != nil {
			return errors.Join(ErrUnknown, err)
		}
		// 普通Observe/gate必须零写；绝不以可见complete、后继fsync或新marker猜原完成持久。
		if _, ok := arrayPopulateDurabilityWitnesses.Load(key); !ok {
			return errors.Join(ErrUnknown, fmt.Errorf("complete array chain lacks this Root durability witness; explicit recovery required"))
		}
	}
	return nil
}

// 私有IO边界仅供精确故障模型；生产Append/Recover始终使用下面固定实现。
// 不暴露HTTP/config布尔开关，不允许调用者注入成功见证或省略文件同步。
type arrayPopulateDurabilityIO struct {
	create   func(*lockedState, string, []byte) error
	syncFile func(*lockedState, string) error
	syncDir  func(*lockedState) error
}

func arrayPopulateDiskDurability() arrayPopulateDurabilityIO {
	return arrayPopulateDurabilityIO{
		create:   func(s *lockedState, name string, raw []byte) error { return s.createPhysicalFile(name, raw) },
		syncFile: func(s *lockedState, name string) error { return s.syncPhysicalFile(name) },
		syncDir:  func(s *lockedState) error { return s.dir.Sync() },
	}
}

// 原完成与显式恢复共用完整持久前缀：先验证原四链，逐文件+目录fsync，再
// 重新读取相同身份/原字节后登记本进程见证。任何失败只留Unknown，绝不删partial。
func arrayPopulateConfirmDurableLocked(s *lockedState, nonce string, disk arrayPopulateDurabilityIO) error {
	before, err := arrayPopulateCompleteKeyLocked(s, nonce)
	if err != nil {
		return errors.Join(ErrUnknown, err)
	}
	arrayPopulateDurabilityWitnesses.Delete(before)
	if disk.syncFile == nil || disk.syncDir == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original array durability IO missing"))
	}
	for _, phase := range []string{"pending", "child", "prepared", "complete"} {
		if err := disk.syncFile(s, arrayPopulateName(nonce, phase)); err != nil {
			return errors.Join(ErrUnknown, fmt.Errorf("array completion prefix sync %s: %w", phase, err))
		}
	}
	if err := disk.syncDir(s); err != nil {
		return errors.Join(ErrUnknown, fmt.Errorf("array completion directory sync: %w", err))
	}
	after, err := arrayPopulateCompleteKeyLocked(s, nonce)
	if err != nil || after != before {
		return errors.Join(ErrUnknown, fmt.Errorf("array completion changed during durability confirmation"), err)
	}
	arrayPopulateDurabilityWitnesses.Store(after, struct{}{})
	return nil
}

// 显式私有恢复仅确认已退出的完整原四链。旧Root的完成文件不能自动解除Unknown；
// 本入口不启动helper、不发送populate token、不补缺失记录、不重放或修改原nonce。
func recoverArrayPopulateCompletedLocked(s *lockedState, nonce string) error {
	return arrayPopulateConfirmDurableLocked(s, nonce, arrayPopulateDiskDurability())
}

func arrayPopulateAppend(s *lockedState, nonce, phase string, v any) ([]byte, error) {
	return arrayPopulateAppendWithDurability(s, nonce, phase, v, arrayPopulateDiskDurability())
}
func arrayPopulateAppendWithDurability(s *lockedState, nonce, phase string, v any, disk arrayPopulateDurabilityIO) ([]byte, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	if disk.create == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original array create IO missing"))
	}
	if e = disk.create(s, arrayPopulateName(nonce, phase), b); e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	if phase == "complete" {
		if e = arrayPopulateConfirmDurableLocked(s, nonce, disk); e != nil {
			return nil, e
		}
	}
	return b, nil
}

func arrayPopulateKernelIdentity(c arrayPopulateContext) error {
	if runtime.GOARCH != "amd64" || os.Getpagesize() != 4096 || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fmt.Errorf("array helper requires supported real Root")
	}
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if e != nil {
		return e
	}
	release, e := kernelText("/proc/sys/kernel/osrelease")
	if e != nil || release != ref.KernelRelease {
		return fmt.Errorf("array helper exact369 kernel differs")
	}
	notes, e := physicalKernelNotes()
	if e != nil || physicalDigest(notes) != ref.KernelNotesSHA256 {
		return fmt.Errorf("array helper exact369 notes differ")
	}
	boot, e := kernelText("/proc/sys/kernel/random/boot_id")
	if e != nil || boot != c.Original.Identity.GuestBootID {
		return fmt.Errorf("array helper original Guest differs")
	}
	membership, e := bootProcGroup(os.Getpid())
	if e != nil || membership != strings.TrimPrefix(systemCgroup, "/sys/fs/cgroup") {
		return fmt.Errorf("array helper must start from original System domain")
	}
	swaps, e := kernelText("/proc/swaps")
	if e != nil || len(strings.Split(swaps, "\n")) != 1 || strings.Join(strings.Fields(swaps), " ") != "Filename Type Size Used Priority" {
		return fmt.Errorf("array populate requires original no-swap domain")
	}
	return arrayPopulateCaller()
}

// uid65534目标的取消必须有本tracer线程真实CAP_KILL；Root UID本身不够。
func arrayPopulateCaller() error {
	if e := nativeFilterAuditCaller(); e != nil {
		return e
	}
	var h unix.CapUserHeader
	h.Version = 0x20080522
	var data [2]unix.CapUserData
	if e := unix.Capget(&h, &data[0]); e != nil {
		return e
	}
	if data[0].Effective&(1<<uint(unix.CAP_KILL)) == 0 {
		return fmt.Errorf("array helper parent requires actual CAP_KILL")
	}
	return nil
}
func arrayPopulateVerifyFDs(c arrayPopulateContext, files [4]*os.File) error {
	for i, f := range files {
		if f == nil {
			return fmt.Errorf("original array FD missing")
		}
		var st unix.Stat_t
		var fs unix.Statfs_t
		if unix.Fstat(int(f.Fd()), &st) != nil || unix.Fstatfs(int(f.Fd()), &fs) != nil || fs.Type != unix.TMPFS_MAGIC || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Dev != uint64(c.Files[i].Device) || st.Ino != uint64(c.Files[i].Inode) || st.Size != int64(c.Files[i].FileBytes) {
			return fmt.Errorf("original array shmem identity differs")
		}
		flags, e := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
		if e != nil || flags&unix.O_ACCMODE != unix.O_RDWR {
			return fmt.Errorf("original array FD must be RW")
		}
	}
	return nil
}
func arrayPopulateGroups(c arrayPopulateContext, child int) error {
	a, e := splitReadNode(arrayPopulateCgroup)
	if e != nil {
		return e
	}
	if a.Device != c.ArraysDevice || a.Inode != c.ArraysInode || !a.RootControlled || !a.MemoryHighUnlimited || a.MemoryMaxBytes != c.ArraysMaxBytes || uint64(a.MemoryMaxBytes) >= uint64(^uint64(0)>>1) || a.SwapMaxBytes != 0 || a.PidsMaxTasks != 1 {
		return fmt.Errorf("original finite arrays domain differs")
	}
	entries, e := splitChildren(arrayPopulateCgroup)
	if e != nil || len(entries) != 0 {
		return fmt.Errorf("array helper requires leaf domain")
	}
	sd, si, e := systemCgroupIdentity(uint64(c.SystemDevice))
	if e != nil || sd != uint64(c.SystemDevice) || si != uint64(c.SystemInode) {
		return fmt.Errorf("original System domain differs")
	}
	system, e := splitReadNode(systemCgroup)
	if e != nil || !system.RootControlled || !system.MemoryHighUnlimited || system.MemoryMaxBytes != c.SystemMaxBytes || system.SwapMaxBytes != 0 {
		return fmt.Errorf("original fixed System cap differs")
	}
	pids, e := groupPIDs(arrayPopulateCgroup)
	if e != nil {
		return e
	}
	if child == 0 {
		if len(pids) != 0 || !a.Empty {
			return fmt.Errorf("arrays domain already has producer")
		}
	} else {
		if len(pids) != 1 || pids[0] != child {
			return fmt.Errorf("arrays domain original producer differs")
		}
		threads, e := kernelText(arrayPopulateCgroup + "/cgroup.threads")
		if e != nil || threads != strconv.Itoa(child) {
			return fmt.Errorf("arrays producer not single original task")
		}
	}
	return nil
}

type arrayPopulateProcFacts struct{ VmSize, Locked uint64 }

func arrayPopulateProcess(c arrayPopulateContext, child arrayPopulateChild, wantGroup string, ready arrayPopulateStatus, image []arrayPopulateImageSpan, imageBytes uint64, procfd int) (arrayPopulateProcFacts, error) {
	var out arrayPopulateProcFacts
	raw, e := nativeTraceProcText(procfd, "stat")
	if e != nil {
		return out, e
	}
	ticks, e := postmasterTicks(raw, child.PID)
	if e != nil || uint64(child.StartTicks) != ticks {
		return out, fmt.Errorf("original helper PID start differs")
	}
	group, e := nativeTraceProcText(procfd, "cgroup")
	if e != nil || strings.TrimSpace(group) != "0::"+strings.TrimPrefix(wantGroup, "/sys/fs/cgroup") {
		return out, fmt.Errorf("original helper membership differs")
	}
	b, e := nativeTraceProcText(procfd, "status")
	if e != nil || len(b) > 64*1024 {
		return out, fmt.Errorf("helper status missing/excessive: %v", e)
	}
	fields := map[string]string{}
	for _, l := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(l, ":")
		if ok {
			if _, exists := fields[k]; exists {
				return out, fmt.Errorf("duplicate helper status field")
			}
			fields[k] = strings.TrimSpace(v)
		}
	}
	if fields["Threads"] != "1" || fields["NoNewPrivs"] != "1" || fields["Seccomp"] != "2" || fields["Uid"] != "65534\t65534\t65534\t65534" || fields["Gid"] != "65534\t65534\t65534\t65534" {
		return out, fmt.Errorf("original helper credentials/NNP/thread source differs")
	}
	for _, k := range []string{"CapEff", "CapPrm", "CapInh", "CapAmb"} {
		if fields[k] != "0000000000000000" {
			return out, fmt.Errorf("helper retained capabilities")
		}
	}
	parseKB := func(k string) (uint64, error) {
		f := strings.Fields(fields[k])
		if len(f) != 2 || f[1] != "kB" {
			return 0, fmt.Errorf("helper size field invalid")
		}
		n, e := strconv.ParseUint(f[0], 10, 64)
		if e != nil || n > ^uint64(0)/1024 {
			return 0, fmt.Errorf("helper size overflow")
		}
		return n * 1024, nil
	}
	if out.VmSize, e = parseKB("VmSize"); e != nil {
		return out, e
	}
	if out.Locked, e = parseKB("VmLck"); e != nil {
		return out, e
	}
	if out.VmSize > uint64(c.AddressSpaceMaxBytes) || out.Locked != ready.LockedBytes || out.Locked == 0 || out.Locked > uint64(c.MemlockUpperBytes) {
		return out, fmt.Errorf("helper current AS/locked source outside original bound")
	}
	limits, e := nativeTraceProcText(procfd, "limits")
	if e != nil {
		return out, e
	}
	for _, v := range []struct {
		label string
		want  uint64
		unit  string
	}{{"Max address space", uint64(c.AddressSpaceMaxBytes), "bytes"}, {"Max locked memory", uint64(c.MemlockUpperBytes), "bytes"}, {"Max open files", 11, "files"}} {
		found := 0
		for _, line := range strings.Split(limits, "\n") {
			fields := strings.Fields(line)
			if len(fields) < 3 || strings.Join(fields[:3], " ") != v.label {
				continue
			}
			found++
			want := strconv.FormatUint(v.want, 10)
			if len(fields) != 6 || fields[3] != want || fields[4] != want || fields[5] != v.unit {
				return out, fmt.Errorf("helper original pinned hard limits differ")
			}
		}
		if found != 1 {
			return out, fmt.Errorf("helper original pinned limits missing")
		}
	}

	// 完整核原ELF每个LOAD页lo/非on-fault/实际resident，并拒目标array被锁。
	smaps, e := nativeTraceProcText(procfd, "smaps")
	if e != nil || len(smaps) > 1<<20 {
		return out, fmt.Errorf("helper smaps absent/excessive")
	}
	if out.Locked != imageBytes {
		return out, fmt.Errorf("helper original LOAD lock total differs")
	}
	if e = arrayPopulateVerifyLockedSmaps(string(smaps), image, imageBytes, ready.Ranges); e != nil {
		return out, e
	}
	return out, nil
}

func arrayPopulateRecv(ctx context.Context, fd int) ([]byte, error) {
	b := make([]byte, arrayPopulateStatusBytes)
	for {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		p := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN | unix.POLLHUP | unix.POLLERR}}
		_, e := unix.Poll(p, 25)
		if e == unix.EINTR {
			continue
		}
		if e != nil {
			return nil, e
		}
		if p[0].Revents == 0 {
			continue
		}
		n, _, flags, _, e := unix.Recvmsg(fd, b, nil, unix.MSG_CMSG_CLOEXEC)
		if e != nil {
			return nil, e
		}
		if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || n != len(b) {
			return nil, fmt.Errorf("helper status truncated or EOF")
		}
		return b, nil
	}
}
func arrayPopulateSend(fd int, b []byte) error {
	n, e := unix.Write(fd, b)
	if e != nil {
		return e
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}
func arrayPopulateFilterBytes(program []unix.SockFilter) []byte {
	raw := make([]byte, 8*len(program))
	for i, x := range program {
		b := raw[8*i : 8*i+8]
		binary.LittleEndian.PutUint16(b[:2], x.Code)
		b[2], b[3] = x.Jt, x.Jf
		binary.LittleEndian.PutUint32(b[4:], x.K)
	}
	return raw
}

// 原typed目标已确认停止后只读完整链。expected由独立Go按固定C源生成，
// 不借造PG request/bundle；只支持Root本身Seccomp0且helper唯一一个精确filter。
func arrayPopulateAudit(pid, tracerTID int, expected []unix.SockFilter) error {
	if pid <= 1 || tracerTID != unix.Gettid() || len(expected) == 0 || len(expected) > 128 {
		return fmt.Errorf("original stopped helper audit source invalid")
	}
	if e := nativeFilterAuditCaller(); e != nil {
		return e
	}
	read := func(offset uint32, dst []unix.SockFilter) (int, error) {
		var address *unix.SockFilter
		if len(dst) > 0 {
			address = &dst[0]
		}
		n, _, eno := unix.Syscall6(unix.SYS_PTRACE, ptraceSeccompGetFilter, uintptr(pid), uintptr(offset), uintptr(unsafe.Pointer(address)), 0, 0)
		runtime.KeepAlive(dst)
		if eno != 0 {
			return 0, eno
		}
		if n > 128 {
			return 0, fmt.Errorf("helper kernel program exceeds bound")
		}
		return int(n), nil
	}
	return arrayPopulateCompareInstalled(expected, read)
}

// reader仅供明确模型U；生产固定调用上面的真实ptrace读取器。
func arrayPopulateCompareInstalled(expected []unix.SockFilter, read func(uint32, []unix.SockFilter) (int, error)) error {
	if read == nil || len(expected) == 0 || len(expected) > 128 {
		return fmt.Errorf("original helper filter source missing")
	}
	n, e := read(0, nil)
	if e != nil || n != len(expected) {
		return fmt.Errorf("helper actual filter length differs: %v", e)
	}
	actual := make([]unix.SockFilter, n)
	got, e := read(0, actual)
	if e != nil || got != n {
		return fmt.Errorf("helper actual filter read differs: %v", e)
	}
	for i, x := range actual {
		if x != expected[i] {
			return fmt.Errorf("helper actual filter instruction %d differs", i)
		}
	}
	if _, e = read(1, nil); !errors.Is(e, unix.ENOENT) {
		return fmt.Errorf("helper actual filter terminal is not ENOENT: %v", e)
	}
	return nil
}

// typed停点会话必须已同步清理/detach或确认专用M退役。外层取消仍只对原
// spawn原子pidfd发SIGKILL并实际Wait；不因PID数值相同kill新对象或假定物理free。
// os/exec在内部pidfd dup失败时可能回退numeric os.Process；本组件不调用
// Cmd.Wait，而在外部原CLONE_PIDFD上直接waitid。Cmd没有pipe/Context watcher，
// 结束后只Release其内部句柄。UID只能是固定C在降权前0或降权后65534。
func arrayPopulateDecodeExit(info [128]byte, pid int) (unix.WaitStatus, error) {
	uid := binary.LittleEndian.Uint32(info[20:24])
	if uid != 0 && uid != 65534 {
		return 0, fmt.Errorf("original helper exit UID differs")
	}
	got, status, e := decodeNativeOriginalWait(info, pid, uid)
	if e != nil || got != pid || !status.Exited() && !status.Signaled() {
		return 0, fmt.Errorf("original helper terminal wait differs: %v", e)
	}
	return status, nil
}
func arrayPopulateWaitOriginal(pidfd, pid int) (unix.WaitStatus, error) {
	if pidfd < 0 || pid <= 1 {
		return 0, fmt.Errorf("original helper wait source missing")
	}
	for {
		var info [128]byte
		_, _, eno := unix.Syscall6(unix.SYS_WAITID, 3, uintptr(pidfd), uintptr(unsafe.Pointer(&info[0])), unix.WEXITED|unix.WALL, 0, 0)
		runtime.KeepAlive(info)
		if eno == unix.EINTR {
			continue
		}
		if eno != 0 {
			return 0, eno
		}
		return arrayPopulateDecodeExit(info, pid)
	}
}
func arrayPopulateAbortWait(cmd *exec.Cmd, pidfd int) (bool, error) {
	if cmd == nil || cmd.Process == nil || pidfd < 0 {
		return false, fmt.Errorf("original atomic child missing")
	}
	var report error
	signalErr := unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0)
	if signalErr != nil && !errors.Is(signalErr, unix.ESRCH) {
		report = errors.Join(ErrUnknown, fmt.Errorf("original helper SIGKILL failed: %w", signalErr))
	}
	_, e := arrayPopulateWaitOriginal(pidfd, cmd.Process.Pid)
	if e != nil {
		return false, errors.Join(report, ErrUnknown, fmt.Errorf("original helper P_PIDFD exit unconfirmed: %w", e))
	}
	return true, report
}

// 仅原Root私有调用；调用者已持同resource flock，必须把该writer与后继所有
// cap writer接同一个pending门禁。本组件目前无生产handler/launcher接线。
// 原UID/进程/PG boot/FD来源仍由外层真实producer给出，绝不拿本结果发normal许可。
func runNativeArrayPopulate(ctx context.Context, s *lockedState, c arrayPopulateContext, files [4]*os.File, observe arrayPopulateProgressObserver) (result arrayPopulateResult, err error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if ctx == nil || ctx.Err() != nil {
		return result, fmt.Errorf("live original context required")
	}
	deadline, bounded := ctx.Deadline()
	if !bounded || !deadline.After(time.Now()) || deadline.After(time.Now().Add(30*time.Second)) {
		return result, fmt.Errorf("bounded original helper context required before birth")
	}
	if e := c.validate(); e != nil {
		return result, e
	}
	if e := arrayPopulateKernelIdentity(c); e != nil {
		return result, e
	}
	if e := arrayPopulateVerifyFDs(c, files); e != nil {
		return result, e
	}
	if e := arrayPopulateGroups(c, 0); e != nil {
		return result, e
	}
	if e := rejectOutstandingArrayPopulateLocked(s); e != nil {
		return result, e
	}
	helper, e := nativeBirthOpenRO(arrayPopulateHelperPath)
	if e != nil {
		return result, e
	}
	defer helper.Close()
	h, e := nativeBirthFileHash(helper)
	if e != nil || h != c.HelperSHA256 {
		return result, fmt.Errorf("original fixed helper ELF differs")
	}
	image, imageBytes, e := arrayPopulateLockedImage(helper)
	if e != nil || imageBytes > uint64(c.MemlockUpperBytes) {
		return result, fmt.Errorf("helper original static LOAD image exceeds lock bound: %v", e)
	}
	helperInfo, e := helper.Stat()
	if e != nil {
		return result, e
	}
	fileUpper := (uint64(helperInfo.Size()) + 4095) &^ uint64(4095)
	// 这是A预算的保守重复预留，不声称ELF既有cache已从System/K迁费到A。
	if uint64(c.HelperReserveBytes) < uint64(c.AddressSpaceMaxBytes)+fileUpper {
		return result, fmt.Errorf("original helper A reserve lacks AS plus full ELF file upper")
	}
	cgroupFD, e := unix.Open(arrayPopulateCgroup, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return result, e
	}
	defer unix.Close(cgroupFD)
	var groupStat unix.Stat_t
	var groupFS unix.Statfs_t
	if unix.Fstat(cgroupFD, &groupStat) != nil || unix.Fstatfs(cgroupFD, &groupFS) != nil || groupFS.Type != unix.CGROUP2_SUPER_MAGIC || groupStat.Dev != uint64(c.ArraysDevice) || groupStat.Ino != uint64(c.ArraysInode) {
		return result, fmt.Errorf("original pinned arrays birth domain differs")
	}
	var st, lock unix.Stat_t
	if e = unix.Fstat(int(s.dir.Fd()), &st); e != nil {
		return result, e
	}
	if e = unix.Fstat(int(s.lock.Fd()), &lock); e != nil {
		return result, e
	}
	if privateRegular(int(s.dir.Fd()), unix.S_IFDIR) != nil || privateRegular(int(s.lock.Fd()), unix.S_IFREG) != nil {
		return result, fmt.Errorf("original private resource state missing")
	}
	fdinfo, e := os.ReadFile("/proc/self/fdinfo/" + strconv.Itoa(int(s.lock.Fd())))
	if e != nil || !strings.Contains(string(fdinfo), "FLOCK  ADVISORY  WRITE") {
		return result, fmt.Errorf("original resource OFD flock not observed")
	}
	parent, e := nativeBirthCurrentConsumerIdentity()
	if e != nil {
		return result, e
	}
	nonce, e := newNativeBirthNonce()
	if e != nil {
		return result, e
	}
	result.Nonce = nonce
	pending := arrayPopulatePending{ProtocolVersion: 1, Scope: arrayPopulateScope, Nonce: nonce, Parent: parent, Context: c, ParentGuestBootID: c.Original.Identity.GuestBootID, StateDevice: control.Uint64(st.Dev), StateInode: control.Uint64(st.Ino), LockDevice: control.Uint64(lock.Dev), LockInode: control.Uint64(lock.Ino)}
	rawPending, e := arrayPopulateAppend(s, nonce, "pending", pending)
	if e != nil {
		return result, e
	}
	wire, e := pending.wire()
	if e != nil {
		return result, errors.Join(ErrUnknown, e)
	}
	pair, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if e != nil {
		return result, errors.Join(ErrUnknown, e)
	}
	parentSocket := os.NewFile(uintptr(pair[0]), "array-parent-control")
	childSocket := os.NewFile(uintptr(pair[1]), "array-child-control")
	defer parentSocket.Close()
	defer childSocket.Close()
	parentFD, e := unix.PidfdOpen(os.Getpid(), 0)
	if e != nil {
		return result, errors.Join(ErrUnknown, e)
	}
	parentFile := os.NewFile(uintptr(parentFD), "array-original-parent-pidfd")
	defer parentFile.Close()
	pidfd := -1
	cmd := exec.Command("/proc/self/fd/3")
	cmd.Env = []string{}
	cmd.ExtraFiles = []*os.File{helper, s.lock, files[0], files[1], files[2], files[3], childSocket, parentFile}
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &pidfd, Pdeathsig: syscall.SIGKILL, UseCgroupFD: true, CgroupFD: cgroupFD}
	if e = cmd.Start(); e != nil {
		return result, errors.Join(ErrUnknown, e)
	}
	childSocket.Close()
	defer cmd.Process.Release()
	var stoppedTarget *nativeTraceOriginalStoppedTarget
	waited := false
	defer func() {
		if !waited {
			_ = parentSocket.Close() // 断开后不能再发下一数组token；已进入同步fault的前缀仍保原charge。
			if stoppedTarget != nil && stoppedTarget.observedExit {
				result.ExitObserved = true // 同原pidfd的tracer已经真实wait exit，仍保partial Unknown。
			} else if pidfd >= 0 {
				confirmed, cleanupErr := arrayPopulateAbortWait(cmd, pidfd)
				result.ExitObserved = confirmed
				err = errors.Join(err, cleanupErr)
			} else {
				err = errors.Join(err, ErrUnknown, fmt.Errorf("atomic child pidfd missing; original pending retained"))
			}
			waited = true
		}
		if pidfd >= 0 {
			unix.Close(pidfd)
		}
		if err != nil {
			err = errors.Join(ErrUnknown, err)
		}
	}()
	if pidfd < 0 {
		return result, fmt.Errorf("atomic original child pidfd unavailable")
	}
	procfd, e := unix.Open("/proc/"+strconv.Itoa(cmd.Process.Pid), unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return result, e
	}
	defer unix.Close(procfd)
	rawStat, e := nativeTraceProcText(procfd, "stat")
	if e != nil {
		return result, e
	}
	ticks, e := postmasterTicks(string(rawStat), cmd.Process.Pid)
	if e != nil {
		return result, e
	}
	var pfst unix.Stat_t
	if e = unix.Fstat(pidfd, &pfst); e != nil {
		return result, e
	}
	result.Child = arrayPopulateChild{ProtocolVersion: 1, PendingSHA256: physicalDigest(rawPending), PID: cmd.Process.Pid, StartTicks: control.Uint64(ticks), PIDFDDevice: control.Uint64(pfst.Dev), PIDFDInode: control.Uint64(pfst.Ino)}
	rawChild, e := arrayPopulateAppend(s, nonce, "child", result.Child)
	if e != nil {
		return result, e
	}
	if e = arrayPopulateSend(pair[0], wire); e != nil {
		return result, e
	}
	rawReady, e := arrayPopulateRecv(ctx, pair[0])
	if e != nil {
		return result, e
	}
	ready, e := decodeArrayPopulateStatus(rawReady, pending, nil)
	if e != nil {
		return result, e
	}
	if ready.Phase != 1 || ready.Errno != 0 || ready.PID != uint64(cmd.Process.Pid) {
		return result, fmt.Errorf("helper failed original preparation")
	}
	result.LockedBytes = control.Uint64(ready.LockedBytes)
	program := arrayPopulateFilter(ready.Ranges)
	if len(program) != int(ready.FilterCount) {
		return result, fmt.Errorf("helper deterministic filter source differs")
	}
	pidfdCopy, e := unix.FcntlInt(uintptr(pidfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return result, e
	}
	pidfdSource := os.NewFile(uintptr(pidfdCopy), "original-helper-spawn-pidfd")
	defer pidfdSource.Close()
	procCopy, e := unix.FcntlInt(uintptr(procfd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return result, e
	}
	procSource := os.NewFile(uintptr(procCopy), "original-helper-pinned-proc")
	defer procSource.Close()
	executableSHA, e := nativeBirthDigest(c.HelperSHA256)
	if e != nil {
		return result, e
	}
	target, e := newNativeTraceOriginalStoppedTarget(nativeTraceOriginalTargetSource{PID: result.Child.PID, StartTicks: uint64(result.Child.StartTicks), UID: 65534, GID: 65534, GuestBootID: c.Original.Identity.GuestBootID, PIDFD: pidfdSource, ProcDirectory: procSource, Executable: helper, ExecutableSHA256: executableSHA})
	if e != nil {
		return result, e
	}
	stoppedTarget = target
	defer target.close()
	var rawPrepared []byte
	trace, e := nativeTraceWithOriginalStoppedTarget(ctx, target, func(ctx context.Context, original *nativeTraceOriginalStoppedTarget) error {
		if e := arrayPopulateGroups(c, result.Child.PID); e != nil {
			return e
		}
		facts, e := arrayPopulateProcess(c, result.Child, arrayPopulateCgroup, ready, image, imageBytes, original.procfd)
		if e != nil {
			return e
		}
		// KCMP_FILE只读证明原FD4确是同一OFD；inode相等不是同OFD证明。
		same, _, eno := unix.Syscall6(unix.SYS_KCMP, uintptr(os.Getpid()), uintptr(original.pid), 0, uintptr(s.lock.Fd()), 4, 0)
		runtime.KeepAlive(s.lock)
		if eno != 0 || same != 0 {
			return fmt.Errorf("helper original OFD not independently matched: %v", eno)
		}
		if e = arrayPopulateAudit(original.pid, unix.Gettid(), program); e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		prepared := arrayPopulatePrepared{ProtocolVersion: 1, TracerTID: unix.Gettid(), ChildSHA256: physicalDigest(rawChild), StatusSHA256: physicalDigest(rawReady), FilterSHA256: physicalDigest(arrayPopulateFilterBytes(program)), VmSizeBytes: control.Uint64(facts.VmSize), LockedBytes: control.Uint64(facts.Locked)}
		rawPrepared, e = arrayPopulateAppend(s, nonce, "prepared", prepared)
		return e
	})
	if e != nil {
		return result, e
	}
	if !trace.Detached || !trace.CallbackCompleted || len(rawPrepared) == 0 {
		return result, errors.Join(ErrUnknown, fmt.Errorf("original helper stopped preparation not durably detached"))
	}

	for i, spec := range c.Files {
		if spec.Length == 0 {
			continue
		}
		if e = ctx.Err(); e != nil {
			return result, e
		}
		if _, e = arrayPopulateProcess(c, result.Child, arrayPopulateCgroup, ready, image, imageBytes, procfd); e != nil {
			return result, e
		}
		if e = arrayPopulateSend(pair[0], []byte{byte(0xa0 + i)}); e != nil {
			return result, e
		}
		raw, e := arrayPopulateRecv(ctx, pair[0])
		if e != nil {
			return result, e
		}
		status, e := decodeArrayPopulateStatus(raw, pending, &ready)
		if e != nil {
			return result, e
		}
		if status.Phase != 2 || status.Errno != 0 || status.Index != uint32(i) {
			return result, fmt.Errorf("original helper populate failed or phase differs")
		}
		result.Progress[i] = true
		if observe != nil {
			if e = observe(result, i, pidfd); e != nil {
				return result, e
			}
		}
	}
	if e = ctx.Err(); e != nil {
		return result, e
	}
	if e = arrayPopulateSend(pair[0], []byte{0xef}); e != nil {
		return result, e
	}
	status, e := arrayPopulateWaitOriginal(pidfd, cmd.Process.Pid)
	waited = e == nil
	if e != nil || !status.Exited() || status.ExitStatus() != 0 {
		return result, fmt.Errorf("original helper successful P_PIDFD exit missing: %v", e)
	}
	result.ExitObserved = true
	if e = arrayPopulateGroups(c, 0); e != nil {
		return result, e
	}
	if _, e = arrayPopulateAppend(s, nonce, "complete", arrayPopulateComplete{ProtocolVersion: 1, PreparedSHA256: physicalDigest(rawPrepared), PID: cmd.Process.Pid, ExitObserved: true, Progress: result.Progress}); e != nil {
		return result, e
	}
	result.Completed = true
	return result, nil
}
