//go:build linux

package resourceguard

import (
	"fmt"
	"io"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const pgCgroup = "/sys/fs/cgroup/neon-postgres"
const hugeRoot = "/sys/kernel/mm/hugepages"

type linuxKernel struct{ config Config }

// Open仅在真实Linux Guest/root下启用。固定生产路径不接受请求覆盖；目录及写文件
// 必须root控制。官方旧cgconfig把memory控制权交给postgres，该配置会明确拒绝，
// 不能把可被被约束方自行放宽的文件当生产预算。调用方须先完成受管启动权限接线。
func Open(config Config) (*Guard, error) {
	if os.Getuid() != 0 || os.Geteuid() != 0 || control.ValidateID(config.VMUID, "VM UID") != nil || control.ValidateID(config.ComputeInstanceID, "Compute instance") != nil {
		return nil, fmt.Errorf("resource guard requires root and trusted VM binding")
	}
	if err := secureParents(config.StateDir); err != nil {
		return nil, err
	}
	k := &linuxKernel{config: config}
	if _, err := k.Identity(); err != nil {
		return nil, err
	}
	return &Guard{config: config, kernel: k}, nil
}

// 不跟随任何目录符号链接。root可信目录内的文件逐次O_NOFOLLOW打开，写前再
// 核验UID/权限；本模块不接受用户文件路径、shell命令或任意/proc PID控制文件。
func secureParents(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("unclean trusted kernel/state path")
	}
	for p := path; ; p = filepath.Dir(p) {
		var st unix.Stat_t
		if err := unix.Lstat(p, &st); err != nil {
			return err
		}
		if st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != 0 || st.Mode&0022 != 0 {
			return fmt.Errorf("trusted directory is not root controlled: %s", p)
		}
		if p == "/" {
			break
		}
	}
	return nil
}

func rootFile(path string, write bool) (*os.File, error) {
	if err := secureParents(filepath.Dir(path)); err != nil {
		return nil, err
	}
	flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if write {
		flags = unix.O_WRONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	}
	fd, err := unix.Open(path, flags, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		f.Close()
		return nil, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != 0 || st.Mode&0022 != 0 {
		f.Close()
		return nil, fmt.Errorf("kernel control is not root controlled: %s", path)
	}
	return f, nil
}

func kernelText(path string) (string, error) {
	f, err := rootFile(path, false)
	if err != nil {
		return "", err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 256*1024+1))
	if err != nil {
		return "", err
	}
	if len(data) > 256*1024 {
		return "", fmt.Errorf("kernel observation too large")
	}
	return strings.TrimSpace(string(data)), nil
}

func readNumber(path string, unlimited bool) (uint64, error) {
	text, err := kernelText(path)
	if err != nil {
		return 0, err
	}
	if unlimited && text == "max" {
		return math.MaxUint64, nil
	}
	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil || text != strconv.FormatUint(n, 10) {
		return 0, fmt.Errorf("invalid kernel counter: %s", path)
	}
	return n, nil
}

func writeNumber(path string, n uint64) error {
	f, err := rootFile(path, true)
	if err != nil {
		return err
	}
	// 不使用O_TRUNC、不把ctx取消包装为未执行。Linux6.12不假定支持较新
	// memory.max的O_NONBLOCK语义；同步调用可能等待回收，整个期间一直持文件锁。
	data := []byte(strconv.FormatUint(n, 10) + "\n")
	written, err := f.Write(data)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	return closeErr
}

func (k *linuxKernel) Identity() (Identity, error) {
	var id Identity
	if err := secureParents(pgCgroup); err != nil {
		return id, err
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(pgCgroup, &fs); err != nil {
		return id, err
	}
	if fs.Type != unix.CGROUP2_SUPER_MAGIC {
		return id, fmt.Errorf("real unified cgroup v2 required")
	}
	// 只支持memory与HugeTLB池不重叠的核算模式。启用新内核的hugetlb计入memory
	// 时必须另写去重适配器，不能直接memory.max+整个池造成不清楚的重复记账。
	mounts, err := kernelText("/proc/" + strconv.Itoa(os.Getpid()) + "/mountinfo")
	if err != nil {
		return id, err
	}
	for _, line := range strings.Split(mounts, "\n") {
		parts := strings.SplitN(line, " - ", 2)
		if len(parts) != 2 {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) >= 3 && fields[0] == "cgroup2" {
			for _, option := range strings.Split(fields[2], ",") {
				if option == "memory_hugetlb_accounting" {
					return id, fmt.Errorf("HugeTLB memory-controller accounting needs a separate nonoverlapping adapter")
				}
			}
		}
	}
	var st unix.Stat_t
	if err := unix.Stat(pgCgroup, &st); err != nil {
		return id, err
	}
	boot, err := kernelText("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return id, err
	}
	if !validGuestBoot(boot) {
		return id, fmt.Errorf("invalid Guest boot ID")
	}
	for _, p := range []string{"memory.max", "memory.swap.max", "cgroup.procs"} {
		f, err := rootFile(filepath.Join(pgCgroup, p), false)
		if err != nil {
			return id, err
		}
		f.Close()
	}
	// 父组迁出权限也不能交给postgres；只锁当前组而允许写根cgroup.procs仍可逃逸。
	f, err := rootFile("/sys/fs/cgroup/cgroup.procs", false)
	if err != nil {
		return id, err
	}
	f.Close()
	id = Identity{VMUID: k.config.VMUID, ComputeInstanceID: k.config.ComputeInstanceID, GuestBootID: boot, CgroupDevice: control.Uint64(st.Dev), CgroupInode: control.Uint64(st.Ino)}
	if k.config.SystemCgroup {
		dev, ino, err := systemCgroupIdentity(uint64(st.Dev))
		if err != nil {
			return Identity{}, err
		}
		id.SystemCgroupDevice = control.Uint64(dev)
		id.SystemCgroupInode = control.Uint64(ino)
	}
	return id, nil
}

func (k *linuxKernel) postmaster(c Command) error {
	base := "/proc/" + strconv.Itoa(c.PostmasterPID)
	// /proc/<pid>文件归进程UID所有，不能调用只接受root控制文件的kernelText。
	// 固定/proc路径、进程身份前后读取及starttime共同避免PID复用；请求无路径输入。
	read := func(name string) (string, error) {
		b, err := os.ReadFile(base + "/" + name)
		if len(b) > 64*1024 {
			return "", fmt.Errorf("process observation exceeds bound")
		}
		return strings.TrimSpace(string(b)), err
	}
	first, err := read("stat")
	if err != nil {
		return err
	}
	ticks, err := postmasterTicks(first, c.PostmasterPID)
	if err != nil || ticks != uint64(c.PostmasterStartTicks) {
		return fmt.Errorf("Postmaster process incarnation differs")
	}
	group, err := read("cgroup")
	if err != nil {
		return err
	}
	if group != "0::/neon-postgres" {
		return fmt.Errorf("Postmaster is not in the guarded cgroup")
	}
	account, err := user.Lookup("postgres")
	if err != nil {
		return fmt.Errorf("trusted postgres account unavailable")
	}
	uid, err := strconv.ParseUint(account.Uid, 10, 32)
	if err != nil || uid == 0 {
		return fmt.Errorf("invalid postgres UID")
	}
	status, err := read("status")
	if err != nil {
		return err
	}
	if err := processUID(status, uid); err != nil {
		return err
	}
	// Birth在exec前写，PID/starttime和cgroup尚不足以证明已经执行PG。比较
	// 实际exe与root提供的固定PG文件，拒绝仍是SUID helper、被替换的程序或
	// 同组任意进程；前后starttime核验仍不可省略，避免PID复用竞争。
	program, err := filepath.EvalSymlinks(FixedPostgresPath)
	if err != nil {
		return err
	}
	trusted, err := rootFile(program, false)
	if err != nil {
		return err
	}
	installed, err := trusted.Stat()
	_ = trusted.Close()
	if err != nil {
		return err
	}
	actual, err := os.Stat(base + "/exe")
	if err != nil || !os.SameFile(installed, actual) {
		return fmt.Errorf("original Postmaster has not execed the fixed PG program")
	}
	if c.AddressSpaceMaxBytes != 0 {
		if _, err := inspectProcessAddressSpace(c.PostmasterPID, c.AddressSpaceMaxBytes, uid); err != nil {
			return err
		}
	}
	second, err := read("stat")
	if err != nil {
		return err
	}
	ticks, err = postmasterTicks(second, c.PostmasterPID)
	if err != nil || ticks != uint64(c.PostmasterStartTicks) {
		return fmt.Errorf("Postmaster changed during membership verification")
	}
	return nil
}

func (k *linuxKernel) VerifyProcess(p ProcessIdentity) error {
	if err := p.Validate(); err != nil {
		return err
	}
	id, err := k.Identity()
	if err != nil {
		return err
	}
	if id != p.Identity {
		return fmt.Errorf("Postmaster birth belongs to another Guest")
	}
	// 此内部值仅传身份给只读postmaster核验，不是可派发资源命令。
	return k.postmaster(Command{ProtocolVersion: 0, Identity: p.Identity, IssuerEpoch: 0, Revision: 0, BudgetCommitmentID: "", PostmasterPID: p.PostmasterPID, PostmasterStartTicks: p.PostmasterStartTicks, MemoryMaxBytes: 0, PidsMaxTasks: 0, AddressSpaceMaxBytes: p.AddressSpaceMaxBytes, SystemMemoryMaxBytes: 0, OutsideUpperBytes: 0, HugePools: nil})
}

func poolDirectory(size control.Uint64) string {
	return filepath.Join(hugeRoot, "hugepages-"+strconv.FormatUint(uint64(size), 10)+"kB")
}

func allPools() ([]HugePool, error) {
	if err := secureParents(hugeRoot); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(hugeRoot)
	if err != nil {
		return nil, err
	}
	pools := []HugePool{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "hugepages-") || !strings.HasSuffix(name, "kB") {
			return nil, fmt.Errorf("unknown HugeTLB hstate")
		}
		text := strings.TrimSuffix(strings.TrimPrefix(name, "hugepages-"), "kB")
		n, err := strconv.ParseUint(text, 10, 64)
		if err != nil || text != strconv.FormatUint(n, 10) || n == 0 || len(pools) == 32 {
			return nil, fmt.Errorf("invalid HugeTLB hstate")
		}
		if err := secureParents(poolDirectory(control.Uint64(n))); err != nil {
			return nil, err
		}
		pools = append(pools, HugePool{PageSizeKB: control.Uint64(n)})
	}
	if len(pools) == 0 {
		return nil, fmt.Errorf("HugeTLB hstates unavailable")
	}
	sortedPools(pools)
	return pools, nil
}

func (k *linuxKernel) Read(c Command) (Evidence, error) {
	if err := k.postmaster(c); err != nil {
		return Evidence{}, err
	}
	e, err := k.ReadBounds(c.Identity, c.limits())
	if err != nil {
		return e, err
	}
	if err := k.postmaster(c); err != nil {
		return e, err
	}
	return e, nil
}

func (k *linuxKernel) ReadBounds(expected Identity, c Limits) (Evidence, error) {
	e := Evidence{ProtocolVersion: 1}
	if uint64(c.MemoryMaxBytes)%uint64(os.Getpagesize()) != 0 || uint64(c.SystemMemoryMaxBytes)%uint64(os.Getpagesize()) != 0 {
		return e, fmt.Errorf("memory.max target must be an exact Guest base-page multiple")
	}
	id, err := k.Identity()
	if err != nil {
		return e, err
	}
	if id != expected {
		return e, fmt.Errorf("Guest/cgroup incarnation differs")
	}
	if err := systemIdentityMatches(id, c.SystemMemoryMaxBytes); err != nil {
		return e, err
	}
	e.Identity = id
	if c.SystemMemoryMaxBytes != 0 {
		e.System, err = k.readSystemBound()
		if err != nil {
			return e, err
		}
	}
	events, err := kernelText(filepath.Join(pgCgroup, "cgroup.events"))
	if err != nil {
		return e, err
	}
	populatedSeen := false
	for _, line := range strings.Split(events, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "populated" {
			if populatedSeen || (fields[1] != "0" && fields[1] != "1") {
				return e, fmt.Errorf("invalid cgroup populated state")
			}
			populatedSeen = true
			e.CgroupPopulated = fields[1] == "1"
		}
	}
	if !populatedSeen {
		return e, fmt.Errorf("cgroup populated state unavailable")
	}
	for _, entry := range []struct {
		name      string
		dest      *control.Uint64
		unlimited bool
	}{{"memory.max", &e.MemoryMaxBytes, true}, {"memory.current", &e.MemoryCurrentBytes, false}, {"memory.swap.max", &e.SwapMaxBytes, true}, {"memory.swap.current", &e.SwapCurrentBytes, false}} {
		n, err := readNumber(filepath.Join(pgCgroup, entry.name), entry.unlimited)
		if err != nil {
			return e, err
		}
		*entry.dest = control.Uint64(n)
	}
	if c.PidsMaxTasks != 0 {
		// pid_max是当前目标内核的可用PID空间，不假定编译配置等于开发机。
		// 允许启动前pids.max为max，只记录实际值；matchesTasks在写后严格确认。
		// 任务控制器支持threaded子树，本文实现只接受完整domain。迁移入口与父组
		// threads同样必须root独占，避免postgres绕过本组有限上限或另造子组。
		kind, err := kernelText(filepath.Join(pgCgroup, "cgroup.type"))
		if err != nil || kind != "domain" {
			return e, fmt.Errorf("finite task limit requires a domain cgroup")
		}
		for _, path := range []string{filepath.Join(pgCgroup, "cgroup.threads"), "/sys/fs/cgroup/cgroup.threads"} {
			f, err := rootFile(path, false)
			if err != nil {
				return e, err
			}
			f.Close()
		}
		pidMax, err := readNumber("/proc/sys/kernel/pid_max", false)
		if err != nil || uint64(c.PidsMaxTasks) > pidMax {
			return e, fmt.Errorf("requested task bound exceeds actual Guest PID range")
		}
		maximum, err := readNumber(filepath.Join(pgCgroup, "pids.max"), true)
		if err != nil {
			return e, err
		}
		current, err := readNumber(filepath.Join(pgCgroup, "pids.current"), false)
		if err != nil {
			return e, err
		}
		e.TaskLimit = &TaskLimitEvidence{MaxTasks: control.Uint64(maximum), CurrentTasks: control.Uint64(current)}
	}
	mem, err := kernelText("/proc/meminfo")
	if err != nil {
		return e, err
	}
	for _, line := range strings.Split(mem, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "MemTotal:" && fields[2] == "kB" {
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil || n > math.MaxUint64/1024 {
				return e, fmt.Errorf("invalid Guest MemTotal")
			}
			e.GuestMemTotalBytes = control.Uint64(n * 1024)
		}
	}
	if e.GuestMemTotalBytes == 0 {
		return e, fmt.Errorf("Guest MemTotal unavailable")
	}
	pools, err := allPools()
	if err != nil {
		return e, err
	}
	if len(pools) != len(c.HugePools) {
		return e, fmt.Errorf("command omits or adds HugeTLB hstates")
	}
	e.HugeOvercommitDisabled = true
	for i, p := range pools {
		if p.PageSizeKB != c.HugePools[i].PageSizeKB {
			return e, fmt.Errorf("HugeTLB hstate binding differs")
		}
		dir := poolDirectory(p.PageSizeKB)
		nr, err := readNumber(filepath.Join(dir, "nr_hugepages"), false)
		if err != nil {
			return e, err
		}
		surplus, err := readNumber(filepath.Join(dir, "surplus_hugepages"), false)
		if err != nil {
			return e, err
		}
		overcommit, err := readNumber(filepath.Join(dir, "nr_overcommit_hugepages"), false)
		if err != nil {
			return e, err
		}
		if surplus > math.MaxUint64-nr || uint64(p.PageSizeKB) > math.MaxUint64/1024 || nr+surplus > math.MaxUint64/(uint64(p.PageSizeKB)*1024) || surplus > math.MaxUint64-uint64(e.HugeSurplusPages) {
			return e, fmt.Errorf("HugeTLB observation overflows")
		}
		bytes := (nr + surplus) * uint64(p.PageSizeKB) * 1024
		if bytes > math.MaxUint64-uint64(e.WholePoolUpperBytes) {
			return e, fmt.Errorf("whole pool overflows")
		}
		e.WholePoolUpperBytes += control.Uint64(bytes)
		e.HugeSurplusPages += control.Uint64(surplus)
		e.HugeOvercommitDisabled = e.HugeOvercommitDisabled && overcommit == 0
		p.Pages = control.Uint64(nr)
		e.HugePools = append(e.HugePools, p)
	}
	if c.AddressSpaceMaxBytes != 0 {
		e.AddressSpace, err = readAddressSpaces(c)
		if err != nil {
			return e, err
		}
	}
	return e, nil
}

func (k *linuxKernel) Apply(c Command) error {
	if err := k.postmaster(c); err != nil {
		return err
	}
	return k.ApplyBounds(c.limits())
}

func (k *linuxKernel) ApplyBounds(c Limits) error {
	// 原WAL已落盘；系统组尚空时先落实不可变额度，再由独立归组步骤迁入PID1。
	// 运行期仅重写原值，Guard写前已核原值/占用/归组；不因缩容调低系统额度。
	if c.SystemMemoryMaxBytes != 0 {
		if err := writeNumber(filepath.Join(systemCgroup, "memory.swap.max"), 0); err != nil {
			return err
		}
		if err := writeNumber(filepath.Join(systemCgroup, "memory.max"), uint64(c.SystemMemoryMaxBytes)); err != nil {
			return err
		}
	}
	// 原意图已fsync且持资源锁，有限pids在PG出生前落实。此处不迁入/杀进程，
	// 也不把写成功当数量证明；后续任一写/读失败保留原Unknown，不放宽N回max。
	// runtime原N已在Guard写前冻结，重写相同值不会偷改后台任务余量。
	if c.PidsMaxTasks != 0 {
		if err := writeNumber(filepath.Join(pgCgroup, "pids.max"), uint64(c.PidsMaxTasks)); err != nil {
			return err
		}
	}
	// 所有hstate先禁用surplus扩张，再调整持久池。内核可能只能部分完成、将
	// 在用页转为surplus，或HVO延迟释放；写返回成功也不能作为池已收缩的证明。
	// 不主动写低于已用+预留的池，避免撤销仍被PG承诺的页；最终Read还要求surplus0。
	for _, p := range c.HugePools {
		dir := poolDirectory(p.PageSizeKB)
		nr, err := readNumber(filepath.Join(dir, "nr_hugepages"), false)
		if err != nil {
			return err
		}
		surplus, err := readNumber(filepath.Join(dir, "surplus_hugepages"), false)
		if err != nil {
			return err
		}
		free, err := readNumber(filepath.Join(dir, "free_hugepages"), false)
		if err != nil {
			return err
		}
		reserved, err := readNumber(filepath.Join(dir, "resv_hugepages"), false)
		if err != nil {
			return err
		}
		if surplus > math.MaxUint64-nr || free > nr+surplus || reserved > free || reserved > math.MaxUint64-(nr+surplus-free) || uint64(p.Pages) < nr+surplus-free+reserved {
			return fmt.Errorf("HugeTLB used/reserved pages do not fit target")
		}
	}
	for _, p := range c.HugePools {
		if err := writeNumber(filepath.Join(poolDirectory(p.PageSizeKB), "nr_overcommit_hugepages"), 0); err != nil {
			return err
		}
	}
	for _, p := range c.HugePools {
		if err := writeNumber(filepath.Join(poolDirectory(p.PageSizeKB), "nr_hugepages"), uint64(p.Pages)); err != nil {
			return err
		}
	}
	if err := writeNumber(filepath.Join(pgCgroup, "memory.swap.max"), 0); err != nil {
		return err
	}
	return writeNumber(filepath.Join(pgCgroup, "memory.max"), uint64(c.MemoryMaxBytes))
}
