//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

const normalKernelPrepV2ControlPath = "/proc/sys/user/max_user_namespaces"

func normalKernelPrepV2Marker(line string) error {
	n := 0
	for _, v := range strings.Fields(line) {
		k, _, _ := strings.Cut(v, "=")
		if k == "neonvm.dsga.kernel_bootstrap" {
			n++
			if v != NormalV2KernelBootstrapArg {
				return fmt.Errorf("v2 original marker differs")
			}
		}
	}
	if n != 1 {
		return fmt.Errorf("v2 marker missing/duplicate")
	}
	return nil
}
func (g *Guard) normalKernelPrepV2Inputs() (kernelPrepRawInputs, error) {
	var in kernelPrepRawInputs
	if e := normalKernelPrepV2Caller(); e != nil {
		return in, e
	}
	s, raw, e := runtimeSource()
	if e != nil {
		return in, e
	}
	if s.Config != g.config {
		return in, fmt.Errorf("v2 original runtime config differs")
	}
	in.RuntimeRaw = raw
	line, e := kernelText("/proc/cmdline")
	if e != nil {
		return in, e
	}
	if e = normalKernelPrepV2Marker(line); e != nil {
		return in, e
	}
	in.PhysicalPolicyRaw, e = readKernelBootstrapISO(FixedPhysicalKernelPolicyPath)
	if e != nil {
		return in, e
	}
	in.PolicyRaw, e = readKernelBootstrapISO(FixedNormalV1KernelBootstrapPolicyPath)
	if e != nil {
		return in, e
	}
	_, e = decodeNormalKernelPrepV2(in, g.config)
	return in, e
}
func normalKernelPrepV2Caller() error {
	if os.Getuid() != 0 || os.Geteuid() != 0 || os.Getgid() != 0 || os.Getegid() != 0 || os.Getppid() != 1 {
		return fmt.Errorf("v2 requires original initial Root PID1 child")
	}
	fd, e := unix.Open("/proc/thread-self", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	text, e := nativeTraceProcText(fd, "status")
	if e != nil {
		return e
	}
	if e = normalSystemRoleServicePrivilegeStatus(text); e != nil {
		return e
	}
	for _, key := range []string{"Uid", "Gid"} {
		v, e := nativeTraceStatusValues(text, key, 4)
		if e != nil {
			return e
		}
		for _, x := range v {
			if x != 0 {
				return fmt.Errorf("v2 original Root credentials differ")
			}
		}
	}
	var h = unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	var d [2]unix.CapUserData
	if e = unix.Capget(&h, &d[0]); e != nil {
		return e
	}
	if d[0].Effective&(1<<unix.CAP_SYS_RESOURCE) == 0 {
		return fmt.Errorf("v2 initial Root lacks fixed sysctl capability")
	}
	return nil
}

type normalKernelPrepV2Backend interface {
	kernelPrepV2Read() (normalKernelPrepV2Snapshot, error)
	kernelPrepV2ZeroUserNamespaces(normalKernelPrepV2Object) error
	kernelPrepWrite(string, uint64) error
	kernelPrepPGEmpty() error
	kernelPrepCensus(uint32) error
	close()
}
type normalKernelPrepV2LinuxBackend struct {
	*linuxKernel
	mu         sync.Mutex
	control    *os.File
	namespaces *normalSystemRoleNamespaceSource
	task       *normalSystemOriginalTask
	root       nativeBirthConsumer
	elf        *os.File
	elfSHA     string
	object     normalKernelPrepV2Object
	closed     bool
}

func normalKernelPrepV2ControlObject(fd int) (normalKernelPrepV2Object, error) {
	var st unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(fd, &st) != nil || unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != 0 || st.Gid != 0 || st.Mode&0777 != 0644 {
		return normalKernelPrepV2Object{}, fmt.Errorf("v2 control is not fixed Root procfs leaf")
	}
	o, e := normalFilesystemFDObject(fd)
	if e != nil {
		return normalKernelPrepV2Object{}, e
	}
	return normalKernelPrepV2Object{Device: st.Dev, Inode: st.Ino, MountID: o.MountID, Mode: st.Mode, UID: st.Uid, GID: st.Gid}, nil
}
func normalKernelPrepV2ControlValue(f *os.File) (control.Uint64, error) {
	if _, e := f.Seek(0, io.SeekStart); e != nil {
		return 0, e
	}
	b, e := io.ReadAll(io.LimitReader(f, 64))
	if e != nil || len(b) >= 64 {
		return 0, fmt.Errorf("v2 control value exceeds bound")
	}
	v, e := strconv.ParseUint(strings.TrimSuffix(string(b), "\n"), 10, 31)
	if e != nil || string(b) != strconv.FormatUint(v, 10)+"\n" {
		return 0, fmt.Errorf("v2 control value noncanonical")
	}
	return control.Uint64(v), nil
}
func newNormalKernelPrepV2LinuxBackend(ctx context.Context, k *linuxKernel) (b *normalKernelPrepV2LinuxBackend, err error) {
	if ctx == nil || ctx.Err() != nil || k == nil {
		return nil, ErrUnknown
	}
	if e := normalKernelPrepV2Caller(); e != nil {
		return nil, e
	}
	b = &normalKernelPrepV2LinuxBackend{linuxKernel: k}
	good := false
	defer func() {
		if !good {
			b.close()
		}
	}()
	b.root, err = nativeBirthCurrentConsumerIdentity()
	if err != nil {
		return nil, err
	}
	b.namespaces, err = captureNormalSystemRoleNamespaces()
	if err != nil {
		return nil, err
	}
	ids, err := b.namespaces.snapshot()
	if err != nil {
		return nil, err
	}
	for _, n := range ids {
		var init unix.Stat_t
		if unix.Stat("/proc/1/ns/"+n.Name, &init) != nil || uint64(init.Dev) != n.Device || init.Ino != n.Inode {
			return nil, fmt.Errorf("v2 producer left original PID1 namespaces")
		}
		if n.Name == "user" && n.Inode != 0xEFFFFFFD {
			return nil, fmt.Errorf("v2 producer user namespace is not initial")
		}
	}
	boot, err := kernelText("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return nil, err
	}
	pf, e := unix.PidfdOpen(os.Getpid(), 0)
	if e != nil {
		return nil, e
	}
	pid := os.NewFile(uintptr(pf), "original v2 producer pidfd")
	defer pid.Close()
	proc, e := os.Open("/proc/" + strconv.Itoa(os.Getpid()))
	if e != nil {
		return nil, e
	}
	defer proc.Close()
	b.task, err = newNormalSystemOriginalTask(normalSystemOriginalTaskSource{PID: b.root.PID, StartTicks: uint64(b.root.StartTicks), GuestBootID: boot, UID: 0, GID: 0, PIDFD: pid, ProcDirectory: proc})
	if err != nil {
		return nil, err
	}
	b.elf, err = os.Open("/proc/self/exe")
	if err != nil {
		return nil, err
	}
	b.elfSHA, err = nativeBirthFileHash(b.elf)
	if err != nil {
		return nil, err
	}
	fd, e := unix.Open(normalKernelPrepV2ControlPath, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	b.control = os.NewFile(uintptr(fd), "original v2 read-only userns control")
	b.object, err = normalKernelPrepV2ControlObject(fd)
	if err != nil {
		return nil, err
	}
	good = true
	return b, nil
}
func (b *normalKernelPrepV2LinuxBackend) close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	if b.control != nil {
		b.control.Close()
	}
	if b.namespaces != nil {
		b.namespaces.close()
	}
	if b.task != nil {
		b.task.close()
	}
	if b.elf != nil {
		b.elf.Close()
	}
}
func (b *normalKernelPrepV2LinuxBackend) verify() error {
	if b.closed {
		return ErrUnknown
	}
	if e := normalKernelPrepV2Caller(); e != nil {
		return e
	}
	root, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || root != b.root {
		return fmt.Errorf("v2 original producer changed")
	}
	if e = b.task.verify(); e != nil {
		return e
	}
	if e = b.namespaces.verify(-1); e != nil {
		return e
	}
	object, e := normalKernelPrepV2ControlObject(int(b.control.Fd()))
	if e != nil || object != b.object {
		return fmt.Errorf("v2 original userns control object changed")
	}
	sha, e := nativeBirthFileHash(b.elf)
	if e != nil || sha != b.elfSHA {
		return fmt.Errorf("v2 original producer ELF changed")
	}
	return nil
}
func (b *normalKernelPrepV2LinuxBackend) kernelPrepV2Read() (normalKernelPrepV2Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var s normalKernelPrepV2Snapshot
	if e := b.verify(); e != nil {
		return s, e
	}
	k, e := b.linuxKernel.kernelPrepV2BaseRead()
	if e != nil {
		return s, e
	}
	ns, e := b.namespaces.snapshot()
	if e != nil {
		return s, e
	}
	value, e := normalKernelPrepV2ControlValue(b.control)
	if e != nil {
		return s, e
	}
	s = normalKernelPrepV2Snapshot{Kernel: k, OriginalRoot: b.root, ProducerELFSHA256: b.elfSHA, Namespaces: ns, Control: b.object, MaxUserNamespaces: value}
	return s, b.verify()
}

// 仅原事务在intent持久后调用；固定路径/固定0，绝不暴露任意值setter。临时RW FD即刻关闭。
func (b *normalKernelPrepV2LinuxBackend) kernelPrepV2ZeroUserNamespaces(want normalKernelPrepV2Object) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := b.verify(); e != nil {
		return e
	}
	if want != b.object {
		return ErrUnknown
	}
	fd, e := unix.Open(normalKernelPrepV2ControlPath, unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	got, e := normalKernelPrepV2ControlObject(fd)
	if e != nil || got != want {
		return fmt.Errorf("v2 fixed zero writer object differs")
	}
	n, e := unix.Write(fd, []byte("0\n"))
	if e != nil || n != 2 {
		return errors.Join(ErrUnknown, e, fmt.Errorf("v2 fixed zero write unconfirmed"))
	}
	value, e := normalKernelPrepV2ControlValue(b.control)
	if e != nil || value != 0 {
		return fmt.Errorf("v2 fixed zero readback differs")
	}
	return b.verify()
}

// 仅从这三个固定ISO入口进生产；没有raw receipt恢复或当前0认领入口。
func (g *Guard) PrepareKernelBeforeOriginV2(ctx context.Context) (NormalV2KernelBootstrapEvidence, error) {
	return g.normalKernelPreparationV2(ctx, kernelPrepPrepare)
}
func (g *Guard) ObserveKernelBeforeOriginV2(ctx context.Context) (NormalV2KernelBootstrapEvidence, error) {
	return g.normalKernelPreparationV2(ctx, kernelPrepObserve)
}
func (g *Guard) ReplayKernelBeforeOriginV2(ctx context.Context) (NormalV2KernelBootstrapEvidence, error) {
	return g.normalKernelPreparationV2(ctx, kernelPrepReplay)
}
func (g *Guard) normalKernelPreparationV2(ctx context.Context, op kernelPrepOperation) (NormalV2KernelBootstrapEvidence, error) {
	var zero NormalV2KernelBootstrapEvidence
	if g == nil || ctx == nil || ctx.Err() != nil {
		return zero, ErrUnknown
	}
	in, e := g.normalKernelPrepV2Inputs()
	if e != nil {
		return zero, e
	}
	k, ok := g.kernel.(*linuxKernel)
	if !ok {
		return zero, fmt.Errorf("v2 exact Linux producer required")
	}
	probe, e := newNormalKernelPrepV2LinuxBackend(ctx, k)
	if e != nil {
		return zero, e
	}
	snap, e := probe.kernelPrepV2Read()
	if e != nil {
		probe.close()
		return zero, e
	}
	key := normalKernelPrepV2HistoryKey(snap)
	var backend normalKernelPrepV2Backend = probe
	normalKernelPrepV2PrivateHistory.Lock()
	original := normalKernelPrepV2PrivateHistory.backends[key]
	normalKernelPrepV2PrivateHistory.Unlock()
	if original != nil {
		probe.close()
		backend = original
	}
	defer func() {
		normalKernelPrepV2PrivateHistory.Lock()
		held := normalKernelPrepV2PrivateHistory.backends[key] == backend
		normalKernelPrepV2PrivateHistory.Unlock()
		if !held {
			backend.close()
		}
	}()
	state, e := lockState(ctx, g.config.StateDir)
	if e != nil {
		return zero, e
	}
	defer state.close()
	return g.normalKernelPreparationV2Locked(ctx, in, op, state, backend)
}

type normalKernelPrepV2Source struct {
	mu        sync.Mutex
	closed    bool
	backend   normalKernelPrepV2Backend
	evidence  NormalV2KernelBootstrapEvidence
	policy    normalKernelPrepV2Decoded
	witnesses map[string]normalSystemRoleFileWitness
}

func (s *normalKernelPrepV2Source) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.backend.close()
	}
}
func (s *normalKernelPrepV2Source) referenceLocked(ctx context.Context, state *lockedState) (string, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || state == nil {
		return "", ErrUnknown
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.witnesses) != 2 {
		return "", ErrUnknown
	}
	current, e := normalKernelPrepV2ReadLocked(s.backend, state)
	if e != nil {
		return "", e
	}
	if e = s.evidence.After.fixedMatch(current); e != nil {
		return "", e
	}
	if !current.target(s.policy.policy) {
		return "", errors.Join(ErrUnknown, fmt.Errorf("v2 original zero/allocator source drifted"))
	}
	for n, w := range s.witnesses {
		got, e := readNormalSystemRoleFileWitness(state, n)
		if e != nil || got != w {
			return "", errors.Join(ErrUnknown, e, fmt.Errorf("v2 durable private witness changed"))
		}
	}
	return s.evidence.ReferenceSHA256()
}
func normalKernelPrepV2MakeSource(state *lockedState, b normalKernelPrepV2Backend, d normalKernelPrepV2Decoded, e NormalV2KernelBootstrapEvidence) (*normalKernelPrepV2Source, error) {
	source := &normalKernelPrepV2Source{backend: b, policy: d, evidence: e, witnesses: map[string]normalSystemRoleFileWitness{}}
	for _, n := range []string{normalKernelPrepV2IntentName, normalKernelPrepV2CompleteName} {
		w, x := readNormalSystemRoleFileWitness(state, n)
		if x != nil {
			return nil, x
		}
		source.witnesses[n] = w
	}
	return source, nil
}

var _ json.Unmarshaler = (*NormalV2KernelBootstrapPolicy)(nil)

func (k *linuxKernel) kernelPrepV2BaseRead() (kernelPrepSnapshot, error) {
	var s kernelPrepSnapshot
	if runtime.GOARCH != "amd64" || os.Getpagesize() != 4096 {
		return s, fmt.Errorf("kernel preparation requires exact amd64 base pages")
	}
	var e error
	s.Identity, e = k.Identity()
	if e != nil {
		return s, e
	}
	s.KernelRelease, e = kernelText("/proc/sys/kernel/osrelease")
	if e != nil {
		return s, e
	}
	notes, e := physicalKernelNotes()
	if e != nil {
		return s, e
	}
	s.KernelNotesSHA256 = physicalDigest(notes)
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil || !ref.StrictPageCounterIRQBoundary() || !ref.ControlMetadataGuard() || s.KernelRelease != ref.KernelRelease || s.KernelNotesSHA256 != ref.KernelNotesSHA256 {
		return s, fmt.Errorf("kernel preparation exact strict build differs")
	}
	s.CommandLine, e = kernelText("/proc/cmdline")
	if e != nil {
		return s, e
	}
	if e = normalKernelPrepV2Marker(s.CommandLine); e != nil {
		return s, e
	}
	s.HardPIDMax, e = readPhysicalKernelPIDHardLimit(s.CommandLine)
	if e != nil {
		return s, e
	}
	pid, e := readNumber("/proc/sys/kernel/pid_max", false)
	if e != nil || pid != uint64(s.HardPIDMax) {
		return s, fmt.Errorf("kernel preparation runtime PID bound differs from original hard source")
	}
	s.PIDMax = uint32(pid)
	s.Capabilities, e = readPhysicalKernelRuntimeCapabilities()
	if e != nil {
		return s, e
	}
	h := s.HardPIDMax
	s.Capabilities.HardPIDMaxLimit = &h
	boot, e := kernelText("/proc/sys/kernel/random/boot_id")
	if e != nil || boot != s.Identity.GuestBootID {
		return s, fmt.Errorf("kernel preparation boot differs")
	}
	for _, p := range []string{"/sys/kernel/mm/transparent_hugepage", "/sys/kernel/mm/ksm"} {
		if _, e := os.Lstat(p); !os.IsNotExist(e) {
			return s, fmt.Errorf("THP/KSM must remain compiled out")
		}
	}
	swaps, e := kernelText("/proc/swaps")
	if e != nil || strings.Join(strings.Fields(swaps), " ") != "Filename Type Size Used Priority" {
		return s, fmt.Errorf("kernel preparation requires no swap")
	}
	online, e := kernelText("/sys/devices/system/memory/auto_online_blocks")
	if e != nil || online != "online_movable" {
		return s, fmt.Errorf("kernel preparation hotplug policy differs")
	}
	possible, e := kernelText("/sys/devices/system/cpu/possible")
	if e != nil {
		return s, e
	}
	s.PossibleCPUs, e = physicalPossibleCPUs(possible)
	if e != nil {
		return s, e
	}
	zones, e := kernelText("/proc/zoneinfo")
	if e != nil {
		return s, e
	}
	s.Zones, e = parsePhysicalZoneInfo(zones)
	if e != nil {
		return s, e
	}
	modules, e := kernelText("/proc/modules")
	if e != nil {
		return s, e
	}
	s.LoadedModules = []string{}
	for _, line := range strings.Split(modules, "\n") {
		if fields := strings.Fields(line); len(fields) > 0 {
			s.LoadedModules = append(s.LoadedModules, fields[0])
		}
	}
	value, e := readNumber("/proc/sys/kernel/modules_disabled", false)
	if e != nil || value > 1 {
		return s, fmt.Errorf("invalid modules_disabled source")
	}
	s.ModulesDisabled = uint32(value)
	value, e = readNumber("/proc/sys/kernel/numa_balancing", false)
	if e != nil || value > 3 {
		return s, fmt.Errorf("invalid NUMA balancing source")
	}
	s.NUMABalancing = uint32(value)
	// 准确369配置NUMA+MIGRATION，必须实读现有控制；ENOENT不能猜成无demotion。
	demotion, e := kernelText("/sys/kernel/mm/numa/demotion_enabled")
	if e != nil {
		return s, e
	}
	if demotion != "true" && demotion != "false" {
		return s, fmt.Errorf("unrecognized demotion source")
	}
	s.DemotionEnabled = demotion == "true"
	s.Namespaces, e = kernelBootstrapNamespaces()
	if e != nil {
		return s, e
	}
	var fs unix.Statfs_t
	var st, root unix.Stat_t
	if e = unix.Statfs(k.config.StateDir, &fs); e != nil {
		return s, e
	}
	if e = unix.Stat(k.config.StateDir, &st); e != nil {
		return s, e
	}
	if e = unix.Stat("/", &root); e != nil {
		return s, e
	}
	if fs.Type != unix.EXT4_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY != 0 || st.Ino == 0 {
		return s, fmt.Errorf("kernel original intent requires writable persistent ext4 state")
	}
	// 旧独立ext4状态盘路径原样保留。同SB仅原显式normal镜像的固定持久bind孔，
	// 必须实际核完整Layout2/原ISO+vminit；不能泛化为任意RW目录或消费未出生FS complete。
	if st.Dev == root.Dev {
		if err := normalFilesystemPreflightSameSuperblockState(k.config); err != nil {
			return s, fmt.Errorf("same-superblock original state layout: %w", err)
		}
	}
	s.StateDevice, s.StateInode = control.Uint64(st.Dev), control.Uint64(st.Ino)
	return s, nil
}

// 同一原producer的内存见证；新进程/同字节替换文件不能靠重读WAL补来源。
var normalKernelPrepV2PrivateHistory = struct {
	sync.Mutex
	entries  map[string]map[string]normalSystemRoleFileWitness
	backends map[string]normalKernelPrepV2Backend
	files    map[string]map[string]*os.File
}{entries: map[string]map[string]normalSystemRoleFileWitness{}, backends: map[string]normalKernelPrepV2Backend{}, files: map[string]map[string]*os.File{}}

func normalKernelPrepV2HistoryKey(s normalKernelPrepV2Snapshot) string {
	b, _ := json.Marshal(struct {
		Root          nativeBirthConsumer
		Identity      Identity
		Device, Inode control.Uint64
	}{s.OriginalRoot, s.Kernel.Identity, s.Kernel.StateDevice, s.Kernel.StateInode})
	return physicalDigest(b)
}

type normalKernelPrepV2WALFault struct {
	beforeOpen func() error
	syncFile   func(*os.File) error
	syncDir    func(*os.File) error
}

func normalKernelPrepV2PersistOriginal(state *lockedState, before normalKernelPrepV2Snapshot, name string, raw []byte) error {
	return normalKernelPrepV2WriteOriginalWAL(state, before, name, raw, nil)
}
func normalKernelPrepV2PinnedWitness(f *os.File) (normalSystemRoleFileWitness, error) {
	var a, z unix.Stat_t
	if unix.Fstat(int(f.Fd()), &a) != nil || a.Mode&unix.S_IFMT != unix.S_IFREG || a.Uid != 0 || a.Gid != 0 || a.Mode&0777 != 0600 || a.Nlink != 1 || a.Size <= 0 || a.Size > 64<<10 {
		return normalSystemRoleFileWitness{}, fmt.Errorf("v2 created WAL FD differs")
	}
	raw := make([]byte, int(a.Size))
	n, e := f.ReadAt(raw, 0)
	if e != nil || n != len(raw) || unix.Fstat(int(f.Fd()), &z) != nil || a.Dev != z.Dev || a.Ino != z.Ino || a.Size != z.Size || a.Ctim != z.Ctim || a.Mtim != z.Mtim || a.Mode != z.Mode || a.Uid != z.Uid || a.Gid != z.Gid || a.Nlink != z.Nlink {
		return normalSystemRoleFileWitness{}, fmt.Errorf("v2 created WAL FD changed")
	}
	return normalSystemRoleFileWitness{Device: a.Dev, Inode: a.Ino, Size: a.Size, CTime: a.Ctim, SHA256: physicalDigest(raw)}, nil
}

// 故障注入仅在本私有写者边界；生产总传nil。open前失败绝不读已有路径补登记。
func normalKernelPrepV2WriteOriginalWAL(state *lockedState, before normalKernelPrepV2Snapshot, name string, raw []byte, fault *normalKernelPrepV2WALFault) error {
	if state == nil || state.dir == nil || state.lock == nil || (name != normalKernelPrepV2IntentName && name != normalKernelPrepV2CompleteName) || len(raw) == 0 || len(raw) > 64<<10 {
		return ErrUnknown
	}
	var dir, lock unix.Stat_t
	if unix.Fstat(int(state.dir.Fd()), &dir) != nil || dir.Dev != uint64(before.Kernel.StateDevice) || dir.Ino != uint64(before.Kernel.StateInode) || unix.Fstat(int(state.lock.Fd()), &lock) != nil || lock.Dev != before.Lock.Device || lock.Ino != before.Lock.Inode {
		return fmt.Errorf("v2 original WAL resource objects differ")
	}
	if fault != nil && fault.beforeOpen != nil {
		if e := fault.beforeOpen(); e != nil {
			return e
		}
	}
	fd, e := unix.Openat(int(state.dir.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if e != nil {
		return e
	}
	created := os.NewFile(uintptr(fd), "original O_EXCL v2 WAL creator")
	defer created.Close()
	n, e := created.Write(raw)
	if e != nil || n != len(raw) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("v2 original WAL write incomplete"))
	}
	witness, e := normalKernelPrepV2PinnedWitness(created)
	if e != nil || witness.SHA256 != physicalDigest(raw) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("v2 original created FD bytes differ"))
	}
	ro, e := unix.Openat(int(state.dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	pin := os.NewFile(uintptr(ro), "original v2 readonly WAL pin")
	actual, e := normalKernelPrepV2PinnedWitness(pin)
	if e != nil || actual != witness {
		pin.Close()
		return errors.Join(ErrUnknown, e, fmt.Errorf("v2 original WAL path no longer matches created FD"))
	}
	key := normalKernelPrepV2HistoryKey(before)
	normalKernelPrepV2PrivateHistory.Lock()
	if normalKernelPrepV2PrivateHistory.entries[key] == nil {
		normalKernelPrepV2PrivateHistory.entries[key] = map[string]normalSystemRoleFileWitness{}
	}
	if normalKernelPrepV2PrivateHistory.files[key] == nil {
		normalKernelPrepV2PrivateHistory.files[key] = map[string]*os.File{}
	}
	if old := normalKernelPrepV2PrivateHistory.files[key][name]; old != nil {
		old.Close()
	}
	normalKernelPrepV2PrivateHistory.entries[key][name] = witness
	normalKernelPrepV2PrivateHistory.files[key][name] = pin
	normalKernelPrepV2PrivateHistory.Unlock()
	if fault != nil && fault.syncFile != nil {
		e = fault.syncFile(created)
	} else {
		e = created.Sync()
	}
	if e != nil {
		return e
	}
	if fault != nil && fault.syncDir != nil {
		return fault.syncDir(state.dir)
	}
	return state.dir.Sync()
}
func normalKernelPrepV2VerifyPrivateHistory(state *lockedState, before normalKernelPrepV2Snapshot, name string) error {
	key := normalKernelPrepV2HistoryKey(before)
	normalKernelPrepV2PrivateHistory.Lock()
	wanted, ok := normalKernelPrepV2PrivateHistory.entries[key][name]
	normalKernelPrepV2PrivateHistory.Unlock()
	if !ok {
		return errors.Join(ErrUnknown, fmt.Errorf("v2 original producer private history missing"))
	}
	got, e := readNormalSystemRoleFileWitness(state, name)
	if e != nil || got != wanted {
		return errors.Join(ErrUnknown, e, fmt.Errorf("v2 original durable inode/ctime/hash changed"))
	}
	return nil
}

func normalKernelPrepV2RetainOriginalBackend(before normalKernelPrepV2Snapshot, b normalKernelPrepV2Backend) {
	key := normalKernelPrepV2HistoryKey(before)
	normalKernelPrepV2PrivateHistory.Lock()
	defer normalKernelPrepV2PrivateHistory.Unlock()
	if _, ok := normalKernelPrepV2PrivateHistory.entries[key][normalKernelPrepV2IntentName]; ok && normalKernelPrepV2PrivateHistory.backends[key] == nil {
		normalKernelPrepV2PrivateHistory.backends[key] = b
	}
}

func normalKernelPrepV2ReadLocked(b normalKernelPrepV2Backend, state *lockedState) (normalKernelPrepV2Snapshot, error) {
	s, e := b.kernelPrepV2Read()
	if e != nil {
		return s, e
	}
	if state == nil || state.lock == nil {
		return s, ErrUnknown
	}
	var st unix.Stat_t
	if unix.Fstat(int(state.lock.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != 0 || st.Gid != 0 || st.Mode&0777 != 0600 || st.Nlink != 1 {
		return s, fmt.Errorf("v2 original resource lock differs")
	}
	o, e := normalFilesystemFDObject(int(state.lock.Fd()))
	if e != nil {
		return s, e
	}
	s.Lock = normalKernelPrepV2Object{Device: st.Dev, Inode: st.Ino, MountID: o.MountID, Mode: st.Mode, UID: st.Uid, GID: st.Gid}
	return s, nil
}
