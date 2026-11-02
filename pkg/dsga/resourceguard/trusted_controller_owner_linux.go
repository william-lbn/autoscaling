//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const trustedControllerComputePath = "/usr/local/bin/compute_ctl"

// 可信节点部署只缩小启动所有者合同，不产生normal预算或VM许可。所有者持有
// 原server.lock的独立FD，并持续绑定同Root实例、原Guest/资源目录及System硬限。
type trustedControllerOwner struct {
	mu                 sync.Mutex
	guard              *Guard
	lock               *os.File
	lockStat           unix.Stat_t
	consumer           nativeBirthConsumer
	identity           Identity
	floor              control.Uint64
	stateDev, stateIno uint64
	source             SourceConfig
	compute            *trustedControllerComputePin
	closed             bool
}
type trustedControllerComputePin struct {
	owner         *trustedControllerOwner
	task          *normalSystemOriginalTask
	executable    *os.File
	exeStat       unix.Stat_t
	exeSHA, nonce string
	floor         control.Uint64
	witness       normalSystemRoleFileWitness
}

func newTrustedControllerOwner(ctx context.Context, g *Guard, source SourceConfig, lock *os.File) (*trustedControllerOwner, error) {
	if ctx == nil || ctx.Err() != nil || g == nil || source.Config != g.config || g.config.ExecutionModel != TrustedControllerExecutionModel || !g.config.SystemCgroup || lock == nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return nil, ErrUnknown
	}
	if e := trustedControllerImageContract(); e != nil {
		return nil, e
	}
	fd, e := unix.FcntlInt(lock.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	o := &trustedControllerOwner{guard: g, source: source, lock: os.NewFile(uintptr(fd), "trusted-controller-original-server-lock")}
	ok := false
	defer func() {
		if !ok {
			o.close()
		}
	}()
	if e = unix.Fstat(fd, &o.lockStat); e != nil || o.lockStat.Uid != 0 || o.lockStat.Mode&unix.S_IFMT != unix.S_IFREG || o.lockStat.Mode&0077 != 0 {
		return nil, ErrUnknown
	}
	o.consumer, e = nativeBirthCurrentConsumerIdentity()
	if e != nil {
		return nil, e
	}
	e = g.withRecord(ctx, func(r *record, s *lockedState) error {
		if r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Bootstrap.SystemPlacementObserved || sameBootstrapSource(source, r.Bootstrap.Original) != nil {
			return ErrUnknown
		}
		var st unix.Stat_t
		if unix.Fstat(int(s.dir.Fd()), &st) != nil {
			return ErrUnknown
		}
		o.identity = r.Identity
		o.floor = r.Floor
		o.stateDev = st.Dev
		o.stateIno = st.Ino
		return o.ensureRootSystemLocked(r)
	})
	if e != nil {
		return nil, e
	}
	ok = true
	return o, nil
}

// 只允许原封存Guest且原Bootstrap已完成后迁移当前Root自身；不创建组、不迁移
// 其他进程。cgroup.procs写入整个线程组，随后再核原inode/有限硬限与实际归属。
func (o *trustedControllerOwner) ensureRootSystemLocked(r *record) error {
	if o == nil || r == nil || r.Identity != o.identity || r.systemLimit() == 0 {
		return ErrUnknown
	}
	if e := o.verifySystemLocked(r); e != nil {
		return e
	}
	group, e := bootProcGroup(os.Getpid())
	if e != nil {
		return e
	}
	if group != "/neon-system" {
		ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
		if e != nil {
			return e
		}
		release, e := kernelText("/proc/sys/kernel/osrelease")
		if e != nil || release != ref.KernelRelease || group != "/" {
			return errors.Join(ErrUnknown, fmt.Errorf("trusted Root is outside original Guest System domain"))
		}
		// 原只读部署来源与Guest唯一boot已经由构造器核实；宿主服务层级不能进入此门。
		f, e := rootFile(filepath.Join(systemCgroup, "cgroup.procs"), true)
		if e != nil {
			return e
		}
		_, e = fmt.Fprintln(f, os.Getpid())
		closeErr := f.Close()
		if e != nil || closeErr != nil {
			return errors.Join(e, closeErr)
		}
	}
	group, e = bootProcGroup(os.Getpid())
	if e != nil || group != "/neon-system" {
		return errors.Join(ErrUnknown, fmt.Errorf("trusted Root System membership readback differs"), e)
	}
	return o.verifySystemLocked(r)
}
func (o *trustedControllerOwner) verifySystemLocked(r *record) error {
	if r == nil || r.Identity != o.identity || r.systemLimit() == 0 {
		return ErrUnknown
	}
	dev, ino, e := systemCgroupIdentity(uint64(r.Identity.CgroupDevice))
	if e != nil || dev != uint64(r.Identity.SystemCgroupDevice) || ino != uint64(r.Identity.SystemCgroupInode) {
		return ErrUnknown
	}
	node, e := splitReadNode(systemCgroup)
	if e != nil || !node.RootControlled || node.MemoryMaxBytes != r.systemLimit() || node.SwapMaxBytes != 0 || !node.MemoryHighUnlimited {
		return ErrUnknown
	}
	return nil
}
func (o *trustedControllerOwner) verifyLocked(ctx context.Context, s *lockedState, r *record) error {
	if o == nil || ctx == nil || ctx.Err() != nil || s == nil || r == nil {
		return ErrUnknown
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.guard == nil || o.guard.config.ExecutionModel != TrustedControllerExecutionModel || o.lock == nil || r.Identity != o.identity || r.Floor < o.floor || r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Bootstrap.SystemPlacementObserved || sameBootstrapSource(o.source, r.Bootstrap.Original) != nil {
		return ErrUnknown
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != o.consumer {
		return ErrUnknown
	}
	var actual, held, dir unix.Stat_t
	if unix.Fstat(int(o.lock.Fd()), &held) != nil || unix.Lstat(filepath.Join(o.guard.config.StateDir, "server.lock"), &actual) != nil || actual.Dev != o.lockStat.Dev || actual.Ino != o.lockStat.Ino || held.Dev != actual.Dev || held.Ino != actual.Ino || actual.Uid != 0 || actual.Mode&unix.S_IFMT != unix.S_IFREG || actual.Mode&0077 != 0 || unix.Fstat(int(s.dir.Fd()), &dir) != nil || dir.Dev != o.stateDev || dir.Ino != o.stateIno {
		return ErrUnknown
	}
	if e = o.verifySystemLocked(r); e != nil {
		return e
	}
	group, e := bootProcGroup(os.Getpid())
	if e != nil || group != "/neon-system" {
		return ErrUnknown
	}
	return ctx.Err()
}
func (o *trustedControllerOwner) close() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	o.closed = true
	if o.compute != nil {
		o.compute.close()
		o.compute = nil
	}
	if o.lock != nil {
		o.lock.Close()
		o.lock = nil
	}
}
func (p *trustedControllerComputePin) close() {
	if p == nil {
		return
	}
	if p.task != nil {
		p.task.close()
		p.task = nil
	}
	if p.executable != nil {
		p.executable.Close()
		p.executable = nil
	}
}
func (p *trustedControllerComputePin) duplicate() (*trustedControllerComputePin, error) {
	if p == nil || p.task == nil || p.executable == nil {
		return nil, ErrUnknown
	}
	t, e := duplicateNormalSystemOriginalTask(p.task)
	if e != nil {
		return nil, e
	}
	fd, e := unix.FcntlInt(p.executable.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		t.close()
		return nil, e
	}
	out := *p
	out.task = t
	out.executable = os.NewFile(uintptr(fd), "trusted-original-compute-exe")
	return &out, nil
}
func (p *trustedControllerComputePin) verify(ctx context.Context, s *lockedState, r *record) error {
	if p == nil || r == nil || p.owner == nil || p.task == nil || p.executable == nil || p.floor > r.Floor {
		return ErrUnknown
	}
	if e := p.owner.verifyLocked(ctx, s, r); e != nil {
		return e
	}
	got, e := readNormalSystemRoleFileWitness(s, "trusted-compute-"+p.nonce+".json")
	if e != nil || got != p.witness {
		return ErrUnknown
	}
	p.task.mu.Lock()
	defer p.task.mu.Unlock()
	if e = p.task.verify(); e != nil {
		return e
	}
	group, e := nativeTraceProcText(p.task.procfd, "cgroup")
	if e != nil || strings.TrimSpace(group) != "0::/neon-system" {
		return ErrUnknown
	}
	fd, e := unix.Openat(p.task.procfd, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Dev != p.exeStat.Dev || st.Ino != p.exeStat.Ino || st.Size != p.exeStat.Size || st.Mtim != p.exeStat.Mtim || st.Ctim != p.exeStat.Ctim {
		return ErrUnknown
	}
	return ctx.Err()
}
func (o *trustedControllerOwner) currentComputeLocked(ctx context.Context, s *lockedState, r *record) (*trustedControllerComputePin, error) {
	if e := o.verifyLocked(ctx, s, r); e != nil {
		return nil, e
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.compute == nil {
		return nil, ErrUnknown
	}
	out, e := o.compute.duplicate()
	if e != nil {
		return nil, e
	}
	// verify在owner锁外做，调用者仍持原resource flock。
	return out, nil
}
func (o *trustedControllerOwner) registerComputeLocked(ctx context.Context, s *lockedState, r *record, peer *unix.Ucred, pidfd int) (*trustedControllerComputePin, error) {
	if e := o.verifyLocked(ctx, s, r); e != nil {
		return nil, e
	}
	if peer == nil || peer.Pid <= 1 || peer.Uid == 0 || peer.Gid == 0 {
		return nil, ErrUnknown
	}
	o.mu.Lock()
	var existing *trustedControllerComputePin
	var duplicateErr error
	if !o.closed && o.compute != nil {
		existing, duplicateErr = o.compute.duplicate()
	}
	o.mu.Unlock()
	if duplicateErr != nil {
		return nil, duplicateErr
	}
	if existing != nil {
		if e := existing.verify(ctx, s, r); e != nil {
			existing.close()
			return nil, e
		}
		if existing.task.source.UID != peer.Uid || existing.task.source.GID != peer.Gid || arrayMatchPIDFD(existing.task.pidfd, pidfd) != nil {
			existing.close()
			return nil, ErrUnknown
		}
		return existing, nil
	}
	account, e := user.Lookup("postgres")
	if e != nil {
		return nil, e
	}
	uid, e := strconv.ParseUint(account.Uid, 10, 32)
	if e != nil {
		return nil, e
	}
	gid, e := strconv.ParseUint(account.Gid, 10, 32)
	if e != nil || uint32(uid) != peer.Uid || uint32(gid) != peer.Gid {
		return nil, ErrUnknown
	}
	proc, e := unix.Open("/proc/"+strconv.Itoa(int(peer.Pid)), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(proc), "trusted-compute-peer-proc")
	defer f.Close()
	stat, e := nativeTraceProcText(proc, "stat")
	if e != nil {
		return nil, e
	}
	ticks, e := postmasterTicks(stat, int(peer.Pid))
	if e != nil {
		return nil, e
	}
	peerfd, e := arrayRegistryDup(pidfd)
	if e != nil {
		return nil, e
	}
	pf := os.NewFile(uintptr(peerfd), "trusted-compute-peer-pidfd")
	defer pf.Close()
	task, e := newNormalSystemOriginalTask(normalSystemOriginalTaskSource{PID: int(peer.Pid), StartTicks: ticks, UID: peer.Uid, GID: peer.Gid, GuestBootID: r.Identity.GuestBootID, PIDFD: pf, ProcDirectory: f})
	if e != nil {
		return nil, e
	}
	pin := &trustedControllerComputePin{owner: o, task: task, floor: r.Floor}
	ok := false
	defer func() {
		if !ok {
			pin.close()
		}
	}()
	canonical, e := filepath.EvalSymlinks(trustedControllerComputePath)
	if e != nil || secureParents(filepath.Dir(canonical)) != nil {
		return nil, ErrUnknown
	}
	expected, e := rootFile(canonical, false)
	if e != nil {
		return nil, e
	}
	defer expected.Close()
	fd, e := unix.Openat(proc, "exe", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	pin.executable = os.NewFile(uintptr(fd), "trusted-original-compute-exe")
	var want unix.Stat_t
	if unix.Fstat(fd, &pin.exeStat) != nil || unix.Fstat(int(expected.Fd()), &want) != nil || want.Dev != pin.exeStat.Dev || want.Ino != pin.exeStat.Ino || want.Mode&0111 == 0 {
		return nil, ErrUnknown
	}
	pin.exeSHA, e = nativeBirthFileHash(pin.executable)
	if e != nil {
		return nil, e
	}
	pin.nonce, e = newNativeBirthNonce()
	if e != nil {
		return nil, e
	}
	raw, e := json.Marshal(struct {
		Version          uint32              `json:"protocol_version"`
		Scope            string              `json:"scope"`
		Consumer         nativeBirthConsumer `json:"consumer"`
		Identity         Identity            `json:"identity"`
		Floor            control.Uint64      `json:"issuer_epoch"`
		Nonce            string              `json:"nonce"`
		PID              int                 `json:"pid"`
		StartTicks       uint64              `json:"start_ticks"`
		UID, GID         uint32
		ExecutableSHA256 string `json:"executable_sha256"`
	}{1, "trusted-original-compute-v1", o.consumer, r.Identity, r.Floor, pin.nonce, task.source.PID, task.source.StartTicks, peer.Uid, peer.Gid, pin.exeSHA})
	if e != nil {
		return nil, e
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.compute != nil {
		return nil, ErrUnknown
	}
	name := "trusted-compute-" + pin.nonce + ".json"
	if e = s.createPhysicalFile(name, raw); e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	pin.witness, e = readNormalSystemRoleFileWitness(s, name)
	if e != nil || pin.witness.SHA256 != physicalDigest(raw) {
		return nil, ErrUnknown
	}
	// 原Task核验后仍须实际System归属，不能以账号/路径代替受限工作域。
	group, e := nativeTraceProcText(task.procfd, "cgroup")
	if e != nil || strings.TrimSpace(group) != "0::/neon-system" {
		return nil, ErrUnknown
	}
	owned, e := pin.duplicate()
	if e != nil {
		return nil, e
	}
	o.compute = owned
	ok = true
	return pin, nil
}
func (p *trustedControllerComputePin) verifyLaunchParent(launch *arrayLaunchPin) error {
	if p == nil || p.task == nil || launch == nil || launch.procfd < 0 {
		return ErrUnknown
	}
	status, e := nativeTraceProcText(launch.procfd, "status")
	if e != nil {
		return e
	}
	parent, e := nativeTraceStatusValues(status, "PPid", 1)
	if e != nil || parent[0] != uint64(p.task.source.PID) {
		return ErrUnknown
	}
	p.task.mu.Lock()
	defer p.task.mu.Unlock()
	return p.task.verify()
}
