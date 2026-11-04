//go:build linux

package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"golang.org/x/sys/unix"
)

const normalMemoryHostReferenceFile = "normal-memory-origin-host.reference.json"
const normalMemoryHostAckFile = "normal-memory-origin-host.guest.json"

// 只由同一原initial完成生命周期构造。私有字段不能从HTTP/JSON恢复成能力；
// 调用者持原engine flock及processGuard.RLock，锁序与drive一致，不消费Wait。
type normalMemoryHostOriginSource struct {
	reference bootprotocol.NormalMemoryOriginHostReference
	source    initialBootSource
	process   *initialBootProcess
	wal       [8]string
	walFiles  [8][3]uint64
	directory [4]uint64
	proof     InitialBootstrapPublication
	files     [2][3]uint64
	ack       *bootprotocol.NormalMemoryOriginGuestAck
	owner     *linuxInitialLifecycle
	runtime   []byte
	selection uint32
	commit2   *normalMemoryHostCommitEvidence
}

func normalMemoryHostWAL(s *lockedState, proof *InitialBootstrapPublication) ([8]string, [4]uint64, error) {
	var w [8]string
	var dir [4]uint64
	if s == nil || s.dir == nil || s.lock == nil || proof == nil {
		return w, dir, fmt.Errorf("original memory Host lock/proof missing")
	}
	var ds, ls unix.Stat_t
	if unix.Fstat(int(s.dir.Fd()), &ds) != nil || unix.Fstat(int(s.lock.Fd()), &ls) != nil || privateFile(int(s.dir.Fd()), unix.S_IFDIR) != nil || privateFile(int(s.lock.Fd()), unix.S_IFREG) != nil || ds.Dev != ls.Dev {
		return w, dir, fmt.Errorf("original memory Host state identity differs")
	}
	dir = [4]uint64{ds.Dev, ds.Ino, ls.Dev, ls.Ino}
	es, e := initialBootRead(s)
	if e != nil || len(es) != 8 || !bytes.Equal(initialBootCanonical(es[7]), proof.CompletedEntryJSON) {
		return w, dir, fmt.Errorf("original memory Host eight-stage source differs: %v", e)
	}
	for i := range w {
		b, e := initialBootReadFile(s, initialBootFile(i))
		if e != nil {
			return w, dir, e
		}
		w[i] = initialBootSHA(b)
	}
	return w, dir, nil
}
func normalMemoryNeutralInputs(v memoryGeometryInputs) bootprotocol.NormalMemoryQMPInputs {
	return bootprotocol.NormalMemoryQMPInputs{Version: bytes.Clone(v.Version), Machine: bytes.Clone(v.Machine), Devices: bytes.Clone(v.Devices), Summary: bytes.Clone(v.Summary), Backends: bytes.Clone(v.Backends), BackendSize: bytes.Clone(v.BackendSize)}
}
func normalMemoryCrossGeometry(source initialBootSource, in memoryGeometryInputs) (bootprotocol.NormalMemoryGeometry, error) {
	old, e := decodeOriginalMemoryGeometry(source.Identity, source.OriginalBase, in)
	if e != nil {
		return bootprotocol.NormalMemoryGeometry{}, e
	}
	n, e := bootprotocol.DecodeNormalMemoryGeometry(bootprotocol.RunnerIdentity(source.Identity), source.OriginalBase, normalMemoryNeutralInputs(in))
	if e != nil {
		return n, e
	}
	if old.Identity != Identity(n.Identity) || old.BaseBytes != n.BaseBytes || old.BackendBytes != n.BackendBytes || old.MaximumBytes != n.MaximumBytes || old.DeviceAddress != n.DeviceAddress || old.RequestedBytes != n.RequestedBytes || old.ActualBytes != n.ActualBytes || old.QEMUVersion != n.QEMUVersion {
		return n, fmt.Errorf("neutral and original strict geometry decoders disagree")
	}
	return n, nil
}

// fixed original channel显式新协议分支才可调用；旧complete从不自动调用本producer。
// 六份QMP原return两读都保留并用原/中立decoder双验，不接scalar最大值或trusted bool。
func captureNormalMemoryHostOriginLocked(ctx context.Context, s *lockedState, l *linuxInitialLifecycle, proof *InitialBootstrapPublication) (*normalMemoryHostOriginSource, error) {
	if ctx == nil || ctx.Err() != nil || l == nil || l.r == nil || l.r.engine == nil || l.q == nil || l.r.birth.InitialMode == nil || proof == nil {
		return nil, fmt.Errorf("live original memory Host lifecycle missing")
	}
	if e := validateInitialLifecyclePublication(l.r.birth, l.r.engine.binding, *proof); e != nil {
		return nil, e
	}
	source, p, e := l.sourceAndProcessLocked()
	if e != nil {
		return nil, e
	}
	if source.RuntimeSHA256 != l.r.birth.InitialMode.RuntimeSHA256 || source.KernelBuildSHA256 != l.r.birth.InitialMode.KernelBuildSHA256 {
		return nil, fmt.Errorf("original memory runtime source differs")
	}
	if e = l.recheckInitialAuthority(ctx, source); e != nil {
		return nil, e
	}
	wal, dir, e := normalMemoryHostWAL(s, proof)
	if e != nil {
		return nil, e
	}
	es, e := initialBootRead(s)
	if e != nil {
		return nil, e
	}
	if es[7].Source != source {
		return nil, fmt.Errorf("memory capture differs from original eight-stage Source")
	}
	var evidence initialBootCompleteEvidence
	if e = json.Unmarshal(es[7].Evidence, &evidence); e != nil {
		return nil, e
	}
	var base struct {
		Geometry originalMemoryGeometry `json:"geometry"`
		Hardware initialBootHardware    `json:"hardware"`
	}
	if e = json.Unmarshal(es[1].Evidence, &base); e != nil {
		return nil, e
	}
	hardware, e := readInitialBootHardware(ctx, l.q, p, source)
	if e != nil {
		return nil, e
	}
	if hardware != base.Hardware {
		return nil, fmt.Errorf("original memory hardware inventory drifted")
	}
	c, e := l.q.connect(ctx)
	if e != nil {
		return nil, e
	}
	defer c.close()
	if e = p.checkPeer(c, source); e != nil {
		return nil, e
	}
	first, e := initialBootGeometryReadContext(ctx, l.q, c)
	if e != nil {
		return nil, e
	}
	one, e := normalMemoryCrossGeometry(source, first)
	if e != nil {
		return nil, e
	}
	if base.Geometry.Identity != source.Identity || base.Geometry.BaseBytes != one.BaseBytes || base.Geometry.BackendBytes != one.BackendBytes || base.Geometry.MaximumBytes != one.MaximumBytes || base.Geometry.DeviceAddress != one.DeviceAddress || base.Geometry.QEMUVersion != one.QEMUVersion {
		return nil, fmt.Errorf("memory capture changed original base-stage maximum geometry")
	}
	second, e := initialBootGeometryReadContext(ctx, l.q, c)
	if e != nil {
		return nil, e
	}
	two, e := normalMemoryCrossGeometry(source, second)
	if e != nil {
		return nil, e
	}
	if one != two || one.RequestedBytes != source.Identity.MinimumBytes || one.ActualBytes != source.Identity.MinimumBytes {
		return nil, fmt.Errorf("original memory QMP initial geometry is not stable at business minimum")
	}
	if e = p.checkPeer(c, source); e != nil {
		return nil, e
	}
	plan, e := normalMemoryHostPlan(l.config.RuntimeJSON, source)
	if e != nil {
		return nil, e
	}
	ref := bootprotocol.NormalMemoryOriginHostReference{ProtocolVersion: bootprotocol.NormalMemoryOriginProtocolVersion, Scope: bootprotocol.NormalMemoryOriginScope, ProfileID: plan.ProfileID(), Prepared: evidence.Preboot, Movable: evidence.Movable, Completion: proof.Completion, Process: bootprotocol.NormalMemoryQEMUReference{PID: uint32(p.pid), StartTicks: p.start, ExecutableDevice: source.QEMUDevice, ExecutableInode: source.QEMUInode, ExecutableSHA256: source.QEMUSHA256, CommandSHA256: source.CommandSHA256, LaunchNonce: source.LaunchNonce}, MaximumVCPUs: hardware.MaximumVCPUs, HardwareSHA256: initialBootSHA(initialBootCanonical(hardware)), QMP: normalMemoryNeutralInputs(first)}
	if e = ref.Validate(); e != nil {
		return nil, e
	}
	after, ad, e := normalMemoryHostWAL(s, proof)
	if e != nil || after != wal || ad != dir {
		return nil, fmt.Errorf("original memory Host source changed across QMP: %v", e)
	}
	if e = l.recheckInitialAuthority(ctx, source); e != nil {
		return nil, e
	}
	if e = normalMemoryHostWriteOnce(s, normalMemoryHostReferenceFile, initialBootCanonical(ref)); e != nil {
		return nil, e
	}
	proofCopy := *proof
	proofCopy.CompletedEntryJSON = bytes.Clone(proof.CompletedEntryJSON)
	selection, e := normalMemoryHostSelection(l.config.RuntimeJSON, source)
	if e != nil {
		return nil, e
	}
	out := &normalMemoryHostOriginSource{reference: ref, source: source, process: p, wal: wal, directory: dir, proof: proofCopy, owner: l, runtime: bytes.Clone(l.config.RuntimeJSON), selection: selection}
	out.walFiles, e = normalMemoryHostWALFileIDs(s, wal)
	if e != nil {
		return nil, e
	}
	out.files[0], e = normalMemoryHostFileIdentity(s, normalMemoryHostReferenceFile, initialBootCanonical(ref))
	if e != nil {
		return nil, e
	}
	return out, nil
}

// 原文件同字节也必须实际file+directory Sync；不以可见性或摘要代替耐久。
func normalMemoryHostWriteOnce(s *lockedState, name string, raw []byte) error {
	if s == nil || s.dir == nil || (name != normalMemoryHostReferenceFile && name != normalMemoryHostAckFile && name != normalMemoryHostCommitFile) || len(raw) == 0 || len(raw) > bootprotocol.MaxFrameBytes {
		return fmt.Errorf("fixed bounded memory Host WAL required")
	}
	old, e := initialBootReadFile(s, name)
	if e != nil {
		return e
	}
	if old != nil {
		if !bytes.Equal(old, raw) {
			return fmt.Errorf("original memory Host WAL body changed")
		}
		fd, e := unix.Openat(int(s.dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
		if e != nil {
			return e
		}
		f := os.NewFile(uintptr(fd), name)
		defer f.Close()
		if e = privateFile(fd, unix.S_IFREG); e != nil {
			return e
		}
		if e = f.Sync(); e != nil {
			return e
		}
		return s.dir.Sync()
	}
	nonce, e := randomProcessNonce()
	if e != nil {
		return e
	}
	tmp := "normal-memory-origin-" + nonce + ".tmp"
	fd, e := unix.Openat(int(s.dir.Fd()), tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), tmp)
	defer unix.Unlinkat(int(s.dir.Fd()), tmp, 0)
	if n, e := f.Write(raw); e != nil || n != len(raw) {
		f.Close()
		if e != nil {
			return e
		}
		return io.ErrShortWrite
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = unix.Linkat(int(s.dir.Fd()), tmp, int(s.dir.Fd()), name, 0); e != nil {
		return e
	}
	return s.dir.Sync()
}

// 单次kind5→6→7；只接受真实capture的opaque源，失败关流且不自动重发。
// 原complete仍即时关闭；只有固定ISO版本选择的新调用者可改走本方法。
func (c *initialBootChannel) completeWithNormalMemoryOrigin(ctx context.Context, s *lockedState, origin *normalMemoryHostOriginSource) (bootprotocol.NormalMemoryOriginGuestAck, error) {
	var zero bootprotocol.NormalMemoryOriginGuestAck
	if c == nil || origin == nil || origin.process == nil {
		return zero, fmt.Errorf("original opaque memory Host source required")
	}
	if origin.owner == nil || origin.selection != 1 {
		return zero, fmt.Errorf("original ISO did not explicitly select memory origin wire v1")
	}
	actualSelection, e := normalMemoryHostSelection(origin.owner.config.RuntimeJSON, origin.source)
	if e != nil || actualSelection != 1 || !bytes.Equal(origin.runtime, origin.owner.config.RuntimeJSON) {
		return zero, fmt.Errorf("original memory origin protocol/source changed: %v", e)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.process != origin.process || !reflect.DeepEqual(c.source, origin.source) || origin.reference.Prepared != c.prepared || origin.reference.Movable != c.observed {
		return zero, c.failLocked(fmt.Errorf("memory source changed original channel"))
	}
	w, d, e := normalMemoryHostWAL(s, &origin.proof)
	if e != nil || w != origin.wal || d != origin.directory {
		return zero, c.failLocked(fmt.Errorf("memory source eight-WAL changed before send: %v", e))
	}
	file, e := normalMemoryHostFileIdentity(s, normalMemoryHostReferenceFile, initialBootCanonical(origin.reference))
	if e != nil || file != origin.files[0] {
		return zero, c.failLocked(fmt.Errorf("memory reference durable inode changed: %v", e))
	}
	walIDs, e := normalMemoryHostWALFileIDs(s, origin.wal)
	if e != nil || walIDs != origin.walFiles {
		return zero, c.failLocked(fmt.Errorf("memory source original WAL inode drifted: %v", e))
	}
	want := origin.reference.Completion
	if e := want.MatchReceipt(c.observed); e != nil {
		return zero, c.failLocked(e)
	}
	stop, e := c.beginLocked(ctx, 2)
	if e != nil {
		return zero, e
	}
	defer stop()
	if e = bootprotocol.WriteInitialCompletion(c.conn, want); e != nil {
		return zero, c.failLocked(e)
	}
	if e = bootprotocol.WriteNormalMemoryOriginHostReference(c.conn, origin.reference); e != nil {
		return zero, c.failLocked(e)
	}
	ack, e := bootprotocol.ReadNormalMemoryOriginGuestAck(c.conn)
	if e != nil {
		return zero, c.failLocked(e)
	}
	if e = ack.MatchReference(origin.reference); e != nil {
		return zero, c.failLocked(e)
	}
	if e = c.process.checkPeer(&qmpConnection{conn: c.conn}, c.source); e != nil {
		return zero, c.failLocked(e)
	}
	if e = normalMemoryHostWriteOnce(s, normalMemoryHostAckFile, initialBootCanonical(ack)); e != nil {
		return zero, c.failLocked(e)
	}
	origin.files[1], e = normalMemoryHostFileIdentity(s, normalMemoryHostAckFile, initialBootCanonical(ack))
	if e != nil {
		return zero, c.failLocked(e)
	}
	origin.ack = &ack
	c.phase = 3
	c.closeLocked()
	return ack, nil
}

// 仅供下游同原flock内重核；不重新采集/更换原几何，不签持续写者或normal资格。
func (o *normalMemoryHostOriginSource) recheckLocked(ctx context.Context, s *lockedState, l *linuxInitialLifecycle, proof *InitialBootstrapPublication) error {
	if e := o.recheckOriginalLocked(ctx, s, l, proof); e != nil {
		return e
	}
	if o.selection == 2 {
		return o.recheckCommit2Locked(s)
	}
	return nil
}
func (o *normalMemoryHostOriginSource) recheckOriginalLocked(ctx context.Context, s *lockedState, l *linuxInitialLifecycle, proof *InitialBootstrapPublication) error {
	if ctx == nil || ctx.Err() != nil || o == nil || o.process == nil || o.ack == nil || l == nil || l.r == nil || o.reference.Validate() != nil {
		return fmt.Errorf("opaque original memory Host source absent")
	}
	if o.owner != l || (o.selection != 1 && o.selection != 2) || !bytes.Equal(o.runtime, l.config.RuntimeJSON) {
		return fmt.Errorf("original memory source protocol selection drifted")
	}
	selected, e := normalMemoryHostSelection(l.config.RuntimeJSON, o.source)
	if e != nil || selected != o.selection {
		return fmt.Errorf("original memory source protocol unavailable: %v", e)
	}
	if _, e := l.r.authorize(ctx); e != nil {
		return e
	}
	for i, n := range []string{normalMemoryHostReferenceFile, normalMemoryHostAckFile} {
		raw := initialBootCanonical(o.reference)
		if i == 1 {
			raw = initialBootCanonical(*o.ack)
		}
		actual, e := normalMemoryHostFileIdentity(s, n, raw)
		if e != nil || actual != o.files[i] {
			return fmt.Errorf("memory Host durable source inode changed: %v", e)
		}
	}
	w, d, e := normalMemoryHostWAL(s, proof)
	if e != nil || w != o.wal || d != o.directory {
		return fmt.Errorf("memory Host WAL identity drifted: %v", e)
	}
	walIDs, e := normalMemoryHostWALFileIDs(s, o.wal)
	if e != nil || walIDs != o.walFiles {
		return fmt.Errorf("memory source WAL inode changed: %v", e)
	}
	source, p, e := l.sourceAndProcessLocked()
	if e != nil || source != o.source || p != o.process {
		return fmt.Errorf("memory Host original process changed: %v", e)
	}
	c, e := l.q.connect(ctx)
	if e != nil {
		return e
	}
	defer c.close()
	if e = p.checkPeer(c, source); e != nil {
		return e
	}
	in, e := initialBootGeometryReadContext(ctx, l.q, c)
	if e != nil {
		return e
	}
	now, e := normalMemoryCrossGeometry(source, in)
	if e != nil {
		return e
	}
	original, e := bootprotocol.DecodeNormalMemoryGeometry(bootprotocol.RunnerIdentity(source.Identity), source.OriginalBase, o.reference.QMP)
	if e != nil {
		return e
	}
	if now.Identity != original.Identity || now.BaseBytes != original.BaseBytes || now.BackendBytes != original.BackendBytes || now.MaximumBytes != original.MaximumBytes || now.DeviceAddress != original.DeviceAddress || now.QEMUVersion != original.QEMUVersion {
		return fmt.Errorf("original memory Host maximum changed")
	}
	if e = p.checkPeer(c, source); e != nil {
		return e
	}
	// QEMU原monitor只服务单连接；完成本次几何/peer复核后先释放，
	// 再用硬件reader建立下一连接。defer仍负责所有前置错误的幂等关闭。
	c.close()
	hardware, e := readInitialBootHardware(ctx, l.q, p, source)
	if e != nil {
		return e
	}
	if initialBootSHA(initialBootCanonical(hardware)) != o.reference.HardwareSHA256 {
		return fmt.Errorf("original memory Host hardware changed")
	}
	_, e = l.r.authorize(ctx)
	return e
}

// 固定已成功sync的原文件FD/bytes/inode；同字节替换不得继承opaque耐久事实。
func normalMemoryHostFileIdentity(s *lockedState, name string, want []byte) ([3]uint64, error) {
	var out [3]uint64
	allowed := name == normalMemoryHostReferenceFile || name == normalMemoryHostAckFile || name == normalMemoryHostCommitFile
	for i := range initialBootStages {
		allowed = allowed || name == initialBootFile(i)
	}
	if s == nil || s.dir == nil || !allowed {
		return out, fmt.Errorf("fixed memory Host file required")
	}
	fd, e := unix.Openat(int(s.dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if e != nil {
		return out, e
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	if e = privateFile(fd, unix.S_IFREG); e != nil {
		return out, e
	}
	var a, b unix.Stat_t
	if e = unix.Fstat(fd, &a); e != nil {
		return out, e
	}
	if a.Size <= 0 || a.Size > 16384 || a.Size != int64(len(want)) {
		return out, fmt.Errorf("memory Host source size changed")
	}
	raw, e := io.ReadAll(io.LimitReader(f, int64(len(want))+1))
	if e != nil || !bytes.Equal(raw, want) {
		return out, fmt.Errorf("memory Host source bytes differ: %v", e)
	}
	if e = unix.Fstatat(int(s.dir.Fd()), name, &b, unix.AT_SYMLINK_NOFOLLOW); e != nil || a.Dev != b.Dev || a.Ino != b.Ino || a.Size != b.Size {
		return out, fmt.Errorf("memory Host source path drift: %v", e)
	}
	return [3]uint64{a.Dev, a.Ino, uint64(a.Size)}, nil
}

func normalMemoryHostWALFileIDs(s *lockedState, sha [8]string) ([8][3]uint64, error) {
	var out [8][3]uint64
	for i := range out {
		raw, e := initialBootReadFile(s, initialBootFile(i))
		if e != nil || initialBootSHA(raw) != sha[i] {
			return out, fmt.Errorf("original memory WAL raw bytes drifted: %v", e)
		}
		out[i], e = normalMemoryHostFileIdentity(s, initialBootFile(i), raw)
		if e != nil {
			return out, e
		}
	}
	return out, nil
}

// 仅从固定原Runtime全组资产重渲染派生；不接bool/env，不以profile名隐式启6/7。
func normalMemoryHostSelection(raw []byte, source initialBootSource) (uint32, error) {
	plan, e := normalMemoryHostPlan(raw, source)
	if e != nil {
		return 0, e
	}
	return plan.MemoryOriginProtocolVersion(), nil
}
func normalMemoryHostPlan(raw []byte, source initialBootSource) (resourceguard.RuntimeBootRequirements, error) {
	var zero resourceguard.RuntimeBootRequirements
	if initialBootSHA(raw) != source.RuntimeSHA256 {
		return zero, fmt.Errorf("memory protocol changed original runtime bytes")
	}
	assets, e := resourceguard.RenderRuntimePolicyAssets(raw)
	if e != nil {
		return zero, e
	}
	assets[resourceguard.RuntimeResourceAssetName] = bytes.Clone(raw)
	plan, e := resourceguard.DeriveRuntimeBootRequirements(raw, assets)
	if e != nil {
		return zero, e
	}
	legacy := plan.ProfileID() == bootprotocol.NormalMemoryOriginProfileID && plan.ProtocolVersion() == 1
	metadata := plan.ProfileID() == bootprotocol.NormalMemoryOriginMetadataProfileID && plan.ProtocolVersion() == 2 && plan.KernelPreparationProtocolVersion() == 2 && plan.MemoryOriginProtocolVersion() == 2 && plan.InitialWALFamily() == "initial8-kernel-prep-userns-v2" && plan.KernelBuildReference().KernelArtifactSHA256 == bootprotocol.NormalMemoryOriginMetadataKernelSHA256
	if !plan.Enabled() || !(legacy || metadata) || plan.KernelBuildReference().KernelArtifactSHA256 != source.KernelBuildSHA256 || plan.ResourceSourceSHA256() != source.ResourceSHA256 {
		return zero, fmt.Errorf("memory protocol differs from original full source")
	}
	v := plan.MemoryOriginProtocolVersion()
	if v > 2 {
		return zero, fmt.Errorf("memory origin protocol unsupported")
	}
	return plan, nil
}
