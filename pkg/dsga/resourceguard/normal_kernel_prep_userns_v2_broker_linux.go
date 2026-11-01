//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"sync"
)

// R2握手时R1仍活；原clone/proof/task/8NS在consumer单独核，不能复用R1的PPID1门。
func normalKernelPrepV2R2Caller() error {
	if os.Getuid() != 0 || os.Geteuid() != 0 || os.Getgid() != 0 || os.Getegid() != 0 {
		return fmt.Errorf("kernel2 R2 requires original Root credentials")
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
	var header = unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	var caps [2]unix.CapUserData
	if e = unix.Capget(&header, &caps[0]); e != nil {
		return e
	}
	return normalKernelPrepV2R2PrivilegeStatus(text, caps)
}
func normalKernelPrepV2R2PrivilegeStatus(text string, caps [2]unix.CapUserData) error {
	if e := normalSystemRoleServicePrivilegeStatus(text); e != nil {
		return e
	}
	for _, key := range []string{"Uid", "Gid"} {
		values, e := nativeTraceStatusValues(text, key, 4)
		if e != nil {
			return e
		}
		for _, v := range values {
			if v != 0 {
				return fmt.Errorf("kernel2 original R2 credentials differ")
			}
		}
	}
	if caps[0].Effective&(1<<unix.CAP_SYS_RESOURCE) == 0 {
		return fmt.Errorf("kernel2 R2 lacks fixed original Root capability")
	}
	return nil
}

func normalKernelPrepV2TransferredControl(file *os.File, want normalKernelPrepV2Object) error {
	if file == nil {
		return ErrUnknown
	}
	flags, e := unix.FcntlInt(file.Fd(), unix.F_GETFL, 0)
	if e != nil || flags&unix.O_ACCMODE != unix.O_RDONLY || flags&unix.O_PATH != 0 {
		return errors.Join(ErrUnknown, e, fmt.Errorf("kernel2 transferred FD must be readable RO, not O_PATH"))
	}
	got, e := normalKernelPrepV2ControlObject(int(file.Fd()))
	if e != nil || got != want {
		return errors.Join(ErrUnknown, e, fmt.Errorf("kernel2 transferred original procfs object differs"))
	}
	return nil
}

// SCM/F_DUPFD共享OFD；ReadAt独立offset0，不使用Seek，不重新open路径。
func normalKernelPrepV2ReadAtControl(file *os.File) (control.Uint64, error) {
	if file == nil {
		return 0, ErrUnknown
	}
	var raw [64]byte
	n, e := file.ReadAt(raw[:], 0)
	if e != nil && !errors.Is(e, io.EOF) {
		return 0, errors.Join(ErrUnknown, e)
	}
	if n != 2 || !bytes.Equal(raw[:n], []byte("0\n")) {
		return 0, errors.Join(ErrUnknown, fmt.Errorf("kernel2 original RO control is not canonical zero"))
	}
	return 0, nil
}
func normalKernelPrepV2MatchFilesystemReference(state *lockedState, fs *normalFilesystemOriginSource, ref normalKernelPrepV2HandoffReference) error {
	if state == nil || fs == nil || ref.validate() != nil || fs.intent.ProtocolVersion != 2 || fs.intent.Memory == nil {
		return ErrUnknown
	}
	memory := *fs.intent.Memory
	if memory.InitialWALFamily != normalInitialWALFamilyKernelPrepV2 || normalFilesystemValidateMemoryReference(fs.intent) != nil || memory.OriginalRoot != ref.OriginalRoot || memory.GuestBootID != ref.Identity.GuestBootID || memory.RuntimeDocumentSHA256 != ref.RuntimeDocumentSHA256 || memory.InitialWAL.Device != ref.StateDevice || memory.InitialWAL.Inode != ref.StateInode || memory.InitialWAL.LockDevice != ref.OriginalLock.Device || memory.InitialWAL.LockInode != ref.OriginalLock.Inode {
		return errors.Join(ErrUnknown, fmt.Errorf("kernel2 handoff does not match original FS2/memory2"))
	}
	for i, w := range ref.OriginalWAL {
		a := memory.InitialWAL.Files[i]
		if memory.InitialWAL.SHA256[i] != w.SHA256 || a.Device != w.Device || a.Inode != w.Inode || a.Bytes != w.Size || a.ChangeSeconds != w.CTime.Sec || a.ChangeNanoseconds != w.CTime.Nsec {
			return ErrUnknown
		}
	}
	var initial initialPrebootGuestComplete
	if _, exists, e := initialPrebootReadRecord(state, initialPrebootGuestCompleteName, &initial); e != nil || !exists || initial.KernelPreparationSHA256 != ref.PreparationSHA256 {
		return errors.Join(ErrUnknown, e)
	}
	return nil
}

type normalKernelPrepV2BrokerConsumerSource struct {
	mu         sync.Mutex
	closed     bool
	g          *Guard
	current    nativeBirthConsumer
	reference  normalKernelPrepV2HandoffReference
	control    *os.File
	namespaces *normalSystemRoleNamespaceSource
	task       *normalSystemOriginalTask
	files      [2]*os.File
	policy     normalKernelPrepV2Decoded
	after      normalKernelPrepV2Snapshot
}
type normalKernelPrepV2BrokerConsumerWitness struct {
	Guard     *Guard
	Consumer  nativeBirthConsumer
	Reference normalKernelPrepV2HandoffReference
}

var normalKernelPrepV2BrokerConsumerWitnesses sync.Map

func (g *Guard) bindNormalKernelPrepV2BrokerConsumerLocked(ctx context.Context, state *lockedState, fs *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof) (out *normalKernelPrepV2BrokerConsumerSource, err error) {
	fail := func(e error) (*normalKernelPrepV2BrokerConsumerSource, error) { return nil, errors.Join(ErrUnknown, e) }
	if g == nil || ctx == nil || ctx.Err() != nil || state == nil || fs == nil || fs.g != g || proof == nil {
		return fail(fmt.Errorf("kernel2 original R2 typed inputs absent"))
	}
	if _, e := fs.referenceLocked(ctx, state); e != nil {
		return fail(e)
	}
	reader, ok := any(proof).(normalKernelPrepV2TransferReader)
	if !ok {
		return fail(fmt.Errorf("kernel2 needs explicit declaration3/transfer2 original fifth FD"))
	}
	ref, fd, e := reader.originalKernelPrepV2TransferLocked(ctx, state, fs.system)
	if e != nil {
		if fd != nil {
			fd.Close()
		}
		return fail(e)
	}
	if fd == nil {
		return fail(fmt.Errorf("kernel2 original fifth FD missing"))
	}
	out = &normalKernelPrepV2BrokerConsumerSource{g: g, reference: ref, control: fd}
	created := out
	good := false
	defer func() {
		if !good {
			created.close()
		}
	}()
	if e = normalKernelPrepV2MatchFilesystemReference(state, fs, ref); e != nil {
		return fail(e)
	}
	if e = normalKernelPrepV2TransferredControl(fd, ref.OriginalControl); e != nil {
		return fail(e)
	}
	out.current, e = nativeBirthCurrentConsumerIdentity()
	if e != nil || out.current == ref.OriginalRoot {
		return fail(errors.Join(e, fmt.Errorf("kernel2 requires distinct original R2 consumer")))
	}
	out.task, e = proof.originalTaskForFilesystemLocked(ctx, state, fs.system)
	if e != nil {
		return fail(e)
	}
	if out.task.source.PID != out.current.PID || out.task.source.StartTicks != uint64(out.current.StartTicks) {
		return fail(fmt.Errorf("kernel2 clone proof differs from R2"))
	}
	out.namespaces, e = captureNormalSystemRoleNamespaces()
	if e != nil {
		return fail(e)
	}
	for i, n := range []string{normalKernelPrepV2IntentName, normalKernelPrepV2CompleteName} {
		raw, e := unix.Openat(int(state.dir.Fd()), n, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if e != nil {
			return fail(e)
		}
		out.files[i] = os.NewFile(uintptr(raw), "kernel2 original historical WAL pin")
		got, e := normalKernelPrepV2PinnedWitness(out.files[i])
		if e != nil || got != ref.OriginalWAL[i] {
			return fail(errors.Join(e, fmt.Errorf("kernel2 original WAL identity differs")))
		}
	}
	var in normalKernelPrepV2Intent
	raw, e := normalKernelPrepV2PinnedRaw(out.files[0])
	if e != nil {
		return fail(e)
	}
	if e = control.UnmarshalObject(raw, &in); e != nil {
		return fail(e)
	}
	canonical, e := json.Marshal(in)
	if e != nil || !bytes.Equal(raw, canonical) || in.ProtocolVersion != 2 {
		return fail(errors.Join(e, fmt.Errorf("kernel2 original intent is not canonical")))
	}
	out.policy, e = decodeNormalKernelPrepV2(in.Inputs, g.config)
	if e != nil {
		return fail(e)
	}
	if physicalDigest(in.Inputs.RuntimeRaw) != ref.RuntimeDocumentSHA256 || physicalDigest(in.Inputs.PhysicalPolicyRaw) != ref.PhysicalPolicySHA256 || physicalDigest(in.Inputs.PolicyRaw) != ref.KernelPolicySHA256 {
		return fail(fmt.Errorf("kernel2 original policy bytes differ"))
	}
	var done NormalV2KernelBootstrapEvidence
	raw, e = normalKernelPrepV2PinnedRaw(out.files[1])
	if e != nil {
		return fail(e)
	}
	if e = control.UnmarshalObject(raw, &done); e != nil {
		return fail(e)
	}
	canonical, e = json.Marshal(done)
	if e != nil || !bytes.Equal(raw, canonical) {
		return fail(errors.Join(e, fmt.Errorf("kernel2 completion noncanonical")))
	}
	h, e := done.ReferenceSHA256()
	if e != nil || h != ref.PreparationSHA256 || done.PolicySHA256 != ref.KernelPolicySHA256 || done.RuntimeDocumentSHA256 != ref.RuntimeDocumentSHA256 || done.PhysicalOriginPolicySHA256 != ref.PhysicalPolicySHA256 || done.IntentSHA256 != ref.OriginalWAL[0].SHA256 || done.After.OriginalRoot != ref.OriginalRoot || done.After.Kernel.Identity != ref.Identity || done.After.Control != ref.OriginalControl || done.After.Lock != ref.OriginalLock || done.After.ProducerELFSHA256 != ref.ProducerELFSHA256 {
		return fail(errors.Join(e, fmt.Errorf("kernel2 original completion source differs")))
	}
	if e = done.After.validate(out.policy); e != nil || !done.After.target(out.policy.policy) {
		return fail(errors.Join(e, fmt.Errorf("kernel2 original target is incomplete")))
	}
	if e = in.Before.validate(out.policy); e != nil || in.Before.fixedMatch(done.After) != nil || !in.Before.zeroMovable() || in.Before.MaxUserNamespaces == 0 || in.Before.Kernel.ModulesDisabled != 0 {
		return fail(errors.Join(e, fmt.Errorf("kernel2 original preactor facts differ")))
	}
	out.after = done.After
	// 显式R2消费可同步原固定2WAL；运行期observer没有这条路径。
	if e = out.readCurrentLocked(ctx, state, false); e != nil {
		return fail(e)
	}
	for _, name := range []string{normalKernelPrepV2IntentName, normalKernelPrepV2CompleteName} {
		if e = state.syncPhysicalFile(name); e != nil {
			return fail(e)
		}
	}
	if e = state.dir.Sync(); e != nil {
		return fail(e)
	}
	if e = out.readCurrentLocked(ctx, state, false); e != nil {
		return fail(e)
	}
	normalKernelPrepV2BrokerConsumerWitnesses.Store(out, normalKernelPrepV2BrokerConsumerWitness{g, out.current, ref})
	good = true
	return out, nil
}
func normalKernelPrepV2PinnedRaw(f *os.File) ([]byte, error) {
	if f == nil {
		return nil, ErrUnknown
	}
	st, e := f.Stat()
	if e != nil || st.Size() <= 0 || st.Size() > 64<<10 {
		return nil, errors.Join(ErrUnknown, e)
	}
	raw := make([]byte, st.Size())
	n, e := f.ReadAt(raw, 0)
	if e != nil || n != len(raw) {
		return nil, errors.Join(ErrUnknown, e)
	}
	return raw, nil
}

// 调用者持自身mu；不回FS，不重R1存活，不加载state.json/floor，不要求PGEmpty。
func (o *normalKernelPrepV2BrokerConsumerSource) readCurrentLocked(ctx context.Context, state *lockedState, requireWitness bool) error {
	if o == nil || o.closed || ctx == nil || ctx.Err() != nil || state == nil || state.dir == nil || state.lock == nil {
		return ErrUnknown
	}
	if requireWitness {
		w, ok := normalKernelPrepV2BrokerConsumerWitnesses.Load(o)
		if !ok || w != (normalKernelPrepV2BrokerConsumerWitness{o.g, o.current, o.reference}) {
			return ErrUnknown
		}
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != o.current {
		return errors.Join(ErrUnknown, e)
	}
	if e = normalKernelPrepV2R2Caller(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if o.task == nil || o.namespaces == nil {
		return ErrUnknown
	}
	if e = o.task.verify(); e != nil {
		return e
	}
	if e = o.namespaces.verify(-1); e != nil {
		return e
	}
	names, e := o.namespaces.snapshot()
	if e != nil || len(names) != len(o.reference.OriginalNamespaces) {
		return errors.Join(ErrUnknown, e)
	}
	expected := map[string]normalSystemRoleNamespaceIdentity{}
	for _, n := range o.reference.OriginalNamespaces {
		expected[n.Name] = n
	}
	for _, n := range names {
		if expected[n.Name] != n {
			return ErrUnknown
		}
	}
	if e = normalKernelPrepV2TransferredControl(o.control, o.reference.OriginalControl); e != nil {
		return e
	}
	if _, e = normalKernelPrepV2ReadAtControl(o.control); e != nil {
		return e
	}
	st, e := initialPrebootStateIdentity(state)
	if e != nil || st.Dev != o.reference.StateDevice || st.Ino != o.reference.StateInode {
		return errors.Join(ErrUnknown, e)
	}
	var lk unix.Stat_t
	if e = unix.Fstat(int(state.lock.Fd()), &lk); e != nil {
		return e
	}
	obj, e := normalFilesystemFDObject(int(state.lock.Fd()))
	if e != nil {
		return e
	}
	got := normalKernelPrepV2Object{Device: lk.Dev, Inode: lk.Ino, MountID: obj.MountID, Mode: lk.Mode, UID: lk.Uid, GID: lk.Gid}
	if got != o.reference.OriginalLock {
		return ErrUnknown
	}
	k, ok := o.g.kernel.(*linuxKernel)
	if !ok {
		return ErrUnknown
	}
	now, e := k.kernelPrepV2BaseRead()
	if e != nil {
		return e
	}
	if e = o.after.Kernel.fixedMatch(now); e != nil {
		return e
	}
	if now.Identity != o.reference.Identity || now.controls() != o.policy.policy.Controls || now.ModulesDisabled != 1 || now.NUMABalancing != 0 || now.DemotionEnabled || len(now.LoadedModules) != 0 {
		return ErrUnknown
	}
	for i, n := range []string{normalKernelPrepV2IntentName, normalKernelPrepV2CompleteName} {
		held, e := normalKernelPrepV2PinnedWitness(o.files[i])
		if e != nil || held != o.reference.OriginalWAL[i] {
			return errors.Join(ErrUnknown, e)
		}
		visible, e := readNormalSystemRoleFileWitness(state, n)
		if e != nil || visible != held {
			return errors.Join(ErrUnknown, e)
		}
	}
	return nil
}
func (o *normalKernelPrepV2BrokerConsumerSource) referenceLocked(ctx context.Context, state *lockedState) (normalKernelPrepV2HandoffReference, error) {
	var zero normalKernelPrepV2HandoffReference
	if o == nil {
		return zero, ErrUnknown
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if e := o.readCurrentLocked(ctx, state, true); e != nil {
		return zero, errors.Join(ErrUnknown, e)
	}
	return o.reference, nil
}
func (o *normalKernelPrepV2BrokerConsumerSource) preparationReferenceLocked(ctx context.Context, state *lockedState) (string, error) {
	r, e := o.referenceLocked(ctx, state)
	if e != nil {
		return "", e
	}
	return r.PreparationSHA256, nil
}
func (o *normalKernelPrepV2BrokerConsumerSource) requireContinuousQualification() error {
	return errors.Join(ErrUnknown, fmt.Errorf("kernel2 R2 handoff does not close future writers or normal"))
}
func (o *normalKernelPrepV2BrokerConsumerSource) close() {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	o.closed = true
	normalKernelPrepV2BrokerConsumerWitnesses.Delete(o)
	if o.control != nil {
		o.control.Close()
	}
	for _, f := range o.files {
		if f != nil {
			f.Close()
		}
	}
	if o.namespaces != nil {
		o.namespaces.close()
	}
	if o.task != nil {
		o.task.close()
	}
}
