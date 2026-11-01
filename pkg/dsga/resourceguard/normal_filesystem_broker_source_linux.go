//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
)

// 仅消费原initialRoot通过已核端点转来的clone PIDFD/proc/ns/root与protected
// exec/released证明。不是resource角色枚举，不能从JSON、self PIDFD或当前census认领。
// 调用者已持resource flock；proof的Locked变体不能再次取同锁。
func (g *Guard) rebindNormalFilesystemOriginForBrokerLocked(ctx context.Context, state *lockedState, system *normalSystemLifecycleSource, proof *normalSystemRoleBrokerExecProof, reference normalFilesystemOriginReference, namespace, root *os.File) (_ *normalFilesystemOriginSource, err error) {
	fail := func(e error) (*normalFilesystemOriginSource, error) { return nil, errors.Join(ErrUnknown, e) }
	if g == nil || ctx == nil || ctx.Err() != nil || state == nil || state.dir == nil || state.lock == nil || system == nil || proof == nil || namespace == nil || root == nil || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fail(fmt.Errorf("original broker filesystem typed context missing"))
	}
	if reference.LayoutVersion != normalFilesystemLayoutVersion || reference.ProtocolVersion != 1 {
		return fail(fmt.Errorf("original broker filesystem reference version differs"))
	}
	// 先校准确原引用；producer关闭或别的transfer不能借同namespace补当前资格。
	proof.mu.Lock()
	same := !proof.closed && proof.system == system && proof.transfer.Origin.Filesystem == reference
	proof.mu.Unlock()
	if !same {
		return fail(fmt.Errorf("original broker transfer filesystem reference differs"))
	}
	system.mu.Lock()
	version := system.manifest.ProtocolVersion
	system.mu.Unlock()
	if version != 3 {
		return fail(fmt.Errorf("original broker filesystem needs explicit System v3"))
	}
	originalTask, e := proof.originalTaskForFilesystemLocked(ctx, state, system)
	if e != nil {
		return fail(e)
	}
	defer originalTask.close()
	binding, _, vminit, e := normalFilesystemSourceMetadata(system)
	if e != nil {
		return fail(e)
	}
	if originalTask.source.PID != os.Getpid() || originalTask.source.GuestBootID != binding.GuestBootID || originalTask.source.UID != 0 || originalTask.source.GID != 0 || binding.RootConsumer == reference.OriginalRoot {
		return fail(fmt.Errorf("broker filesystem current/original Root pair differs"))
	}
	in, _, e := normalFilesystemReadComplete(state)
	if e != nil {
		return fail(e)
	}
	if e = normalFilesystemReferenceMatches(system, in); e != nil {
		return fail(e)
	}
	if in.VminitSHA256 != vminit {
		return fail(fmt.Errorf("broker filesystem original vminit changed"))
	}
	a, exists, e := state.readPhysicalFile(normalFilesystemOriginIntentName)
	if e != nil || !exists {
		return fail(fmt.Errorf("broker filesystem original intent missing: %v", e))
	}
	b, exists, e := state.readPhysicalFile(normalFilesystemOriginCompleteName)
	if e != nil || !exists {
		return fail(fmt.Errorf("broker filesystem original complete missing: %v", e))
	}
	expected := normalFilesystemReferenceFromIntent(in, physicalDigest(a), physicalDigest(b))
	if expected != reference {
		return fail(fmt.Errorf("broker filesystem original complete reference differs"))
	}
	_, _, prefix, e := normalFilesystemInitialPrefix(state, binding, false)
	if e != nil || prefix.SHA256 != in.InitialPrefixSHA256 || prefix.Device != in.StateDevice || prefix.Inode != in.StateInode || prefix.LockDevice != in.LockDevice || prefix.LockInode != in.LockInode {
		return fail(fmt.Errorf("broker filesystem original preparation changed: %v", e))
	}
	ns, e := normalFilesystemDuplicateFile(namespace)
	if e != nil {
		return fail(e)
	}
	rt, e := normalFilesystemDuplicateFile(root)
	if e != nil {
		ns.Close()
		return fail(e)
	}
	owned := false
	defer func() {
		if !owned {
			ns.Close()
			rt.Close()
		}
	}()
	object, e := normalFilesystemFDObject(int(ns.Fd()))
	if e != nil || object != in.Namespace {
		return fail(fmt.Errorf("broker filesystem original namespace FD differs"))
	}
	object, e = normalFilesystemFDObject(int(rt.Fd()))
	if e != nil || object != in.Root {
		return fail(fmt.Errorf("broker filesystem original root FD differs"))
	}
	out := &normalFilesystemOriginSource{g: g, system: system, intent: in, current: binding.RootConsumer}
	good := false
	defer func() {
		if !good {
			out.close()
		}
	}()
	if out.pid1, e = normalFilesystemPinInitialTask(true); e != nil {
		return fail(e)
	}
	if out.self, e = normalFilesystemPinInitialTask(false); e != nil {
		return fail(e)
	}
	if out.pid1.start != in.PID1StartTicks || out.pid1.ns.Device != in.Namespace.Device || out.pid1.ns.Inode != in.Namespace.Inode || out.self.ns.Device != in.Namespace.Device || out.self.ns.Inode != in.Namespace.Inode || out.pid1.rootObject != in.Root || out.self.rootObject != in.Root {
		return fail(fmt.Errorf("broker filesystem original PID1/namespace/root changed"))
	}
	// self只用于读回当前root/ns；资格仍由跨进程移交的原clone PIDFD实际对应。
	originalTask.mu.Lock()
	e = arrayMatchPIDFD(int(out.self.pidfd.Fd()), originalTask.pidfd)
	originalTask.mu.Unlock()
	if e != nil {
		return fail(e)
	}
	for name, task := range map[string]*normalFilesystemTask{"/neonvm/bin/busybox": out.pid1, "/neonvm/bin/dsga-resource-guard": out.self} {
		if e = normalFilesystemTaskExecutable(task, system, name); e != nil {
			return fail(e)
		}
	}
	mounts, _, e := normalFilesystemCurrentGraph(ctx, in.Namespace, in.Root)
	if e != nil || !reflect.DeepEqual(mounts, in.Mounts) {
		return fail(fmt.Errorf("broker filesystem original graph changed: %v", e))
	}
	// 只有全部typed输入通过后同步已存在10WAL；不重拍初始两task或改oldRoot nonce。
	fingerprint := func() (normalFilesystemOriginWitnessKey, error) {
		_, _, p, e := normalFilesystemInitialPrefix(state, binding, false)
		if e != nil {
			return normalFilesystemOriginWitnessKey{}, e
		}
		return normalFilesystemWitnessLocked(g, state, binding.RootConsumer, p)
	}
	out.witness, e = normalFilesystemConfirmPrefix(ctx, state.syncPhysicalFile, fingerprint)
	if e != nil {
		return fail(e)
	}
	normalFilesystemOriginWitnesses.Store(out.witness, struct{}{})
	out.pid1.namespace.Close()
	out.pid1.root.Close()
	out.pid1.namespace = ns
	out.pid1.root = rt
	owned = true
	good = true
	return out, nil
}
