//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
)

const normalFilesystemOriginScopeV2 = "original-normal-filesystem-origin-v2"

func normalFilesystemProtocolMatches(version uint32, scope string) bool {
	return version == 1 && scope == normalFilesystemOriginScope || version == 2 && scope == normalFilesystemOriginScopeV2
}

// 协议版本是合同选择；仅历史投影不产生当前Root/进程/VM权限。
func normalFilesystemValidateMemoryReference(in normalFilesystemOriginIntent) error {
	if !normalFilesystemProtocolMatches(in.ProtocolVersion, in.Scope) {
		return errors.Join(ErrUnknown, fmt.Errorf("filesystem origin protocol/scope differs"))
	}
	if in.ProtocolVersion == 1 {
		if in.Memory != nil {
			return errors.Join(ErrUnknown, fmt.Errorf("legacy filesystem cannot adopt a memory2 reference"))
		}
		return nil
	}
	if in.Memory == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("filesystem2 original confirmed memory absent"))
	}
	m := *in.Memory
	if _, e := m.referenceSHA256(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if m.OriginalRoot != in.System.RootConsumer || m.GuestBootID != in.System.GuestBootID || m.RuntimeDocumentSHA256 != in.System.RuntimeDocumentSHA256 || m.InitialWAL.Device != in.StateDevice || m.InitialWAL.Inode != in.StateInode || m.InitialWAL.LockDevice != in.LockDevice || m.InitialWAL.LockInode != in.LockInode || m.InitialWAL.SHA256 != in.InitialPrefixSHA256 {
		return errors.Join(ErrUnknown, fmt.Errorf("filesystem2 original memory Root/boot/runtime/WAL binding differs"))
	}
	return nil
}

func normalFilesystemMemoryReferenceSHA(in normalFilesystemOriginIntent) (string, error) {
	if e := normalFilesystemValidateMemoryReference(in); e != nil {
		return "", e
	}
	if in.Memory == nil {
		return "", nil
	}
	return in.Memory.referenceSHA256()
}

func normalFilesystemValidateReferenceVersion(ref normalFilesystemOriginReference) error {
	if ref.LayoutVersion != normalFilesystemLayoutVersion {
		return ErrUnknown
	}
	switch ref.ProtocolVersion {
	case 1:
		if ref.MemorySourceSHA256 == "" {
			return nil
		}
	case 2:
		if physicalDigestValid(ref.MemorySourceSHA256) {
			return nil
		}
	}
	return errors.Join(ErrUnknown, fmt.Errorf("filesystem reference version/memory digest differs"))
}

func (g *Guard) captureNormalFilesystemMemoryLocked(ctx context.Context, s *lockedState, in initialPrebootGuestIntent, system normalSystemLifecycleBinding, prefix initialPrebootGuestWitnessKey) (*normalMemoryOriginConfirmedReference, error) {
	inputs, e := g.initialPrebootInputs(in.Challenge)
	if e != nil {
		return nil, e
	}
	if inputs.requirements.MemoryOriginProtocolVersion() != 2 {
		return nil, nil
	}
	source, e := g.normalMemoryOriginalConfirmedSourceLocked(ctx, s)
	if e != nil {
		return nil, e
	}
	ref, e := source.confirmedReferenceLocked(ctx, s, g)
	if e != nil {
		return nil, e
	}
	if ref.OriginalRoot != system.RootConsumer || ref.InitialWAL != prefix || ref.GuestBootID != system.GuestBootID || ref.RuntimeDocumentSHA256 != system.RuntimeDocumentSHA256 || ref.ResourceSourceSHA256 != in.Challenge.ResourceSHA256 {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("filesystem capture original opaque memory differs"))
	}
	return &ref, nil
}

// R2消费原R1已确认的历史，不修改R1 source.currentRoot，不从文件恢复那个opaque。
// 原R2 exec proof、同Guard、typed FS、历史11文件和当前物理区域缺一即Unknown。
type normalMemoryBrokerConsumerSource struct {
	g          *Guard
	filesystem *normalFilesystemOriginSource
	proof      *normalSystemRoleBrokerExecProof
	intent     normalMemoryOriginIntent
	reference  normalMemoryOriginConfirmedReference
	kernel     *normalKernelPrepV2BrokerConsumerSource
	witness    normalMemoryBrokerConsumerWitness
}
type normalMemoryBrokerConsumerWitness struct {
	Guard      *Guard
	Consumer   nativeBirthConsumer
	Filesystem normalFilesystemOriginReference
	Memory     normalMemoryOriginConfirmedReference
	KernelSHA  string
}

var normalMemoryBrokerConsumerWitnesses sync.Map

func (g *Guard) bindNormalMemoryBrokerConsumerLocked(ctx context.Context, s *lockedState, filesystem *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof) (*normalMemoryBrokerConsumerSource, error) {
	fail := func(e error) (*normalMemoryBrokerConsumerSource, error) { return nil, errors.Join(ErrUnknown, e) }
	if g == nil || ctx == nil || ctx.Err() != nil || s == nil || filesystem == nil || proof == nil || filesystem.g != g || os.Getuid() != 0 || os.Geteuid() != 0 {
		return fail(fmt.Errorf("original broker memory typed context absent"))
	}
	ref, e := filesystem.referenceLocked(ctx, s)
	if e != nil {
		return fail(e)
	}
	in := filesystem.intent
	if in.ProtocolVersion != 2 || in.Memory == nil || normalFilesystemValidateMemoryReference(in) != nil || normalFilesystemValidateReferenceVersion(ref) != nil {
		return fail(fmt.Errorf("original broker memory requires filesystem2"))
	}
	task, e := proof.originalTaskForFilesystemLocked(ctx, s, filesystem.system)
	if e != nil {
		return fail(e)
	}
	defer task.close()
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != filesystem.current || current == in.System.RootConsumer || task.source.PID != current.PID || task.source.StartTicks != uint64(current.StartTicks) || proof.transfer.Origin.Parent.RootConsumer != in.System.RootConsumer || proof.transfer.Origin.Filesystem != ref {
		return fail(fmt.Errorf("original broker memory R1/R2 producer pair differs"))
	}
	if e = normalMemoryObserveConfirmedReferenceLocked(s, *in.Memory); e != nil {
		return fail(e)
	}
	var original normalMemoryOriginIntent
	if _, exists, err := initialPrebootReadRecord(s, normalMemoryGuestIntentFile, &original); err != nil || !exists {
		return fail(errors.Join(fmt.Errorf("original memory intent absent"), err))
	}
	if _, e = normalMemoryBrokerCurrentLocked(ctx, g, s, original, filesystem.kernelConsumer); e != nil {
		return fail(e)
	}
	// 显式当前Root消费同步；观察器不调用本工厂，也不重新要求原R1仍存活/PGEmpty。
	before := *in.Memory
	initial, e := normalInitialWALFiles(before.InitialWALFamily)
	if e != nil {
		return fail(e)
	}
	names := append([]string(nil), initial[:]...)
	for _, name := range normalMemoryConfirmedFiles() {
		names = append(names, name)
	}
	for _, name := range names {
		if e = ctx.Err(); e != nil {
			return fail(e)
		}
		if e = s.syncPhysicalFile(name); e != nil {
			return fail(e)
		}
	}
	if e = normalMemoryObserveConfirmedReferenceLocked(s, before); e != nil {
		return fail(e)
	}
	if _, e = normalMemoryBrokerCurrentLocked(ctx, g, s, original, filesystem.kernelConsumer); e != nil {
		return fail(e)
	}
	after, e := filesystem.referenceLocked(ctx, s)
	if e != nil || after != ref {
		return fail(errors.Join(fmt.Errorf("original filesystem changed during memory consumption"), e))
	}
	kernelSHA := ""
	if before.InitialWALFamily == normalInitialWALFamilyKernelPrepV2 {
		if filesystem.kernelConsumer == nil {
			return fail(fmt.Errorf("memory2 requires original independent Kernel2 R2 consumer"))
		}
		ref, e := filesystem.kernelConsumer.referenceLocked(ctx, s)
		if e != nil {
			return fail(e)
		}
		kernelSHA, e = ref.referenceSHA256()
		if e != nil {
			return fail(e)
		}
	}
	out := &normalMemoryBrokerConsumerSource{g: g, filesystem: filesystem, proof: proof, intent: original, reference: before, kernel: filesystem.kernelConsumer}
	out.witness = normalMemoryBrokerConsumerWitness{Guard: g, Consumer: current, Filesystem: ref, Memory: before, KernelSHA: kernelSHA}
	normalMemoryBrokerConsumerWitnesses.Store(out, out.witness)
	return out, nil
}

func (o *normalMemoryBrokerConsumerSource) recheckLocked(ctx context.Context, s *lockedState, g *Guard) error {
	if o == nil || g == nil || o.g != g || o.filesystem == nil || o.proof == nil {
		return ErrUnknown
	}
	witness, ok := normalMemoryBrokerConsumerWitnesses.Load(o)
	if !ok || witness != o.witness {
		return ErrUnknown
	}
	current, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || current != o.witness.Consumer {
		return errors.Join(ErrUnknown, e)
	}
	// 同resource锁→FS/proof/task锁，绝不回调broker.mu或execMu。
	ref, e := o.filesystem.referenceLocked(ctx, s)
	if e != nil || ref != o.witness.Filesystem {
		return errors.Join(ErrUnknown, e, fmt.Errorf("original broker memory filesystem changed"))
	}
	task, e := o.proof.originalTaskForFilesystemLocked(ctx, s, o.filesystem.system)
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	defer task.close()
	if e = normalMemoryObserveConfirmedReferenceLocked(s, o.reference); e != nil {
		return e
	}
	if o.reference.InitialWALFamily == normalInitialWALFamilyKernelPrepV2 {
		if o.kernel == nil {
			return ErrUnknown
		}
		ref, e := o.kernel.referenceLocked(ctx, s)
		if e != nil {
			return e
		}
		h, e := ref.referenceSHA256()
		if e != nil || h != o.witness.KernelSHA {
			return errors.Join(ErrUnknown, e)
		}
	}
	_, e = normalMemoryBrokerCurrentLocked(ctx, g, s, o.intent, o.kernel)
	return e
}

func (o *normalMemoryBrokerConsumerSource) requireContinuousQualification() error {
	return errors.Join(ErrUnknown, fmt.Errorf("memory broker handoff alone cannot qualify continuous writers, PG or normal decrease"))
}
