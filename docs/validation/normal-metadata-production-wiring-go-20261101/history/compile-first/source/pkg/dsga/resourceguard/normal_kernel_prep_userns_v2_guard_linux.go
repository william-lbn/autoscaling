//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type normalKernelPrepV2OriginalGuardSource struct {
	g              *Guard
	source         *normalKernelPrepV2Source
	preparationSHA string
}

var normalKernelPrepV2OriginalGuardSources sync.Map

// 只由固定initial-boot成功分支安装；原Guard/原durable opaque缺一不可。
func (g *Guard) installNormalKernelPrepV2OriginalLocked(ctx context.Context, state *lockedState, e NormalV2KernelBootstrapEvidence) error {
	if g == nil || state == nil || ctx == nil || ctx.Err() != nil {
		return ErrUnknown
	}
	original, x := e.originalUserNamespaceBarrierSource()
	if x != nil {
		return x
	}
	if original.policy.source.Config != g.config {
		return ErrUnknown
	}
	k, ok := original.backend.(*normalKernelPrepV2LinuxBackend)
	if !ok || k.linuxKernel != g.kernel {
		return errors.Join(ErrUnknown, fmt.Errorf("production kernel2 needs original same-Guard Linux backend"))
	}
	h, x := original.referenceLocked(ctx, state)
	if x != nil {
		return x
	}
	want, x := e.ReferenceSHA256()
	if x != nil || h != want {
		return ErrUnknown
	}
	slot := &normalKernelPrepV2OriginalGuardSource{g: g, source: original, preparationSHA: h}
	existing, loaded := normalKernelPrepV2OriginalGuardSources.LoadOrStore(g, slot)
	if loaded {
		old, ok := existing.(*normalKernelPrepV2OriginalGuardSource)
		if !ok || old.g != g || old.preparationSHA != h {
			return errors.Join(ErrUnknown, fmt.Errorf("kernel2 original Guard slot cannot be replaced"))
		}
		_, x = old.preparationReferenceLocked(ctx, state)
		return x
	}
	return nil
}
func (g *Guard) normalKernelPrepV2OriginalLocked(ctx context.Context, state *lockedState) (*normalKernelPrepV2OriginalGuardSource, error) {
	if g == nil {
		return nil, ErrUnknown
	}
	value, ok := normalKernelPrepV2OriginalGuardSources.Load(g)
	if !ok {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("same original Guard kernel2 opaque absent"))
	}
	original, ok := value.(*normalKernelPrepV2OriginalGuardSource)
	if !ok || original.g != g {
		return nil, ErrUnknown
	}
	if _, e := original.preparationReferenceLocked(ctx, state); e != nil {
		return nil, e
	}
	return original, nil
}
func (o *normalKernelPrepV2OriginalGuardSource) preparationReferenceLocked(ctx context.Context, state *lockedState) (string, error) {
	if o == nil || o.g == nil || o.source == nil {
		return "", ErrUnknown
	}
	v, ok := normalKernelPrepV2OriginalGuardSources.Load(o.g)
	if !ok || v != o {
		return "", ErrUnknown
	}
	h, e := o.source.referenceLocked(ctx, state)
	if e != nil || h != o.preparationSHA {
		return "", errors.Join(ErrUnknown, e)
	}
	return h, nil
}
func (g *Guard) discardNormalKernelPrepV2OriginalSource() {
	if g == nil {
		return
	}
	value, ok := normalKernelPrepV2OriginalGuardSources.LoadAndDelete(g)
	if ok {
		if o, ok := value.(*normalKernelPrepV2OriginalGuardSource); ok && o.source != nil {
			o.source.close()
		}
	}
}

type normalKernelPreparationReferenceReader interface {
	preparationReferenceLocked(context.Context, *lockedState) (string, error)
}

// 原初始三阶段用的只读reference。新family只消费原Guard指针，不重新open sysctl。
func (g *Guard) initialKernelPreparationReferenceLocked(ctx context.Context, state *lockedState, plan RuntimeBootRequirements) (string, error) {
	family, e := normalInitialWALFamilyForRequirements(plan)
	if e != nil {
		return "", e
	}
	if family == normalInitialWALFamilyKernelPrepV2 {
		o, e := g.normalKernelPrepV2OriginalLocked(ctx, state)
		if e != nil {
			return "", e
		}
		return o.preparationReferenceLocked(ctx, state)
	}
	in, e := g.normalKernelBootstrapInputs()
	if e != nil {
		return "", e
	}
	observed, e := g.normalKernelPreparationLocked(ctx, in, kernelPrepObserve, state)
	if e != nil {
		return "", e
	}
	return observed.ReferenceSHA256()
}

// 原kernel完成后aggregate已有后继WAL时，使用同Guard原opaque，不重入组件的首次边界。
func (g *Guard) initialKernelPreparationV2ForBoot(ctx context.Context, op initialPrebootGuestOperation) (NormalV2KernelBootstrapEvidence, error) {
	var zero NormalV2KernelBootstrapEvidence
	state, e := lockState(ctx, g.config.StateDir)
	if e != nil {
		return zero, e
	}
	if _, yes := normalKernelPrepV2OriginalGuardSources.Load(g); yes {
		original, e := g.normalKernelPrepV2OriginalLocked(ctx, state)
		state.close()
		if e != nil {
			return zero, e
		}
		evidence := original.source.evidence
		evidence.source = original.source
		evidence.durabilityConfirmed = true
		return evidence, nil
	}
	_, has, e := state.readPhysicalFile(normalKernelPrepV2IntentName)
	state.close()
	if e != nil {
		return zero, e
	}
	var evidence NormalV2KernelBootstrapEvidence
	if op == initialPrebootGuestRecover && has {
		evidence, e = g.ReplayKernelBeforeOriginV2(ctx)
	} else {
		evidence, e = g.PrepareKernelBeforeOriginV2(ctx)
	}
	if e != nil {
		return zero, e
	}
	if !evidence.DurabilityConfirmed() {
		return zero, ErrUnknown
	}
	state, e = lockState(ctx, g.config.StateDir)
	if e != nil {
		return zero, e
	}
	defer state.close()
	if e = g.installNormalKernelPrepV2OriginalLocked(ctx, state, evidence); e != nil {
		return zero, e
	}
	return evidence, nil
}
