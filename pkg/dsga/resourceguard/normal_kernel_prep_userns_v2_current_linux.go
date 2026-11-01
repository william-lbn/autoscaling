//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

func readGuestMemoryMaximumForRequirements(boot string, plan RuntimeBootRequirements) (guestMemoryMaximumObservation, error) {
	family, e := normalInitialWALFamilyForRequirements(plan)
	if e != nil {
		return guestMemoryMaximumObservation{}, e
	}
	if family == normalInitialWALFamilyKernelPrepV2 {
		return readGuestMemoryMaximumMetadataComponent(boot)
	}
	return readGuestMemoryMaximumComponent(boot)
}

func normalMemoryBrokerCurrentLocked(ctx context.Context, g *Guard, state *lockedState, in normalMemoryOriginIntent, kernel *normalKernelPrepV2BrokerConsumerSource) (guestMemoryMaximumObservation, error) {
	if in.Host.ProfileID == NormalSplitMetadataV1PhysicalKernelProfileID {
		if kernel == nil {
			return guestMemoryMaximumObservation{}, errors.Join(ErrUnknown, fmt.Errorf("metadata memory R2 cannot adopt JSON/current0 as Kernel2"))
		}
		return g.normalMemoryOriginCurrentMetadataLocked(ctx, state, in, kernel)
	}
	return g.normalMemoryOriginCurrentLocked(ctx, state, in)
}

// 只读共同几何；caller显式给原R1 Guard槽或独立R2 consumer。该层不回FS。
func (g *Guard) normalMemoryOriginCurrentMetadataLocked(ctx context.Context, state *lockedState, v normalMemoryOriginIntent, kernel normalKernelPreparationReferenceReader) (guestMemoryMaximumObservation, error) {
	var zero guestMemoryMaximumObservation
	fail := func(e error) (guestMemoryMaximumObservation, error) { return zero, errors.Join(ErrUnknown, e) }
	if g == nil || state == nil || state.dir == nil || state.lock == nil || ctx == nil || ctx.Err() != nil || kernel == nil || v.OriginalRoot.PID <= 1 || v.OriginalRoot.StartTicks == 0 || !physicalDigestValid(v.OriginalRoot.InstanceNonce) || v.ProtocolVersion != 1 || v.Scope != normalMemoryGuestScope || v.Host.Validate() != nil || v.Physical.Validate() != nil || v.Region.validate() != nil {
		return fail(fmt.Errorf("metadata memory original source malformed"))
	}
	in, e := g.initialPrebootInputs(v.Host.Prepared.Challenge)
	if e != nil {
		return fail(e)
	}
	family, e := normalInitialWALFamilyForRequirements(in.requirements)
	if e != nil || family != normalInitialWALFamilyKernelPrepV2 || v.Host.ProfileID != NormalSplitMetadataV1PhysicalKernelProfileID || in.requirements.MemoryOriginProtocolVersion() != 2 || v.Region.ProtocolVersion != 2 {
		return fail(errors.Join(e, fmt.Errorf("metadata memory explicit plan2/memory2/region2 required")))
	}
	before, e := initialPrebootGuestWitnessFamilyLocked(state, v.Host.Prepared.Challenge, family)
	if e != nil || before != v.InitialWAL {
		return fail(errors.Join(e, fmt.Errorf("metadata memory original8 source changed")))
	}
	var aggregate initialPrebootGuestIntent
	var done initialPrebootGuestComplete
	if _, yes, e := initialPrebootReadRecord(state, initialPrebootGuestIntentName, &aggregate); e != nil || !yes {
		return fail(e)
	}
	if _, yes, e := initialPrebootReadRecord(state, initialPrebootGuestCompleteName, &done); e != nil || !yes {
		return fail(e)
	}
	prepared, e := initialPrebootOriginalReceipt(aggregate, done)
	if e != nil || prepared != v.Host.Prepared {
		return fail(errors.Join(e, fmt.Errorf("metadata memory original prepared receipt differs")))
	}
	sha, e := kernel.preparationReferenceLocked(ctx, state)
	if e != nil || sha != prepared.KernelPreparationSHA256 {
		return fail(errors.Join(e, fmt.Errorf("metadata memory original kernel2 reference changed")))
	}
	physical, ok := g.kernel.(physicalOriginKernel)
	if !ok {
		return fail(fmt.Errorf("metadata physical reader absent"))
	}
	now, e := physical.physicalOriginRead()
	if e != nil {
		return fail(e)
	}
	if e = v.Physical.Match(now); e != nil {
		return fail(e)
	}
	if now.Identity.GuestBootID != prepared.GuestBootID {
		return fail(fmt.Errorf("metadata memory boot drifted"))
	}
	one, e := readGuestMemoryMaximumMetadataComponent(now.Identity.GuestBootID)
	if e != nil {
		return fail(e)
	}
	if e = v.Region.match(one); e != nil {
		return fail(e)
	}
	two, e := readGuestMemoryMaximumMetadataComponent(now.Identity.GuestBootID)
	if e != nil {
		return fail(e)
	}
	if e = one.match(two); e != nil {
		return fail(e)
	}
	geometry, e := bootprotocol.DecodeNormalMemoryGeometry(v.Host.Prepared.Challenge.Identity, v.Host.Prepared.Challenge.OriginalBase, v.Host.QMP)
	if e != nil {
		return fail(e)
	}
	maximum, e := compareGuestMaximumGeometry(one, uint64(geometry.BaseBytes), uint64(geometry.MaximumBytes), uint64(geometry.DeviceAddress), uint64(geometry.BackendBytes))
	if e != nil || maximum != v.MaximumBytes {
		return fail(errors.Join(e, fmt.Errorf("metadata original Host/Guest maximum differs")))
	}
	after, e := initialPrebootGuestWitnessFamilyLocked(state, v.Host.Prepared.Challenge, family)
	if e != nil || after != before {
		return fail(errors.Join(e, fmt.Errorf("metadata original8 drifted during observe")))
	}
	return one, nil
}
