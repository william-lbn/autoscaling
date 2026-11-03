//go:build linux && amd64

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"reflect"
)

// InspectGuestQualificationArtifact只消费ServeGuest已准备的原来源；不prepare、不fsync、
// 不写cap/WAL、不生成出生或资格。没有原producer或发生漂移只返回Unknown。
func (g *Guard) InspectGuestQualificationArtifact(ctx context.Context) (GuestQualificationArtifact, error) {
	var out GuestQualificationArtifact
	if g == nil || ctx == nil || ctx.Err() != nil || g.config.ExecutionModel != TrustedControllerExecutionModel {
		return out, ErrUnknown
	}
	value, ok := normalQuotaBudgetSources.Load(g)
	if !ok {
		return out, fmt.Errorf("%w: original normal budget source not prepared", ErrUnknown)
	}
	source, ok := value.(*normalQuotaBudgetSource)
	if !ok || source.guard != g {
		return out, ErrUnknown
	}
	inputs, e := g.normalSplitRuntimeInputs(ctx)
	if e != nil {
		return out, e
	}
	e = g.withRecord(ctx, func(r *record, s *lockedState) error {
		if r.Bootstrap == nil || !r.Bootstrap.Applied || r.Birth == nil {
			return ErrUnknown
		}
		facts, terms, cpu, e := source.freshLocked(ctx, s, r)
		if e != nil {
			return e
		}
		topology, e := g.normalSplitLocked(ctx, inputs, normalSplitObserve, s)
		if e != nil {
			return e
		}
		before := topology.Current
		if before.Private == nil || before.Arrays == nil {
			return ErrUnknown
		}
		limitsNow := r.Bootstrap.Original.Limits
		if r.Held != nil {
			limitsNow = r.Held.limits()
		}
		actual, e := g.kernel.ReadBounds(r.Identity, limitsNow)
		if e != nil {
			return e
		}
		view := r.view(actual)
		limits, e := view.ConfirmedLimits()
		if e != nil {
			return e
		}
		leaves, e := normalInflightSum(uint64(before.Private.MemoryMaxBytes), uint64(before.Arrays.MemoryMaxBytes))
		if e != nil {
			return e
		}
		plan, e := normalInflightPlanCPUFixed(facts, normalInflightPlanningInput{source.origin.MaximumBytes, leaves, uint64(limits.SystemMemoryMaxBytes)}, facts.Build, cpu)
		if e != nil {
			return e
		}
		outside, e := qualificationOutside(uint64(limits.SystemMemoryMaxBytes), plan, terms)
		if e != nil {
			return e
		}
		stateSHA, e := qualificationStateSHA(view)
		if e != nil {
			return e
		}
		boot, e := json.Marshal(view.Bootstrap)
		if e != nil {
			return e
		}
		factRaw, e := json.Marshal(facts)
		if e != nil {
			return e
		}
		termRaw, e := json.Marshal(terms)
		if e != nil {
			return e
		}
		origin := source.origin.Host.Prepared.Challenge
		out = GuestQualificationArtifact{ProtocolVersion: 1, Scope: GuestQualificationArtifactScope, VMShrinkPermit: false, Identity: r.Identity, Incarnation: origin.Identity.Incarnation, ConfigSecretUID: origin.ConfigSecretUID, ResourceSourceSHA256: origin.ResourceSHA256, RuntimeDocumentSHA256: origin.RuntimeSHA256, OriginalSourceSHA256: source.sourceSHA256, RootStateSHA256: stateSHA, RootBootstrapSHA256: physicalDigest(boot), MinimumVMBytes: origin.BusinessMinimum, MaximumVMBytes: origin.Maximum, BlockBytes: origin.Identity.BlockBytes, ObservedGuestMemTotalBytes: actual.GuestMemTotalBytes, MaximumVCPUs: uint32(cpu), PrivateMemoryMaxBytes: before.Private.MemoryMaxBytes, ArraysMemoryMaxBytes: before.Arrays.MemoryMaxBytes, CurrentLimits: limits, OriginalPhysical: source.origin.Physical, CurrentFacts: factRaw, InflightTerms: termRaw, MinimumNormalBytes: control.Uint64(plan.ConditionalNormalTargetBytes), Outside: outside}
		if e = out.Validate(); e != nil {
			return e
		}
		// 输出前再次核原kernel/硬域/WAL；facts只含稳定硬界/拓扑/配置，不含RSS/free/current。
		// MOVABLE拓扑或CPU/terms并发漂移有意Unknown；不丢弃第二次CPU/分项返回值。
		again, againTerms, againCPU, e := source.freshLocked(ctx, s, r)
		if e != nil {
			return e
		}
		b, e := json.Marshal(again)
		if e != nil || string(b) != string(factRaw) || !reflect.DeepEqual(againTerms, terms) || againCPU != cpu {
			return errors.Join(ErrUnknown, fmt.Errorf("qualification source changed across read"))
		}
		return nil
	})
	if e != nil {
		return GuestQualificationArtifact{}, e
	}
	return out, nil
}
