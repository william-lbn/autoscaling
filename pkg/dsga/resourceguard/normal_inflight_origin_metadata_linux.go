//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

const normalInflightMetadataOriginScope = "original-memory2-metadata-inflight-source-only-v1"

// 只投影真实原memory2/Kernel2来源和当前条件物理分项；不是normal/Host容量许可。
// Mmax来自原六QMP/Guest region，不取请求或环境；CPU预算仍是编译C64。
type normalInflightMetadataOriginReference struct {
	Scope                    string                               `json:"scope"`
	Identity                 Identity                             `json:"identity"`
	Memory                   normalMemoryOriginConfirmedReference `json:"memory"`
	HostReferenceSHA256      string                               `json:"host_reference_sha256"`
	KernelReferenceSHA256    string                               `json:"kernel_reference_sha256"`
	Build                    PhysicalKernelBuildReference         `json:"build"`
	MaximumBytes             uint64                               `json:"maximum_bytes"`
	BaseBytes                uint64                               `json:"base_bytes"`
	BackendBytes             uint64                               `json:"backend_bytes"`
	DeviceAddress            uint64                               `json:"device_address"`
	ObservedHostMaximumVCPUs uint32                               `json:"observed_host_maximum_vcpus"`
	CompiledCPUCeiling       uint64                               `json:"compiled_cpu_ceiling"`
}

// 字段不导出且工厂登记同一指针见证；JSON/同配置新Guard不能重建能力。
// 不另持FD；每次fresh读都消费原R2对象自己的FS/proof/kernel/11WAL存续检查。
type normalInflightMetadataOriginSource struct {
	guard    *Guard
	memory   *normalMemoryBrokerConsumerSource
	original normalInflightMetadataOriginReference
}
type normalInflightMetadataOriginWitness struct {
	guard     *Guard
	memory    *normalMemoryBrokerConsumerSource
	reference normalInflightMetadataOriginReference
}

var normalInflightMetadataOriginWitnesses sync.Map

func normalInflightMetadataBuild() (PhysicalKernelBuildReference, error) {
	ref, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	if e != nil || !ref.StrictPageCounterIRQBoundary() || !ref.ControlMetadataGuard() {
		return PhysicalKernelBuildReference{}, errors.Join(ErrUnknown, e)
	}
	return ref, nil
}

// 调用者已持原resource flock，顺序沿用memory.recheckLocked→FS/proof/task；
// 不跨broker/exec mutex、不回调SQL，也不修改原8WAL/准备/控制。
func (g *Guard) captureNormalInflightMetadataOriginLocked(ctx context.Context, state *lockedState, memory *normalMemoryBrokerConsumerSource) (*normalInflightMetadataOriginSource, normalInflightSourceFacts, normalInflightSourceTerms, error) {
	var facts normalInflightSourceFacts
	var terms normalInflightSourceTerms
	fail := func(e error) (*normalInflightMetadataOriginSource, normalInflightSourceFacts, normalInflightSourceTerms, error) {
		return nil, normalInflightSourceFacts{}, normalInflightSourceTerms{}, errors.Join(ErrUnknown, e)
	}
	if g == nil || ctx == nil || ctx.Err() != nil || state == nil || state.dir == nil || state.lock == nil || memory == nil || memory.g != g {
		return fail(fmt.Errorf("original R2 memory2 inflight context absent"))
	}
	ref, e := normalInflightMetadataOriginalReference(g, memory)
	if e != nil {
		return fail(e)
	}
	out := &normalInflightMetadataOriginSource{guard: g, memory: memory, original: ref}
	facts, terms, e = out.freshLocked(ctx, state)
	if e != nil {
		return fail(e)
	}
	normalInflightMetadataOriginWitnesses.Store(out, normalInflightMetadataOriginWitness{g, memory, ref})
	return out, facts, terms, nil
}

func normalInflightMetadataOriginalReference(g *Guard, memory *normalMemoryBrokerConsumerSource) (normalInflightMetadataOriginReference, error) {
	var zero normalInflightMetadataOriginReference
	if g == nil || memory == nil || memory.g != g || memory.kernel == nil || memory.reference.InitialWALFamily != normalInitialWALFamilyKernelPrepV2 || memory.intent.Host.ProfileID != NormalSplitMetadataV1PhysicalKernelProfileID {
		return zero, ErrUnknown
	}
	ref, e := normalInflightMetadataBuild()
	if e != nil {
		return zero, e
	}
	host := memory.intent.Host
	geometry, e := bootprotocol.DecodeNormalMemoryGeometry(host.Prepared.Challenge.Identity, host.Prepared.Challenge.OriginalBase, host.QMP)
	if e != nil {
		return zero, e
	}
	maximum, e := compareGuestMaximumGeometry(memory.intent.Region, uint64(geometry.BaseBytes), uint64(geometry.MaximumBytes), uint64(geometry.DeviceAddress), uint64(geometry.BackendBytes))
	if e != nil || maximum != memory.intent.MaximumBytes || memory.intent.Region.Build != ref || memory.intent.Physical.Identity.GuestBootID != memory.reference.GuestBootID {
		return zero, errors.Join(ErrUnknown, e)
	}
	hostSHA, e := host.SHA256()
	if e != nil {
		return zero, e
	}
	if _, e := memory.reference.referenceSHA256(); e != nil {
		return zero, e
	}
	return normalInflightMetadataOriginReference{Scope: normalInflightMetadataOriginScope, Identity: memory.intent.Physical.Identity, Memory: memory.reference, HostReferenceSHA256: hostSHA, KernelReferenceSHA256: memory.witness.KernelSHA, Build: ref, MaximumBytes: maximum, BaseBytes: uint64(geometry.BaseBytes), BackendBytes: uint64(geometry.BackendBytes), DeviceAddress: uint64(geometry.DeviceAddress), ObservedHostMaximumVCPUs: host.MaximumVCPUs, CompiledCPUCeiling: normalInflightCompiledCPUCeiling}, nil
}

func (o *normalInflightMetadataOriginSource) freshLocked(ctx context.Context, state *lockedState) (normalInflightSourceFacts, normalInflightSourceTerms, error) {
	fail := func(e error) (normalInflightSourceFacts, normalInflightSourceTerms, error) {
		return normalInflightSourceFacts{}, normalInflightSourceTerms{}, errors.Join(ErrUnknown, e)
	}
	if o == nil || ctx == nil || ctx.Err() != nil || o.guard == nil || o.memory == nil || state == nil || state.dir == nil || state.lock == nil {
		return fail(ErrUnknown)
	}
	if e := o.memory.recheckLocked(ctx, state, o.guard); e != nil {
		return fail(e)
	}
	before, e := normalInflightMetadataOriginalReference(o.guard, o.memory)
	if e != nil || before != o.original {
		return fail(errors.Join(e, fmt.Errorf("original metadata inflight reference changed")))
	}
	first, e := readNormalInflightSourceFixed(o.original.Identity, o.original.Build)
	if e != nil {
		return fail(e)
	}
	second, e := readNormalInflightSourceFixed(o.original.Identity, o.original.Build)
	if e != nil {
		return fail(e)
	}
	if e := o.memory.intent.Physical.Match(first.Physical); e != nil {
		return fail(e)
	}
	if !reflect.DeepEqual(first, second) {
		return fail(fmt.Errorf("metadata inflight observations changed across double read"))
	}
	terms, e := normalInflightCalculateFixed(second, o.original.Build)
	if e != nil {
		return fail(e)
	}
	if e := o.memory.recheckLocked(ctx, state, o.guard); e != nil {
		return fail(e)
	}
	after, e := normalInflightMetadataOriginalReference(o.guard, o.memory)
	if e != nil || after != before || ctx.Err() != nil {
		return fail(errors.Join(e, fmt.Errorf("metadata inflight source changed during read")))
	}
	return second, terms, nil
}

func (o *normalInflightMetadataOriginSource) referenceLocked(ctx context.Context, state *lockedState, g *Guard) (normalInflightMetadataOriginReference, normalInflightSourceFacts, normalInflightSourceTerms, error) {
	var zero normalInflightMetadataOriginReference
	if o == nil || g == nil || o.guard != g {
		return zero, normalInflightSourceFacts{}, normalInflightSourceTerms{}, ErrUnknown
	}
	witness, ok := normalInflightMetadataOriginWitnesses.Load(o)
	if !ok || witness != (normalInflightMetadataOriginWitness{o.guard, o.memory, o.original}) {
		return zero, normalInflightSourceFacts{}, normalInflightSourceTerms{}, ErrUnknown
	}
	facts, terms, e := o.freshLocked(ctx, state)
	if e != nil {
		return zero, normalInflightSourceFacts{}, normalInflightSourceTerms{}, e
	}
	final, ok := normalInflightMetadataOriginWitnesses.Load(o)
	if !ok || final != witness {
		return zero, normalInflightSourceFacts{}, normalInflightSourceTerms{}, ErrUnknown
	}
	return o.original, facts, terms, nil
}

// 只撤本对象资格；没有FD/预算可释放，不修改原memory/FS/Kernel来源。
func (o *normalInflightMetadataOriginSource) close() {
	if o != nil {
		normalInflightMetadataOriginWitnesses.Delete(o)
	}
}

// 本来源不证明未来所有writer/force/FS封闭，更不证明Host可兑现Mmax驻留。
// 有预算计算需求时Mmax仍从原对象取，调用者不能传另一个candidate来升级资格。
func (o *normalInflightMetadataOriginSource) planLocked(ctx context.Context, state *lockedState, g *Guard, normalLeavesBytes, systemBytes uint64) (normalInflightPlanningResult, error) {
	ref, facts, _, e := o.referenceLocked(ctx, state, g)
	if e != nil {
		return normalInflightPlanningResult{}, e
	}
	return normalInflightPlanFixed(facts, normalInflightPlanningInput{MaximumBytes: ref.MaximumBytes, NormalLeavesBytes: normalLeavesBytes, SystemBytes: systemBytes}, ref.Build)
}
