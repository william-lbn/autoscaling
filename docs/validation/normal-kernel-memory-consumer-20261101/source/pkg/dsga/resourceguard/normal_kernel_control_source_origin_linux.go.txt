//go:build linux && amd64

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// opaque拼接不接受自报标志，不重放任何KernelPrep/Origin/PreSys写。
// caller必须已持原resource flock。旧Root不能凭可见WAL收养当前Root的角色来源。
type normalKernelControlSource struct {
	guard                                *Guard
	memory                               *normalMemoryOriginSource
	brokerMemory                         *normalMemoryBrokerConsumerSource
	system                               *normalSystemLifecycleSource
	consumer                             nativeBirthConsumer
	runtimeSHA, systemSHA, kernelPrepSHA string
	roles                                map[string]*normalKernelRoleControlSource
}

func captureNormalKernelControlSourceLocked(ctx context.Context, state *lockedState, g *Guard, memory *normalMemoryOriginSource, system *normalSystemLifecycleSource, roles []*normalKernelRoleControlSource) (*normalKernelControlSource, error) {
	return captureNormalKernelControlSourceWithMemoryLocked(ctx, state, g, memory, nil, system, roles)
}

// 后继R2仅从同Guard原typedFS取其独立consumer；不把JSON、当前Root或R1
// source.currentRoot覆写成R2身份。旧R1工厂与新R2工厂共用所有剩余核验。
func captureNormalKernelControlSourceForBrokerLocked(ctx context.Context, state *lockedState, g *Guard, filesystem *normalFilesystemOriginSource, system *normalSystemLifecycleSource, roles []*normalKernelRoleControlSource) (*normalKernelControlSource, error) {
	if filesystem == nil || g == nil || system == nil {
		return nil, ErrUnknown
	}
	filesystem.mu.Lock()
	memory := filesystem.memoryConsumer
	same := !filesystem.closed && filesystem.g == g && filesystem.system == system && memory != nil && memory.filesystem == filesystem
	filesystem.mu.Unlock()
	if !same {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original broker kernel controls require same-Guard typed memory/FS source"))
	}
	return captureNormalKernelControlSourceWithMemoryLocked(ctx, state, g, nil, memory, system, roles)
}

func captureNormalKernelControlSourceWithMemoryLocked(ctx context.Context, state *lockedState, g *Guard, memory *normalMemoryOriginSource, brokerMemory *normalMemoryBrokerConsumerSource, system *normalSystemLifecycleSource, roles []*normalKernelRoleControlSource) (*normalKernelControlSource, error) {
	if ctx == nil || ctx.Err() != nil || state == nil || g == nil || system == nil {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original kernel control opaque inputs absent"))
	}
	memoryIntent, memoryConsumer, e := normalKernelControlMemoryBindingLocked(ctx, state, g, memory, brokerMemory)
	if e != nil {
		return nil, e
	}
	observed, e := system.snapshot()
	if e != nil {
		return nil, e
	}
	consumer, e := nativeBirthCurrentConsumerIdentity()
	if e != nil {
		return nil, e
	}
	if consumer != observed.Binding.RootConsumer || consumer != memoryConsumer || observed.Binding.GuestBootID != memoryIntent.Physical.Identity.GuestBootID || observed.Binding.VMUID != g.config.VMUID || observed.Binding.ComputeInstanceID != g.config.ComputeInstanceID || observed.Binding.RuntimeDocumentSHA256 != memoryIntent.Host.Prepared.Challenge.RuntimeSHA256 {
		return nil, errors.Join(ErrUnknown, fmt.Errorf("original Root/runtime/Guest control sources differ"))
	}
	if e = nativeFilterAuditCaller(); e != nil {
		return nil, e
	} // auditor必须原样unfiltered
	systemSHA, e := system.referenceSHA256()
	if e != nil {
		return nil, e
	}
	src := &normalKernelControlSource{guard: g, memory: memory, brokerMemory: brokerMemory, system: system, consumer: consumer, runtimeSHA: observed.Binding.RuntimeDocumentSHA256, systemSHA: systemSHA, roles: map[string]*normalKernelRoleControlSource{}}
	expected := normalKernelControlExpectedManifest()
	if len(roles) != len(expected.Roles) {
		return nil, fmt.Errorf("all nine original ordinary control sources required")
	}
	for _, role := range roles {
		if role == nil || role.plan == nil || role.task == nil || role.plan.source != system || !role.observation.OriginalExecStopped || !role.observation.LandlockFromFixedGateSequence || role.observation.NormalDecreaseAuthorized {
			return nil, fmt.Errorf("ordinary original enforced source absent")
		}
		name := role.plan.rule.Role
		if src.roles[name] != nil {
			return nil, fmt.Errorf("duplicate original ordinary role source")
		}
		wanted, e := expected.role(name)
		if e != nil {
			return nil, e
		}
		compiledRaw, _ := json.Marshal(normalKernelControlFilter())
		if role.observation.BPFProgramSHA256 != physicalDigest(compiledRaw) {
			return nil, fmt.Errorf("ordinary compiled full-filter source differs")
		}
		if role.observation.Binding != observed.Binding || role.observation.SourceSHA256 != systemSHA || role.observation.Capabilities != wanted.KeepCapabilities || role.observation.NoNewPrivileges != 1 {
			return nil, fmt.Errorf("ordinary role control source differs")
		}
		if e = role.plan.recheck(ctx); e != nil {
			return nil, e
		}
		src.roles[name] = role
	}
	// 与原8WAL绑定的准确policy/evidence，不用当前标量替代出生前准备。
	in, e := g.normalKernelBootstrapInputs()
	if e != nil {
		return nil, e
	}
	prep, e := g.normalKernelPreparationLocked(ctx, in, kernelPrepObserve, state)
	if e != nil {
		return nil, e
	}
	src.kernelPrepSHA, e = prep.ReferenceSHA256()
	if e != nil {
		return nil, e
	}
	if src.kernelPrepSHA != memoryIntent.Host.Prepared.KernelPreparationSHA256 {
		return nil, fmt.Errorf("original kernel preparation reference differs")
	}
	if _, _, e = normalKernelControlMemoryBindingLocked(ctx, state, g, memory, brokerMemory); e != nil {
		return nil, e
	}
	return src, nil
}
func (s *normalKernelControlSource) recheckLocked(ctx context.Context, state *lockedState, g *Guard) error {
	if s == nil || s.guard != g || s.system == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original kernel control source absent"))
	}
	consumer, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || consumer != s.consumer {
		return errors.Join(ErrUnknown, fmt.Errorf("original kernel control Root changed"))
	}
	sha, e := s.system.referenceSHA256()
	if e != nil || sha != s.systemSHA {
		return errors.Join(ErrUnknown, fmt.Errorf("original kernel control System source changed"))
	}
	if _, _, e = normalKernelControlMemoryBindingLocked(ctx, state, g, s.memory, s.brokerMemory); e != nil {
		return e
	}
	for _, role := range s.roles {
		if e = role.plan.recheck(ctx); e != nil {
			return e
		}
	}
	in, e := g.normalKernelBootstrapInputs()
	if e != nil {
		return e
	}
	prep, e := g.normalKernelPreparationLocked(ctx, in, kernelPrepObserve, state)
	if e != nil {
		return e
	}
	sha, e = prep.ReferenceSHA256()
	if e != nil || sha != s.kernelPrepSHA {
		return errors.Join(ErrUnknown, fmt.Errorf("original kernel control preparation changed"))
	}
	return nil
}
func (s *normalKernelControlSource) requireContinuousQualification() error {
	// 普通9的真实不可逆约束不等于尚未接通的Root/PID1/Compute/全部respawn来源。
	// 后继需原opaque生产者补齐；没有修改Inflight MissingSources或normal拒绝。
	return errors.Join(ErrUnknown, fmt.Errorf("kernel control source lacks original Root/PID1/Compute/all-respawn producer qualification"))
}

// 精确封闭的两种私有来源，不接受可自行实现的泛化接口/自报当前Root。
// 两者都缺或同时提供都拒；R2必须真实消费原确认历史并维持原exec/FS见证。
func normalKernelControlMemoryBindingLocked(ctx context.Context, state *lockedState, g *Guard, original *normalMemoryOriginSource, broker *normalMemoryBrokerConsumerSource) (normalMemoryOriginIntent, nativeBirthConsumer, error) {
	var empty normalMemoryOriginIntent
	var root nativeBirthConsumer
	if ctx == nil || ctx.Err() != nil || state == nil || g == nil || (original == nil) == (broker == nil) {
		return empty, root, errors.Join(ErrUnknown, fmt.Errorf("exactly one original typed memory authority required"))
	}
	if original != nil {
		if e := original.recheckLocked(ctx, state, g); e != nil {
			return empty, root, e
		}
		return original.intent, original.currentRoot, nil
	}
	if e := broker.recheckLocked(ctx, state, g); e != nil {
		return empty, root, e
	}
	return broker.intent, broker.witness.Consumer, nil
}
