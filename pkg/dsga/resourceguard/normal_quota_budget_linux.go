//go:build linux && amd64

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"golang.org/x/sys/unix"
)

// 受信节点的内存控制profile：唯一Root执行已审核的内存/数组操作，operator不得
// 并发改挂载、驱动、CPU槽、NUMA或allocator配置。它不是防恶意Root的整机证明。
// 所有普通任务必须实际进入固定P/A/System硬计费域；原启动页由固定nonMOVABLE K
// 覆盖。无swap/FUSE/huge/模块、只允许受审FS，才可消费18c的F<=K和Q1/I定理。
// 旧strict Source的Unknown与旧C64组件保持原样，没有将其改成“已闭合”。
type normalQuotaBudgetSource struct {
	guard        *Guard
	actor        nativeBirthConsumer
	origin       normalMemoryOriginIntent
	sourceSHA256 string
	files        map[string]normalSystemRoleFileWitness
}

var normalQuotaBudgetSources sync.Map

func normalQuotaReadCanonical(s *lockedState, name string, v any) ([]byte, error) {
	raw, has, e := s.readPhysicalFile(name)
	if e != nil || !has || len(raw) == 0 || len(raw) > 64<<10 {
		return nil, errors.Join(ErrUnknown, e, fmt.Errorf("normal quota original %s absent", name))
	}
	if e = control.UnmarshalObject(raw, v); e != nil {
		return nil, errors.Join(ErrUnknown, e)
	}
	canonical, e := json.Marshal(v)
	if e != nil || !bytes.Equal(raw, canonical) {
		return nil, errors.Join(ErrUnknown, e, fmt.Errorf("normal quota original %s noncanonical", name))
	}
	return raw, nil
}

// 此恢复只消费已完成的原memory2事实，并重新读真实kernel/区域/硬域；权限来自
// 原只读execution model和ServeGuest原server.lock。它不重放Host或收养旧strict opaque。
func (g *Guard) prepareNormalQuotaBudgetSource(ctx context.Context) error {
	if g == nil || ctx == nil || ctx.Err() != nil || g.config.ExecutionModel != TrustedControllerExecutionModel {
		return ErrUnknown
	}
	return g.withRecord(ctx, func(r *record, s *lockedState) error {
		if r.Bootstrap == nil || !r.Bootstrap.Applied {
			return ErrUnknown
		}
		actor, e := nativeBirthCurrentConsumerIdentity()
		if e != nil {
			return e
		}
		var in normalMemoryOriginIntent
		iraw, e := normalQuotaReadCanonical(s, normalMemoryGuestIntentFile, &in)
		if e != nil {
			return e
		}
		var done normalMemoryOriginCompletion
		craw, e := normalQuotaReadCanonical(s, normalMemoryGuestCompleteFile, &done)
		if e != nil {
			return e
		}
		var commit normalMemoryGuestCommitRecord
		_, e = normalQuotaReadCanonical(s, normalMemoryGuestCommitFile, &commit)
		if e != nil {
			return e
		}
		if in.ProtocolVersion != 1 || in.Scope != normalMemoryGuestScope || in.Host.Validate() != nil || in.Physical.Validate() != nil || in.Region.validate() != nil || in.Host.ProfileID != NormalSplitMetadataV1PhysicalKernelProfileID || in.Physical.Identity != r.Identity || in.OriginalRoot.PID <= 1 || !physicalDigestValid(in.OriginalRoot.InstanceNonce) {
			return ErrUnknown
		}
		if done.ProtocolVersion != 1 || done.Scope != normalMemoryGuestScope || done.IntentSHA256 != physicalDigest(iraw) || done.Ack.GuestSourceSHA256 != done.IntentSHA256 || done.Ack.MatchReference(in.Host) != nil || done.Ack.RootInstanceNonce != in.OriginalRoot.InstanceNonce {
			return ErrUnknown
		}
		if commit.ProtocolVersion != 2 || commit.Scope != normalMemoryGuestCommitScope || commit.IntentSHA256 != physicalDigest(iraw) || commit.CompletionSHA256 != physicalDigest(craw) || commit.OriginalRoot != in.OriginalRoot || commit.Commit.MatchReferenceAndAck(in.Host, done.Ack) != nil {
			return ErrUnknown
		}
		family, e := normalInitialWALFamilyForHost(in.Host)
		if e != nil || family != normalInitialWALFamilyKernelPrepV2 {
			return errors.Join(ErrUnknown, e)
		}
		inputs, e := g.initialPrebootInputs(in.Host.Prepared.Challenge)
		if e != nil {
			return e
		}
		if inputs.requirements.MemoryOriginProtocolVersion() != 2 || inputs.requirements.ProfileID() != NormalSplitMetadataV1PhysicalKernelProfileID {
			return ErrUnknown
		}
		prefix, e := initialPrebootGuestWitnessFamilyLocked(s, in.Host.Prepared.Challenge, family)
		if e != nil || prefix != in.InitialWAL {
			return errors.Join(ErrUnknown, e)
		}
		var initial initialPrebootGuestIntent
		var complete initialPrebootGuestComplete
		if _, e = normalQuotaReadCanonical(s, initialPrebootGuestIntentName, &initial); e != nil {
			return e
		}
		if _, e = normalQuotaReadCanonical(s, initialPrebootGuestCompleteName, &complete); e != nil {
			return e
		}
		receipt, e := initialPrebootOriginalReceipt(initial, complete)
		if e != nil || receipt != in.Host.Prepared {
			return errors.Join(ErrUnknown, e)
		}
		var ki normalKernelPrepV2Intent
		if _, e = normalQuotaReadCanonical(s, normalKernelPrepV2IntentName, &ki); e != nil {
			return e
		}
		decoded, e := decodeNormalKernelPrepV2(ki.Inputs, g.config)
		if e != nil {
			return e
		}
		var ke NormalV2KernelBootstrapEvidence
		_, e = normalQuotaReadCanonical(s, normalKernelPrepV2CompleteName, &ke)
		if e != nil {
			return e
		}
		kh, e := ke.ReferenceSHA256()
		if e != nil || kh != in.Host.Prepared.KernelPreparationSHA256 || ke.PolicySHA256 != physicalDigest(ki.Inputs.PolicyRaw) || ke.RuntimeDocumentSHA256 != physicalDigest(ki.Inputs.RuntimeRaw) || ke.PhysicalOriginPolicySHA256 != physicalDigest(ki.Inputs.PhysicalPolicyRaw) || !ke.After.target(decoded.policy) {
			return errors.Join(ErrUnknown, e)
		}
		names, e := normalInitialWALFiles(family)
		if e != nil {
			return e
		}
		all := append([]string(nil), names[:]...)
		all = append(all, normalMemoryGuestIntentFile, normalMemoryGuestCompleteFile, normalMemoryGuestCommitFile, normalKernelPrepV2IntentName, normalKernelPrepV2CompleteName)
		source := &normalQuotaBudgetSource{guard: g, actor: actor, origin: in, files: map[string]normalSystemRoleFileWitness{}}
		for _, name := range all {
			before, e := readNormalSystemRoleFileWitness(s, name)
			if e != nil {
				return e
			}
			if e = s.syncPhysicalFile(name); e != nil {
				return errors.Join(ErrUnknown, e)
			}
			after, e := readNormalSystemRoleFileWitness(s, name)
			if e != nil || after != before {
				return errors.Join(ErrUnknown, e)
			}
			source.files[name] = after
		}
		if e = s.dir.Sync(); e != nil {
			return errors.Join(ErrUnknown, e)
		}
		raw, e := json.Marshal(struct {
			Model  string
			Origin normalMemoryOriginIntent
			Files  map[string]normalSystemRoleFileWitness
		}{TrustedControllerExecutionModel, in, source.files})
		if e != nil {
			return e
		}
		source.sourceSHA256 = physicalDigest(raw)
		if _, _, _, e = source.freshLocked(ctx, s, r); e != nil {
			return e
		}
		existing, loaded := normalQuotaBudgetSources.LoadOrStore(g, source)
		if loaded {
			old, ok := existing.(*normalQuotaBudgetSource)
			if !ok || old.sourceSHA256 != source.sourceSHA256 || old.actor != actor {
				return ErrUnknown
			}
		}
		return nil
	})
}

func (o *normalQuotaBudgetSource) freshLocked(ctx context.Context, s *lockedState, r *record) (normalInflightSourceFacts, normalInflightSourceTerms, uint64, error) {
	fail := func(e error) (normalInflightSourceFacts, normalInflightSourceTerms, uint64, error) {
		return normalInflightSourceFacts{}, normalInflightSourceTerms{}, 0, errors.Join(ErrUnknown, e)
	}
	if o == nil || o.guard == nil || ctx == nil || ctx.Err() != nil || s == nil || r == nil || o.guard.config.ExecutionModel != TrustedControllerExecutionModel || r.Identity != o.origin.Physical.Identity {
		return fail(ErrUnknown)
	}
	actor, e := nativeBirthCurrentConsumerIdentity()
	if e != nil || actor != o.actor {
		return fail(e)
	}
	for name, want := range o.files {
		got, e := readNormalSystemRoleFileWitness(s, name)
		if e != nil || got != want {
			return fail(e)
		}
	}
	ref, e := normalInflightMetadataBuild()
	if e != nil {
		return fail(e)
	}
	a, e := readNormalInflightSourceFixed(r.Identity, ref)
	if e != nil {
		return fail(e)
	}
	if e = o.origin.Physical.Match(a.Physical); e != nil {
		return fail(e)
	}
	k, ok := o.guard.kernel.(*linuxKernel)
	if !ok || !k.config.SystemCgroup {
		return fail(ErrUnknown)
	}
	dev, ino, e := systemCgroupIdentity(uint64(r.Identity.CgroupDevice))
	if e != nil || control.Uint64(dev) != r.Identity.SystemCgroupDevice || control.Uint64(ino) != r.Identity.SystemCgroupInode {
		return fail(errors.Join(e, fmt.Errorf("normal quota original System inode differs")))
	}
	sys, e := k.readSystemBound()
	if e != nil {
		return fail(e)
	}
	if e = (Evidence{Identity: r.Identity, System: sys}).systemPlacementMatches(r.systemLimit()); e != nil {
		return fail(e)
	}
	if e = normalQuotaOrdinaryDomains(); e != nil {
		return fail(e)
	}
	swaps, e := kernelText("/proc/swaps")
	if e != nil || strings.Join(strings.Fields(swaps), " ") != "Filename Type Size Used Priority" {
		return fail(errors.Join(e, fmt.Errorf("normal quota requires original no-swap profile")))
	}
	region, e := readGuestMemoryMaximumMetadataComponent(r.Identity.GuestBootID)
	if e != nil || o.origin.Region.match(region) != nil {
		return fail(errors.Join(e, fmt.Errorf("normal quota original region changed")))
	}
	geometry, e := bootprotocol.DecodeNormalMemoryGeometry(o.origin.Host.Prepared.Challenge.Identity, o.origin.Host.Prepared.Challenge.OriginalBase, o.origin.Host.QMP)
	if e != nil {
		return fail(e)
	}
	maximum, e := compareGuestMaximumGeometry(region, uint64(geometry.BaseBytes), uint64(geometry.MaximumBytes), uint64(geometry.DeviceAddress), uint64(geometry.BackendBytes))
	if e != nil || maximum == 0 || maximum != o.origin.MaximumBytes {
		return fail(errors.Join(e, fmt.Errorf("normal quota original maximum differs")))
	}
	cpu := uint64(o.origin.Host.MaximumVCPUs)
	if cpu == 0 || cpu > normalInflightCompiledCPUCeiling || cpu != uint64(o.origin.Physical.PossibleCPUs) || cpu != uint64(a.Physical.PossibleCPUs) {
		return fail(fmt.Errorf("normal quota fixed CPU slots/possible differ"))
	}
	b, e := readNormalInflightSourceFixed(r.Identity, ref)
	if e != nil || !reflect.DeepEqual(a, b) {
		return fail(errors.Join(e, fmt.Errorf("normal quota kernel source changed across read")))
	}
	t, e := normalInflightCalculateCPUFixed(b, ref, cpu)
	if e != nil {
		return fail(e)
	}
	return b, t, cpu, nil
}

// 普通task全部受硬域持续限制；kernel thread没有普通用户mm，按K/I预算。
// 进程消失允许重试下一次完整操作，不能把无法读取的活task当kernel线程。
func normalQuotaOrdinaryDomains() error {
	paths, e := os.ReadDir("/proc")
	if e != nil {
		return e
	}
	for _, p := range paths {
		if _, e = strconv.ParseUint(p.Name(), 10, 32); e != nil {
			continue
		}
		status, e := os.ReadFile("/proc/" + p.Name() + "/status")
		if e != nil {
			if os.IsNotExist(e) {
				continue
			}
			return e
		}
		kernel := false
		for _, line := range strings.Split(string(status), "\n") {
			if strings.TrimSpace(line) == "Kthread:\t1" {
				kernel = true
			}
		}
		if kernel {
			continue
		}
		cg, e := os.ReadFile("/proc/" + p.Name() + "/cgroup")
		if e != nil {
			if os.IsNotExist(e) {
				continue
			}
			return e
		}
		v := strings.TrimSpace(string(cg))
		if v != "0::/neon-system" && v != "0::/neon-postgres/private" && v != "0::/neon-postgres/arrays" {
			return fmt.Errorf("normal quota ordinary task is outside fixed memory domains")
		}
	}
	var st unix.Stat_t
	if e = unix.Stat(systemCgroup, &st); e != nil {
		return e
	}
	return nil
}

func (g *Guard) normalQuotaBudgetLocked(ctx context.Context, s *lockedState, r *record, before NormalSplitTopologySnapshot, targetArrays control.Uint64) (normalQuotaBudget, error) {
	fail := func(e error) (normalQuotaBudget, error) { return normalQuotaBudget{}, errors.Join(ErrUnknown, e) }
	if g == nil || ctx == nil || ctx.Err() != nil || r == nil || before.Private == nil || before.Arrays == nil || targetArrays == 0 || g.config.ExecutionModel != TrustedControllerExecutionModel {
		return fail(ErrUnknown)
	}
	value, ok := normalQuotaBudgetSources.Load(g)
	if !ok {
		return fail(fmt.Errorf("normal quota original memory budget source not prepared"))
	}
	source, ok := value.(*normalQuotaBudgetSource)
	if !ok || source.guard != g {
		return fail(ErrUnknown)
	}
	facts, terms, cpu, e := source.freshLocked(ctx, s, r)
	if e != nil {
		return fail(e)
	}
	leaves, e := normalInflightSum(uint64(before.Private.MemoryMaxBytes), uint64(targetArrays))
	if e != nil {
		return fail(e)
	}
	plan, e := normalInflightPlanCPUFixed(facts, normalInflightPlanningInput{MaximumBytes: source.origin.MaximumBytes, NormalLeavesBytes: leaves, SystemBytes: uint64(r.systemLimit())}, facts.Build, cpu)
	if e != nil {
		return fail(e)
	}
	// System已含Root及全部普通服务。Outside保留初始页/内核、所有在途与allocator
	// 余量；这些分项不再重复加入PG私有P或实际四数组A。
	outside, e := normalInflightSum(uint64(r.systemLimit()), plan.OutsideKernelAndInitialRootBytes, terms.KnownConditionalOccupiedBytes, terms.CompactionIsolatedFreeReserveBytes, terms.CPUStockPaidBytes, plan.PCPTotalFreeReserveBytes, plan.MovableWatermarkReserveBytes)
	if e != nil {
		return fail(e)
	}
	return normalQuotaBudget{forceUpperBytes: control.Uint64(plan.ConditionalForceChargeBytes), counterGapBytes: control.Uint64(terms.CounterHierarchyGapBytes), outsideUpperBytes: control.Uint64(outside), physicalUpperBytes: control.Uint64(plan.TotalRequiredBytes), sourceSHA256: source.sourceSHA256}, nil
}
