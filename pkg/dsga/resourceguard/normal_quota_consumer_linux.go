//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"math"
	"reflect"
)

func (g *Guard) ApplyNormalQuota(ctx context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return g.normalQuota(ctx, v, "apply")
}
func (g *Guard) ObserveNormalQuota(ctx context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return g.normalQuota(ctx, v, "observe")
}
func (g *Guard) ReplayNormalQuota(ctx context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return g.normalQuota(ctx, v, "replay")
}
func (g *Guard) RecoverNormalQuota(ctx context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return g.normalQuota(ctx, v, "recover")
}

type normalQuotaStoredEnvelope struct {
	ProtocolVersion        uint32              `json:"protocol_version"`
	Envelope               NormalQuotaEnvelope `json:"envelope"`
	FromBudgetCommitmentID string              `json:"from_budget_commitment_id"`
}

func normalQuotaEnvelopeName(c Command) string {
	return "normal-quota-envelope." + normalSplitQuotaCommandKey(c) + ".json"
}

// 外层只传已落账原事实；真正cap授权只在同resource flock内生产。锁外读取原只读
// topology输入，避免ObservePhysicalKernelOrigin重取resource锁；锁内仍fresh核原内核。
func (g *Guard) normalQuota(ctx context.Context, v NormalQuotaEnvelope, mode string) (NormalQuotaEvidence, error) {
	var out NormalQuotaEvidence
	if g == nil || ctx == nil || g.config.ExecutionModel != TrustedControllerExecutionModel || v.Validate() != nil {
		return out, ErrUnknown
	}
	v = cloneNormalQuota(v)
	inputs, e := g.normalSplitRuntimeInputs(ctx)
	if e != nil {
		return out, errors.Join(ErrUnknown, e)
	}
	e = g.withRecord(ctx, func(r *record, s *lockedState) error {
		raw, err := json.Marshal(v)
		if err != nil || len(raw) > 16<<10 {
			return ErrUnknown
		}
		stored, exists, err := s.readPhysicalFile(normalQuotaEnvelopeName(v.Command))
		if err != nil {
			return errors.Join(ErrUnknown, err)
		}
		if r.Bootstrap == nil {
			return ErrUnknown
		}
		fromBudget := r.Bootstrap.Original.BudgetCommitmentID
		if r.Held != nil {
			fromBudget = r.Held.BudgetCommitmentID
		}
		entry := normalQuotaStoredEnvelope{ProtocolVersion: 1, Envelope: v, FromBudgetCommitmentID: fromBudget}
		if exists {
			if control.UnmarshalObject(stored, &entry) != nil || entry.ProtocolVersion != 1 || control.ValidateID(entry.FromBudgetCommitmentID, "original budget") != nil {
				return ErrUnknown
			}
			retained, err := json.Marshal(entry.Envelope)
			if err != nil || !bytes.Equal(raw, retained) {
				return errors.Join(ErrUnknown, fmt.Errorf("normal quota original envelope changed"))
			}
			fromBudget = entry.FromBudgetCommitmentID
		}
		if !exists && mode != "apply" {
			return errors.Join(ErrUnknown, fmt.Errorf("normal quota original envelope absent"))
		}
		top, err := g.normalSplitLocked(ctx, inputs, normalSplitObserve, s)
		if err != nil {
			return err
		}
		original, count, _, err := normalSplitQuotaRead(s, normalSplitQuotaCommandKey(v.Command))
		if err != nil {
			return err
		}
		if exists && mode == "apply" && (count != 4 || !r.Applied || r.Original == nil || !reflect.DeepEqual(*r.Original, v.Command)) {
			return errors.Join(ErrUnknown, fmt.Errorf("normal quota retained envelope requires explicit replay"))
		}
		if count == 0 {
			original.Original = top.Current
		}
		auth, err := g.normalQuotaAuthorizeLocked(ctx, s, r, v, top, original.Original, fromBudget)
		if err != nil {
			return err
		}
		if count > 0 && !reflect.DeepEqual(auth.original, original) {
			return errors.Join(ErrUnknown, fmt.Errorf("normal quota retained authorization changed"))
		}
		if !exists {
			encodedEntry, encodeErr := json.Marshal(entry)
			if encodeErr != nil {
				return encodeErr
			}
			if err = s.createPhysicalFile(normalQuotaEnvelopeName(v.Command), encodedEntry); err != nil {
				return errors.Join(ErrUnknown, err)
			}
		} else if mode == "replay" || mode == "recover" {
			// 明确恢复只同步同一原信封；普通Observe严格零写。
			if err = s.syncPhysicalFile(normalQuotaEnvelopeName(v.Command)); err != nil {
				return errors.Join(ErrUnknown, err)
			}
		}
		effective := mode
		if mode == "replay" && count == 0 && (r.Original == nil || !reflect.DeepEqual(*r.Original, v.Command)) {
			effective = "apply"
		}
		actual, err := g.normalQuotaHandoffLocked(ctx, s, r, auth, effective, normalQuotaHandoffDiskIO())
		if err != nil {
			return err
		}
		out = NormalQuotaEvidence{ProtocolVersion: 1, EnvelopeSHA256: physicalDigest(raw), Command: cloneCommand(v.Command), Actual: actual}
		return out.validateFor(v)
	})
	if e != nil {
		return NormalQuotaEvidence{}, errors.Join(ErrUnknown, e)
	}
	return out, nil
}

func (g *Guard) normalQuotaAuthorizeLocked(ctx context.Context, s *lockedState, r *record, v NormalQuotaEnvelope, top NormalSplitTopologyResult, before NormalSplitTopologySnapshot, fromBudget string) (normalSplitQuotaAuthorization, error) {
	fail := func(e error) (normalSplitQuotaAuthorization, error) {
		return normalSplitQuotaAuthorization{}, errors.Join(ErrUnknown, e)
	}
	c := v.Command
	if ctx.Err() != nil || r.Birth == nil || r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Birth.matches(c) || r.Identity != c.Identity ||
		c.IssuerEpoch > r.Floor || c.PidsMaxTasks != r.taskLimit() || c.SystemMemoryMaxBytes != r.systemLimit() || c.AddressSpaceMaxBytes != r.addressSpaceLimit() {
		return fail(fmt.Errorf("normal quota original identity or fixed bounds differ"))
	}
	if e := g.kernel.VerifyProcess(*r.Birth); e != nil {
		return fail(e)
	}
	if e := g.rejectArrayPendingLocked(s, r); e != nil {
		return fail(e)
	}
	arrays, generation, e := g.normalQuotaArraysLocked(ctx, s, r, v)
	if e != nil {
		return fail(e)
	}
	budget, e := g.normalQuotaBudgetLocked(ctx, s, r, before, arrays)
	if e != nil {
		return fail(e)
	}
	// 真实来源的全部物理/外域上界必须落入原目标；不能用一次current或conditional
	// planning填空，也不能缩小保留的Outside预算。
	if !physicalDigestValid(budget.sourceSHA256) || budget.physicalUpperBytes == 0 || budget.physicalUpperBytes > v.PGRequest.TargetVMMemoryBytes || budget.outsideUpperBytes > c.OutsideUpperBytes {
		return fail(fmt.Errorf("normal quota physical/outside budget unavailable"))
	}
	req, _ := json.Marshal(v.PGRequest)
	original := normalSplitQuotaOriginal{Command: c, SourceSHA256: budget.sourceSHA256, SplitIntentSHA256: top.IntentSHA256, SplitCompletionSHA256: top.CompletionSHA256, PGRequestSHA256: physicalDigest(req), MappingGeneration: generation, Original: before, TargetArraysBytes: arrays, ForceUpperBytes: budget.forceUpperBytes, CounterGapBytes: budget.counterGapBytes}
	if e := original.validate(); e != nil {
		return fail(e)
	}
	handoff := &normalQuotaBudgetHandoff{fromBudgetCommitmentID: fromBudget, toBudgetCommitmentID: c.BudgetCommitmentID, requestSHA256: original.PGRequestSHA256}
	if !normalQuotaBudgetHeldMatches(r, c, handoff) {
		return fail(fmt.Errorf("normal quota old/new budget handoff changed"))
	}
	auth := normalSplitQuotaAuthorization{original: original, budgetHandoff: handoff}
	// callback固定在本生产工厂创建，绝非HTTP/测试可替换授权检查；每步重新消费同原
	// PG对象/实际helper/内核预算，不能保存一次成功观察跨交易使用。
	auth.check = func(checkCtx context.Context, state *lockedState, record *record, want normalSplitQuotaOriginal) error {
		if checkCtx == nil || checkCtx.Err() != nil || !reflect.DeepEqual(want, original) || record.Birth == nil || !record.Birth.matches(c) || !normalQuotaBudgetHeldMatches(record, c, handoff) {
			return ErrUnknown
		}
		if e := g.kernel.VerifyProcess(*record.Birth); e != nil {
			return e
		}
		if e := g.rejectArrayPendingLocked(state, record); e != nil {
			return e
		}
		a, gen, e := g.normalQuotaArraysLocked(checkCtx, state, record, v)
		if e != nil || a != arrays || gen != generation {
			return errors.Join(ErrUnknown, e)
		}
		next, e := g.normalQuotaBudgetLocked(checkCtx, state, record, before, arrays)
		if e != nil || next != budget {
			return errors.Join(ErrUnknown, e, fmt.Errorf("normal quota budget source changed"))
		}
		return nil
	}
	return auth, nil
}

// 原resource flock→registry.mu；helper RPC同一resource锁串行。持续A硬cap与固定
// reserve覆盖未来合法helper；原helper未唯一退出/旧代未知时不允许本交易。
func (g *Guard) normalQuotaArraysLocked(ctx context.Context, state *lockedState, r *record, v NormalQuotaEnvelope) (control.Uint64, control.Uint64, error) {
	value, ok := arrayGuardBindings.Load(g)
	if !ok {
		return 0, 0, ErrUnknown
	}
	b, ok := value.(*arrayGuardBinding)
	if !ok || b == nil || b.original == nil {
		return 0, 0, ErrUnknown
	}
	o := b.original
	o.mu.Lock()
	defer o.mu.Unlock()
	if ctx.Err() != nil || o.verifyOriginal() != nil || o.current == nil || o.current.retiring || len(o.retained) != 0 || r.Birth == nil || *r.Birth != o.pending.Original || o.current.request.PGBoot != uint64(v.PGRequest.Key.Instance.PGBootID) {
		return 0, 0, ErrUnknown
	}
	if err := g.normalQuotaPGControlsLocked(ctx, state, o); err != nil {
		return 0, 0, err
	}
	current := o.current
	for _, done := range current.startup {
		if !done {
			return 0, 0, ErrUnknown
		}
	}
	files, ids, e := arrayRegistryOpenMappings(o.procfd, o.pidfd, current.request)
	if e != nil {
		return 0, 0, e
	}
	for _, f := range files {
		defer f.Close()
	}
	if ids != current.identities {
		return 0, 0, ErrUnknown
	}
	snapshot := v.PGFence.Observed
	if v.PGResponse != nil {
		snapshot = v.PGResponse.Observed
	} else if v.PGShared != nil {
		snapshot = v.PGShared.Observed
	}
	policy := o.pending.Source.Policy
	if validateNativeArrayHelperPolicy(policy, true) != nil {
		return 0, 0, ErrUnknown
	}
	total := uint64(policy.ArrayHelperReserveBytes)
	for i, a := range snapshot.Arrays {
		span := current.request.Ranges[i]
		n := uint64(v.PGRequest.TargetBuffers)
		if n > math.MaxUint64/span.Element {
			return 0, 0, ErrUnknown
		}
		upper := (n*span.Element + span.Page - 1) &^ (span.Page - 1)
		if v.Phase == "released-shrink" {
			actualUpper, err := normalQuotaReleasedArrayUpper(a, span, n)
			if err != nil {
				return 0, 0, err
			}
			upper = actualUpper
		}
		if v.Phase == "pre-grow" && uint64(a.CommittedUpperBytes) > upper {
			upper = uint64(a.CommittedUpperBytes)
		}
		if upper > span.Mapped || total > math.MaxUint64-upper {
			return 0, 0, ErrUnknown
		}
		total += upper
	}
	if total > math.MaxInt64-(64<<10-1) {
		return 0, 0, ErrUnknown
	}
	target := (total + (64 << 10) - 1) &^ uint64((64<<10)-1)
	return control.Uint64(target), control.Uint64(current.request.MappingGeneration), nil
}
