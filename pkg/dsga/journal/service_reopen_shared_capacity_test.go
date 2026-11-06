package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"math"
	"os"
	"strings"
	"testing"
)

func w157SafeOK(t *testing.T, stage string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	switch err.Error() {
	case "committed service cannot retain atomic operation coverage", "service reopen cannot retain complete bounded lifecycle evidence":
		t.Fatalf("W157 %s rejected by original capacity: %s", stage, err.Error())
	default:
		t.Fatalf("W157 %s failed (private proof omitted)", stage)
	}
}
func w157Digest(t *testing.T, v any) string {
	t.Helper()
	b, e := json.Marshal(v)
	w157SafeOK(t, "digest", e)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func w157Fixture(t *testing.T) (Model, control.AdmissionLeaseResponse, string) {
	t.Helper()
	path := os.Getenv("DSGA_W157_FIXTURE")
	if path == "" {
		t.Skip("optional private W157 model fixture")
	}
	raw, e := os.ReadFile(path)
	w157SafeOK(t, "read", e)
	h := sha256.Sum256(raw)
	if hex.EncodeToString(h[:]) != "1d89ef0a6c8e7cfbbea685fb847e2c2b9b61e462841c8b0e8ab9dcee3927494c" {
		t.Fatal("W157 fixture SHA mismatch")
	}
	var m Model
	w157SafeOK(t, "decode", json.Unmarshal(raw, &m))
	w157SafeOK(t, "validate", m.Validate())
	if m.Owner.Epoch != 36 || m.Sequence != 157 || m.ServiceClosure == nil || m.ServiceClosure.Reopen == nil || m.ServiceClosure.Reopen.Rearm == nil || m.ServiceClosure.Reopen.Rearm.Phase != Settled || m.ServiceClosure.Reopen.Rearm.Receipt == nil {
		t.Fatal("W157 original rearm36 settlement missing")
	}
	r := m.ServiceClosure.Reopen.Rearm
	before := cloneLeaseResponse(r.Receipt.Current)
	before.Request = control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: before.Observed.CapacityGeneration, ExpectedAdmissionRevision: before.Observed.AdmissionRevision, Action: "observe"}
	before.Renewed = false
	before.Expired = false
	w157SafeOK(t, "synthetic observe", before.ValidateFor(before.Request, *m.PGInstance))
	if before.Observed.CapacityGeneration != 3 || before.Observed.AdmissionRevision != 21 || before.Observed.AdmissionOpen || before.Observed.IssuerFloorEpoch != 36 {
		t.Fatal("W157 original observed boundary differs")
	}
	_, _, _, selector := m.ServiceClosure.Reopen.Closing.Close.Selection()
	if selector != "5764e5b1eca3118932d8b85a703fb9b655c2010a0e53af2db8d2f0e063f234ba" {
		t.Fatal("W157 selector mismatch")
	}
	return m, before, selector
}

func TestW157RetryReopenMinimalHistoryCapacityActualRecovery(t *testing.T) {
	m, before, selector := w157Fixture(t)
	original := w157Digest(t, m)
	active := w157Digest(t, m.Active)
	held := w157Digest(t, []any{m.BudgetHeld, m.InitialHeld, m.CapacityHeld})
	identity := w157Digest(t, []any{m.Owner, m.ResourceIdentity, m.PGInstance, m.HighWater})
	lifecycle := w157Digest(t, m.ServiceLifecycle)
	n, e := m.RetryServiceReopen(m.Owner, selector, before)
	w157SafeOK(t, "Retry", e)
	if w157Digest(t, m) != original {
		t.Fatal("W157 input mutated")
	}
	dropped := len(m.History) - len(n.History)
	if dropped != 1 || n.ExpiredResults != m.ExpiredResults+1 || w157Digest(t, m.History[dropped:]) != w157Digest(t, n.History) {
		t.Fatal("W157 requires exactly minimal FIFO1/ExpiredResults1")
	}
	if active != w157Digest(t, n.Active) || held != w157Digest(t, []any{n.BudgetHeld, n.InitialHeld, n.CapacityHeld}) || identity != w157Digest(t, []any{n.Owner, n.ResourceIdentity, n.PGInstance, n.HighWater}) || lifecycle != w157Digest(t, n.ServiceLifecycle) {
		t.Fatal("W157 changed Active/Unknown/threeHeld/source/identity/HWM/lifecycle")
	}
	var prior ServiceReopen
	w157SafeOK(t, "retired decode", json.Unmarshal(n.ServiceClosure.Reopen.Retired.Context, &prior))
	if w157Digest(t, prior) != w157Digest(t, *m.ServiceClosure.Reopen) {
		t.Fatal("W157 prior closure/rearm/Unknown evidence changed")
	}
	restored := n.Clone()
	restored.History = m.Clone().History
	restored.ExpiredResults = m.ExpiredResults
	w157SafeOK(t, "restored pre-expiry model", restored.Validate())
	if restored.reopenCompletionCapacity() == nil {
		t.Fatal("W157 expired more than required or old boundary no longer fails")
	}
	w157SafeOK(t, "planned model", n.Validate())
	w157SafeOK(t, "planned capacity", n.reopenCompletionCapacity())
	_, wire, e := encode(n, MaxEncodedBytes)
	w157SafeOK(t, "Retry encode", e)
	loaded, e := decode(wire, m.VMUID, m.Incarnation)
	w157SafeOK(t, "Retry decode", e)
	if w157Digest(t, n) != w157Digest(t, loaded) {
		t.Fatal("W157 encode/decode changed original model")
	}
	reopened, e := loaded.RecordServiceReopen(loaded.Owner, reopenFact(t, loaded), false)
	w157SafeOK(t, "Record", e)
	current := cloneLeaseResponse(before)
	current.Observed = reopened.ServiceClosure.Reopen.Receipt.Observed
	done, e := reopened.CompleteServiceReopen(reopened.Owner, reopened.ServiceClosure.Reopen.CommandSHA256, current)
	w157SafeOK(t, "Complete", e)
	w157SafeOK(t, "Complete validate", done.Validate())
	if done.Active != nil || done.ServiceClosure != nil || done.CapacityHeld == nil || done.CapacityHeld.ProtocolVersion != 3 || done.History[len(done.History)-1].Outcome != RecoveredWithActualBudget || active != w157Digest(t, &done.History[len(done.History)-1].Operation) {
		t.Fatal("W157 original actual recovery completion changed")
	}
	_, _, e = encode(done, MaxEncodedBytes)
	w157SafeOK(t, "Complete encode", e)
	t.Logf("W157 safe summary: FIFO=%d expired_delta=%d original_active_preserved=true", dropped, n.ExpiredResults-m.ExpiredResults)
}

func TestW157NextOwnerRearmRetryCapacityActualRecovery(t *testing.T) {
	original, before, _ := w157Fixture(t)
	originalSHA := w157Digest(t, original)
	active := w157Digest(t, original.Active)
	held := w157Digest(t, []any{original.BudgetHeld, original.InitialHeld, original.CapacityHeld})
	identity := w157Digest(t, []any{original.ResourceIdentity, original.PGInstance, original.HighWater})
	lifecycle := w157Digest(t, original.ServiceLifecycle)
	m, e := original.Takeover(original.Owner, "W157-pure-model-owner37")
	w157SafeOK(t, "Takeover37", e)
	if m.Owner.Epoch != 37 || m.Owner.HolderID == original.Owner.HolderID {
		t.Fatal("W157 must use original higher-owner takeover")
	}
	taken := w157Digest(t, m)
	oldRearm := m.ServiceClosure.Reopen.Rearm
	before.Request.IssuerEpoch = 37
	before.Observed.IssuerFloorEpoch = 37
	before.Expired = true
	// synthetic expired36事实只供模型；原实际rearm36/Closing/Actual34保持原文。
	if before.LeaseEpoch != 36 {
		t.Fatal("W157 original lease36 missing")
	}
	w157SafeOK(t, "expired36 observe for37", before.ValidateFor(before.Request, *m.PGInstance))
	rearmed, e := m.StartReopenLeaseRecovery(m.Owner, oldRearm.CommandSHA256, before)
	w157SafeOK(t, "StartRearm37", e)
	if w157Digest(t, m) != taken {
		t.Fatal("W157 Takeover37 input mutated")
	}
	var priorRearm ReopenLeaseRecovery
	w157SafeOK(t, "prior rearm retirement", json.Unmarshal(rearmed.ServiceClosure.Reopen.Rearm.Retired.Context, &priorRearm))
	previous := *oldRearm
	previous.Retired = nil
	if w157Digest(t, priorRearm) != w157Digest(t, previous) {
		t.Fatal("W157 old rearm36 changed")
	}
	fact := reopenRearmFact(t, rearmed)
	w157SafeOK(t, "rearm37 fact", fact.ValidateFor(rearmed.ServiceClosure.Reopen.Rearm.Request, *m.PGInstance))
	recorded, e := rearmed.RecordReopenLeaseRecovery(rearmed.Owner, fact)
	w157SafeOK(t, "RecordRearm37", e)
	live := cloneLeaseResponse(fact.Current)
	live.Request = control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: 37, ExpectedCapacityGeneration: live.Observed.CapacityGeneration, ExpectedAdmissionRevision: live.Observed.AdmissionRevision, Action: "observe"}
	live.Renewed = false
	live.Expired = false
	w157SafeOK(t, "synthetic live37 observe", live.ValidateFor(live.Request, *m.PGInstance))
	if live.LeaseEpoch != 37 || live.DeadlineNS < fact.AppliedDeadlineNS {
		t.Fatal("W157 Retry37 must follow original confirmed new rearm")
	}
	_, _, _, selector := recorded.ServiceClosure.Reopen.Closing.Close.Selection()
	recordedSHA := w157Digest(t, recorded)
	n, e := recorded.RetryServiceReopen(recorded.Owner, selector, live)
	w157SafeOK(t, "Retry37", e)
	if w157Digest(t, recorded) != recordedSHA || w157Digest(t, original) != originalSHA {
		t.Fatal("W157 original/recorded inputs mutated")
	}
	dropped := len(original.History) - len(n.History)
	if dropped != 1 || n.ExpiredResults != original.ExpiredResults+1 || w157Digest(t, original.History[dropped:]) != w157Digest(t, n.History) {
		t.Fatal("W157 full nextOwner chain must expire only FIFO1")
	}
	for _, state := range []Model{rearmed, recorded, n} {
		if active != w157Digest(t, state.Active) || held != w157Digest(t, []any{state.BudgetHeld, state.InitialHeld, state.CapacityHeld}) || identity != w157Digest(t, []any{state.ResourceIdentity, state.PGInstance, state.HighWater}) || lifecycle != w157Digest(t, state.ServiceLifecycle) {
			t.Fatal("W157 nextOwner chain changed Active33/threeHeld/Actual34/source/HWM/lifecycle")
		}
		closure := state.Clone()
		closure.ServiceClosure.Reopen = original.Clone().ServiceClosure.Reopen
		if w157Digest(t, closure.ServiceClosure) != w157Digest(t, original.ServiceClosure) {
			t.Fatal("W157 original outer closure/Actual34 changed")
		}
	}
	var prior ServiceReopen
	w157SafeOK(t, "retry37 prior", json.Unmarshal(n.ServiceClosure.Reopen.Retired.Context, &prior))
	wantPrior := *recorded.ServiceClosure.Reopen
	wantPrior.Retired = nil
	if w157Digest(t, prior) != w157Digest(t, wantPrior) {
		t.Fatal("W157 full original reopen/rearm retirement changed")
	}
	restored := n.Clone()
	restored.History = original.Clone().History
	restored.ExpiredResults = original.ExpiredResults
	w157SafeOK(t, "restore nextOwner FIFO", restored.Validate())
	if restored.reopenCompletionCapacity() == nil {
		t.Fatal("W157 nextOwner history expiration not minimal")
	}
	_, wire, e := encode(n, MaxEncodedBytes)
	w157SafeOK(t, "Retry37 encode", e)
	n, e = decode(wire, n.VMUID, n.Incarnation)
	w157SafeOK(t, "Retry37 decode", e)
	n, e = n.RecordServiceReopen(n.Owner, reopenFact(t, n), false)
	w157SafeOK(t, "RecordOpen37", e)
	live.Observed = n.ServiceClosure.Reopen.Receipt.Observed
	done, e := n.CompleteServiceReopen(n.Owner, n.ServiceClosure.Reopen.CommandSHA256, live)
	w157SafeOK(t, "Complete37", e)
	w157SafeOK(t, "Complete37 validate", done.Validate())
	if done.Active != nil || done.ServiceClosure != nil || done.CapacityHeld == nil || done.CapacityHeld.ProtocolVersion != 3 || done.History[len(done.History)-1].Outcome != RecoveredWithActualBudget || active != w157Digest(t, &done.History[len(done.History)-1].Operation) {
		t.Fatal("W157 nextOwner actual budget completion changed original Unknown")
	}
	_, wire, e = encode(done, MaxEncodedBytes)
	w157SafeOK(t, "Complete37 encode", e)
	loaded, e := decode(wire, done.VMUID, done.Incarnation)
	w157SafeOK(t, "Complete37 decode", e)
	if w157Digest(t, loaded) != w157Digest(t, done) {
		t.Fatal("W157 nextOwner completion wire lost original evidence")
	}
	t.Log("W157 safe nextOwner37 summary: original takeover/rearm/retirement/Retry/Record/Complete/Encode/Decode PASS; synthetic lease only")
}

func TestW157SharedReopenCapacityRejectsInvalidAndCounterOverflow(t *testing.T) {
	m, before, selector := w157Fixture(t)
	for _, mode := range []string{"selector", "expired", "capacity", "owner", "expired-counter"} {
		t.Run(mode, func(t *testing.T) {
			input := m.Clone()
			live := cloneLeaseResponse(before)
			selected := selector
			actor := m.Owner
			switch mode {
			case "selector":
				selected = strings.Repeat("b", 64)
			case "expired":
				live.Expired = true
			case "capacity":
				live.Observed.CapacityGeneration++
			case "owner":
				actor.Epoch++
			case "expired-counter":
				input.ExpiredResults = control.Uint64(math.MaxUint64)
			}
			w157SafeOK(t, "negative original valid model", input.Validate())
			original := w157Digest(t, input)
			returned, e := input.RetryServiceReopen(actor, selected, live)
			if e == nil {
				t.Fatal("W157 negative Retry must reject")
			}
			if mode == "expired-counter" && !strings.Contains(e.Error(), "expired-result counter exhausted") {
				t.Fatal("W157 did not exercise original expiry counter overflow")
			}
			if w157Digest(t, input) != original || w157Digest(t, returned) != original {
				t.Fatal("W157 rejection changed input or returned partially expired model")
			}
		})
	}
}

func TestSharedReopenCapacityKeepsIdleLastHistoryWhenAlreadyFits(t *testing.T) {
	m, _, _ := serviceClosureModel(t)
	request := testRequest(m, 1, control.Grow)
	m = beginModel(t, m, request)
	body := []byte(`{"complete_domain_body":"` + strings.Repeat("<", 24*1024) + `"}`)
	for _, action := range []Action{PrepareBudget, ReserveHost, ResizeVM, GuestReady, ResizePG} {
		var e error
		m, e = m.Start(m.Owner, request.Key, action, request.Key.Instance.ComputeInstanceID, testCommand)
		w157SafeOK(t, "idle history Start", e)
		var step Step
		for _, s := range m.Active.Steps {
			if s.Action == action {
				step = s
			}
		}
		fact := observation(m, step, Completed)
		fact.Data = body
		m, e = m.Record(m.Owner, request.Key, fact)
		w157SafeOK(t, "idle history Record", e)
	}
	var e error
	m, e = m.RequestAbort(m.Owner, request.Key)
	w157SafeOK(t, "idle history abort", e)
	m, e = m.StartCleanup(m.Owner, request.Key, request.Key.Instance.ComputeInstanceID, testCommand)
	w157SafeOK(t, "idle history cleanup Start", e)
	fact := observation(m, *m.Active.Cleanup, Completed)
	fact.Data = body
	m, e = m.Record(m.Owner, request.Key, fact)
	w157SafeOK(t, "idle history cleanup Record", e)
	m, e = m.Finish(m.Owner, request.Key)
	w157SafeOK(t, "idle history Finish", e)
	_, closing, closed := serviceClosureModel(t)
	m, e = m.StartServiceClosure(m.Owner, closing)
	w157SafeOK(t, "idle closure Start", e)
	m, e = m.RecordServiceClosure(m.Owner, closed, false)
	w157SafeOK(t, "idle closure Record", e)
	_, before := reopenModel(t, "idle")
	before.Request.IssuerEpoch = m.Owner.Epoch
	before.Observed.IssuerFloorEpoch = m.Owner.Epoch
	before.LeaseEpoch = m.Owner.Epoch
	if m.Active != nil || len(m.History) != 1 {
		t.Fatal("idle-last fixture must have exactly one terminal history")
	}
	w157SafeOK(t, "idle-last input", m.Validate())
	_, _, e = encode(m, MaxEncodedBytes)
	w157SafeOK(t, "idle-last original bounded encoding", e)
	original := w157Digest(t, m)

	w157SafeOK(t, "already-fits observe", before.ValidateFor(before.Request, *m.PGInstance))
	w157SafeOK(t, "already-fits stable recovery", m.CheckStableServiceRecovery())
	_, _, _, selected := m.ServiceClosure.Selection()
	returned, e := m.StartServiceReopen(m.Owner, selected, before)
	if errors.Is(e, errReopenLifecycleEvidenceCapacity) || errors.Is(e, errReopenAtomicCoverageCapacity) {
		t.Fatal("already-fits fixture unexpectedly exceeds original capacity")
	}
	w157SafeOK(t, "already-fits Start", e)
	w157SafeOK(t, "already-fits next", returned.Validate())
	w157SafeOK(t, "already-fits capacity", returned.reopenCompletionCapacity())
	frozen := returned.Clone()
	frozen.ProtocolVersion = 20
	frozen.ServiceLifecycle = nil
	if frozen.RetainedCoverage != nil {
		t.Fatal("already-fits fixture unexpectedly contains retained coverage")
	}
	frozen.ServiceClosure.FormatVersion = 0
	raw, e := json.Marshal(frozen)
	w157SafeOK(t, "already-fits frozen raw", e)
	future := len(raw) + MaxObservationBytes + 4096
	if future > MaxRawBytes/2 || frozen.Active != nil {
		t.Fatal("already-fits fixture does not prove original idle capacity bound")
	}
	if len(returned.History) != 1 || returned.ExpiredResults != m.ExpiredResults || w157Digest(t, returned.History) != w157Digest(t, m.History) || original != w157Digest(t, m) {
		t.Fatal("already-fits selection evicted history or mutated original input")
	}
	if returned.Sequence != m.Sequence+1 || returned.ServiceClosure.Reopen == nil || returned.ServiceClosure.Reopen.Phase != Unconfirmed {
		t.Fatal("already-fits selection lost original write-ahead semantics")
	}
	t.Logf("already-fits safe summary: frozen_raw=%d future=%d limit=%d FIFO=0 History=1 ExpiredResults unchanged", len(raw), future, MaxRawBytes/2)

}
