package journal

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"io"
	"os"
	"testing"
)

func w148SafeOK(t *testing.T, stage string, err error) {
	t.Helper()
	if err != nil {
		if err.Error() == "committed service cannot retain atomic operation coverage" || err.Error() == "service reopen cannot retain complete bounded lifecycle evidence" {
			t.Fatalf("W148 %s rejected by original capacity: %s", stage, err.Error())
		}
		t.Fatalf("W148 %s failed: %T (private proof omitted)", stage, err)
	}
}
func w148Digest(t *testing.T, v any) string {
	t.Helper()
	b, e := json.Marshal(v)
	w148SafeOK(t, "digest", e)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func w148Fixture(t *testing.T) (Model, control.AdmissionLeaseResponse) {
	t.Helper()
	fixture := os.Getenv("DSGA_W148_FIXTURE")
	if fixture == "" {
		t.Skip("optional private W148 fixture: set DSGA_W148_FIXTURE for selected private model gate")
	}
	b, e := os.ReadFile(fixture)
	w148SafeOK(t, "read secret", e)
	z, e := zlib.NewReader(bytes.NewReader(b))
	w148SafeOK(t, "read compressed secret", e)
	raw, e := io.ReadAll(io.LimitReader(z, MaxRawBytes+1))
	w148SafeOK(t, "decompress secret", e)
	_ = z.Close()
	h := sha256.Sum256(raw)
	if hex.EncodeToString(h[:]) != "78a4f3cb8a8a736cc8c8ea735093f29daa61f0a24655ed0c8a7adb9048a65e26" {
		t.Fatal("W148 private fixture SHA mismatch")
	}
	var m Model
	w148SafeOK(t, "decode", json.Unmarshal(raw, &m))
	w148SafeOK(t, "validate", m.Validate())
	r := m.ServiceClosure.LatestRearm()
	before := cloneLeaseResponse(r.Receipt.Current)
	before.Request = control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: *m.PGInstance, IssuerEpoch: m.Owner.Epoch, ExpectedCapacityGeneration: before.Observed.CapacityGeneration, ExpectedAdmissionRevision: before.Observed.AdmissionRevision, Action: "observe"}
	before.Renewed = false
	before.Expired = false
	before.DeadlineNS = r.Receipt.AppliedDeadlineNS + 1
	w148SafeOK(t, "legal renewed observe", before.ValidateFor(before.Request, *m.PGInstance))
	return m, before
}
func TestW148ReopenHistoryCapacityActualRecovery(t *testing.T) {
	m, before := w148Fixture(t)
	original := w148Digest(t, m)
	active := w148Digest(t, m.Active)
	held := w148Digest(t, m.BudgetHeld)
	initialHeld := w148Digest(t, m.InitialHeld)
	capacityHeld := w148Digest(t, m.CapacityHeld)
	identityAndWater := w148Digest(t, []any{m.ResourceIdentity, m.PGInstance, m.HighWater, m.Owner})
	closure := w148Digest(t, m.ServiceClosure)
	lifecycle := w148Digest(t, m.ServiceLifecycle)
	_, _, _, selector := m.ServiceClosure.Selection()
	started, e := m.StartServiceReopen(m.Owner, selector, before)
	w148SafeOK(t, "Start", e)
	if original != w148Digest(t, m) {
		t.Fatal("W148 input mutated")
	}
	dropped := len(m.History) - len(started.History)
	if dropped != 2 || started.ExpiredResults != m.ExpiredResults+control.Uint64(dropped) {
		t.Fatal("W148 expected minimal FIFO2/ExpiredResults2")
	}
	if w148Digest(t, m.History[dropped:]) != w148Digest(t, started.History) || active != w148Digest(t, started.Active) || held != w148Digest(t, started.BudgetHeld) || initialHeld != w148Digest(t, started.InitialHeld) || capacityHeld != w148Digest(t, started.CapacityHeld) || identityAndWater != w148Digest(t, []any{started.ResourceIdentity, started.PGInstance, started.HighWater, started.Owner}) || lifecycle != w148Digest(t, started.ServiceLifecycle) {
		t.Fatal("W148 changed retained input beyond exact history prefix")
	}
	prior := started.Clone()
	prior.ServiceClosure.Reopen = nil
	prior.setClosureFormatVersion(m.ClosureFormatVersion())
	if closure != w148Digest(t, prior.ServiceClosure) {
		t.Fatal("W148 original closure/source changed")
	}
	w148SafeOK(t, "started model", started.Validate())
	encoded, wire, e := encode(started, MaxEncodedBytes)
	w148SafeOK(t, "started encode", e)
	loaded, e := decode(wire, m.VMUID, m.Incarnation)
	w148SafeOK(t, "started decode", e)
	if w148Digest(t, encoded) != w148Digest(t, loaded) {
		t.Fatal("W148 encoded model changed")
	}
	reopened, e := loaded.RecordServiceReopen(loaded.Owner, reopenFact(t, loaded), false)
	w148SafeOK(t, "Reopen", e)
	current := cloneLeaseResponse(before)
	current.Observed = reopened.ServiceClosure.Reopen.Receipt.Observed
	done, e := reopened.CompleteServiceReopen(reopened.Owner, reopened.ServiceClosure.Reopen.CommandSHA256, current)
	w148SafeOK(t, "Complete", e)
	w148SafeOK(t, "done Validate", done.Validate())
	if done.Active != nil || done.ServiceClosure != nil || done.CapacityHeld == nil || done.CapacityHeld.ProtocolVersion != 3 || done.History[len(done.History)-1].Outcome != RecoveredWithActualBudget {
		t.Fatal("W148 failed original actual-budget completion")
	}
	if active != w148Digest(t, &done.History[len(done.History)-1].Operation) {
		t.Fatal("W148 current operation/Unknown was rewritten")
	}
	_, _, e = encode(done, MaxEncodedBytes)
	w148SafeOK(t, "complete encode", e)
	t.Logf("W148 safe summary: FIFO=%d expired_delta=%d before_capacity_generation=%d held_protocol=%d", dropped, started.ExpiredResults-m.ExpiredResults, before.Observed.CapacityGeneration, done.CapacityHeld.ProtocolVersion)
}
