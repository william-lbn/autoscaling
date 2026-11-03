//go:build linux && amd64

package resourceguard

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type qualificationHTTPUnitBackend struct {
	*Guard
	artifact    GuestQualificationArtifact
	artifactErr error
	calls       int
}

func (b *qualificationHTTPUnitBackend) InspectGuestQualificationArtifact(context.Context) (GuestQualificationArtifact, error) {
	b.calls++
	return b.artifact, b.artifactErr
}
func qualificationHTTPUnit(t *testing.T) (*resourceHTTP, ed25519.PrivateKey, *qualificationHTTPUnitBackend, Command) {
	t.Helper()
	g, k, b, c := systemFixture(t)
	ctx := context.Background()
	if _, e := g.Bootstrap(ctx, b); e != nil {
		t.Fatal(e)
	}
	if _, e := g.placeSystem(ctx, fixturePlacement(k)); e != nil {
		t.Fatal(e)
	}
	v, e := g.InspectState(ctx)
	if e != nil {
		t.Fatal(e)
	}
	a := qualificationArtifactUnit(t)
	var f normalInflightSourceFacts
	if e = json.Unmarshal(a.CurrentFacts, &f); e != nil {
		t.Fatal(e)
	}
	f.Physical.Identity = v.Identity
	a.Identity = v.Identity
	a.OriginalPhysical = f.Physical
	a.CurrentLimits = b.Limits
	a.ObservedGuestMemTotalBytes = v.Actual.GuestMemTotalBytes
	terms, e := normalInflightCalculateCPUFixed(f, f.Build, uint64(a.MaximumVCPUs))
	if e != nil {
		t.Fatal(e)
	}
	plan, e := normalInflightPlanCPUFixed(f, normalInflightPlanningInput{uint64(a.MaximumVMBytes), uint64(a.PrivateMemoryMaxBytes + a.ArraysMemoryMaxBytes), uint64(b.Limits.SystemMemoryMaxBytes)}, f.Build, uint64(a.MaximumVCPUs))
	if e != nil {
		t.Fatal(e)
	}
	a.Outside, e = qualificationOutside(uint64(b.Limits.SystemMemoryMaxBytes), plan, terms)
	if e != nil {
		t.Fatal(e)
	}
	a.MinimumNormalBytes = control.Uint64(plan.ConditionalNormalTargetBytes)
	a.CurrentFacts, _ = json.Marshal(f)
	a.InflightTerms, _ = json.Marshal(terms)
	a.RootStateSHA256, e = qualificationStateSHA(v)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(v.Bootstrap)
	a.RootBootstrapSHA256 = physicalDigest(raw)
	backend := &qualificationHTTPUnitBackend{Guard: g, artifact: a}
	public, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	source := sourceFixture(c)
	source.Config.SystemCgroup = true
	source.ControlPublicKey = base64.RawURLEncoding.EncodeToString(public)
	h, e := newResourceHandler(source, backend)
	if e != nil {
		t.Fatal(e)
	}
	h.source.Config.ExecutionModel = TrustedControllerExecutionModel
	return h, key, backend, c
}
func TestGuestQualificationArtifactHTTPAndTLSClientOriginalEnvelope(t *testing.T) {
	h, key, b, c := qualificationHTTPUnit(t)
	token := resourceToken(t, h, key, nil)
	client := resourceClientFixture(t, h, c, token, nil)
	defer client.CloseIdleConnections()
	v, e := client.InspectState(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if v.GuestQualification == nil || v.GuestQualificationUnavailable != nil || v.GuestQualification.MatchView(v) != nil {
		t.Fatal("authenticated read did not retain artifact")
	}
	if b.calls != 1 {
		t.Fatal("unexpected collector calls")
	}
	raw, _ := json.Marshal(v)
	if len(raw) > 32<<10 {
		t.Fatal("existing response limit exceeded")
	}
}
func TestGuestQualificationArtifactHTTPUnavailableAndUnauthorized(t *testing.T) {
	h, key, b, c := qualificationHTTPUnit(t)
	b.artifactErr = ErrUnknown
	token := resourceToken(t, h, key, nil)
	client := resourceClientFixture(t, h, c, token, nil)
	defer client.CloseIdleConnections()
	v, e := client.InspectState(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if v.GuestQualification != nil || v.GuestQualificationUnavailable == nil || v.GuestQualificationUnavailable.Reason != "original_source_unconfirmed" {
		t.Fatal("state success became qualification success")
	}
	before := b.calls
	req := httptest.NewRequest(http.MethodGet, "/dsg/v1/resource/state", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 401 || b.calls != before {
		t.Fatal("unauthorized request reached artifact source")
	}
}
func TestGuestQualificationArtifactHTTPLegacyAbsentAndMalformedRejected(t *testing.T) {
	h, key, b, c := qualificationHTTPUnit(t)
	h.source.Config.ExecutionModel = ""
	token := resourceToken(t, h, key, nil)
	client := resourceClientFixture(t, h, c, token, nil)
	defer client.CloseIdleConnections()
	v, e := client.InspectState(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if v.GuestQualification != nil || v.GuestQualificationUnavailable != nil || b.calls != 0 {
		t.Fatal("legacy response changed")
	}
	raw, _ := json.Marshal(v)
	bad := strings.TrimSuffix(string(raw), "}") + `,"guest_qualification_unavailable":{"protocol_version":1,"reason":"made-up"}}`
	var view View
	if json.Unmarshal([]byte(bad), &view) == nil {
		t.Fatal("unknown availability accepted")
	}
}
func TestGuestQualificationArtifactResponseSizeRefusesBeforeDecode(t *testing.T) {
	a := qualificationArtifactUnit(t)
	a.CurrentFacts = json.RawMessage(`{"padding":"` + strings.Repeat("x", MaxGuestQualificationArtifactBytes) + `"}`)
	if e := a.Validate(); e == nil || !strings.Contains(e.Error(), "exceeds existing response envelope") {
		t.Fatal("oversize artifact accepted", e)
	}
}
