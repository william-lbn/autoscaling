package resourceguard

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
)

// PG17-U-ARRAY-MODE-001：callback不能启新模式；原版本单调，缺supplier不降级。
func TestArrayAuthorityClientModeRequiresRootSourceAndNeverDowngrades(t *testing.T) {
	g, _, bootstrap, ordinary := freshBootstrapFixture(t)
	if _, err := g.Bootstrap(context.Background(), bootstrap); err != nil {
		t.Fatal(err)
	}
	var version atomic.Uint32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dsg/v1/resource/state" {
			t.Error("unexpected action")
			w.WriteHeader(409)
			return
		}
		// 完整state模型沿原真实fixtureGuard，只替换明确能力字段；不自报资源许可。
		v, err := g.InspectState(context.Background())
		if err != nil {
			t.Error(err)
			w.WriteHeader(409)
			return
		}
		v.ArrayAuthorityProtocolVersion = version.Load()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(v)
	})
	client := resourceClientFixture(t, handler, ordinary, "test-only", nil)
	client.config.ArrayAuthorityToken = func(context.Context, string, string, string) (string, error) {
		t.Fatal("no action should request token")
		return "", nil
	}
	if client.ArrayAuthorityEnabled() {
		t.Fatal("callback self-enabled array profile")
	}
	if _, err := client.InspectState(context.Background()); err != nil {
		t.Fatal(err)
	}
	if client.ArrayAuthorityEnabled() {
		t.Fatal("legacy Source enabled new profile")
	}
	version.Store(1)
	if _, err := client.InspectState(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !client.ArrayAuthorityEnabled() {
		t.Fatal("required original profile lost")
	}
	client.config.ArrayAuthorityToken = nil
	if !client.ArrayAuthorityEnabled() {
		t.Fatal("missing signer downgraded required profile")
	}
	version.Store(0)
	if _, err := client.InspectState(context.Background()); err == nil {
		t.Fatal("original required profile silently downgraded")
	}
}

// PG17-U-ARRAY-MODE-002：旧View JSON省略能力字段，新未知版本拒绝。
func TestArrayAuthorityViewVersionAndLegacyOmission(t *testing.T) {
	g, _, bootstrap, _ := freshBootstrapFixture(t)
	if _, err := g.Bootstrap(context.Background(), bootstrap); err != nil {
		t.Fatal(err)
	}
	v, err := g.InspectState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	_ = json.Unmarshal(raw, &object)
	if _, present := object["array_authority_protocol_version"]; present {
		t.Fatal("legacy JSON shape changed")
	}
	v.ArrayAuthorityProtocolVersion = 2
	if v.Validate() == nil {
		t.Fatal("unknown capability version accepted")
	}
}
