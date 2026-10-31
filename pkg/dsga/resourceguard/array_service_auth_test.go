package resourceguard

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type arrayHTTPFixture struct {
	*Guard
	calls    atomic.Int32
	original ArrayAuthorityCommand
}

func (b *arrayHTTPFixture) answer(c ArrayAuthorityCommand) (ArrayAuthorityEvidence, error) {
	b.calls.Add(1)
	raw, _ := json.Marshal(c)
	return ArrayAuthorityEvidence{ProtocolVersion: 1, Command: c, CommandSHA256: physicalDigest(raw), ResourceFloor: c.IssuerEpoch, Recorded: true}, nil
}
func (b *arrayHTTPFixture) AuthorizeArrays(_ context.Context, c ArrayAuthorityCommand) (ArrayAuthorityEvidence, error) {
	return b.answer(c)
}
func (b *arrayHTTPFixture) ObserveArrayAuthority(_ context.Context, c ArrayAuthorityCommand) (ArrayAuthorityEvidence, error) {
	return b.answer(c)
}
func (b *arrayHTTPFixture) ReplayArrayAuthority(_ context.Context, c ArrayAuthorityCommand) (ArrayAuthorityEvidence, error) {
	return b.answer(c)
}
func arrayHTTPOriginal(t *testing.T) (*resourceHTTP, ed25519.PrivateKey, *arrayHTTPFixture, *fixtureKernel, Command, ArrayAuthorityCommand) {
	t.Helper()
	h, key, g, k, c := resourceHTTPFixture(t)
	req := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: c.Identity.VMUID, ComputeInstanceID: c.Identity.ComputeInstanceID, PGBootID: 17}, IssuerEpoch: 1, Revision: 1, OperationID: control.OperationID{Low: 1}}, Kind: control.Grow, ExpectedCapacityGeneration: 1, TargetBuffers: 1024, TargetVMMemoryBytes: 1 << 30, BudgetCommitmentID: c.BudgetCommitmentID, DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Minute).UnixMilli())}
	cmd := ArrayAuthorityCommand{ProtocolVersion: 1, Identity: c.Identity, IssuerEpoch: 1, ResourceReferenceSHA256: strings.Repeat("a", 64), Request: req}
	if err := cmd.Validate(); err != nil {
		t.Fatal(err)
	}
	backend := &arrayHTTPFixture{Guard: g, original: cmd}
	h.guard = backend
	return h, key, backend, k, c, cmd
}
func arraySignedToken(t *testing.T, h *resourceHTTP, key ed25519.PrivateKey, path string, body []byte, change func(*resourceClaims)) string {
	return resourceToken(t, h, key, func(c *resourceClaims) {
		c.Scope = arrayAuthorityScope
		c.Request = &signedArrayRequest{ProtocolVersion: 1, Method: http.MethodPost, Path: path, BodySHA256: physicalDigest(body)}
		if change != nil {
			change(c)
		}
	})
}

// PG17-U-ARRAY-AUTH-001：真实Ed25519签名+HTTP原字节，fixture backend只计调用。
func TestArrayAuthorityHTTPExactSignedBodyAndScopeIsolation(t *testing.T) {
	h, key, b, k, c, command := arrayHTTPOriginal(t)
	raw, _ := json.Marshal(command)
	path := "/dsg/v1/resource/array-authorize"
	cases := []struct {
		name, path string
		body       []byte
		change     func(*resourceClaims)
	}{
		{"generic_scope", path, raw, func(c *resourceClaims) { c.Scope = "compute_ctl:dsga"; c.Request = nil }},
		{"missing_binding", path, raw, func(c *resourceClaims) { c.Request = nil }},
		{"binding_protocol", path, raw, func(c *resourceClaims) { c.Request.ProtocolVersion = 2 }},
		{"binding_method", path, raw, func(c *resourceClaims) { c.Request.Method = "GET" }},
		{"binding_path", path, raw, func(c *resourceClaims) { c.Request.Path = "/dsg/v1/resource/array-observe" }},
		{"zero_digest", path, raw, func(c *resourceClaims) { c.Request.BodySHA256 = strings.Repeat("0", 64) }},
		{"upper_digest", path, raw, func(c *resourceClaims) { c.Request.BodySHA256 = strings.ToUpper(c.Request.BodySHA256) }},
		{"body_changed", path, append([]byte(" "), raw...), nil},
		{"query", path + "?x=1", raw, nil},
		{"encoded_alias", "/dsg/v1/resource/%61rray-authorize", raw, nil},
		{"epoch", path, raw, func(c *resourceClaims) { c.IssuerEpoch = 2 }},
		{"foreign_VM", path, raw, func(c *resourceClaims) { c.VMUID = "other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token := arraySignedToken(t, h, key, path, raw, tc.change)
			w := resourceRequest(h, tc.path, token, tc.body)
			if w.Code == 200 {
				t.Fatalf("unbound body accepted %s", w.Body)
			}
		})
	}
	ordinary, _ := json.Marshal(c)
	token := arraySignedToken(t, h, key, path, raw, nil)
	if w := resourceRequest(h, "/dsg/v1/resource/apply", token, ordinary); w.Code != http.StatusForbidden {
		t.Fatalf("special scope entered ordinary apply: %d", w.Code)
	}
	if b.calls.Load() != 0 || resourceHTTPKernelWrites(k) != 0 {
		t.Fatal("rejected binding reached backend/kernel")
	}
	for _, action := range []string{"array-authorize", "array-observe", "array-replay-original"} {
		p := "/dsg/v1/resource/" + action
		w := resourceRequest(h, p, arraySignedToken(t, h, key, p, raw, nil), raw)
		if w.Code != 200 {
			t.Fatalf("exact signed body rejected: %d %s", w.Code, w.Body)
		}
	}
	if b.calls.Load() != 3 || resourceHTTPKernelWrites(k) != 0 {
		t.Fatal("unexpected backend/kernel effects")
	}
}

// PG17-U-ARRAY-AUTH-002：真实HTTPS断回复，普通调用不重发，完整原Pending深拷贝。
func TestArrayAuthorityClientLostReplyRetainsOriginalAndExplicitObserve(t *testing.T) {
	h, key, b, _, ordinary, command := arrayHTTPOriginal(t)
	var posts atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dsg/v1/resource/array-authorize" {
			posts.Add(1)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			if rec.Code != 200 {
				t.Errorf("fixture authorize %d", rec.Code)
			}
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		h.ServeHTTP(w, r)
	})
	client := resourceClientFixture(t, handler, ordinary, resourceToken(t, h, key, nil), nil)
	client.config.ArrayAuthorityToken = func(_ context.Context, method, path, digest string) (string, error) {
		if method != "POST" {
			t.Fatal(method)
		}
		return resourceToken(t, h, key, func(c *resourceClaims) {
			c.Scope = arrayAuthorityScope
			c.Request = &signedArrayRequest{1, method, path, digest}
		}), nil
	}
	_, err := client.arrayAuthorityCall(context.Background(), command, "array-authorize", false)
	if err == nil || client.ArrayAuthorityPending() == nil {
		t.Fatal("lost reply cleared original")
	}
	pending := client.ArrayAuthorityPending()
	pending.Request.TargetBuffers++
	if *pending == *client.ArrayAuthorityPending() {
		t.Fatal("pending pointer aliases client")
	}
	if _, err = client.arrayAuthorityCall(context.Background(), command, "array-authorize", false); err == nil || posts.Load() != 1 {
		t.Fatal("ordinary retry resent POST")
	}
	if _, err = client.Apply(context.Background(), ordinary); err == nil {
		t.Fatal("resource writer passed array unknown")
	}
	if _, err = client.ObserveArrayAuthority(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if client.ArrayAuthorityPending() != nil || b.calls.Load() != 2 || posts.Load() != 1 {
		t.Fatal("explicit original observation did not reconcile")
	}
}

// PG17-U-ARRAY-AUTH-003：signed正文即使有效也必须严格原JSON；不接受重复或别名。
func TestArrayAuthorityHTTPStrictCommandBeforeBackend(t *testing.T) {
	h, key, b, _, _, command := arrayHTTPOriginal(t)
	raw, _ := json.Marshal(command)
	p := "/dsg/v1/resource/array-authorize"
	cases := [][]byte{append([]byte(`{"protocol_version":1,`), raw[1:]...), bytes.Replace(raw, []byte(`"request":`), []byte(`"Request":`), 1), bytes.Replace(raw, []byte(`"issuer_epoch":"1"`), []byte(`"issuer_epoch":1`), 1), bytes.Replace(raw, []byte(`"body_sha256"`), []byte(`"Body_sha256"`), 1)}
	// 最后一项无该key，改成真实未知字段，避免镜像式空负例。
	cases[len(cases)-1] = append(raw[:len(raw)-1:len(raw)-1], []byte(`,"unknown":true}`)...)
	for i, body := range cases {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			if w := resourceRequest(h, p, arraySignedToken(t, h, key, p, body, nil), body); w.Code == 200 {
				t.Fatalf("strict JSON bypass %s", body)
			}
		})
	}
	if b.calls.Load() != 0 {
		t.Fatal("malformed original reached backend")
	}
}

// PG17-U-ARRAY-SOURCE-003：即使JWT/body完整，packet不能补造原Birth/pin。
func TestArrayAuthoritySignedPacketCannotConstructOriginalRegistry(t *testing.T) {
	h, key, backend, k, _, command := arrayHTTPOriginal(t)
	h.guard = backend.Guard
	h.arrayProtocolVersion = 1
	raw, _ := json.Marshal(command)
	for _, action := range []string{"array-authorize", "array-observe", "array-replay-original"} {
		t.Run(action, func(t *testing.T) {
			path := "/dsg/v1/resource/" + action
			w := resourceRequest(h, path, arraySignedToken(t, h, key, path, raw, nil), raw)
			if w.Code != http.StatusConflict {
				t.Fatalf("missing original registry became success: %d %s", w.Code, w.Body)
			}
		})
	}
	if resourceHTTPKernelWrites(k) != 0 {
		t.Fatal("packet/source failure wrote a cap")
	}
	entries, err := os.ReadDir(backend.Guard.config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "array-authority.") {
			t.Fatal("missing original producer created authority WAL")
		}
	}
}
