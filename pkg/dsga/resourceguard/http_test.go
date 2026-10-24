package resourceguard

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 实际Ed25519/HTTP/TLS/客户端及Mac文件锁和fsync；fixtureKernel明确不是真实
// Linux cgroup/HugeTLB，也不是K8s/TokenReview/完整预算/物理回收端到端验收。
func resourceHTTPFixture(t *testing.T) (*resourceHTTP, ed25519.PrivateKey, *Guard, *fixtureKernel, Command) {
	t.Helper()
	g, k, c := fixtureGuard(t)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	source := sourceFixture(c)
	source.ControlPublicKey = base64.RawURLEncoding.EncodeToString(public)
	h, err := newResourceHandler(source, g)
	if err != nil {
		t.Fatal(err)
	}
	return h, private, g, k, c
}
func resourceToken(t *testing.T, h *resourceHTTP, key ed25519.PrivateKey, change func(*resourceClaims)) string {
	t.Helper()
	now := uint64(h.now().Unix())
	c := resourceClaims{Audience: "neon-dsga", Scope: "compute_ctl:dsga", ComputeID: "compute-one", VMUID: h.source.Config.VMUID, ComputeInstanceID: h.source.Config.ComputeInstanceID, IssuerEpoch: 1, Expires: now + 60, NotBefore: now - 2}
	if change != nil {
		change(&c)
	}
	header, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "JWT", "kid": h.kid})
	payload, _ := json.Marshal(c)
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	return input + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(input)))
}
func resourceRequest(h http.Handler, path, token string, body []byte) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestResourceHTTPAuthenticatesOriginalKeyScopeAndTimeBeforeWrites(t *testing.T) {
	h, key, _, k, c := resourceHTTPFixture(t)
	body, _ := json.Marshal(c)
	for name, change := range map[string]func(*resourceClaims){
		"foreign_vm":           func(c *resourceClaims) { c.VMUID = "foreign" },
		"foreign_instance":     func(c *resourceClaims) { c.ComputeInstanceID = "foreign" },
		"initialization_scope": func(c *resourceClaims) { c.Scope = "compute_ctl:dsga_initialization" },
		"wrong_audience":       func(c *resourceClaims) { c.Audience = "general" },
		"expired":              func(c *resourceClaims) { c.Expires = c.NotBefore + 1 },
		"future":               func(c *resourceClaims) { c.NotBefore += 120; c.Expires += 120 },
		"overlong_lifetime":    func(c *resourceClaims) { c.Expires = c.NotBefore + 301 },
		"zero_epoch":           func(c *resourceClaims) { c.IssuerEpoch = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			w := resourceRequest(h, "/dsg/v1/resource/apply", resourceToken(t, h, key, change), body)
			if w.Code != 401 || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("got %d", w.Code)
			}
		})
	}
	_, foreign, _ := ed25519.GenerateKey(rand.Reader)
	for _, token := range []string{"", "unsigned", resourceToken(t, h, foreign, nil)} {
		if w := resourceRequest(h, "/dsg/v1/resource/apply", token, body); w.Code != 401 {
			t.Fatalf("foreign signature %d", w.Code)
		}
	}
	token := resourceToken(t, h, key, nil)
	for _, malformed := range [][]byte{
		[]byte(`{"protocol_version":1}`),
		bytes.Replace(body, []byte(`"issuer_epoch":"1"`), []byte(`"issuer_epoch":1`), 1),
		bytes.Replace(body, []byte(`"revision":"1"`), []byte(`"revision":null`), 1),
		append([]byte(`{"protocol_version":1,`), body[1:]...),
	} {
		if w := resourceRequest(h, "/dsg/v1/resource/apply", token, malformed); w.Code == 200 {
			t.Fatal("malformed resource original accepted")
		}
	}
	r := httptest.NewRequest(http.MethodPost, "/dsg/v1/resource/apply", bytes.NewReader(body))
	r.Header.Add("Authorization", "Bearer "+token)
	r.Header.Add("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("duplicate authorization accepted")
	}
	for _, path := range []string{"/dsg/v1/resource/initialize", "/dsg/v1/resource/bootstrap", "/dsg/v1/resource/apply?override=1", "/dsg/v1/resource/%61pply"} {
		if w := resourceRequest(h, path, token, body); w.Code == 200 {
			t.Fatalf("unsafe path accepted %s", path)
		}
	}
	if k.writes != 0 {
		t.Fatal("unauthorized write")
	}
	if w := resourceRequest(h, "/dsg/v1/resource/apply", token, body); w.Code != 200 {
		t.Fatalf("valid original rejected %d %s", w.Code, w.Body)
	}
	if k.writes != 1 {
		t.Fatal("expected one actual executor call")
	}
}

func TestResourceHTTPBoundFenceRetainsOriginalAcrossLargeEpoch(t *testing.T) {
	h, key, g, k, c := resourceHTTPFixture(t)
	ctx := context.Background()
	if _, err := g.Apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	const epoch = control.Uint64(9007199254740993)
	token := resourceToken(t, h, key, func(c *resourceClaims) { c.IssuerEpoch = epoch })
	request := FenceRequest{ProtocolVersion: 1, Identity: c.Identity, IssuerEpoch: epoch}
	wrong := request
	wrong.Identity.CgroupInode++
	raw, _ := json.Marshal(wrong)
	if w := resourceRequest(h, "/dsg/v1/resource/fence", token, raw); w.Code != 409 {
		t.Fatalf("foreign Guest fenced %d", w.Code)
	}
	// 原epoch仍可写，证明错误身份没有提前推进floor。
	if _, err := g.Apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(request)
	if w := resourceRequest(h, "/dsg/v1/resource/fence", token, raw); w.Code != 200 {
		t.Fatalf("bound fence failed %d", w.Code)
	}
	raw, _ = json.Marshal(c)
	if w := resourceRequest(h, "/dsg/v1/resource/observe", token, raw); w.Code != 200 {
		t.Fatalf("new owner cannot read retained original %d", w.Code)
	}
	if w := resourceRequest(h, "/dsg/v1/resource/replay-original", token, raw); w.Code != 403 {
		t.Fatalf("old epoch replay authorized %d", w.Code)
	}
	if w := resourceRequest(h, "/dsg/v1/resource/apply", resourceToken(t, h, key, nil), raw); w.Code != 409 {
		t.Fatalf("stale signed owner wrote %d", w.Code)
	}
	if k.writes != 1 {
		t.Fatal("fence/readback caused unexpected write")
	}
}

func resourceClientFixture(t *testing.T, h http.Handler, c Command, token string, pending *Command) *Client {
	t.Helper()
	s := httptest.NewTLSServer(h)
	t.Cleanup(s.Close)
	roots := x509.NewCertPool()
	roots.AddCert(s.Certificate())
	client, err := NewClient(ClientConfig{Context: context.Background(), BaseURL: s.URL, Identity: c.Identity, IssuerEpoch: c.IssuerEpoch, Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}, Token: func(context.Context) (string, error) { return token, nil }, Pending: pending})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func TestResourceHTTPClientUnknownHasNoAutomaticReplayAndRestoresOriginal(t *testing.T) {
	h, key, _, k, c := resourceHTTPFixture(t)
	k.fail = true
	client := resourceClientFixture(t, h, c, resourceToken(t, h, key, nil), nil)
	if _, err := client.Apply(context.Background(), c); err == nil || client.Pending() == nil {
		t.Fatal("partial write lost original")
	}
	if _, err := client.Apply(context.Background(), c); err == nil || k.writes != 1 {
		t.Fatal("ordinary apply replayed unknown")
	}
	p := client.Pending()
	p.HugePools[0].Pages++
	if reflect.DeepEqual(*p, *client.Pending()) {
		t.Fatal("pending exposes shared pool slice")
	}
	restored := resourceClientFixture(t, h, c, resourceToken(t, h, key, nil), client.Pending())
	k.mu.Lock()
	k.fail = false
	k.mu.Unlock()
	if _, err := restored.ReplayOriginal(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if restored.Pending() != nil || k.writes != 2 {
		t.Fatal("explicit original not completed")
	}
	if _, err := restored.InspectIdentity(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestResourceHTTPClientLostCompletionUsesReadbackAndRejectsIncompleteEvidence(t *testing.T) {
	h, key, _, k, c := resourceHTTPFixture(t)
	var calls atomic.Int32
	drop := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.HasSuffix(r.URL.Path, "/apply") {
			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, r)
			if recorder.Code != 200 {
				t.Error("executor did not complete")
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
	client := resourceClientFixture(t, drop, c, resourceToken(t, h, key, nil), nil)
	if _, err := client.Apply(context.Background(), c); err == nil || client.Pending() == nil || calls.Load() != 1 || k.writes != 1 {
		t.Fatal("lost reply was replayed or original lost")
	}
	if _, err := client.Observe(context.Background(), c); err != nil || client.Pending() != nil || k.writes != 1 {
		t.Fatal("readback failed or caused write", err)
	}
	bad := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, `{"protocol_version":1}`)
	})
	malformed := resourceClientFixture(t, bad, c, resourceToken(t, h, key, nil), nil)
	if _, err := malformed.Apply(context.Background(), c); err == nil || malformed.Pending() == nil {
		t.Fatal("incomplete evidence cleared pending")
	}
}
