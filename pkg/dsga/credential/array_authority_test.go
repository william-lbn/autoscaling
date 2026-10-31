package credential

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type arrayCredentialTransport func(*http.Request) (*http.Response, error)

func (f arrayCredentialTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// PG17-U-ARRAY-CREDENTIAL-001：签发器为明确HTTP模型；真实TLS身份沿旧直接回归。
func TestArrayCredentialExactBindingNoGenericCache(t *testing.T) {
	_, _, _, client, _ := fixture(t, false)
	calls := 0
	binding := arrayCredentialBinding{1, "POST", "/dsg/v1/resource/array-authorize", strings.Repeat("a", 64)}
	client.cache = grant{AccessToken: "generic-must-not-be-used", ExpiresAt: time.Now().Add(time.Minute).Unix()}
	client.http.Transport = arrayCredentialTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		var request arrayCredentialRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.ProtocolVersion != 1 || request.Owner != testProof() || request.Request != binding || r.URL.Path != "/computes/"+testProof().VMName+"/dsga-array-credentials" || r.Header.Get("Authorization") != "Bearer projected-SA-token" {
			t.Fatal("original owner/body binding changed")
		}
		result := arrayCredentialGrant{TokenType: "Bearer", AccessToken: "bound-test-only-token", ExpiresAt: client.now().Add(time.Minute).Unix(), ComputeID: testProof().VMName, VMUID: string(testProof().VMUID), ComputeInstanceID: "compute-1", IssuerEpoch: 2, Scope: arrayAuthorityScope, Request: binding}
		raw, _ := json.Marshal(result)
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}, "Cache-Control": []string{"no-store"}}, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
	})
	for i := 0; i < 2; i++ {
		token, err := client.ArrayAuthorityToken(context.Background(), binding.Method, binding.Path, binding.BodySHA256)
		if err != nil || token != "bound-test-only-token" {
			t.Fatal(token, err)
		}
	}
	if calls != 2 || client.cache.AccessToken != "generic-must-not-be-used" {
		t.Fatal("special token reused generic cache or poisoned it")
	}
}

// PG17-U-ARRAY-CREDENTIAL-002：未知/0摘要/别名在投影SA与HTTP之前拒绝。
func TestArrayCredentialRejectsBindingAndGrantDrift(t *testing.T) {
	for _, name := range []string{"zero", "uppercase", "query", "alias", "method", "empty"} {
		t.Run(name, func(t *testing.T) {
			_, _, _, c, _ := fixture(t, false)
			m, p, d := "POST", "/dsg/v1/resource/array-authorize", strings.Repeat("a", 64)
			switch name {
			case "zero":
				d = strings.Repeat("0", 64)
			case "uppercase":
				d = strings.Repeat("A", 64)
			case "query":
				p += "?x=1"
			case "alias":
				p = "/dsg/v1/resource/%61rray-authorize"
			case "method":
				m = "GET"
			case "empty":
				d = ""
			}
			c.http.Transport = arrayCredentialTransport(func(*http.Request) (*http.Response, error) { t.Fatal("bad binding sent SA"); return nil, nil })
			if _, err := c.ArrayAuthorityToken(context.Background(), m, p, d); err == nil {
				t.Fatal("bad binding accepted")
			}
		})
	}
	for _, name := range []string{"scope", "path", "hash", "epoch", "instance", "expiry", "unknown", "nested_alias"} {
		t.Run(name, func(t *testing.T) {
			_, _, _, c, _ := fixture(t, false)
			binding := arrayCredentialBinding{1, "POST", "/dsg/v1/resource/array-authorize", strings.Repeat("a", 64)}
			c.instance = "compute-1"
			c.http.Transport = arrayCredentialTransport(func(*http.Request) (*http.Response, error) {
				g := arrayCredentialGrant{"Bearer", "test-only-token", c.now().Add(time.Minute).Unix(), testProof().VMName, string(testProof().VMUID), "compute-1", 2, arrayAuthorityScope, binding}
				switch name {
				case "scope":
					g.Scope = "compute_ctl:dsga"
				case "path":
					g.Request.Path = "/dsg/v1/resource/array-observe"
				case "hash":
					g.Request.BodySHA256 = strings.Repeat("b", 64)
				case "epoch":
					g.IssuerEpoch = 3
				case "instance":
					g.ComputeInstanceID = "compute-2"
				case "expiry":
					g.ExpiresAt = c.now().Unix()
				}
				raw, _ := json.Marshal(g)
				if name == "unknown" {
					raw = append(raw[:len(raw)-1:len(raw)-1], []byte(`,"unknown":1}`)...)
				}
				if name == "nested_alias" {
					raw = []byte(strings.Replace(string(raw), `"body_sha256":`, `"Body_sha256":`, 1))
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}, "Cache-Control": []string{"no-store"}}, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
			})
			if _, err := c.ArrayAuthorityToken(context.Background(), binding.Method, binding.Path, binding.BodySHA256); err == nil {
				t.Fatal("drift grant accepted")
			}
		})
	}
}
