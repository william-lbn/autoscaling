package credential

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
)

const arrayAuthorityScope = "compute_ctl:dsga:array-authority"

// 专用凭据只签一个准确HTTP动作；摘要由可信executor的canonical正文产生，
// 不从Guest PG packet/JournSHA反推。此类型与旧通用Token/cache完全分离。
type arrayCredentialBinding struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Method          string `json:"method"`
	Path            string `json:"path"`
	BodySHA256      string `json:"body_sha256"`
}

func (b *arrayCredentialBinding) UnmarshalJSON(raw []byte) error {
	type plain arrayCredentialBinding
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*b = arrayCredentialBinding(next)
	return nil
}
func (b arrayCredentialBinding) validate() error {
	if b.ProtocolVersion != 1 || b.Method != http.MethodPost || len(b.BodySHA256) != 64 || b.BodySHA256 == strings.Repeat("0", 64) {
		return failure("invalid_array_binding")
	}
	switch b.Path {
	case "/dsg/v1/resource/array-authorize", "/dsg/v1/resource/array-observe", "/dsg/v1/resource/array-replay-original":
	default:
		return failure("invalid_array_binding")
	}
	decoded, err := hex.DecodeString(b.BodySHA256)
	if err != nil || hex.EncodeToString(decoded) != b.BodySHA256 {
		return failure("invalid_array_binding")
	}
	return nil
}

type arrayCredentialRequest struct {
	ProtocolVersion uint32                 `json:"protocol_version"`
	Owner           owner.Ownership        `json:"owner"`
	Request         arrayCredentialBinding `json:"request"`
}
type arrayCredentialGrant struct {
	TokenType         string                 `json:"token_type"`
	AccessToken       string                 `json:"access_token"`
	ExpiresAt         int64                  `json:"expires_at"`
	ComputeID         string                 `json:"compute_id"`
	VMUID             string                 `json:"vm_uid"`
	ComputeInstanceID string                 `json:"compute_instance_id"`
	IssuerEpoch       control.Uint64         `json:"issuer_epoch"`
	Scope             string                 `json:"scope"`
	Request           arrayCredentialBinding `json:"request"`
}

// ArrayAuthorityToken由真实Start CAS之后的原executor调用。专用token不缓存，
// 因而不存在以同epoch通用缓存或另一body/path的缓存替代本次签名的途径。
// 新Compute实例仍与本Client首次可信grant永久绑定，失败不清旧资源Pending。
func (client *Client) ArrayAuthorityToken(parent context.Context, method, path, bodySHA string) (string, error) {
	binding := arrayCredentialBinding{ProtocolVersion: 1, Method: method, Path: path, BodySHA256: bodySHA}
	if err := binding.validate(); err != nil {
		return "", err
	}
	ctx, cancel := client.callContext(parent)
	defer cancel()
	if err := client.check(ctx); err != nil {
		return "", err
	}
	client.mu.Lock()
	poisoned := client.poisoned
	client.mu.Unlock()
	if poisoned {
		return "", failure("compute_instance_changed")
	}
	projected, err := readBoundedFile(client.config.TokenFile, 16*1024)
	if err != nil {
		return "", err
	}
	bearer := strings.TrimSpace(string(projected))
	if !visibleToken(bearer, 16*1024) {
		return "", failure("invalid_projected_token")
	}
	raw, err := json.Marshal(arrayCredentialRequest{ProtocolVersion: 1, Owner: client.proof, Request: binding})
	if err != nil {
		return "", failure("invalid_array_request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.config.BaseURL+"/computes/"+client.proof.VMName+"/dsga-array-credentials", bytes.NewReader(raw))
	if err != nil {
		return "", failure("invalid_array_request")
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Cache-Control", "no-store")
	response, err := client.http.Do(request)
	if err != nil {
		return "", failure("issuer_unavailable")
	}
	defer response.Body.Close()
	media, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	noStore := false
	for _, directive := range strings.Split(response.Header.Get("Cache-Control"), ",") {
		noStore = noStore || strings.EqualFold(strings.TrimSpace(directive), "no-store")
	}
	if response.StatusCode != http.StatusOK || mediaErr != nil || media != "application/json" || !noStore || response.Header.Get("Content-Encoding") != "" {
		return "", failure("issuer_response_rejected")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 16*1024+1))
	if err != nil || len(data) > 16*1024 {
		return "", failure("issuer_response_limit")
	}
	var result arrayCredentialGrant
	// 原严格JSON同时拒未知、大小写别名、重复键，不让回显覆盖签名语义。
	if control.UnmarshalObject(data, &result) != nil || result.TokenType != "Bearer" || !visibleToken(result.AccessToken, 8192) || result.Scope != arrayAuthorityScope || result.Request != binding || result.Request.validate() != nil || result.ComputeID != client.proof.VMName || result.VMUID != string(client.proof.VMUID) || result.IssuerEpoch != client.proof.IssuerEpoch || control.ValidateID(result.ComputeInstanceID, "Compute") != nil || result.ExpiresAt <= client.now().Add(client.refreshGap).Unix() || result.ExpiresAt > client.now().Add(300*time.Second).Unix() {
		return "", failure("invalid_array_grant")
	}
	if err := client.check(ctx); err != nil {
		return "", err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.poisoned || client.instance != "" && client.instance != result.ComputeInstanceID {
		client.poisoned = true
		return "", failure("compute_instance_changed")
	}
	client.instance = result.ComputeInstanceID
	return result.AccessToken, nil
}
