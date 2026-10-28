package resourceguard

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const FixedSocketPath = "/run/neon-dsga/resource.sock"

// FenceRequest不依赖PG boot。完整Guest/cgroup身份随原意图提交，防止把旧Guest
// 的接管请求应用到同名新Guest；身份比较和floor落盘在同一资源锁内完成。
type FenceRequest struct {
	ProtocolVersion uint32         `json:"protocol_version"`
	Identity        Identity       `json:"identity"`
	IssuerEpoch     control.Uint64 `json:"issuer_epoch"`
}

func (v *FenceRequest) UnmarshalJSON(b []byte) error {
	type plain FenceRequest
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	*v = FenceRequest(p)
	return nil
}

// resourceClaims复用正式控制面的短期DSGA控制凭据，而不是接受Pod SA/通用admin
// token。独立root服务重新验Ed25519签名、固定aud/scope/实例和时间；Compute已
// 验签不是root跳过校验的理由。epoch只是调用权限，实际执行仍由持久floor裁决。
type resourceClaims struct {
	Audience          string         `json:"aud"`
	Scope             string         `json:"scope"`
	ComputeID         string         `json:"compute_id"`
	VMUID             string         `json:"vm_uid"`
	ComputeInstanceID string         `json:"compute_instance_id"`
	IssuerEpoch       control.Uint64 `json:"issuer_epoch"`
	Expires           uint64         `json:"exp"`
	NotBefore         uint64         `json:"nbf"`
}

type resourceBackend interface {
	InspectIdentity() (Identity, error)
	InspectProcess(context.Context) (ProcessIdentity, error)
	InspectState(context.Context) (View, error)
	FenceBound(context.Context, Identity, control.Uint64) (Identity, error)
	Apply(context.Context, Command) (Evidence, error)
	Observe(context.Context, Command) (Evidence, error)
	ReplayOriginal(context.Context, Command) (Evidence, error)
	RepairHeld(context.Context, HeldRepairRequest) (Evidence, error)
}

type resourceHTTP struct {
	source SourceConfig
	key    ed25519.PublicKey
	kid    string
	guard  resourceBackend
	now    func() time.Time
}

// newResourceHandler只能由root生产入口使用封存原来源创建。没有initialize或
// bootstrap网络动作；缺/坏日志始终交给受控恢复。固定UDS没有任意路径或命令，
// JWT、请求和错误中的敏感原值都不写响应/日志。内部接口仅用于协议夹具验证。
func newResourceHandler(source SourceConfig, guard resourceBackend) (*resourceHTTP, error) {
	if err := source.Validate(); err != nil {
		return nil, err
	}
	key, _ := base64.RawURLEncoding.DecodeString(source.ControlPublicKey)
	hash := sha256.Sum256(key)
	return &resourceHTTP{source: source, key: key, kid: hex.EncodeToString(hash[:16]), guard: guard, now: time.Now}, nil
}

func canonicalJWTPart(s string, maximum int) ([]byte, error) {
	if len(s) == 0 || len(s) > maximum {
		return nil, fmt.Errorf("invalid token component")
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || base64.RawURLEncoding.EncodeToString(b) != s {
		return nil, fmt.Errorf("invalid token encoding")
	}
	return b, nil
}

func (h *resourceHTTP) verify(r *http.Request) (resourceClaims, error) {
	var c resourceClaims
	values := r.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || len(values[0]) > 8199 {
		return c, fmt.Errorf("credential required")
	}
	parts := strings.Split(strings.TrimPrefix(values[0], "Bearer "), ".")
	if len(parts) != 3 {
		return c, fmt.Errorf("invalid credential")
	}
	header, err := canonicalJWTPart(parts[0], 2048)
	if err != nil {
		return c, err
	}
	var algorithm struct {
		Alg  string `json:"alg"`
		Type string `json:"typ"`
		Kid  string `json:"kid"`
	}
	if control.UnmarshalObject(header, &algorithm) != nil || algorithm.Alg != "EdDSA" || algorithm.Type != "JWT" || algorithm.Kid != h.kid {
		return c, fmt.Errorf("wrong signing key")
	}
	signature, err := canonicalJWTPart(parts[2], 128)
	if err != nil || !ed25519.Verify(h.key, []byte(parts[0]+"."+parts[1]), signature) {
		return c, fmt.Errorf("invalid signature")
	}
	claims, err := canonicalJWTPart(parts[1], 6144)
	if err != nil || control.UnmarshalObject(claims, &c) != nil {
		return c, fmt.Errorf("invalid claims")
	}
	now := h.now().Unix()
	if now < 0 || c.Audience != "neon-dsga" || c.Scope != "compute_ctl:dsga" || control.ValidateID(c.ComputeID, "Compute") != nil || c.VMUID != h.source.Config.VMUID || c.ComputeInstanceID != h.source.Config.ComputeInstanceID || c.IssuerEpoch == 0 || c.Expires <= c.NotBefore || c.Expires-c.NotBefore > 300 || uint64(now) < c.NotBefore || uint64(now) >= c.Expires {
		return resourceClaims{}, fmt.Errorf("unauthorized resource credential")
	}
	return c, nil
}

func (h *resourceHTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	deny := func(code int) {
		w.WriteHeader(code)
		_, _ = io.WriteString(w, `{"error":"resource_unconfirmed","vm_shrink_permit":false}`)
	}
	claims, err := h.verify(r)
	if err != nil {
		deny(http.StatusUnauthorized)
		return
	}
	// Go/Rust/网关均只接受准确路径，禁止通过query/编码别名改变路由。资源动作
	// 不以PG是否running为前提：PG断连时原Guest约束/floor仍必须可以对账。
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" {
		deny(http.StatusBadRequest)
		return
	}
	var result any
	if r.URL.Path == "/dsg/v1/resource/identity" && r.Method == http.MethodGet {
		result, err = h.guard.InspectIdentity()
	} else if r.URL.Path == "/dsg/v1/resource/process" && r.Method == http.MethodGet {
		result, err = h.guard.InspectProcess(r.Context())
	} else if r.URL.Path == "/dsg/v1/resource/state" && r.Method == http.MethodGet {
		result, err = h.guard.InspectState(r.Context())
	} else {
		if r.Method != http.MethodPost {
			deny(http.StatusNotFound)
			return
		}
		switch r.URL.Path {
		case "/dsg/v1/resource/fence", "/dsg/v1/resource/apply", "/dsg/v1/resource/observe", "/dsg/v1/resource/replay-original", "/dsg/v1/resource/repair-held":
		default:
			deny(http.StatusNotFound)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" {
			deny(http.StatusBadRequest)
			return
		}
		raw, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 16*1024))
		if readErr != nil {
			deny(http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/dsg/v1/resource/fence" {
			var request FenceRequest
			if json.Unmarshal(raw, &request) != nil || request.ProtocolVersion != 1 || validateResourceContext(request.Identity, request.IssuerEpoch, "fence") != nil || request.IssuerEpoch != claims.IssuerEpoch || request.Identity.VMUID != claims.VMUID || request.Identity.ComputeInstanceID != claims.ComputeInstanceID {
				deny(http.StatusForbidden)
				return
			}
			result, err = h.guard.FenceBound(r.Context(), request.Identity, request.IssuerEpoch)
		} else if r.URL.Path == "/dsg/v1/resource/repair-held" {
			// 新owner只能用自身短期签名/floor选择完整原持有来源；正文
			// 没有限额/PID/路径，root锁内再次核摘要，不能借网关认证改绑。
			var request HeldRepairRequest
			if json.Unmarshal(raw, &request) != nil || request.IssuerEpoch != claims.IssuerEpoch || request.Identity.VMUID != claims.VMUID || request.Identity.ComputeInstanceID != claims.ComputeInstanceID {
				deny(http.StatusForbidden)
				return
			}
			result, err = h.guard.RepairHeld(r.Context(), request)
		} else {
			var command Command
			observe := r.URL.Path == "/dsg/v1/resource/observe"
			if json.Unmarshal(raw, &command) != nil || command.Validate() != nil || command.Identity.VMUID != claims.VMUID || command.Identity.ComputeInstanceID != claims.ComputeInstanceID || command.IssuerEpoch > claims.IssuerEpoch || (!observe && command.IssuerEpoch != claims.IssuerEpoch) {
				deny(http.StatusForbidden)
				return
			}
			switch r.URL.Path {
			case "/dsg/v1/resource/apply":
				result, err = h.guard.Apply(r.Context(), command)
			case "/dsg/v1/resource/observe":
				result, err = h.guard.Observe(r.Context(), command)
			case "/dsg/v1/resource/replay-original":
				result, err = h.guard.ReplayOriginal(r.Context(), command)
			}
		}
	}
	// 包括参数/内核/持久化/超时失败均不产生NoEffect。上层必须保留写前原意图；
	// 不把内部错误、部分Evidence或可能已写入的限额包装成成功回执。
	if err != nil {
		deny(http.StatusConflict)
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}
