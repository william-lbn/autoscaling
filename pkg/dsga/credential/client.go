// Package credential消费控制面独占TLS身份的DSGA短令牌。它不创造领导权、预算
// 或VM许可；刷新凭据不更换Compute/PG boot，不接触长期SDK的未知原意图栅栏。
package credential

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
)

type Config struct {
	BaseURL   string
	CAFile    string
	TokenFile string
	Timeout   time.Duration
}

// Authority必须是受信协调入口产生的长期owner Session。接口便于本地验证，
// 不是远端授权能力：中心仍独立TokenReview及读取真实Lease/完整journal。
type Authority interface {
	Context() context.Context
	Check(context.Context) error
	Ownership() owner.Ownership
}

type Error struct{ Code string }

func (err *Error) Error() string { return "DSGA credential unavailable: " + err.Code }
func failure(code string) error  { return &Error{Code: code} }

// Identity只含公开绑定，不暴露Bearer。首次Compute实例来自可信签发器；一旦
// 固定便不随刷新改变。PG boot仍由持久恢复/受控发现处理，不从这个返回值猜测。
type Identity struct {
	ComputeID         string
	VMUID             string
	ComputeInstanceID string
	IssuerEpoch       control.Uint64
}

type grant struct {
	TokenType         string         `json:"token_type"`
	AccessToken       string         `json:"access_token"`
	ExpiresAt         int64          `json:"expires_at"`
	ComputeID         string         `json:"compute_id"`
	VMUID             string         `json:"vm_uid"`
	ComputeInstanceID string         `json:"compute_instance_id"`
	IssuerEpoch       control.Uint64 `json:"issuer_epoch"`
}

type Client struct {
	authority  Authority
	config     Config
	proof      owner.Ownership
	root       context.Context
	address    string
	serverName string
	http       *http.Client
	transport  *http.Transport
	refreshGap time.Duration
	now        func() time.Time
	// mu只保护缓存/刷新登记，不跨磁盘、网络或Authority.Check持锁。并发执行/
	// 观察回调只允许一条刷新；等待者有自己的期限，取消一个等待者不取消领导者。
	mu       sync.Mutex
	cache    grant
	instance string
	wait     chan struct{}
	poisoned bool
}

func (client *Client) String() string   { return "DSGA credential client (redacted)" }
func (client *Client) GoString() string { return client.String() }

func New(authority Authority, config Config) (*Client, error) {
	if authority == nil || authority.Context() == nil || authority.Context().Err() != nil {
		return nil, failure("owner_unavailable")
	}
	proof := authority.Ownership()
	endpoint, err := url.Parse(config.BaseURL)
	_, podBound := owner.HolderPodUID(proof.HolderID)
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.Opaque != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") || endpoint.RawPath != "" || !podBound || proof.IssuerEpoch == 0 || proof.LeaseUID == "" ||
		len(validation.IsDNS1123Label(proof.Namespace)) != 0 || len(validation.IsDNS1123Label(proof.VMName)) != 0 || control.ValidateID(string(proof.VMUID), "VM") != nil ||
		!filepath.IsAbs(config.CAFile) || !filepath.IsAbs(config.TokenFile) || config.Timeout <= 0 || config.Timeout > 30*time.Second {
		return nil, failure("invalid_configuration")
	}
	if _, err := managementRoots(config.CAFile); err != nil {
		return nil, err
	}
	port := endpoint.Port()
	if port == "" {
		port = "443"
	}
	var entropy [8]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return nil, failure("refresh_entropy_unavailable")
	}
	client := new(Client)
	client.authority, client.config, client.proof, client.root = authority, config, proof, authority.Context()
	client.config.BaseURL = strings.TrimSuffix(config.BaseURL, "/")
	client.address = net.JoinHostPort(endpoint.Hostname(), port)
	client.serverName = proof.VMName + "-management." + proof.Namespace + ".svc"
	client.now = time.Now
	client.refreshGap = 10*time.Second + time.Duration(binary.LittleEndian.Uint64(entropy[:])%uint64(5*time.Second))
	client.transport = client.ManagementTransport()
	client.transport.MaxConnsPerHost = 1
	client.http = &http.Client{
		Transport: client.transport, Timeout: config.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return client, nil
}

func (client *Client) callContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, client.config.Timeout)
	stop := context.AfterFunc(client.root, cancel)
	if client.root.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}

func (client *Client) check(ctx context.Context) error {
	if ctx.Err() != nil || client.root.Err() != nil || client.authority.Ownership() != client.proof {
		return failure("owner_binding_changed")
	}
	if err := client.authority.Check(ctx); err != nil {
		return failure("owner_unconfirmed")
	}
	if ctx.Err() != nil || client.root.Err() != nil || client.authority.Ownership() != client.proof {
		return failure("owner_binding_changed")
	}
	return nil
}

func (client *Client) fetch(ctx context.Context) (grant, error) {
	var result grant
	data, err := readBoundedFile(client.config.TokenFile, 16*1024)
	if err != nil {
		return result, err
	}
	bearer := strings.TrimSpace(string(data))
	if !visibleToken(bearer, 16*1024) {
		return result, failure("invalid_projected_token")
	}
	payload, _ := json.Marshal(client.proof)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.config.BaseURL+"/computes/"+client.proof.VMName+"/dsga-credentials", bytes.NewReader(payload))
	if err != nil {
		return result, failure("invalid_request")
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Cache-Control", "no-store")
	response, err := client.http.Do(request)
	if err != nil {
		return result, failure("issuer_unavailable")
	}
	defer response.Body.Close()
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	noStore := false
	for _, directive := range strings.Split(response.Header.Get("Cache-Control"), ",") {
		noStore = noStore || strings.EqualFold(strings.TrimSpace(directive), "no-store")
	}
	if response.StatusCode != http.StatusOK || err != nil || media != "application/json" || !noStore || response.Header.Get("Content-Encoding") != "" {
		return result, failure("issuer_response_rejected")
	}
	data, err = io.ReadAll(io.LimitReader(response.Body, 16*1024+1))
	if err != nil || len(data) > 16*1024 {
		return result, failure("issuer_response_limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&result) != nil || decoder.Decode(new(any)) != io.EOF || result.TokenType != "Bearer" || !visibleToken(result.AccessToken, 8192) ||
		result.ComputeID != client.proof.VMName || result.VMUID != string(client.proof.VMUID) || result.IssuerEpoch != client.proof.IssuerEpoch ||
		control.ValidateID(result.ComputeInstanceID, "Compute") != nil || result.ExpiresAt <= client.now().Add(client.refreshGap).Unix() || result.ExpiresAt > client.now().Add(300*time.Second).Unix() {
		return grant{}, failure("invalid_grant")
	}
	if err := client.check(ctx); err != nil {
		return grant{}, err
	}
	return result, nil
}

func visibleToken(token string, maximum int) bool {
	if len(token) == 0 || len(token) > maximum {
		return false
	}
	for _, b := range []byte(token) {
		if b < 0x21 || b > 0x7e {
			return false
		}
	}
	return true
}

// current只缓存同一个已确认owner的短令牌，每次调用仍Check。失败不退回旧缓存；
// Compute实例一旦变化则本Client永久失败关闭，不因下一次回复又变回旧值而恢复。
// 这不清PG Pending，调用方必须按原journal完成受控恢复。
func (client *Client) current(parent context.Context) (grant, error) {
	ctx, cancel := client.callContext(parent)
	defer cancel()
	for {
		if err := client.check(ctx); err != nil {
			return grant{}, err
		}
		client.mu.Lock()
		if client.poisoned {
			client.mu.Unlock()
			return grant{}, failure("compute_instance_changed")
		}
		if client.cache.AccessToken != "" && client.now().Before(time.Unix(client.cache.ExpiresAt, 0).Add(-client.refreshGap)) {
			cached := client.cache
			client.mu.Unlock()
			if ctx.Err() != nil || client.root.Err() != nil {
				return grant{}, failure("owner_unavailable")
			}
			return cached, nil
		}
		if wait := client.wait; wait != nil {
			client.mu.Unlock()
			select {
			case <-ctx.Done():
				return grant{}, failure("refresh_wait_cancelled")
			case <-wait:
				continue
			}
		}
		wait := make(chan struct{})
		client.wait = wait
		client.mu.Unlock()
		result, err := client.fetch(ctx)
		client.mu.Lock()
		if err == nil && client.instance != "" && result.ComputeInstanceID != client.instance {
			client.poisoned = true
			err = failure("compute_instance_changed")
		}
		if err == nil {
			client.instance = result.ComputeInstanceID
			client.cache = result
		} else {
			var empty grant
			client.cache = empty
		}
		client.wait = nil
		close(wait)
		client.mu.Unlock()
		return result, err
	}
}

func (client *Client) Token(ctx context.Context) (string, error) {
	result, err := client.current(ctx)
	if err != nil {
		return "", err
	}
	return result.AccessToken, nil
}

func (client *Client) Identity(ctx context.Context) (Identity, error) {
	result, err := client.current(ctx)
	if err != nil {
		return Identity{}, err
	}
	return Identity{ComputeID: result.ComputeID, VMUID: result.VMUID, ComputeInstanceID: result.ComputeInstanceID, IssuerEpoch: result.IssuerEpoch}, nil
}
func (client *Client) CloseIdleConnections() { client.transport.CloseIdleConnections() }
