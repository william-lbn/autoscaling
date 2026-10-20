// Package compute 是Neon正式DSGA HTTP端点的Go消费者，不把HTTP成功当作VM许可。
// 同一实例长期拥有独立执行/观察连接及内存栅栏；跨进程恢复必须由持久journal驱动。
package compute

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const maxResponseBytes = 32 * 1024

// Config只能来自可信协调器配置/持久恢复，不能由普通业务请求提供。固定完整实例
// 和当前issuer epoch，刷新凭据不改变身份。Token从独立认证服务取得，不写日志/账本。
type Config struct {
	BaseURL     string
	Instance    control.Instance
	IssuerEpoch control.Uint64
	Timeout     time.Duration
	Transport   *http.Transport
	// 执行和观察可同时刷新凭据，此回调须可并发调用；错误可能含秘密，绝不回显。
	Token func(context.Context) (string, error)
	// Pending由持久日志恢复原意图。可为旧boot/epoch，但VM/Compute归属必须一致；
	// 新owner不能继续执行旧意图，只能观察/取消并在外层完成实际效果与预算对账。
	Pending *control.Request
}

// CallError不回显服务端body、Bearer、凭据提供器错误或完整URL。开始Do后的所有
// 错误都保守归为MayHaveExecuted，含HTTP错误/超时/坏JSON，必须保留原意图对账。
type CallError struct {
	MayHaveExecuted bool
	Code            string
}

func (err *CallError) Error() string {
	return fmt.Sprintf("DSGA HTTP call failed: %s (may_have_executed=%t)", err.Code, err.MayHaveExecuted)
}

type Client struct {
	base       string
	instance   control.Instance
	epoch      control.Uint64
	token      func(context.Context) (string, error)
	execution  *http.Client
	observer   *http.Client
	transports [2]*http.Transport
	mu         sync.Mutex
	inFlight   bool
	pending    *control.Request
}

// New验证固定TLS端点并实时协商ABI/boot。不会在断连后自动新建Client或改变boot，
// 否则可能丢失未确认操作；调用者必须将原journal恢复到新Client并处理旧效果。
func New(ctx context.Context, config Config) (*Client, error) {
	if err := config.Instance.Validate(); err != nil {
		return nil, &CallError{MayHaveExecuted: false, Code: "invalid_binding"}
	}
	endpoint, err := url.Parse(config.BaseURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") || endpoint.RawPath != "" || config.IssuerEpoch == 0 || config.Token == nil || config.Timeout <= 0 || config.Timeout > 2*time.Minute {
		return nil, &CallError{MayHaveExecuted: false, Code: "invalid_configuration"}
	}
	transport := config.Transport
	if transport == nil {
		var valid bool
		transport, valid = http.DefaultTransport.(*http.Transport)
		if !valid {
			return nil, &CallError{MayHaveExecuted: false, Code: "invalid_default_transport"}
		}
	}
	if transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify {
		return nil, &CallError{MayHaveExecuted: false, Code: "insecure_transport"}
	}
	client := new(Client)
	client.base = strings.TrimSuffix(config.BaseURL, "/")
	client.instance = config.Instance
	client.epoch = config.IssuerEpoch
	client.token = config.Token
	if config.Pending != nil {
		request := *config.Pending
		if request.Validate() != nil || request.Key.Instance.VMUID != config.Instance.VMUID || request.Key.Instance.ComputeInstanceID != config.Instance.ComputeInstanceID || request.Key.IssuerEpoch > config.IssuerEpoch {
			return nil, &CallError{MayHaveExecuted: false, Code: "invalid_pending_intent"}
		}
		client.pending = &request
	}
	for i := range client.transports {
		owned := transport.Clone()
		// 执行与观察各自拥有连接池，执行阻塞时仍可查询/业务取消。POST不带
		// Idempotency-Key，不启用透明业务重试；重放只能由原完整意图显式发起。
		owned.MaxConnsPerHost = 1
		owned.DisableCompression = true
		client.transports[i] = owned
		httpClient := new(http.Client)
		httpClient.Transport = owned
		httpClient.Timeout = config.Timeout
		// 即使307/308也不重发POST，且绝不将管理凭据带到另一个服务。
		httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		if i == 0 {
			client.execution = httpClient
		} else {
			client.observer = httpClient
		}
	}
	if _, err := client.Capabilities(ctx); err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	return client, nil
}

func (client *Client) CloseIdleConnections() {
	for _, transport := range client.transports {
		transport.CloseIdleConnections()
	}
}

// Pending只返回副本，关闭空闲连接/一次查询成功都不会清除此栅栏。它不替代跨
// 进程journal，不能通过重新构造空Client来绕过外层未知动作或资源承诺。
func (client *Client) Pending() *control.Request {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.pending == nil {
		return nil
	}
	request := *client.pending
	return &request
}

func (client *Client) prepare(ctx context.Context, path, method string, payload []byte) (*http.Request, error) {
	if ctx.Err() != nil {
		return nil, &CallError{MayHaveExecuted: false, Code: "context_cancelled"}
	}
	token, err := client.token(ctx)
	if err != nil || len(token) == 0 || len(token) > 8192 {
		return nil, &CallError{MayHaveExecuted: false, Code: "credential_unavailable"}
	}
	for _, b := range []byte(token) {
		if b < 0x21 || b > 0x7e {
			return nil, &CallError{MayHaveExecuted: false, Code: "invalid_credential"}
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, client.base+path, bytes.NewReader(payload))
	if err != nil {
		return nil, &CallError{MayHaveExecuted: false, Code: "invalid_http_request"}
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Cache-Control", "no-store")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	return request, nil
}

func exchange(httpClient *http.Client, request *http.Request, into any) error {
	if request.Context().Err() != nil {
		return &CallError{MayHaveExecuted: false, Code: "context_cancelled"}
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return &CallError{MayHaveExecuted: true, Code: "transport_unconfirmed"}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &CallError{MayHaveExecuted: true, Code: "http_unconfirmed"}
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	noStore := false
	for _, directive := range strings.Split(response.Header.Get("Cache-Control"), ",") {
		noStore = noStore || strings.EqualFold(strings.TrimSpace(directive), "no-store")
	}
	if err != nil || media != "application/json" || !noStore || response.Header.Get("Content-Encoding") != "" {
		return &CallError{MayHaveExecuted: true, Code: "invalid_response_headers"}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes {
		return &CallError{MayHaveExecuted: true, Code: "response_unconfirmed"}
	}
	if err := json.Unmarshal(body, into); err != nil {
		return &CallError{MayHaveExecuted: true, Code: "invalid_response_schema"}
	}
	return nil
}

func (client *Client) read(ctx context.Context, path string, into any) error {
	request, err := client.prepare(ctx, path, http.MethodGet, nil)
	if err != nil {
		return err
	}
	return exchange(client.observer, request, into)
}

func (client *Client) Capabilities(ctx context.Context) (control.Capabilities, error) {
	var fact control.Capabilities
	if err := client.read(ctx, "/dsg/v1/capabilities", &fact); err != nil {
		return control.Capabilities{}, err
	}
	bound, err := fact.Bind(client.instance.VMUID, client.instance.ComputeInstanceID)
	if err != nil || bound != client.instance {
		return control.Capabilities{}, &CallError{MayHaveExecuted: true, Code: "capability_binding_changed"}
	}
	return fact, nil
}

func (client *Client) Memory(ctx context.Context) (control.MemoryResponse, error) {
	var fact control.MemoryResponse
	if err := client.read(ctx, "/dsg/v1/memory", &fact); err != nil {
		return control.MemoryResponse{}, err
	}
	if err := fact.ValidateFor(client.instance); err != nil {
		return control.MemoryResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_memory_fact"}
	}
	return fact, nil
}

func (client *Client) Resize(ctx context.Context, intent control.Request) (fact control.ControlResponse, err error) {
	if intent.Validate() != nil || intent.Key.Instance != client.instance || intent.Key.IssuerEpoch != client.epoch {
		return fact, &CallError{MayHaveExecuted: false, Code: "invalid_execution_binding"}
	}
	payload, _ := json.Marshal(intent)
	request, err := client.prepare(ctx, "/dsg/v1/resize", http.MethodPost, payload)
	if err != nil {
		return fact, err
	}
	client.mu.Lock()
	if client.inFlight || (client.pending != nil && *client.pending != intent) {
		client.mu.Unlock()
		return fact, &CallError{MayHaveExecuted: false, Code: "unresolved_original_intent"}
	}
	wasNew := client.pending == nil
	retained := intent
	client.pending = &retained
	client.inFlight = true
	client.mu.Unlock()
	resolved := false
	defer func() {
		client.mu.Lock()
		defer client.mu.Unlock()
		client.inFlight = false
		var local *CallError
		knownLocal := errors.As(err, &local)
		if resolved || (wasNew && knownLocal && !local.MayHaveExecuted) {
			client.pending = nil
		}
	}()
	// 网络期间不持mu。超时/ctx取消/panic默认保留原意图，状态/业务取消另走观察池。
	if err := exchange(client.execution, request, &fact); err != nil {
		return control.ControlResponse{}, err
	}
	if fact.ValidateResize(intent, client.instance) != nil || !allowed(fact.Receipt.Disposition, "resize") {
		return control.ControlResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_resize_fact"}
	}
	resolved = fact.Receipt.Disposition == control.ReceiptBusy || fact.Receipt.Disposition == control.ReceiptRejected ||
		(fact.Receipt.Result != nil && fact.Receipt.Result.Operation.Phase != control.Preparing)
	return fact, nil
}

func (client *Client) Status(ctx context.Context, query control.Query) (control.ControlResponse, error) {
	return client.query(ctx, query, "status")
}

func (client *Client) Cancel(ctx context.Context, query control.Query) (control.ControlResponse, error) {
	return client.query(ctx, query, "cancel")
}

func (client *Client) query(ctx context.Context, query control.Query, action string) (control.ControlResponse, error) {
	if query.Validate() != nil || query.Key.Instance.VMUID != client.instance.VMUID || query.Key.Instance.ComputeInstanceID != client.instance.ComputeInstanceID || query.Key.IssuerEpoch > client.epoch {
		return control.ControlResponse{}, &CallError{MayHaveExecuted: false, Code: "invalid_query_binding"}
	}
	payload, _ := json.Marshal(query)
	request, err := client.prepare(ctx, "/dsg/v1/resize/"+action, http.MethodPost, payload)
	if err != nil {
		return control.ControlResponse{}, err
	}
	var fact control.ControlResponse
	if err := exchange(client.observer, request, &fact); err != nil {
		return control.ControlResponse{}, err
	}
	if fact.ValidateFor(query.Key, client.instance) != nil || !allowed(fact.Receipt.Disposition, action) {
		return control.ControlResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_query_fact"}
	}
	return fact, nil
}

func allowed(disposition control.ReceiptDisposition, action string) bool {
	switch action {
	case "resize":
		return disposition == control.ReceiptAccepted || disposition == control.ReceiptReplayed || disposition == control.ReceiptBusy || disposition == control.ReceiptStale || disposition == control.ReceiptConflict || disposition == control.ReceiptRejected
	case "status":
		return disposition == control.ReceiptReplayed || disposition == control.ReceiptStale || disposition == control.ReceiptConflict || disposition == control.ReceiptResultExpired || disposition == control.ReceiptNotFound
	case "cancel":
		return disposition == control.ReceiptCancelRequested || disposition == control.ReceiptTooLate || disposition == control.ReceiptStale || disposition == control.ReceiptConflict || disposition == control.ReceiptResultExpired || disposition == control.ReceiptNotFound
	}
	return false
}
