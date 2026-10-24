package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ClientConfig只能由可信owner/journal构造。身份按Guest而非PG boot冻结，重建时
// 必须恢复Pending完整原意图；本地栅栏不是持久账本，调用前外层仍须写前持久化。
type ClientConfig struct {
	Context     context.Context
	BaseURL     string
	Identity    Identity
	IssuerEpoch control.Uint64
	Timeout     time.Duration
	Transport   *http.Transport
	Token       func(context.Context) (string, error)
	Pending     *Command
}

type ClientError struct {
	MayHaveExecuted bool
	Code            string
}

func (e *ClientError) Error() string { return "DSGA resource call: " + e.Code }

type Client struct {
	config    ClientConfig
	http      [2]*http.Client
	transport [2]*http.Transport
	mu        sync.Mutex
	busy      bool
	pending   *Command
}

func cloneCommand(c Command) Command { c.HugePools = append([]HugePool(nil), c.HugePools...); return c }

func NewClient(c ClientConfig) (*Client, error) {
	return newClient(c, false)
}

func newClient(c ClientConfig, discovery bool) (*Client, error) {
	u, err := url.Parse(c.BaseURL)
	validIdentity := validateResourceContext(c.Identity, c.IssuerEpoch, "client") == nil
	if discovery {
		validIdentity = c.IssuerEpoch != 0 && control.ValidateID(c.Identity.VMUID, "VM") == nil && control.ValidateID(c.Identity.ComputeInstanceID, "Compute") == nil && c.Identity.GuestBootID == "" && c.Identity.CgroupDevice == 0 && c.Identity.CgroupInode == 0 && c.Pending == nil
	}
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || c.Context == nil || c.Context.Err() != nil || c.Token == nil || c.Timeout <= 0 || c.Timeout > 2*time.Minute || !validIdentity {
		return nil, &ClientError{Code: "invalid_configuration"}
	}
	if c.Transport == nil || (c.Transport.TLSClientConfig != nil && c.Transport.TLSClientConfig.InsecureSkipVerify) {
		return nil, &ClientError{Code: "trusted_transport_required"}
	}
	client := &Client{config: c}
	client.config.BaseURL = strings.TrimSuffix(c.BaseURL, "/")
	if c.Pending != nil {
		p := cloneCommand(*c.Pending)
		if p.Validate() != nil || p.Identity != c.Identity || p.IssuerEpoch > c.IssuerEpoch {
			return nil, &ClientError{Code: "invalid_pending"}
		}
		client.pending = &p
	}
	// 独立观察池不被阻塞的执行连接占满；禁用环境proxy/压缩及重定向，不把
	// Bearer发往Location。正式管理transport仍逐次核对CA/SNI和可信服务身份。
	for i := range client.http {
		transport := c.Transport.Clone()
		transport.Proxy = nil
		transport.DisableCompression = true
		client.transport[i] = transport
		client.http[i] = &http.Client{Transport: transport, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	return client, nil
}

// DiscoverIdentity只提供只读发现，不在Client内部改绑。可信协调器必须先确认外层
// 从未有过操作/资源绑定，再将原Guest身份持久化；已有历史或未知动作只能恢复
// 原身份。PG断连不妨碍这个独立资源域，不能借重新发现抹除旧Guest效果。
func DiscoverIdentity(ctx context.Context, config ClientConfig) (Identity, error) {
	c, err := newClient(config, true)
	if err != nil {
		return Identity{}, err
	}
	defer c.CloseIdleConnections()
	var id Identity
	if err := c.call(ctx, true, "identity", nil, &id); err != nil {
		return id, err
	}
	if id.Validate() != nil || id.VMUID != config.Identity.VMUID || id.ComputeInstanceID != config.Identity.ComputeInstanceID {
		return Identity{}, &ClientError{Code: "guest_discovery_unconfirmed"}
	}
	return id, nil
}

func (c *Client) Binding() (Identity, control.Uint64) { return c.config.Identity, c.config.IssuerEpoch }

func (c *Client) InspectState(ctx context.Context) (View, error) {
	var v View
	if err := c.call(ctx, true, "state", nil, &v); err != nil {
		return v, err
	}
	if v.Validate() != nil || v.Identity != c.config.Identity {
		return View{}, &ClientError{Code: "resource_state_unconfirmed"}
	}
	return v, nil
}

func (c *Client) CloseIdleConnections() {
	for _, t := range c.transport {
		t.CloseIdleConnections()
	}
}

func (c *Client) call(ctx context.Context, observer bool, path string, body any, result any) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	stop := context.AfterFunc(c.config.Context, cancel)
	defer func() { stop(); cancel() }()
	if c.config.Context.Err() != nil {
		return &ClientError{Code: "owner_unavailable"}
	}
	token, err := c.config.Token(ctx)
	if err != nil || len(token) == 0 || len(token) > 8192 || strings.ContainsAny(token, "\r\n\t ") {
		return &ClientError{Code: "credential_unavailable"}
	}
	var data []byte
	method := http.MethodGet
	if body != nil {
		data, err = json.Marshal(body)
		method = http.MethodPost
	}
	if err != nil {
		return &ClientError{Code: "invalid_body"}
	}
	r, err := http.NewRequestWithContext(ctx, method, c.config.BaseURL+"/dsg/v1/resource/"+path, bytes.NewReader(data))
	if err != nil {
		return &ClientError{Code: "invalid_request"}
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Accept", "application/json")
	r.Header.Set("Accept-Encoding", "identity")
	r.Header.Set("Cache-Control", "no-store")
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	if ctx.Err() != nil {
		return &ClientError{Code: "context_cancelled"}
	}
	index := 0
	if observer {
		index = 1
	}
	response, err := c.http[index].Do(r)
	if err != nil {
		return &ClientError{MayHaveExecuted: true, Code: "transport_unconfirmed"}
	}
	defer response.Body.Close()
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK || err != nil || media != "application/json" || response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Content-Encoding") != "" {
		return &ClientError{MayHaveExecuted: true, Code: "response_unconfirmed"}
	}
	data, err = io.ReadAll(io.LimitReader(response.Body, 32*1024+1))
	if err != nil || len(data) > 32*1024 || json.Unmarshal(data, result) != nil {
		return &ClientError{MayHaveExecuted: true, Code: "response_unconfirmed"}
	}
	return nil
}

func (c *Client) InspectIdentity(ctx context.Context) (Identity, error) {
	var id Identity
	if err := c.call(ctx, true, "identity", nil, &id); err != nil {
		return id, err
	}
	if id != c.config.Identity {
		return Identity{}, &ClientError{Code: "guest_identity_changed"}
	}
	return id, nil
}

// 可信创建/计划者在任何资源写之前读取并持久冻结。后续命令和恢复仍带完整
// 原PID/starttime，不能因查询得到新PG就改写已有未知命令。
func (c *Client) InspectProcess(ctx context.Context) (ProcessIdentity, error) {
	var p ProcessIdentity
	if err := c.call(ctx, true, "process", nil, &p); err != nil {
		return p, err
	}
	if p.Validate() != nil || p.Identity != c.config.Identity {
		return ProcessIdentity{}, &ClientError{Code: "postmaster_birth_unconfirmed"}
	}
	return p, nil
}

func (c *Client) Fence(ctx context.Context) error {
	var id Identity
	err := c.call(ctx, true, "fence", FenceRequest{ProtocolVersion: 1, Identity: c.config.Identity, IssuerEpoch: c.config.IssuerEpoch}, &id)
	if err != nil {
		return err
	}
	if id != c.config.Identity {
		return &ClientError{MayHaveExecuted: true, Code: "fence_unconfirmed"}
	}
	return nil
}

func (c *Client) Apply(ctx context.Context, command Command) (Evidence, error) {
	return c.command(ctx, command, "apply")
}
func (c *Client) ReplayOriginal(ctx context.Context, command Command) (Evidence, error) {
	return c.command(ctx, command, "replay-original")
}
func (c *Client) Observe(ctx context.Context, command Command) (Evidence, error) {
	return c.command(ctx, command, "observe")
}

func (c *Client) command(ctx context.Context, command Command, action string) (Evidence, error) {
	var e Evidence
	command = cloneCommand(command)
	observe := action == "observe"
	if command.Validate() != nil || command.Identity != c.config.Identity || command.IssuerEpoch > c.config.IssuerEpoch || (!observe && command.IssuerEpoch != c.config.IssuerEpoch) {
		return e, &ClientError{Code: "invalid_command"}
	}
	c.mu.Lock()
	if !observe && (c.busy || (c.pending != nil && (action != "replay-original" || !reflect.DeepEqual(*c.pending, command)))) {
		c.mu.Unlock()
		return e, &ClientError{Code: "original_pending"}
	}
	if action == "replay-original" && c.pending == nil {
		c.mu.Unlock()
		return e, &ClientError{Code: "no_original_to_replay"}
	}
	if !observe {
		c.busy = true
		copy := cloneCommand(command)
		c.pending = &copy
	}
	c.mu.Unlock()
	if !observe {
		defer func() { c.mu.Lock(); c.busy = false; c.mu.Unlock() }()
	}
	// 发出任何字节前已固定本地原意图；取消、错误、HTTP失败或读回不匹配均保留。
	// 即使服务返回200也逐字段核验真实限额/整池/占用/身份且VM许可必须false。
	if err := c.call(ctx, observe, action, command, &e); err != nil {
		return Evidence{}, err
	}
	if err := e.matches(command); err != nil {
		return Evidence{}, &ClientError{MayHaveExecuted: !observe, Code: "evidence_unconfirmed"}
	}
	c.mu.Lock()
	if c.pending != nil && reflect.DeepEqual(*c.pending, command) {
		c.pending = nil
	}
	c.mu.Unlock()
	return e, nil
}

// Pending返回深拷贝用于外层恢复登记，不允许调用者修改共享slice绕过本地栅栏。
// 外层必须在发送前保存原意图，不能等此方法返回后才首次记账。
func (c *Client) Pending() *Command {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending == nil {
		return nil
	}
	p := cloneCommand(*c.pending)
	return &p
}

func (c *Client) String() string { return "DSGA resource client (redacted)" }
