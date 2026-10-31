package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ArrayAuthorityEnabled仅由原Root state的显式profile版本选择；回调存在
// 不等于启用证据。原profile要求1而签发器缺失时，后续调用失败关闭，不能回旧路。
func (c *Client) ArrayAuthorityEnabled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.arrayProtocolVersion == 1
}
func (c *Client) ArrayAuthorityPending() *ArrayAuthorityCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.arrayPending == nil {
		return nil
	}
	copy := *c.arrayPending
	return &copy
}
func (c *Client) arrayAuthorityOriginal(ctx context.Context, request control.Request) (ArrayAuthorityCommand, error) {
	if request.Validate() != nil || request.Key.Instance.VMUID != c.config.Identity.VMUID || request.Key.Instance.ComputeInstanceID != c.config.Identity.ComputeInstanceID || request.Key.IssuerEpoch != c.config.IssuerEpoch {
		return ArrayAuthorityCommand{}, &ClientError{Code: "array_original_binding_changed"}
	}
	view, err := c.InspectState(ctx)
	if err != nil {
		return ArrayAuthorityCommand{}, err
	}
	reference, err := view.ArrayAuthorityReference()
	if err != nil {
		return ArrayAuthorityCommand{}, err
	}
	return ArrayAuthorityCommand{ProtocolVersion: 1, Identity: c.config.Identity, IssuerEpoch: c.config.IssuerEpoch, ResourceReferenceSHA256: reference, Request: request}, nil
}
func (c *Client) AuthorizeArrayRequest(ctx context.Context, request control.Request) (ArrayAuthorityEvidence, error) {
	command, err := c.arrayAuthorityOriginal(ctx, request)
	if err != nil {
		return ArrayAuthorityEvidence{}, err
	}
	return c.arrayAuthorityCall(ctx, command, "array-authorize", false)
}
func (c *Client) ReplayArrayRequest(ctx context.Context, request control.Request) (ArrayAuthorityEvidence, error) {
	// 优先保留本Client完整原ref；重建Client只能在当前ref仍逐字相同时恢复。
	c.mu.Lock()
	var command *ArrayAuthorityCommand
	if c.arrayPending != nil {
		copy := *c.arrayPending
		command = &copy
	}
	c.mu.Unlock()
	if command != nil {
		if command.Request != request {
			return ArrayAuthorityEvidence{}, &ClientError{Code: "array_original_changed"}
		}
		return c.arrayAuthorityCall(ctx, *command, "array-replay-original", true)
	}
	original, err := c.arrayAuthorityOriginal(ctx, request)
	if err != nil {
		return ArrayAuthorityEvidence{}, err
	}
	return c.arrayAuthorityCall(ctx, original, "array-replay-original", true)
}
func (c *Client) ObserveArrayAuthority(ctx context.Context, command ArrayAuthorityCommand) (ArrayAuthorityEvidence, error) {
	return c.arrayAuthorityCall(ctx, command, "array-observe", true)
}
func (c *Client) arrayAuthorityCall(parent context.Context, command ArrayAuthorityCommand, action string, replay bool) (ArrayAuthorityEvidence, error) {
	var evidence ArrayAuthorityEvidence
	if command.Validate() != nil || command.Identity != c.config.Identity || command.IssuerEpoch != c.config.IssuerEpoch || c.config.ArrayAuthorityToken == nil {
		return evidence, &ClientError{Code: "array_binding_unavailable"}
	}
	path := "/dsg/v1/resource/" + action
	if !arrayAuthorityPath(path) {
		return evidence, &ClientError{Code: "array_action_unavailable"}
	}
	observer := action == "array-observe"
	c.mu.Lock()
	if c.busy || c.pending != nil || c.repair != nil || c.arrayPending != nil && *c.arrayPending != command || c.arrayPending != nil && !replay {
		c.mu.Unlock()
		return evidence, &ClientError{Code: "original_pending"}
	}
	c.busy = true
	// 在拿token/发请求前保本地原意图；任何签发/传输/解码失败均不自动清除。
	if !observer {
		copy := command
		c.arrayPending = &copy
	}
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.busy = false; c.mu.Unlock() }()
	ctx, cancel := context.WithTimeout(parent, c.config.Timeout)
	stop := context.AfterFunc(c.config.Context, cancel)
	defer func() { stop(); cancel() }()
	if c.config.Context.Err() != nil {
		return evidence, &ClientError{Code: "owner_unavailable"}
	}
	raw, err := json.Marshal(command)
	if err != nil || len(raw) > arrayAuthorityMaximumBodyBytes {
		return evidence, &ClientError{Code: "invalid_array_body"}
	}
	token, err := c.config.ArrayAuthorityToken(ctx, http.MethodPost, path, physicalDigest(raw))
	if err != nil || len(token) == 0 || len(token) > 8192 || strings.ContainsAny(token, "\r\n\t ") {
		return evidence, &ClientError{Code: "array_credential_unavailable"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.BaseURL+path, bytes.NewReader(raw))
	if err != nil {
		return evidence, &ClientError{Code: "invalid_request"}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Cache-Control", "no-store")
	index := 0
	if observer {
		index = 1
	}
	response, err := c.http[index].Do(req)
	if err != nil {
		return evidence, &ClientError{MayHaveExecuted: true, Code: "array_transport_unconfirmed"}
	}
	defer response.Body.Close()
	media, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK || mediaErr != nil || media != "application/json" || response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Content-Encoding") != "" {
		return evidence, &ClientError{MayHaveExecuted: true, Code: "array_response_unconfirmed"}
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 32*1024+1))
	if err != nil || len(data) > 32*1024 || json.Unmarshal(data, &evidence) != nil || evidence.validateFor(command) != nil {
		return ArrayAuthorityEvidence{}, &ClientError{MayHaveExecuted: true, Code: "array_response_unconfirmed"}
	}
	c.mu.Lock()
	if c.arrayPending != nil && *c.arrayPending == command {
		c.arrayPending = nil
	}
	c.mu.Unlock()
	return evidence, nil
}
