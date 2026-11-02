package resourceguard

import (
	"context"
	"reflect"
)

// 能力只由真实Root显式版本选择；不是预算可用或已获得normal降限许可。
func (c *Client) NormalQuotaEnabled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.normalProtocolVersion == 1
}
func (c *Client) NormalQuotaPending() *NormalQuotaEnvelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.normalPending == nil {
		return nil
	}
	v := cloneNormalQuota(*c.normalPending)
	return &v
}

// 只消费外层原WAL；不查询PG重新生成完成事实，不发RPC/清未知。
func (c *Client) RetainNormalQuota(v NormalQuotaEnvelope) error {
	if v.Validate() != nil || v.Command.Identity != c.config.Identity || v.Command.IssuerEpoch > c.config.IssuerEpoch {
		return &ClientError{Code: "invalid_normal_original"}
	}
	v = cloneNormalQuota(v)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.busy || c.arrayPending != nil || c.repair != nil || c.pending != nil && !reflect.DeepEqual(*c.pending, v.Command) || c.normalPending != nil && !reflect.DeepEqual(*c.normalPending, v) {
		return ErrUnknown
	}
	command := cloneCommand(v.Command)
	c.pending = &command
	c.normalPending = &v
	return nil
}
func (c *Client) ApplyNormalQuota(ctx context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return c.normalQuotaCall(ctx, v, "normal-apply", false)
}
func (c *Client) ObserveNormalQuota(ctx context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return c.normalQuotaCall(ctx, v, "normal-observe", false)
}
func (c *Client) ReplayNormalQuota(ctx context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return c.normalQuotaCall(ctx, v, "normal-replay-original", true)
}
func (c *Client) RecoverNormalQuota(ctx context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return c.normalQuotaCall(ctx, v, "normal-recover", true)
}

// DeliverNormalQuota只在受信协调器显式恢复时交付原信封；Root原prefix已存在而
// 未完成时仍拒绝Apply，必须Replay。不能由普通轮询重发。
func (c *Client) DeliverNormalQuota(ctx context.Context, v NormalQuotaEnvelope) (NormalQuotaEvidence, error) {
	return c.normalQuotaCall(ctx, v, "normal-apply", true)
}
func (c *Client) normalQuotaCall(ctx context.Context, v NormalQuotaEnvelope, action string, explicit bool) (NormalQuotaEvidence, error) {
	var out NormalQuotaEvidence
	observer := action == "normal-observe"
	oldAllowed := observer || action == "normal-recover"
	if v.Validate() != nil || v.Command.Identity != c.config.Identity || v.Command.IssuerEpoch > c.config.IssuerEpoch || !oldAllowed && v.Command.IssuerEpoch != c.config.IssuerEpoch || !normalQuotaPath("/dsg/v1/resource/"+action) {
		return out, &ClientError{Code: "invalid_normal_command"}
	}
	v = cloneNormalQuota(v)
	c.mu.Lock()
	if c.normalProtocolVersion != 1 || !observer && (c.busy || c.arrayPending != nil || c.repair != nil || c.pending != nil && !reflect.DeepEqual(*c.pending, v.Command) || c.normalPending != nil && (!explicit || !reflect.DeepEqual(*c.normalPending, v))) || observer && c.normalPending != nil && !reflect.DeepEqual(*c.normalPending, v) {
		c.mu.Unlock()
		return out, &ClientError{Code: "normal_original_pending_or_unavailable"}
	}
	if explicit && (c.pending == nil || c.normalPending == nil || !reflect.DeepEqual(*c.normalPending, v)) {
		c.mu.Unlock()
		return out, &ClientError{Code: "normal_original_to_recover_missing"}
	}
	if !observer {
		c.busy = true
		command := cloneCommand(v.Command)
		c.pending = &command
		c.normalPending = &v
	}
	c.mu.Unlock()
	if !observer {
		defer func() { c.mu.Lock(); c.busy = false; c.mu.Unlock() }()
	}
	if err := c.call(ctx, observer, action, v, &out); err != nil {
		return NormalQuotaEvidence{}, err
	}
	if err := out.validateFor(v); err != nil {
		return NormalQuotaEvidence{}, &ClientError{MayHaveExecuted: !observer, Code: "normal_evidence_unconfirmed"}
	}
	c.mu.Lock()
	if c.normalPending != nil && reflect.DeepEqual(*c.normalPending, v) {
		c.normalPending = nil
		if c.pending != nil && reflect.DeepEqual(*c.pending, v.Command) {
			c.pending = nil
		}
	}
	c.mu.Unlock()
	return out, nil
}
