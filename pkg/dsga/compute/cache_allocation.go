package compute

import (
	"context"
	"encoding/json"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"net/http"
)

func (c *Client) PendingLFCAllocation() *control.LFCAllocationRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lfcPending == nil {
		return nil
	}
	value := *c.lfcPending
	return &value
}

func (c *Client) LFCAllocationState(ctx context.Context) (control.LFCAllocationResponse, error) {
	var f control.LFCAllocationResponse
	if err := c.read(ctx, "/dsg/v1/cache/lfc", &f); err != nil {
		return f, err
	}
	if f.ValidateFor(c.instance) != nil || f.Disposition != "observed" {
		return control.LFCAllocationResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_lfc_state"}
	}
	return f, nil
}

func (c *Client) LFCAllocationStatus(ctx context.Context, r control.LFCAllocationRequest) (control.LFCAllocationResponse, error) {
	return c.lfcAllocationCall(ctx, r, "status", false)
}

// Acknowledge 只经原执行连接查询完整原意图；它不调用原生setter。
func (c *Client) AcknowledgeLFCAllocation(ctx context.Context, r control.LFCAllocationRequest) (control.LFCAllocationResponse, error) {
	return c.lfcAllocationCall(ctx, r, "acknowledge", true)
}

func (c *Client) AllocateLFC(ctx context.Context, r control.LFCAllocationRequest) (control.LFCAllocationResponse, error) {
	return c.lfcAllocationCall(ctx, r, "allocate", true)
}

func (c *Client) lfcAllocationCall(ctx context.Context, r control.LFCAllocationRequest, action string, serial bool) (fact control.LFCAllocationResponse, err error) {
	if r.Validate() != nil || r.Key.Instance != c.instance || r.Key.IssuerEpoch > c.epoch ||
		action == "allocate" && r.Key.IssuerEpoch != c.epoch {
		return fact, &CallError{Code: "invalid_lfc_binding"}
	}
	payload, _ := json.Marshal(r)
	request, err := c.prepare(ctx, "/dsg/v1/cache/lfc/"+action, http.MethodPost, payload)
	if err != nil {
		return fact, err
	}
	channel := c.observer
	if serial {
		c.mu.Lock()
		if c.inFlight || c.admissionInFlight || c.pending != nil || c.lfcPending != nil && *c.lfcPending != r {
			c.mu.Unlock()
			return fact, &CallError{Code: "original_lfc_intent_unresolved"}
		}
		wasNew := c.lfcPending == nil
		retained := r
		c.lfcPending = &retained
		c.inFlight = true
		c.mu.Unlock()
		resolved := false
		defer func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.inFlight = false
			var local *CallError
			if resolved || wasNew && errors.As(err, &local) && !local.MayHaveExecuted {
				c.lfcPending = nil
			}
		}()
		channel = c.execution
		err = exchange(channel, request, &fact)
		if err != nil {
			return control.LFCAllocationResponse{}, err
		}
		if fact.ValidateRequest(r) != nil {
			return control.LFCAllocationResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_lfc_fact"}
		}
		resolved = fact.AppliedFor(r) || action == "allocate" &&
			(fact.Disposition == "busy" || fact.Disposition == "rejected" || fact.Disposition == "deadline_expired")
		return fact, nil
	}
	if err = exchange(channel, request, &fact); err != nil {
		return control.LFCAllocationResponse{}, err
	}
	if fact.ValidateRequest(r) != nil {
		return control.LFCAllocationResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_lfc_fact"}
	}
	return fact, nil
}
