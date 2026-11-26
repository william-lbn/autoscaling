package compute

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 独立观察只读PG页型，不返还任何normal/WholePool/Host预算，不清其他pending。
func (c *Client) ProviderCost(ctx context.Context) (control.ProviderCostResponse, error) {
	var fact control.ProviderCostResponse
	if err := c.read(ctx, "/dsg/v1/budget/provider", &fact); err != nil {
		return fact, err
	}
	if fact.ValidateFor(c.instance) != nil {
		return control.ProviderCostResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_provider_cost"}
	}
	return fact, nil
}
