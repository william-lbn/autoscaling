package compute

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// PreparedState用原独立观察池读取真实2PC槽，绝不由前台idle补零或清原Pending。
func (client *Client) PreparedState(ctx context.Context) (control.PreparedResponse, error) {
	var fact control.PreparedResponse
	if err := client.read(ctx, "/dsg/v1/work/prepared", &fact); err != nil {
		return fact, err
	}
	if err := fact.ValidateFor(client.instance); err != nil {
		return control.PreparedResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_prepared_observation"}
	}
	return fact, nil
}
