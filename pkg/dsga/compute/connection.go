package compute

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ConnectionState只走独立观察池，消费同点门禁ack/Work2，不清resize或准入Pending。
// Enabled=false是明确未启用，不能由调用方把旧兼容模式解释成持续连接屏障。
func (client *Client) ConnectionState(ctx context.Context) (control.ConnectionResponse, error) {
	var fact control.ConnectionResponse
	if err := client.read(ctx, "/dsg/v1/connections/state", &fact); err != nil {
		return fact, err
	}
	if err := fact.ValidateFor(client.instance); err != nil {
		return control.ConnectionResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_connection_observation"}
	}
	return fact, nil
}
