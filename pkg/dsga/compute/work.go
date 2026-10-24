package compute

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// WorkState只使用独立观察池，验证正式Native实例与计数。不会解除任意resize/
// 准入未知意图，不写Quiesce、预算或VM许可；后续协调器必须另核验全部排空域。
func (client *Client) WorkState(ctx context.Context) (control.WorkResponse, error) {
	var fact control.WorkResponse
	if err := client.read(ctx, "/dsg/v1/work/state", &fact); err != nil {
		return fact, err
	}
	if err := fact.ValidateFor(client.instance); err != nil {
		return control.WorkResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_work_observation"}
	}
	return fact, nil
}
