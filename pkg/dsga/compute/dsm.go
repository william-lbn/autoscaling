package compute

import (
	"context"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// DSMState读取原独立观察通道的真实本boot payload承诺，不清Pending、不重放
// resize或准入；缺能力/无效账本保留原未知资源意图，由完整生产者后续核验。
func (client *Client) DSMState(ctx context.Context) (control.DSMResponse, error) {
	var fact control.DSMResponse
	if err := client.read(ctx, "/dsg/v1/work/dsm", &fact); err != nil {
		return fact, err
	}
	if err := fact.ValidateFor(client.instance); err != nil {
		return control.DSMResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_dsm_observation"}
	}
	return fact, nil
}
