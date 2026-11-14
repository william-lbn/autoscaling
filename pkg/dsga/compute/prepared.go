package compute

import (
	"context"
	"encoding/json"
	"net/http"

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

// SuspensionDrain sends the settled, immutable close to Compute. A newer
// Owner can observe it under its own signed epoch after fencing PostgreSQL.
func (client *Client) SuspensionDrain(ctx context.Context, close control.AdmissionRequest) (control.PreparedResponse, error) {
	var fact control.PreparedResponse
	if close.Validate() != nil || close.Open || close.Instance != client.instance || close.IssuerEpoch > client.epoch {
		return fact, &CallError{Code: "invalid_suspension_close"}
	}
	payload, err := json.Marshal(close)
	if err != nil {
		return fact, err
	}
	request, err := client.prepare(ctx, "/dsg/v1/suspension/drain", http.MethodPost, payload)
	if err != nil {
		return fact, err
	}
	if err := exchange(client.observer, request, &fact); err != nil {
		return fact, err
	}
	if err := fact.ValidateFor(client.instance); err != nil {
		return control.PreparedResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_suspension_drain"}
	}
	return fact, nil
}
