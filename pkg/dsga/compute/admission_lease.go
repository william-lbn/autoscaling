package compute

import (
	"context"
	"encoding/json"
	"net/http"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 原deadline CAS允许丢回复后只观察再核验来源；不会产生新开放/资源动作。
// 不持mu跨网络，也不读取/清除任何resize或admission Pending。
func (client *Client) AdmissionLease(ctx context.Context, intent control.AdmissionLeaseRequest) (control.AdmissionLeaseResponse, error) {
	if intent.Validate() != nil || intent.Instance != client.instance || intent.IssuerEpoch != client.epoch {
		return control.AdmissionLeaseResponse{}, &CallError{MayHaveExecuted: false, Code: "invalid_admission_lease_binding"}
	}
	payload, _ := json.Marshal(intent)
	request, err := client.prepare(ctx, "/dsg/v1/admission/lease", http.MethodPost, payload)
	if err != nil {
		return control.AdmissionLeaseResponse{}, err
	}
	var fact control.AdmissionLeaseResponse
	if err := exchange(client.observer, request, &fact); err != nil {
		return control.AdmissionLeaseResponse{}, err
	}
	if fact.ValidateFor(intent, client.instance) != nil {
		return control.AdmissionLeaseResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_admission_lease_fact"}
	}
	return fact, nil
}
