package compute

import (
	"context"
	"encoding/json"
	"net/http"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 原deadline CAS不会产生新开放/资源动作。renew丢回复后可只观察再核验
// 来源；rearm必须保留外层原WAL并通过AdmissionRearmStatus查原应用事实。
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

// 只读原rearm允许当前owner查询更早epoch；完整原参数不改写。此方法不持
// mu跨网络，不清任何Resize/Admission未知，不把单槽未命中当作未执行。
func (client *Client) AdmissionRearmStatus(ctx context.Context, intent control.AdmissionLeaseRequest) (control.AdmissionRearmStatus, error) {
	if intent.Validate() != nil || intent.Action != "rearm" || intent.Instance != client.instance || intent.IssuerEpoch > client.epoch {
		return control.AdmissionRearmStatus{}, &CallError{MayHaveExecuted: false, Code: "invalid_admission_rearm_status_binding"}
	}
	payload, _ := json.Marshal(intent)
	request, err := client.prepare(ctx, "/dsg/v1/admission/lease/rearm/status", http.MethodPost, payload)
	if err != nil {
		return control.AdmissionRearmStatus{}, err
	}
	var fact control.AdmissionRearmStatus
	if err := exchange(client.observer, request, &fact); err != nil {
		return control.AdmissionRearmStatus{}, err
	}
	if fact.ValidateFor(intent, client.instance) != nil {
		return control.AdmissionRearmStatus{}, &CallError{MayHaveExecuted: false, Code: "invalid_admission_rearm_status_fact"}
	}
	return fact, nil
}
