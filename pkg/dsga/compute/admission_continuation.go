package compute

import (
	"context"
	"encoding/json"
	"net/http"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// PendingAdmissionContinuation返回不可变选择的副本。调用方须从自己的写前
// WAL恢复此完整值，不能由SDK内存或当前closed布尔重新推导原始/前驱身份。
func (client *Client) PendingAdmissionContinuation() *control.AdmissionContinuationRequest {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.admissionContinuation == nil {
		return nil
	}
	retainedValue := *client.admissionContinuation
	return &retainedValue
}

// ContinueAdmissionClosure只发送显式持久选择；Compute端必须先完成原生issuer
// fence才发送新owner关闭。失败/超时保留三份请求，普通轮询不能自动选新revision。
// 关闭不排空旧工作，也不释放Budget/Host；任何重开仍需独立完整预算协议。
func (client *Client) ContinueAdmissionClosure(ctx context.Context, intent control.AdmissionContinuationRequest) (fact control.AdmissionContinuationResponse, err error) {
	if intent.Validate() != nil || !client.admissionBinding(intent.Original, true) || !client.admissionBinding(intent.PreviousClose, true) || !client.admissionBinding(intent.Close, false) {
		return fact, &CallError{MayHaveExecuted: false, Code: "invalid_admission_continuation_binding"}
	}
	payload, _ := json.Marshal(intent)
	request, err := client.prepare(ctx, "/dsg/v1/admission/continue-close", http.MethodPost, payload)
	if err != nil {
		return fact, err
	}
	if err := client.reserveAdmissionContinuation(intent); err != nil {
		return fact, err
	}
	resolved := false
	defer func() { client.finishAdmission(resolved) }()
	if err := exchange(client.observer, request, &fact); err != nil {
		return control.AdmissionContinuationResponse{}, err
	}
	if fact.ValidateFor(intent, client.instance, false) != nil {
		return control.AdmissionContinuationResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_admission_continuation_fact"}
	}
	resolved = true
	return fact, nil
}

// 查询只接受同一完整选择的原生回执；NotFound/Expired保持未知。旧选择或旧
// close-original查询不能清除更新后的接续栅栏，即使原始请求完全相同。
func (client *Client) AdmissionContinuationStatus(ctx context.Context, intent control.AdmissionContinuationRequest) (control.AdmissionContinuationResponse, error) {
	if intent.Validate() != nil || !client.admissionBinding(intent.Original, true) || !client.admissionBinding(intent.PreviousClose, true) || !client.admissionBinding(intent.Close, true) {
		return control.AdmissionContinuationResponse{}, &CallError{MayHaveExecuted: false, Code: "invalid_admission_continuation_query_binding"}
	}
	payload, _ := json.Marshal(intent)
	request, err := client.prepare(ctx, "/dsg/v1/admission/continue-close/status", http.MethodPost, payload)
	if err != nil {
		return control.AdmissionContinuationResponse{}, err
	}
	var fact control.AdmissionContinuationResponse
	if err := exchange(client.observer, request, &fact); err != nil {
		return control.AdmissionContinuationResponse{}, err
	}
	if fact.ValidateFor(intent, client.instance, true) != nil {
		return control.AdmissionContinuationResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_admission_continuation_query_fact"}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.admissionInFlight || (client.admissionPending != nil && *client.admissionPending != intent.Original) ||
		(client.admissionClose != nil && *client.admissionClose != intent.Close) ||
		(client.admissionContinuation != nil && *client.admissionContinuation != intent) {
		return control.AdmissionContinuationResponse{}, &CallError{MayHaveExecuted: false, Code: "unresolved_other_admission"}
	}
	client.admissionPending, client.admissionClose, client.admissionContinuation = nil, nil, nil
	return fact, nil
}

func (client *Client) reserveAdmissionContinuation(intent control.AdmissionContinuationRequest) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.admissionInFlight || (client.admissionPending != nil && *client.admissionPending != intent.Original) {
		return &CallError{MayHaveExecuted: false, Code: "unresolved_other_admission"}
	}
	if prior := client.admissionContinuation; prior != nil {
		if *prior != intent && (prior.Original != intent.Original || prior.Close != intent.PreviousClose) {
			return &CallError{MayHaveExecuted: false, Code: "admission_continuation_predecessor_changed"}
		}
	} else if client.admissionClose != nil {
		if *client.admissionClose != intent.PreviousClose {
			return &CallError{MayHaveExecuted: false, Code: "admission_continuation_predecessor_changed"}
		}
	} else if client.admissionPending != nil && intent.PreviousClose != intent.Original {
		return &CallError{MayHaveExecuted: false, Code: "admission_continuation_predecessor_missing"}
	}
	// 不丢最初未知，只将最近关闭与它的直接前驱同时保留。每次接续严格提高
	// epoch，预算/boot/容量固定，故更早前驱被传递封住，无需无界增长链表。
	original, closeIntent, selection := intent.Original, intent.Close, intent
	client.admissionPending, client.admissionClose, client.admissionContinuation = &original, &closeIntent, &selection
	client.admissionInFlight = true
	return nil
}
