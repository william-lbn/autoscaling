package compute

import (
	"context"
	"encoding/json"
	"net/http"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// PendingAdmission只返回值副本，不能替代外层持久journal。只读普通Memory、
// floor提升、HTTP失败或关闭连接都不能清除此未知栅栏。
func (client *Client) PendingAdmission() (original, closeIntent *control.AdmissionRequest) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.admissionPending != nil {
		copy := *client.admissionPending
		original = &copy
	}
	if client.admissionClose != nil {
		copy := *client.admissionClose
		closeIntent = &copy
	}
	return
}

func (client *Client) admissionBinding(intent control.AdmissionRequest, query bool) bool {
	return intent.Validate() == nil && intent.Instance.VMUID == client.instance.VMUID && intent.Instance.ComputeInstanceID == client.instance.ComputeInstanceID && intent.IssuerEpoch <= client.epoch && (query || (intent.Instance == client.instance && intent.IssuerEpoch == client.epoch))
}

// 网络期间不持mu。关闭走独立观察池，可在resize阻塞时到达；开放和新resize
// 都拒绝越过未知意图。放行只依赖完整原回执，不依赖HTTP状态或当前布尔门禁。
func (client *Client) SetAdmission(ctx context.Context, intent control.AdmissionRequest) (fact control.AdmissionResponse, err error) {
	if !client.admissionBinding(intent, false) {
		return fact, &CallError{Code: "invalid_admission_binding"}
	}
	payload, _ := json.Marshal(intent)
	request, err := client.prepare(ctx, "/dsg/v1/admission", http.MethodPost, payload)
	if err != nil {
		return fact, err
	}
	if err := client.reserveAdmission(intent, nil); err != nil {
		return fact, err
	}
	resolved := false
	defer func() { client.finishAdmission(resolved) }()
	if err := exchange(client.observer, request, &fact); err != nil {
		return control.AdmissionResponse{}, err
	}
	if fact.ValidateFor(intent, client.instance, false) != nil {
		return control.AdmissionResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_admission_fact"}
	}
	resolved = fact.Confirms()
	return fact, nil
}

func (client *Client) AdmissionStatus(ctx context.Context, intent control.AdmissionRequest) (control.AdmissionResponse, error) {
	if !client.admissionBinding(intent, true) {
		return control.AdmissionResponse{}, &CallError{Code: "invalid_admission_query_binding"}
	}
	payload, _ := json.Marshal(intent)
	request, err := client.prepare(ctx, "/dsg/v1/admission/status", http.MethodPost, payload)
	if err != nil {
		return control.AdmissionResponse{}, err
	}
	var fact control.AdmissionResponse
	if err := exchange(client.observer, request, &fact); err != nil {
		return control.AdmissionResponse{}, err
	}
	if fact.ValidateFor(intent, client.instance, true) != nil {
		return control.AdmissionResponse{}, &CallError{MayHaveExecuted: true, Code: "invalid_admission_query_fact"}
	}
	if fact.Confirms() {
		client.mu.Lock()
		if !client.admissionInFlight && client.admissionClose == nil && client.admissionPending != nil && *client.admissionPending == intent {
			client.admissionPending = nil
		}
		client.mu.Unlock()
	}
	return fact, nil
}

// CloseAdmissionOriginal需要显式持久原意图与更高关闭意图。成功只解除未来门禁
// 写栅栏，PriorMayHaveApplied恒true；不能释放旧预算，也不证明旧工作已经排空。
func (client *Client) CloseAdmissionOriginal(ctx context.Context, intent control.AdmissionCloseRequest) (fact control.AdmissionClosure, err error) {
	if intent.Validate() != nil || !client.admissionBinding(intent.Original, true) || !client.admissionBinding(intent.Close, false) {
		return fact, &CallError{Code: "invalid_admission_close_binding"}
	}
	payload, _ := json.Marshal(intent)
	request, err := client.prepare(ctx, "/dsg/v1/admission/close-original", http.MethodPost, payload)
	if err != nil {
		return fact, err
	}
	if err := client.reserveAdmission(intent.Original, &intent.Close); err != nil {
		return fact, err
	}
	resolved := false
	defer func() { client.finishAdmission(resolved) }()
	if err := exchange(client.observer, request, &fact); err != nil {
		return control.AdmissionClosure{}, err
	}
	if fact.ValidateFor(intent, client.instance, false) != nil {
		return control.AdmissionClosure{}, &CallError{MayHaveExecuted: true, Code: "invalid_admission_close_fact"}
	}
	resolved = true
	return fact, nil
}

func (client *Client) AdmissionClosureStatus(ctx context.Context, intent control.AdmissionCloseRequest) (control.AdmissionClosure, error) {
	if intent.Validate() != nil || !client.admissionBinding(intent.Original, true) || !client.admissionBinding(intent.Close, true) {
		return control.AdmissionClosure{}, &CallError{Code: "invalid_admission_close_query_binding"}
	}
	payload, _ := json.Marshal(intent)
	request, err := client.prepare(ctx, "/dsg/v1/admission/close-original/status", http.MethodPost, payload)
	if err != nil {
		return control.AdmissionClosure{}, err
	}
	var fact control.AdmissionClosure
	if err := exchange(client.observer, request, &fact); err != nil {
		return control.AdmissionClosure{}, err
	}
	if fact.ValidateFor(intent, client.instance, true) != nil {
		return control.AdmissionClosure{}, &CallError{MayHaveExecuted: true, Code: "invalid_admission_close_query_fact"}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.admissionInFlight || (client.admissionPending != nil && *client.admissionPending != intent.Original) || (client.admissionClose != nil && *client.admissionClose != intent.Close) {
		return control.AdmissionClosure{}, &CallError{Code: "unresolved_other_admission"}
	}
	client.admissionPending, client.admissionClose = nil, nil
	return fact, nil
}

func (client *Client) reserveAdmission(original control.AdmissionRequest, closeIntent *control.AdmissionRequest) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.admissionInFlight || (client.admissionPending != nil && *client.admissionPending != original) || (client.admissionClose != nil && (closeIntent == nil || *client.admissionClose != *closeIntent)) || (closeIntent == nil && original.Open && (client.inFlight || client.pending != nil)) {
		return &CallError{Code: "unresolved_original_admission_or_resize"}
	}
	retained := original
	client.admissionPending = &retained
	if closeIntent != nil {
		copy := *closeIntent
		client.admissionClose = &copy
	}
	client.admissionInFlight = true
	return nil
}

func (client *Client) finishAdmission(resolved bool) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.admissionInFlight = false
	if resolved {
		client.admissionPending, client.admissionClose = nil, nil
	}
}
