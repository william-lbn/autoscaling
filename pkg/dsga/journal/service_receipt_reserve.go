package journal

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 四个槽仍分别保留原开放、首次关闭观察、接管观察、最后关闭回执。
// 类型上界使用已验证模型固定的实例/预算，不能由调用方换短ID或省略四数组。
// 这是预留策略，不是任意重压缩流完成承诺；每次写入仍须整Model实际encode
// 通过总192KiB门，失败不产生派发票据，也不裁掉Unknown或原预算。
type serviceReceiptReserve struct{ open, before, close, rearm int }

func serviceReceiptWindows(m Model) (serviceReceiptReserve, error) {
	if m.PGInstance == nil {
		return serviceReceiptReserve{}, fmt.Errorf("service receipt reserve lost original PG instance")
	}
	budget, err := m.effectiveServiceBudgetID()
	if err != nil {
		return serviceReceiptReserve{}, err
	}
	open, lease, close, continuation, rearm := maximumServiceReceipts(*m.PGInstance, budget)
	a, err := serviceReceiptWindow(open)
	if err != nil {
		return serviceReceiptReserve{}, err
	}
	b, err := serviceReceiptWindow(lease)
	if err != nil {
		return serviceReceiptReserve{}, err
	}
	// 硬预算配置保存已经验证的能力与 Memory 两个原始响应，其 JSON 大小
	// 不借用有限租约的估算。实例固定，数字用允许的最大宽度，仍按原槽预留。
	if c := m.ServiceClosure; c != nil && c.Reopen != nil && c.Reopen.Before.BudgetBound() {
		caps := c.Reopen.Before.Budget.Capabilities
		history, release := uint32(math.MaxUint32), false
		caps.HistoryCapacity, caps.ProviderCanRelease = &history, &release
		caps.ProductionReady = false
		maximum := control.ServiceAdmission{Budget: &control.BudgetAdmissionRead{
			Capabilities: caps, Memory: control.MemoryResponse{ProtocolVersion: 1, Observed: lease.Observed},
		}}
		b, err = serviceReceiptWindow(maximum)
		if err != nil {
			return serviceReceiptReserve{}, err
		}
	}
	c, err := serviceReceiptWindow(close)
	if err != nil {
		return serviceReceiptReserve{}, err
	}
	d, err := serviceReceiptWindow(continuation)
	if err != nil {
		return serviceReceiptReserve{}, err
	}
	e, err := serviceReceiptWindow(rearm)
	if err != nil {
		return serviceReceiptReserve{}, err
	}
	return serviceReceiptReserve{open: a, before: b, close: max(c, d), rearm: e}, nil
}

// raw上界转为独立zlib流通用膨胀界，再加base64和原每槽4096元数据。
// 整模型重压缩会改变字典，故最终仍由真实encode硬拒保证总界，不能拿此数
// 宣称任意未知正文必有空间。DTO结构变动必须同步下方形状及直接结构锁测试。
func serviceReceiptWindow(v any) (int, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	n := len(raw)
	zlibBound := n + (n >> 12) + (n >> 14) + (n >> 25) + 13
	return base64.StdEncoding.EncodedLen(zlibBound) + 4096, nil
}

// 这些值只用于字节上界，绝不是可派发的Native事实。允许相位互斥字段同时
// 出现以支配全部合法形状；false比true长，数字用最大宽度，hex摘要固定64。
// Snapshot.Validate把Operation/四ReleaseKey全部绑回同Instance；journal的
// reopen/closing/continuation验证再把所有BudgetID绑回同effective预算。
func maximumServiceReceipts(instance control.Instance, budget string) (
	control.AdmissionResponse, control.AdmissionLeaseResponse, control.AdmissionClosure,
	control.AdmissionContinuationResponse, control.AdmissionRearmStatus,
) {
	u := control.Uint64(math.MaxUint64)
	key := control.Key{Instance: instance, IssuerEpoch: u, Revision: u,
		OperationID: control.OperationID{High: u, Low: u}}
	provider := control.ProviderFact{Status: control.ProviderUnsupported, SystemErrno: math.MinInt32,
		RequestedOffset: u, RequestedBytes: u, AffectedOffset: u, AffectedBytes: u}
	arrays := make([]control.ArrayFact, 4)
	for i, kind := range []control.ArrayKind{control.Descriptors, control.Blocks, control.IOCV, control.Checkpoint} {
		k := key
		arrays[i] = control.ArrayFact{Array: kind, CommittedUpperBytes: u, ReleaseState: control.ReleaseWaitCheckpoint,
			ReleaseKey: &k, ReleaseCapacityGeneration: u, Provider: provider}
	}
	snapshot := control.Snapshot{Instance: instance, IssuerFloorEpoch: u, CapacityGeneration: u,
		LowBuffers: math.MaxUint32, HighBuffers: math.MaxUint32, MaximumBuffers: math.MaxUint32,
		AdmissionRevision: u, StartupSharedUpperBytes: u, Arrays: arrays,
		Operation: &control.OperationFact{Key: key, Kind: control.Shrink, Phase: control.Committed,
			Reason: control.ReasonPopulateFailed, TargetBuffers: math.MaxUint32,
			ExpectedCapacityGeneration: u, ContextSHA256: strings.Repeat("f", 64)}}
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: u,
		Revision: u, ExpectedCapacityGeneration: u, BudgetCommitmentID: budget}
	response := control.AdmissionResponse{ProtocolVersion: 1, Request: request,
		Disposition: control.AdmissionResultExpired, ContextSHA256: strings.Repeat("f", 64), Observed: snapshot}
	leaseRequest := control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: u,
		ExpectedCapacityGeneration: u, ExpectedAdmissionRevision: u, ExpectedDeadlineNS: u, Action: "observe"}
	lease := control.AdmissionLeaseResponse{ProtocolVersion: 1, Request: leaseRequest,
		DeadlineNS: u, LeaseEpoch: u, TimeoutMS: 60000, Observed: snapshot}
	close := control.AdmissionClosure{ProtocolVersion: 1, Original: request, ClosedBy: response}
	continuation := control.AdmissionContinuationResponse{ProtocolVersion: 1,
		Request:       control.AdmissionContinuationRequest{ProtocolVersion: 2, Original: request, PreviousClose: request, Close: request},
		ContextSHA256: strings.Repeat("f", 64), ClosedBy: response}
	rearm := control.AdmissionRearmStatus{ProtocolVersion: 1, Request: leaseRequest,
		AppliedDeadlineNS: u, Current: lease}
	return response, lease, close, continuation, rearm
}
