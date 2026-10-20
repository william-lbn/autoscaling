package dsga

import (
	"fmt"
	"math"
)

// 本文件与Neon compute_api/dsg_state.rs的ABI6事实逐字段对应。这里校验事实的
// 内部一致性和实例归属，不生成预算证明、Provider/Host事实或VM缩容许可。
type ReceiptDisposition string

const (
	ReceiptAccepted        ReceiptDisposition = "accepted"
	ReceiptReplayed        ReceiptDisposition = "replayed"
	ReceiptBusy            ReceiptDisposition = "busy"
	ReceiptStale           ReceiptDisposition = "stale"
	ReceiptConflict        ReceiptDisposition = "conflict"
	ReceiptResultExpired   ReceiptDisposition = "result_expired"
	ReceiptNotFound        ReceiptDisposition = "not_found"
	ReceiptCancelRequested ReceiptDisposition = "cancel_requested"
	ReceiptTooLate         ReceiptDisposition = "too_late"
	ReceiptRejected        ReceiptDisposition = "rejected"
)

type OperationPhase string

const (
	Preparing OperationPhase = "preparing"
	Committed OperationPhase = "committed"
	Denied    OperationPhase = "denied"
)

type Reason string

const (
	ReasonNone           Reason = "none"
	ReasonTimeout        Reason = "timeout"
	ReasonPopulateFailed Reason = "populate_failed"
	ReasonInterrupted    Reason = "interrupted"
	ReasonCancelled      Reason = "cancelled"
)

type ArrayKind string

const (
	Descriptors ArrayKind = "descriptors"
	Blocks      ArrayKind = "blocks"
	IOCV        ArrayKind = "iocv"
	Checkpoint  ArrayKind = "checkpoint"
)

type ReleaseState string

const (
	ReleaseNone           ReleaseState = "none"
	ReleasePending        ReleaseState = "pending"
	ReleaseWaitCheckpoint ReleaseState = "wait_checkpoint"
	ReleaseVerified       ReleaseState = "verified"
	ReleaseUnsupported    ReleaseState = "unsupported"
	ReleaseFailed         ReleaseState = "failed"
)

type ProviderStatus string

const (
	ProviderUnknown     ProviderStatus = "unknown"
	ProviderOK          ProviderStatus = "ok"
	ProviderNoop        ProviderStatus = "noop"
	ProviderUnsupported ProviderStatus = "unsupported"
	ProviderFailed      ProviderStatus = "failed"
)

type ProviderFact struct {
	HasResult               bool           `json:"has_result"`
	Status                  ProviderStatus `json:"status"`
	SystemErrno             int32          `json:"system_errno"`
	RequestedOffset         Uint64         `json:"requested_offset"`
	RequestedBytes          Uint64         `json:"requested_bytes"`
	AffectedOffset          Uint64         `json:"affected_offset"`
	AffectedBytes           Uint64         `json:"affected_bytes"`
	PhysicalReleaseVerified bool           `json:"physical_release_verified"`
	AllocationMayBePartial  bool           `json:"allocation_may_be_partial"`
	ReleaseMayBePartial     bool           `json:"release_may_be_partial"`
}

// ReleaseKey可能仍属于前一次release；新的Grow可以使AllocationUnknown变为true。
// 不拼接旧释放回执和当前分配状态，不将四数组占用再与全部bootstrap成本直接相加。
type ArrayFact struct {
	Array                     ArrayKind    `json:"array"`
	CommittedUpperBytes       Uint64       `json:"committed_upper_bytes"`
	AllocationUnknown         bool         `json:"allocation_unknown"`
	ReleaseState              ReleaseState `json:"release_state"`
	ReleaseKey                *Key         `json:"release_key"`
	ReleaseCapacityGeneration Uint64       `json:"release_capacity_generation"`
	Provider                  ProviderFact `json:"provider"`
}

type OperationFact struct {
	Key                        Key            `json:"key"`
	Kind                       Kind           `json:"kind"`
	Phase                      OperationPhase `json:"phase"`
	Reason                     Reason         `json:"reason"`
	TargetBuffers              uint32         `json:"target_buffers"`
	ExpectedCapacityGeneration Uint64         `json:"expected_capacity_generation"`
	ContextSHA256              string         `json:"context_sha256"`
	CancelRequested            bool           `json:"cancel_requested"`
	CancelClosed               bool           `json:"cancel_closed"`
	Replayed                   bool           `json:"replayed"`
}

type Snapshot struct {
	Instance                Instance       `json:"instance"`
	IssuerFloorEpoch        Uint64         `json:"issuer_floor_epoch"`
	CapacityGeneration      Uint64         `json:"capacity_generation"`
	LowBuffers              uint32         `json:"low_buffers"`
	HighBuffers             uint32         `json:"high_buffers"`
	MaximumBuffers          uint32         `json:"maximum_buffers"`
	AdmissionOpen           bool           `json:"admission_open"`
	AdmissionRevision       Uint64         `json:"admission_revision"`
	StartupSharedUpperBytes Uint64         `json:"startup_shared_upper_bytes"`
	Operation               *OperationFact `json:"operation"`
	Arrays                  []ArrayFact    `json:"arrays"`
}

type Receipt struct {
	Key         Key                `json:"key"`
	Disposition ReceiptDisposition `json:"disposition"`
	HasResult   bool               `json:"has_result"`
	Result      *Snapshot          `json:"result"`
}

type ControlResponse struct {
	ProtocolVersion uint32   `json:"protocol_version"`
	VMShrinkPermit  bool     `json:"vm_shrink_permit"`
	Receipt         Receipt  `json:"receipt"`
	Observed        Snapshot `json:"observed"`
}

type MemoryResponse struct {
	ProtocolVersion uint32   `json:"protocol_version"`
	VMShrinkPermit  bool     `json:"vm_shrink_permit"`
	Observed        Snapshot `json:"observed"`
}

type FenceDisposition string

const (
	Fenced       FenceDisposition = "fenced"
	FencePending FenceDisposition = "pending"
	FenceStale   FenceDisposition = "stale"
)

// Fenced只证明PG串行执行者已退出，不等于物理释放或VM完成。Pending已推进
// floor但旧执行者仍在途；二者都不得清除外层原意图/预算或客户端未知请求。
type FenceResponse struct {
	ProtocolVersion   uint32           `json:"protocol_version"`
	VMShrinkPermit    bool             `json:"vm_shrink_permit"`
	RequestedInstance Instance         `json:"requested_instance"`
	RequestedEpoch    Uint64           `json:"requested_epoch"`
	Disposition       FenceDisposition `json:"disposition"`
	Observed          Snapshot         `json:"observed"`
}

func (response *FenceResponse) UnmarshalJSON(data []byte) error {
	type plain FenceResponse
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*response = FenceResponse(decoded)
	return nil
}

func (response FenceResponse) ValidateFor(request FenceRequest, bound Instance) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := validateEnvelope(response.ProtocolVersion, response.VMShrinkPermit); err != nil {
		return err
	}
	if err := response.Observed.Validate(); err != nil {
		return err
	}
	if response.RequestedInstance != request.Instance || response.RequestedEpoch != request.IssuerEpoch || response.Observed.Instance != bound {
		return fmt.Errorf("issuer fence request or observed binding differs")
	}
	switch response.Disposition {
	case Fenced, FencePending:
		if request.Instance != bound || response.Observed.IssuerFloorEpoch != request.IssuerEpoch {
			return fmt.Errorf("issuer fence did not establish requested floor")
		}
		if response.Disposition == Fenced && response.Observed.Operation != nil && response.Observed.Operation.Phase == Preparing {
			return fmt.Errorf("fenced response still has unreconciled operation")
		}
	case FenceStale:
		if request.Instance == bound && response.Observed.IssuerFloorEpoch <= request.IssuerEpoch {
			return fmt.Errorf("stale fence has no newer floor or changed instance")
		}
	default:
		return fmt.Errorf("unknown DSGA issuer fence disposition")
	}
	return nil
}

type DiscoveryIdentity struct {
	VMUID             string  `json:"vm_uid"`
	ComputeInstanceID string  `json:"compute_instance_id"`
	PGBootID          *Uint64 `json:"pg_boot_id"`
}

// 不支持/禁用响应可缺少末三个能力字段，不能因此给它补出合法boot/Provider能力。
type Capabilities struct {
	ProtocolVersion    uint32             `json:"protocol_version"`
	ProductionReady    bool               `json:"production_ready"`
	VMShrinkPermit     bool               `json:"vm_shrink_permit"`
	Supported          bool               `json:"supported"`
	BufferAPIVersion   uint32             `json:"buffer_api_version"`
	Enabled            bool               `json:"enabled"`
	InstanceBound      bool               `json:"instance_bound"`
	HistoryCapacity    *uint32            `json:"history_capacity,omitempty"`
	ProviderCanRelease *bool              `json:"provider_can_release,omitempty"`
	Instance           *DiscoveryIdentity `json:"instance,omitempty"`
}

func validateEnvelope(version uint32, permit bool) error {
	if version != ProtocolVersion || permit {
		return fmt.Errorf("unsupported DSGA response or forbidden PG VM permit")
	}
	return nil
}

func (capabilities Capabilities) Bind(vmUID, computeID string) (Instance, error) {
	if err := validateEnvelope(capabilities.ProtocolVersion, capabilities.VMShrinkPermit); err != nil {
		return Instance{}, err
	}
	if !capabilities.Supported || capabilities.BufferAPIVersion != 6 || !capabilities.Enabled || !capabilities.InstanceBound ||
		capabilities.HistoryCapacity == nil || *capabilities.HistoryCapacity == 0 || capabilities.ProviderCanRelease == nil || capabilities.Instance == nil || capabilities.Instance.PGBootID == nil {
		return Instance{}, fmt.Errorf("DSGA ABI6 bound capabilities are unavailable")
	}
	identity := capabilities.Instance
	instance := Instance{VMUID: identity.VMUID, ComputeInstanceID: identity.ComputeInstanceID, PGBootID: *identity.PGBootID}
	if instance.VMUID != vmUID || instance.ComputeInstanceID != computeID {
		return Instance{}, fmt.Errorf("DSGA discovery instance differs")
	}
	return instance, instance.Validate()
}

func (snapshot Snapshot) Validate() error {
	if err := snapshot.Instance.Validate(); err != nil {
		return err
	}
	if snapshot.CapacityGeneration == 0 || snapshot.LowBuffers < 16 || snapshot.LowBuffers > snapshot.HighBuffers || snapshot.HighBuffers > snapshot.MaximumBuffers ||
		snapshot.MaximumBuffers > uint32((1<<31-1)/2) || snapshot.StartupSharedUpperBytes == 0 || len(snapshot.Arrays) != 4 {
		return fmt.Errorf("invalid DSGA capacity or array facts")
	}
	seen := make(map[ArrayKind]bool)
	for _, array := range snapshot.Arrays {
		if seen[array.Array] || (array.Array != Descriptors && array.Array != Blocks && array.Array != IOCV && array.Array != Checkpoint) {
			return fmt.Errorf("invalid or duplicate DSGA array")
		}
		seen[array.Array] = true
		if err := array.validate(snapshot); err != nil {
			return err
		}
	}
	if snapshot.Operation != nil {
		return snapshot.Operation.validate(snapshot)
	}
	return nil
}

func (array ArrayFact) validate(snapshot Snapshot) error {
	if array.CommittedUpperBytes == 0 {
		return fmt.Errorf("missing DSGA array cost")
	}
	if array.ReleaseState == ReleaseNone {
		if array.ReleaseKey != nil || array.ReleaseCapacityGeneration != 0 {
			return fmt.Errorf("unstarted release has an identity")
		}
	} else {
		if array.ReleaseKey == nil || array.ReleaseCapacityGeneration == 0 || array.ReleaseCapacityGeneration > snapshot.CapacityGeneration {
			return fmt.Errorf("missing DSGA release identity or generation")
		}
		if err := array.ReleaseKey.Validate(); err != nil {
			return err
		}
		if array.ReleaseKey.Instance != snapshot.Instance {
			return fmt.Errorf("release belongs to another instance")
		}
		if array.ReleaseKey.IssuerEpoch > snapshot.IssuerFloorEpoch {
			return fmt.Errorf("release epoch exceeds issuer floor")
		}
	}
	provider := array.Provider
	known := array.ReleaseState == ReleaseVerified || array.ReleaseState == ReleaseUnsupported || array.ReleaseState == ReleaseFailed
	if provider.HasResult != known || provider.SystemErrno < 0 || uint64(provider.RequestedOffset) > math.MaxUint64-uint64(provider.RequestedBytes) ||
		uint64(provider.AffectedOffset) > math.MaxUint64-uint64(provider.AffectedBytes) {
		return fmt.Errorf("inconsistent DSGA Provider result or range")
	}
	validStatus := false
	switch array.ReleaseState {
	case ReleaseNone, ReleasePending, ReleaseWaitCheckpoint:
		validStatus = provider.Status == ProviderUnknown && !provider.PhysicalReleaseVerified
	case ReleaseVerified:
		validStatus = provider.Status == ProviderOK || provider.Status == ProviderNoop
	case ReleaseUnsupported:
		validStatus = provider.Status == ProviderUnsupported
	case ReleaseFailed:
		validStatus = provider.Status == ProviderFailed || provider.Status == ProviderNoop
	}
	if !validStatus || (provider.PhysicalReleaseVerified && (!known || provider.Status != ProviderOK || provider.AffectedBytes == 0 || provider.ReleaseMayBePartial)) {
		return fmt.Errorf("inconsistent DSGA Provider status or physical proof")
	}
	return nil
}

func (operation OperationFact) validate(snapshot Snapshot) error {
	if err := operation.Key.Validate(); err != nil {
		return err
	}
	if operation.Key.Instance != snapshot.Instance || (operation.Kind != Grow && operation.Kind != Shrink) || operation.TargetBuffers < 16 || operation.TargetBuffers > snapshot.MaximumBuffers ||
		operation.Key.IssuerEpoch > snapshot.IssuerFloorEpoch ||
		operation.ExpectedCapacityGeneration == 0 || len(operation.ContextSHA256) != 64 || (operation.CancelRequested && operation.CancelClosed) {
		return fmt.Errorf("invalid DSGA operation identity, target or cancellation")
	}
	for _, b := range []byte(operation.ContextSHA256) {
		if (b < '0' || b > '9') && (b < 'a' || b > 'f') {
			return fmt.Errorf("invalid canonical PG intent hash")
		}
	}
	if operation.Reason != ReasonNone && operation.Reason != ReasonTimeout && operation.Reason != ReasonPopulateFailed && operation.Reason != ReasonInterrupted && operation.Reason != ReasonCancelled {
		return fmt.Errorf("unknown DSGA operation reason")
	}
	switch operation.Phase {
	case Preparing, Denied:
		if snapshot.CapacityGeneration != operation.ExpectedCapacityGeneration || (operation.Kind == Grow && operation.TargetBuffers < snapshot.HighBuffers) ||
			(operation.Kind == Shrink && operation.TargetBuffers > snapshot.HighBuffers) {
			return fmt.Errorf("uncommitted operation changed generation or direction")
		}
	case Committed:
		expected := operation.ExpectedCapacityGeneration
		if snapshot.HighBuffers != operation.TargetBuffers || snapshot.LowBuffers != snapshot.HighBuffers || !operation.CancelClosed ||
			(snapshot.CapacityGeneration != expected && (expected == Uint64(math.MaxUint64) || snapshot.CapacityGeneration != expected+1)) {
			return fmt.Errorf("committed operation did not publish its target")
		}
	default:
		return fmt.Errorf("unknown DSGA operation phase")
	}
	if operation.Phase == Denied {
		if operation.Reason == ReasonNone || snapshot.LowBuffers != snapshot.HighBuffers || (operation.Reason == ReasonCancelled && (!operation.CancelRequested || operation.CancelClosed)) {
			return fmt.Errorf("denied operation lacks restored capacity or cancellation")
		}
	} else if operation.Reason != ReasonNone {
		return fmt.Errorf("active/committed operation contains denial reason")
	}
	return nil
}

// ValidateFor分别核验receipt和observed。历史result允许属于旧boot；当前observed
// 必须属于客户端固定boot。绝不把observed.Operation拼到缺失的receipt.Result中。
func (response ControlResponse) ValidateFor(key Key, bound Instance) error {
	if err := validateEnvelope(response.ProtocolVersion, response.VMShrinkPermit); err != nil {
		return err
	}
	if err := key.Validate(); err != nil {
		return err
	}
	if err := response.Observed.Validate(); err != nil {
		return err
	}
	receipt := response.Receipt
	if receipt.Key != key || response.Observed.Instance != bound || receipt.HasResult != (receipt.Result != nil) {
		return fmt.Errorf("receipt or observed identity differs")
	}
	needsResult := false
	switch receipt.Disposition {
	case ReceiptAccepted, ReceiptReplayed, ReceiptCancelRequested, ReceiptTooLate:
		needsResult = true
	case ReceiptBusy, ReceiptStale, ReceiptConflict, ReceiptResultExpired, ReceiptNotFound, ReceiptRejected:
	default:
		return fmt.Errorf("unknown DSGA receipt disposition")
	}
	if receipt.HasResult != needsResult {
		return fmt.Errorf("disposition contradicts result presence")
	}
	if receipt.Result == nil {
		return nil
	}
	result := receipt.Result
	if err := result.Validate(); err != nil {
		return err
	}
	operation := result.Operation
	if result.Instance != key.Instance || operation == nil || operation.Key != key {
		return fmt.Errorf("historical result belongs to another operation")
	}
	if receipt.Disposition == ReceiptCancelRequested && (operation.Phase != Preparing || !operation.CancelRequested || operation.CancelClosed) {
		return fmt.Errorf("cancellation receipt lacks an open cancelled operation")
	}
	if receipt.Disposition == ReceiptTooLate && operation.Phase == Preparing && !operation.CancelClosed {
		return fmt.Errorf("too-late receipt still has an open cancellation window")
	}
	return nil
}

func (response ControlResponse) ValidateResize(request Request, bound Instance) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := response.ValidateFor(request.Key, bound); err != nil {
		return err
	}
	if response.Receipt.Result != nil {
		operation := response.Receipt.Result.Operation
		hash, err := request.ContextSHA256()
		if err != nil || operation.ContextSHA256 != hash || operation.Kind != request.Kind || operation.TargetBuffers != request.TargetBuffers || operation.ExpectedCapacityGeneration != request.ExpectedCapacityGeneration {
			return fmt.Errorf("receipt changed immutable resize intent")
		}
	}
	return nil
}

func (response MemoryResponse) ValidateFor(bound Instance) error {
	if err := validateEnvelope(response.ProtocolVersion, response.VMShrinkPermit); err != nil {
		return err
	}
	if err := response.Observed.Validate(); err != nil {
		return err
	}
	if response.Observed.Instance != bound {
		return fmt.Errorf("DSGA observed instance or PG boot changed")
	}
	return nil
}

// 下面的解码方法只保证每层严格schema，跨字段/调用上下文须显式ValidateFor。
// 采用局部别名避免递归，完整成功后才覆盖接收者，失败解码不会污染已保存事实。
func (value *ProviderFact) UnmarshalJSON(data []byte) error {
	type plain ProviderFact
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*value = ProviderFact(decoded)
	return nil
}

func (value *ArrayFact) UnmarshalJSON(data []byte) error {
	type plain ArrayFact
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*value = ArrayFact(decoded)
	return nil
}

func (value *OperationFact) UnmarshalJSON(data []byte) error {
	type plain OperationFact
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*value = OperationFact(decoded)
	return nil
}

func (value *Snapshot) UnmarshalJSON(data []byte) error {
	type plain Snapshot
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*value = Snapshot(decoded)
	return nil
}

func (value *Receipt) UnmarshalJSON(data []byte) error {
	type plain Receipt
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*value = Receipt(decoded)
	return nil
}

func (value *ControlResponse) UnmarshalJSON(data []byte) error {
	type plain ControlResponse
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*value = ControlResponse(decoded)
	return nil
}

func (value *MemoryResponse) UnmarshalJSON(data []byte) error {
	type plain MemoryResponse
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*value = MemoryResponse(decoded)
	return nil
}

func (value *DiscoveryIdentity) UnmarshalJSON(data []byte) error {
	type plain DiscoveryIdentity
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*value = DiscoveryIdentity(decoded)
	return nil
}

func (value *Capabilities) UnmarshalJSON(data []byte) error {
	type plain Capabilities
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*value = Capabilities(decoded)
	return nil
}
