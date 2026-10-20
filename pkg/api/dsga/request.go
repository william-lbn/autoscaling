package dsga

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	ProtocolVersion uint32 = 1
	MaxWaitMillis   uint64 = 60_000
)

// encoding/json在调用UnmarshalJSON前会剥掉外层空白。因此网络/持久入口应在
// 原始字节上先检查长度，不能只依赖结构的UnmarshalJSON来限制整个HTTP body。
func DecodeRequest(data []byte) (Request, error) {
	var request Request
	if len(data) > 16*1024 {
		return request, fmt.Errorf("DSGA raw request exceeds 16KiB")
	}
	err := json.Unmarshal(data, &request)
	return request, err
}

type Instance struct {
	VMUID             string `json:"vm_uid"`
	ComputeInstanceID string `json:"compute_instance_id"`
	PGBootID          Uint64 `json:"pg_boot_id"`
}

type OperationID struct {
	High Uint64 `json:"high"`
	Low  Uint64 `json:"low"`
}

type Key struct {
	Instance    Instance    `json:"instance"`
	IssuerEpoch Uint64      `json:"issuer_epoch"`
	Revision    Uint64      `json:"revision"`
	OperationID OperationID `json:"operation_id"`
}

type Kind string

const (
	Grow   Kind = "grow"
	Shrink Kind = "shrink"
)

// Request一经持久接受，全字段均不可变。预算ID只作引用，不自行宣称资源约束已生效。
// 这里的Revision属于DSGA协议，不能从官方传播Revision.Value不加校验地强转。
type Request struct {
	ProtocolVersion            uint32 `json:"protocol_version"`
	Key                        Key    `json:"key"`
	Kind                       Kind   `json:"kind"`
	ExpectedCapacityGeneration Uint64 `json:"expected_capacity_generation"`
	TargetBuffers              uint32 `json:"target_buffers"`
	TargetVMMemoryBytes        Uint64 `json:"target_vm_memory_bytes"`
	BudgetCommitmentID         string `json:"budget_commitment_id"`
	DeadlineUnixMillis         Uint64 `json:"deadline_unix_ms"`
}

type Query struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Key             Key    `json:"key"`
}

// 独立接管只推进PG最低代次，不用虚构resize操作ID。签名/真实所有权与外层
// 持久租约另核验；请求可表示并不能授权资源动作。
type FenceRequest struct {
	ProtocolVersion uint32   `json:"protocol_version"`
	Instance        Instance `json:"instance"`
	IssuerEpoch     Uint64   `json:"issuer_epoch"`
}

func (request FenceRequest) Validate() error {
	if request.ProtocolVersion != ProtocolVersion || request.IssuerEpoch == 0 {
		return fmt.Errorf("invalid DSGA issuer fence protocol or epoch")
	}
	return request.Instance.Validate()
}

func (request *FenceRequest) UnmarshalJSON(data []byte) error {
	type plain FenceRequest
	var decoded plain
	if err := UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	next := FenceRequest(decoded)
	if err := next.Validate(); err != nil {
		return err
	}
	*request = next
	return nil
}

func ValidateID(value, kind string) error {
	if len(value) == 0 || len(value) > 128 {
		return fmt.Errorf("invalid DSGA %s identity length", kind)
	}
	for _, b := range []byte(value) {
		if b < 0x21 || b > 0x7e {
			return fmt.Errorf("invalid DSGA %s identity bytes", kind)
		}
	}
	return nil
}

func (instance Instance) Validate() error {
	if err := ValidateID(instance.VMUID, "VM"); err != nil {
		return err
	}
	if err := ValidateID(instance.ComputeInstanceID, "Compute"); err != nil {
		return err
	}
	if instance.PGBootID == 0 {
		return fmt.Errorf("zero DSGA boot identity")
	}
	return nil
}

func (key Key) Validate() error {
	if err := key.Instance.Validate(); err != nil {
		return err
	}
	if key.IssuerEpoch == 0 || key.Revision == 0 || (key.OperationID.High == 0 && key.OperationID.Low == 0) {
		return fmt.Errorf("zero DSGA operation identity")
	}
	return nil
}

func (request Request) Validate() error {
	if request.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported DSGA protocol")
	}
	if err := request.Key.Validate(); err != nil {
		return err
	}
	if request.Kind != Grow && request.Kind != Shrink {
		return fmt.Errorf("unknown DSGA operation kind")
	}
	if request.TargetBuffers < 16 || request.TargetBuffers > uint32((1<<31-1)/2) || request.ExpectedCapacityGeneration == 0 ||
		request.TargetVMMemoryBytes == 0 || request.DeadlineUnixMillis == 0 {
		return fmt.Errorf("invalid DSGA target/generation/deadline")
	}
	return ValidateID(request.BudgetCommitmentID, "budget commitment")
}

func (query Query) Validate() error {
	if query.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("unsupported DSGA protocol")
	}
	return query.Key.Validate()
}

// 只在历史/幂等查找之后计算新操作等待；不因当前时间拒绝原意图重放或已发动作对账。
func (request Request) RemainingWaitMillis(now uint64) (uint32, bool) {
	deadline := uint64(request.DeadlineUnixMillis)
	if deadline <= now {
		return 0, false
	}
	remaining := deadline - now
	if remaining > MaxWaitMillis {
		remaining = MaxWaitMillis
	}
	return uint32(remaining), true
}

func OperationIDFromBytes(value [16]byte) OperationID {
	return OperationID{High: Uint64(binary.BigEndian.Uint64(value[:8])), Low: Uint64(binary.BigEndian.Uint64(value[8:]))}
}

func NewOperationID() (OperationID, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return OperationID{High: 0, Low: 0}, err
	}
	id := OperationIDFromBytes(value)
	if id.High == 0 && id.Low == 0 {
		return id, fmt.Errorf("random DSGA operation ID is zero")
	}
	return id, nil
}

func (instance *Instance) UnmarshalJSON(data []byte) error {
	type plain Instance
	var value plain
	if err := UnmarshalObject(data, &value); err != nil {
		return err
	}
	if err := Instance(value).Validate(); err != nil {
		return err
	}
	*instance = Instance(value)
	return nil
}

func (id *OperationID) UnmarshalJSON(data []byte) error {
	type plain OperationID
	var value plain
	if err := UnmarshalObject(data, &value); err != nil {
		return err
	}
	if value.High == 0 && value.Low == 0 {
		return fmt.Errorf("zero DSGA operation ID")
	}
	*id = OperationID(value)
	return nil
}

func (key *Key) UnmarshalJSON(data []byte) error {
	type plain Key
	var value plain
	if err := UnmarshalObject(data, &value); err != nil {
		return err
	}
	if err := Key(value).Validate(); err != nil {
		return err
	}
	*key = Key(value)
	return nil
}

func (request *Request) UnmarshalJSON(data []byte) error {
	if len(data) > 16*1024 {
		return fmt.Errorf("DSGA request exceeds 16KiB")
	}
	type plain Request
	var value plain
	if err := UnmarshalObject(data, &value); err != nil {
		return err
	}
	if err := Request(value).Validate(); err != nil {
		return err
	}
	*request = Request(value)
	return nil
}

func (query *Query) UnmarshalJSON(data []byte) error {
	type plain Query
	var value plain
	if err := UnmarshalObject(data, &value); err != nil {
		return err
	}
	if err := Query(value).Validate(); err != nil {
		return err
	}
	*query = Query(value)
	return nil
}
