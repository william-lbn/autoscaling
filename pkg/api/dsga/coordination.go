package dsga

import (
	"encoding/hex"
	"fmt"
)

// CoordinationAction只选择协调器已有的业务入口，不接受调用方提供原生命令、
// 回执、URL、PID或资源上限。恢复选择不是超时重试策略；每次发送都必须来自
// 受信管理主体，普通轮询不得生成这些选择。
type CoordinationAction string

// CoordinationBinding只用于Agent管理协议的端点发现/选择，不能传给PG执行器。
// boot为0时只表示Root恢复阶段；CoordinationCommand再按动作限制其用途。
// 单独定义此类型，保留原Instance对非零PG boot的严格解码，不放宽原生协议。
type CoordinationBinding Instance

func (b CoordinationBinding) Validate() error {
	if err := ValidateID(b.VMUID, "VM"); err != nil {
		return err
	}
	return ValidateID(b.ComputeInstanceID, "Compute")
}

func (b *CoordinationBinding) UnmarshalJSON(data []byte) error {
	type plain CoordinationBinding
	var value plain
	if err := UnmarshalObject(data, &value); err != nil {
		return err
	}
	next := CoordinationBinding(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*b = next
	return nil
}

// ResourceOnly在PG发现之前也可处理，但只准选择原Root事实。零PG boot只表示
// 尚未建立PG执行端点，不能当作任何PG原生命令的通配身份或新出生。
func (a CoordinationAction) ResourceOnly() bool {
	return a == CoordinationResourceState || a == CoordinationRepairHeld || a == CoordinationObserveHeld
}

const (
	CoordinationState            CoordinationAction = "state"
	CoordinationResourceState    CoordinationAction = "resource_state"
	CoordinationBegin            CoordinationAction = "begin"
	CoordinationCancel           CoordinationAction = "cancel"
	CoordinationReplayPG         CoordinationAction = "replay_pg"
	CoordinationReplayQuiesce    CoordinationAction = "replay_quiesce"
	CoordinationReplayAdmission  CoordinationAction = "replay_admission"
	CoordinationReplayResource   CoordinationAction = "replay_resource"
	CoordinationReplayPrepare    CoordinationAction = "replay_prepare"
	CoordinationReplayInitial    CoordinationAction = "replay_initial_admission"
	CoordinationReplayVM         CoordinationAction = "replay_vm"
	CoordinationReplayAbort      CoordinationAction = "replay_abort"
	CoordinationRecoverUnchanged CoordinationAction = "recover_unchanged_admission"
	CoordinationRecoverCapacity  CoordinationAction = "recover_capacity_admission"
	CoordinationRepairHeld       CoordinationAction = "repair_held_resources"
	CoordinationObserveHeld      CoordinationAction = "observe_held_resources"
)

// CoordinationCommand固定接收方完整实例及预期当前owner代次。Key保留原操作代次，
// 因此取消旧操作时可与ExpectedOwnerEpoch不同；重放资格仍由各领域原WAL决定。
// expected代次用于拒绝网络中排队至接管后的旧管理决定，不能从该数字获得执行权。
// Begin携带完整不可变Request，其余操作只可选择Key或原首次开放命令摘要。
// 只有ResourceOnly允许PG boot为0；该端点仍固定原VM/Compute、当前owner和
// 原Root摘要，不允许Begin/取消/PG重放，正式PG端点不能接受0代替其实际boot。
type CoordinationCommand struct {
	ProtocolVersion    uint32              `json:"protocol_version"`
	Instance           CoordinationBinding `json:"instance"`
	ExpectedOwnerEpoch Uint64              `json:"expected_owner_epoch"`
	Action             CoordinationAction  `json:"action"`
	Request            *Request            `json:"request,omitempty"`
	Key                *Key                `json:"key,omitempty"`
	Selector           string              `json:"selector,omitempty"`
}

func (c CoordinationCommand) Validate() error {
	if c.ProtocolVersion != 1 || c.ExpectedOwnerEpoch == 0 || c.Instance.Validate() != nil || c.Instance.PGBootID == 0 && !c.Action.ResourceOnly() {
		return fmt.Errorf("invalid coordination binding")
	}
	switch c.Action {
	case CoordinationState, CoordinationResourceState:
		if c.Request != nil || c.Key != nil || c.Selector != "" {
			return fmt.Errorf("state command has execution parameters")
		}
	case CoordinationBegin:
		if c.Request == nil || c.Request.Validate() != nil || c.Request.Key.Instance != Instance(c.Instance) || c.Request.Key.IssuerEpoch > c.ExpectedOwnerEpoch || c.Key != nil || c.Selector != "" {
			return fmt.Errorf("begin requires one complete request for the selected owner")
		}
	case CoordinationReplayInitial, CoordinationRepairHeld, CoordinationObserveHeld:
		decoded, err := hex.DecodeString(c.Selector)
		if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != c.Selector || c.Request != nil || c.Key != nil {
			return fmt.Errorf("source selection requires the original SHA256")
		}
	case CoordinationCancel, CoordinationReplayPG, CoordinationReplayQuiesce, CoordinationReplayAdmission,
		CoordinationReplayResource, CoordinationReplayPrepare, CoordinationReplayVM, CoordinationReplayAbort,
		CoordinationRecoverUnchanged, CoordinationRecoverCapacity:
		if c.Key == nil || c.Key.Validate() != nil || c.Key.Instance != Instance(c.Instance) || c.Key.IssuerEpoch > c.ExpectedOwnerEpoch || c.Request != nil || c.Selector != "" {
			return fmt.Errorf("coordination action requires only its original operation key")
		}
	default:
		return fmt.Errorf("unknown coordination action")
	}
	return nil
}

func (c *CoordinationCommand) UnmarshalJSON(data []byte) error {
	type plain CoordinationCommand
	var value plain
	if err := UnmarshalObject(data, &value); err != nil {
		return err
	}
	next := CoordinationCommand(value)
	if err := next.Validate(); err != nil {
		return err
	}
	*c = next
	return nil
}

// Clone避免同进程调用者在提交后修改指针内容，与HTTP解码出的私有副本同样
// 固定完整决定；不从共享指针读取下一轮的新目标。
func (c CoordinationCommand) Clone() CoordinationCommand {
	if c.Request != nil {
		request := *c.Request
		c.Request = &request
	}
	if c.Key != nil {
		key := *c.Key
		c.Key = &key
	}
	return c
}
