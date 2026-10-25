package dsga

import "fmt"

// PreparedResponse保留所有原生2PC槽（含尚未valid的准备），与连接/Work同点绑定。
// RuntimePreparesBlocked只禁止新运行期PREPARE；WAL/磁盘恢复仍可增加旧槽。
// 前台idle不能解释为槽为零，此响应不生成完整Quiesce/预算或VM许可。
type PreparedResponse struct {
	ProtocolVersion        uint32             `json:"protocol_version"`
	PreparedAPIVersion     uint32             `json:"prepared_api_version"`
	VMShrinkPermit         bool               `json:"vm_shrink_permit"`
	PreparedSlots          uint32             `json:"prepared_slots"`
	MaximumPreparedSlots   uint32             `json:"maximum_prepared_slots"`
	RuntimePreparesBlocked bool               `json:"runtime_prepares_blocked"`
	Connections            ConnectionResponse `json:"connections"`
}

func (r PreparedResponse) ValidateFor(bound Instance) error {
	if err := r.Connections.ValidateFor(bound); err != nil {
		return err
	}
	if r.ProtocolVersion != 1 || r.PreparedAPIVersion != 1 || r.VMShrinkPermit ||
		r.MaximumPreparedSlots > 1<<31-1 || r.PreparedSlots > r.MaximumPreparedSlots ||
		r.RuntimePreparesBlocked == r.Connections.Work.Observed.AdmissionOpen {
		return fmt.Errorf("prepared observation protocol, slots or runtime gate differs")
	}
	return nil
}

func (r *PreparedResponse) UnmarshalJSON(data []byte) error {
	type plain PreparedResponse
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	*r = PreparedResponse(next)
	return nil
}
