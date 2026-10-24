package dsga

import "fmt"

// ConnectionResponse确认Postmaster已经执行持续公共连接门禁，同点包含Work2。
// 它不证明后台/DSM/保留私有内存排空，不发完整Quiesce或VM许可。必须同时比较
// 串号与开闭位，不能把PG接受请求、当前门禁bool或历史ack拼成完成。
type ConnectionResponse struct {
	ProtocolVersion          uint32       `json:"protocol_version"`
	ConnectionAPIVersion     uint32       `json:"connection_api_version"`
	VMShrinkPermit           bool         `json:"vm_shrink_permit"`
	Enabled                  bool         `json:"enabled"`
	RequestedSerial          uint32       `json:"requested_serial"`
	AppliedSerial            uint32       `json:"applied_serial"`
	RequestedOpen            bool         `json:"requested_open"`
	AppliedOpen              bool         `json:"applied_open"`
	Acknowledged             bool         `json:"acknowledged"`
	PrivateManagementReady   bool         `json:"private_management_ready"`
	PublicConnectionsBlocked bool         `json:"public_connections_blocked"`
	Work                     WorkResponse `json:"work"`
}

func (r ConnectionResponse) ValidateFor(bound Instance) error {
	if err := r.Work.ValidateFor(bound); err != nil {
		return err
	}
	if r.ProtocolVersion != 1 || r.ConnectionAPIVersion != 1 || r.VMShrinkPermit {
		return fmt.Errorf("connection observation protocol or permission differs")
	}
	if r.RequestedSerial > ^uint32(0)>>1 || r.AppliedSerial > r.RequestedSerial {
		return fmt.Errorf("connection acknowledgement serial exceeds request")
	}
	if r.Enabled {
		if r.RequestedSerial == 0 || r.RequestedOpen != r.Work.Observed.AdmissionOpen || (r.AppliedSerial == 0 && r.AppliedOpen) {
			return fmt.Errorf("connection gate contradicts same-point native admission")
		}
	} else if r.RequestedSerial != 0 || r.AppliedSerial != 0 || r.RequestedOpen || r.AppliedOpen || r.PrivateManagementReady {
		return fmt.Errorf("disabled connection gate cannot claim applied management channel")
	}
	// 饱和序号仍可最后关门：相同serial但不同open不是ack，更不能当作公共已阻塞。
	ack := r.Enabled && r.RequestedSerial > 0 && r.RequestedSerial == r.AppliedSerial && r.RequestedOpen == r.AppliedOpen
	if r.Acknowledged != ack || r.PublicConnectionsBlocked != (ack && !r.RequestedOpen) {
		return fmt.Errorf("connection acknowledgement or public blockade differs")
	}
	return nil
}

func (r *ConnectionResponse) UnmarshalJSON(data []byte) error {
	type plain ConnectionResponse
	var next plain
	if err := UnmarshalObject(data, &next); err != nil {
		return err
	}
	*r = ConnectionResponse(next)
	return nil
}
