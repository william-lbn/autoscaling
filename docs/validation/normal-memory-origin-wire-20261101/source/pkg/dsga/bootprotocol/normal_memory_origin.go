package bootprotocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const NormalMemoryOriginProtocolVersion uint32 = 1
const NormalMemoryOriginScope = "normal-memory-origin-host-reference-v1"
const NormalMemoryOriginAckScope = "normal-memory-origin-guest-reference-v1"
const NormalMemoryOriginProfileID = "neon-linux-6.12.26-dsga-normal-split-v1"

// 六份[]byte保留同一QMP连接真实return原字节；不是调用者自报最大值。
// 中立线类型只能核语法/绑定，无法凭JSON证明Host来源或持续控制写者。
type NormalMemoryQMPInputs struct {
	Version     []byte `json:"version"`
	Machine     []byte `json:"machine"`
	Devices     []byte `json:"devices"`
	Summary     []byte `json:"summary"`
	Backends    []byte `json:"backends"`
	BackendSize []byte `json:"backend_size"`
}
type NormalMemoryQEMUReference struct {
	PID              uint32 `json:"pid"`
	StartTicks       uint64 `json:"start_ticks"`
	ExecutableDevice uint64 `json:"executable_device"`
	ExecutableInode  uint64 `json:"executable_inode"`
	ExecutableSHA256 string `json:"executable_sha256"`
	CommandSHA256    string `json:"command_sha256"`
	LaunchNonce      string `json:"launch_nonce"`
}

// 固定profile/原Runtime/Host尾引用来自初始通道。Process仍须Host实际pidfd复核。
type NormalMemoryOriginHostReference struct {
	ProtocolVersion uint32                    `json:"protocol_version"`
	Scope           string                    `json:"scope"`
	ProfileID       string                    `json:"profile_id"`
	Prepared        InitialPrebootReceipt     `json:"prepared"`
	Movable         InitialMovableReceipt     `json:"movable"`
	Completion      InitialCompletion         `json:"completion"`
	Process         NormalMemoryQEMUReference `json:"process"`
	MaximumVCPUs    uint32                    `json:"maximum_vcpus"`
	HardwareSHA256  string                    `json:"hardware_sha256"`
	QMP             NormalMemoryQMPInputs     `json:"qmp"`
}

func (r NormalMemoryOriginHostReference) Validate() error {
	if r.ProtocolVersion != NormalMemoryOriginProtocolVersion || r.Scope != NormalMemoryOriginScope || r.ProfileID != NormalMemoryOriginProfileID ||
		r.Prepared.Validate() != nil || r.Movable.Validate() != nil || r.Movable.Challenge.Preboot != r.Prepared || r.Completion.MatchReceipt(r.Movable) != nil ||
		r.Process.PID <= 1 || r.Process.StartTicks == 0 || r.Process.ExecutableDevice == 0 || r.Process.ExecutableInode == 0 ||
		!digest(r.Process.ExecutableSHA256) || !digest(r.Process.CommandSHA256) || control.ValidateIncarnation(r.Process.LaunchNonce) != nil ||
		r.MaximumVCPUs == 0 || r.MaximumVCPUs > 64 || !digest(r.HardwareSHA256) {
		return fmt.Errorf("normal memory original Host reference differs")
	}
	c := r.Prepared.Challenge
	g, e := DecodeNormalMemoryGeometry(c.Identity, c.OriginalBase, r.QMP)
	if e != nil || g.RequestedBytes != c.BusinessMinimum || g.ActualBytes != c.BusinessMinimum {
		return fmt.Errorf("normal memory initial actual geometry differs: %v", e)
	}
	raw, e := json.Marshal(r)
	if e != nil || len(raw) > MaxFrameBytes {
		return fmt.Errorf("normal memory reference exceeds original wire bound")
	}
	return nil
}
func (r NormalMemoryOriginHostReference) SHA256() (string, error) {
	if e := r.Validate(); e != nil {
		return "", e
	}
	b, e := json.Marshal(r)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
func (r *NormalMemoryOriginHostReference) UnmarshalJSON(raw []byte) error {
	type plain NormalMemoryOriginHostReference
	var p plain
	if e := control.UnmarshalObjectBounded(raw, &p, MaxFrameBytes, 4096); e != nil {
		return e
	}
	n := NormalMemoryOriginHostReference(p)
	if e := n.Validate(); e != nil {
		return e
	}
	*r = n
	return nil
}

// Ack只证明同通道Guest producer保留了原引用，不是normal/VM许可。
type NormalMemoryOriginGuestAck struct {
	ProtocolVersion     uint32 `json:"protocol_version"`
	Scope               string `json:"scope"`
	HostReferenceSHA256 string `json:"host_reference_sha256"`
	GuestSourceSHA256   string `json:"guest_source_sha256"`
	GuestBootID         string `json:"guest_boot_id"`
	RootInstanceNonce   string `json:"root_instance_nonce"`
}

func (a NormalMemoryOriginGuestAck) Validate() error {
	if a.ProtocolVersion != NormalMemoryOriginProtocolVersion || a.Scope != NormalMemoryOriginAckScope || !digest(a.HostReferenceSHA256) || !digest(a.GuestSourceSHA256) || !uuid(a.GuestBootID) || !digest(a.RootInstanceNonce) {
		return fmt.Errorf("normal memory Guest reference differs")
	}
	return nil
}
func (a NormalMemoryOriginGuestAck) MatchReference(r NormalMemoryOriginHostReference) error {
	h, e := r.SHA256()
	if e != nil {
		return e
	}
	if a.Validate() != nil || a.HostReferenceSHA256 != h || a.GuestBootID != r.Prepared.GuestBootID || a.RootInstanceNonce != r.Movable.RootInstanceNonce {
		return fmt.Errorf("normal memory acknowledgement changed original source")
	}
	return nil
}
func (a *NormalMemoryOriginGuestAck) UnmarshalJSON(raw []byte) error {
	type plain NormalMemoryOriginGuestAck
	var p plain
	if e := control.UnmarshalObjectBounded(raw, &p, MaxFrameBytes, 4096); e != nil {
		return e
	}
	n := NormalMemoryOriginGuestAck(p)
	if e := n.Validate(); e != nil {
		return e
	}
	*a = n
	return nil
}
func WriteNormalMemoryOriginHostReference(w io.Writer, r NormalMemoryOriginHostReference) error {
	if e := r.Validate(); e != nil {
		return e
	}
	return writeFrame(w, 6, r)
}
func ReadNormalMemoryOriginHostReference(r io.Reader) (NormalMemoryOriginHostReference, error) {
	var v NormalMemoryOriginHostReference
	if e := readFrame(r, 6, &v); e != nil {
		return v, e
	}
	return v, v.Validate()
}
func WriteNormalMemoryOriginGuestAck(w io.Writer, v NormalMemoryOriginGuestAck) error {
	if e := v.Validate(); e != nil {
		return e
	}
	return writeFrame(w, 7, v)
}
func ReadNormalMemoryOriginGuestAck(r io.Reader) (NormalMemoryOriginGuestAck, error) {
	var v NormalMemoryOriginGuestAck
	if e := readFrame(r, 7, &v); e != nil {
		return v, e
	}
	return v, v.Validate()
}

// 嵌套对象同样要求精确JSON tag；encoding/json默认大小写折叠不能进入新线合同。
func (q *NormalMemoryQMPInputs) UnmarshalJSON(raw []byte) error {
	type plain NormalMemoryQMPInputs
	var p plain
	if e := control.UnmarshalObjectBounded(raw, &p, MaxFrameBytes, 4096); e != nil {
		return e
	}
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(raw, &fields); e != nil {
		return e
	}
	for _, n := range []string{"version", "machine", "devices", "summary", "backends", "backend_size"} {
		v := bytes.TrimSpace(fields[n])
		if len(v) == 0 || v[0] != '"' {
			return fmt.Errorf("QMP original bytes require canonical base64 string")
		}
	}
	*q = NormalMemoryQMPInputs(p)
	return nil
}
func (p *NormalMemoryQEMUReference) UnmarshalJSON(raw []byte) error {
	type plain NormalMemoryQEMUReference
	var q plain
	if e := control.UnmarshalObjectBounded(raw, &q, MaxFrameBytes, 4096); e != nil {
		return e
	}
	*p = NormalMemoryQEMUReference(q)
	return nil
}
