package bootprotocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const NormalMemoryOriginCommitProtocolVersion uint32 = 2
const NormalMemoryOriginCommitScope = "normal-memory-origin-host-commit-v2"

// Commit8只绑定原1..7的准确历史。它不是Guest已FS/PG Ready、Host预算或normal
// 许可。中立codec只验证语法；真实Host producer必须先同步原Host全部前缀。
// Ref6/Ack7的原数据格式仍为1，旧selector0/1和其准确编码完全不变。
type NormalMemoryOriginHostCommit struct {
	ProtocolVersion     uint32 `json:"protocol_version"`
	Scope               string `json:"scope"`
	OriginalNonce       string `json:"original_nonce"`
	HostStageSHA256     string `json:"host_stage_sha256"`
	HostReferenceSHA256 string `json:"host_reference_sha256"`
	GuestAckSHA256      string `json:"guest_ack_sha256"`
	GuestBootID         string `json:"guest_boot_id"`
	RootInstanceNonce   string `json:"root_instance_nonce"`
}

func NormalMemoryOriginGuestAckSHA256(a NormalMemoryOriginGuestAck) (string, error) {
	if e := a.Validate(); e != nil {
		return "", e
	}
	b, e := json.Marshal(a)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
func (c NormalMemoryOriginHostCommit) Validate() error {
	if c.ProtocolVersion != NormalMemoryOriginCommitProtocolVersion || c.Scope != NormalMemoryOriginCommitScope || !digest(c.OriginalNonce) || !digest(c.HostStageSHA256) || !digest(c.HostReferenceSHA256) || !digest(c.GuestAckSHA256) || !uuid(c.GuestBootID) || !digest(c.RootInstanceNonce) {
		return fmt.Errorf("normal memory Host commit2 malformed")
	}
	return nil
}
func (c NormalMemoryOriginHostCommit) MatchReferenceAndAck(r NormalMemoryOriginHostReference, a NormalMemoryOriginGuestAck) error {
	if e := c.Validate(); e != nil {
		return e
	}
	if e := a.MatchReference(r); e != nil {
		return e
	}
	h, e := r.SHA256()
	if e != nil {
		return e
	}
	ah, e := NormalMemoryOriginGuestAckSHA256(a)
	if e != nil {
		return e
	}
	if c.OriginalNonce != r.Prepared.Challenge.Nonce || c.HostStageSHA256 != r.Completion.HostStageSHA256 || c.HostReferenceSHA256 != h || c.GuestAckSHA256 != ah || c.GuestBootID != a.GuestBootID || c.RootInstanceNonce != a.RootInstanceNonce {
		return fmt.Errorf("normal memory commit changed exact original nonce/reference/ack")
	}
	return nil
}

// 纯数据构造器不确认任何磁盘同步/实际peer；不能从其返回值制造opaque来源。
func NewNormalMemoryOriginHostCommit(r NormalMemoryOriginHostReference, a NormalMemoryOriginGuestAck) (NormalMemoryOriginHostCommit, error) {
	var z NormalMemoryOriginHostCommit
	if e := a.MatchReference(r); e != nil {
		return z, e
	}
	h, e := r.SHA256()
	if e != nil {
		return z, e
	}
	ah, e := NormalMemoryOriginGuestAckSHA256(a)
	if e != nil {
		return z, e
	}
	c := NormalMemoryOriginHostCommit{NormalMemoryOriginCommitProtocolVersion, NormalMemoryOriginCommitScope, r.Prepared.Challenge.Nonce, r.Completion.HostStageSHA256, h, ah, a.GuestBootID, a.RootInstanceNonce}
	if e = c.MatchReferenceAndAck(r, a); e != nil {
		return z, e
	}
	return c, nil
}
func (c *NormalMemoryOriginHostCommit) UnmarshalJSON(raw []byte) error {
	type plain NormalMemoryOriginHostCommit
	var v plain
	if e := control.UnmarshalObjectBounded(raw, &v, MaxFrameBytes, 4096); e != nil {
		return e
	}
	n := NormalMemoryOriginHostCommit(v)
	if e := n.Validate(); e != nil {
		return e
	}
	*c = n
	return nil
}
func WriteNormalMemoryOriginHostCommit(w io.Writer, c NormalMemoryOriginHostCommit) error {
	if e := c.Validate(); e != nil {
		return e
	}
	return writeFrame(w, 8, c)
}
func ReadNormalMemoryOriginHostCommit(r io.Reader) (NormalMemoryOriginHostCommit, error) {
	var c NormalMemoryOriginHostCommit
	if e := readFrame(r, 8, &c); e != nil {
		return c, e
	}
	return c, c.Validate()
}
