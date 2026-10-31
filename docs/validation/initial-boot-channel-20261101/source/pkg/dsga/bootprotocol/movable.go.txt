package bootprotocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"io"
)

const InitialMovableScope = "neon-initial-movable-challenge-v1"
const InitialCompletionScope = "neon-initial-completion-v1"

// Host阶段必须是真实actual_target_observed的原持久引用。语法检查本身不认证Host。
type InitialMovableChallenge struct {
	ProtocolVersion uint32                `json:"protocol_version"`
	Scope           string                `json:"scope"`
	Preboot         InitialPrebootReceipt `json:"preboot"`
	HostStageSHA256 string                `json:"host_stage_sha256"`
	TargetBytes     control.Uint64        `json:"target_bytes"`
}

func (c InitialMovableChallenge) Validate() error {
	if c.ProtocolVersion != 1 || c.Scope != InitialMovableScope || c.Preboot.Validate() != nil || !digest(c.HostStageSHA256) || c.TargetBytes != c.Preboot.Challenge.BusinessMinimum || (c.TargetBytes-c.Preboot.Challenge.OriginalBase)%4096 != 0 {
		return fmt.Errorf("original movable challenge differs")
	}
	return nil
}

// 完整Physical原报告留在Guest。这里仅投影固定reader的事实给同一可信通道；
// RootInstanceNonce是当前真实发行者，Preboot仍为Host持久持有的原历史对象，不重写。
type InitialMovableReceipt struct {
	Challenge              InitialMovableChallenge `json:"challenge"`
	GuestBootID            string                  `json:"guest_boot_id"`
	RootInstanceNonce      string                  `json:"root_instance_nonce"`
	PreparedReceiptSHA256  string                  `json:"prepared_receipt_sha256"`
	PhysicalOriginSHA256   string                  `json:"physical_origin_sha256"`
	PreSystemSHA256        string                  `json:"pre_system_sha256"`
	CurrentPhysicalSHA256  string                  `json:"current_physical_sha256"`
	ExpectedIncrementBytes control.Uint64          `json:"expected_increment_bytes"`
	MovablePresentPages    control.Uint64          `json:"movable_present_pages"`
}

func (r InitialMovableReceipt) Validate() error {
	if r.Challenge.Validate() != nil {
		return fmt.Errorf("original movable challenge missing")
	}
	p := r.Challenge.Preboot
	expected := r.Challenge.TargetBytes - p.Challenge.OriginalBase
	h, e := p.SHA256()
	if e != nil || r.GuestBootID != p.GuestBootID || !digest(r.RootInstanceNonce) || r.PreparedReceiptSHA256 != h || r.PhysicalOriginSHA256 != p.PhysicalOriginSHA256 || r.PreSystemSHA256 != p.PreSystemSHA256 || !digest(r.CurrentPhysicalSHA256) || r.ExpectedIncrementBytes != expected || r.MovablePresentPages != expected/4096 {
		return fmt.Errorf("original movable receipt binding/pages differ")
	}
	return nil
}
func (r InitialMovableReceipt) MatchChallenge(c InitialMovableChallenge) error {
	if c.Validate() != nil || r.Validate() != nil || r.Challenge != c {
		return fmt.Errorf("original movable receipt challenge differs")
	}
	return nil
}

// Completion只结束已持有预算的首次启动握手；Guest仍须正式Bootstrap/检查后才启动PG。
// Host必须先持久complete_non_authorizing再发送，不能用本对象签normal或物理free。
type InitialCompletion struct {
	ProtocolVersion      uint32                  `json:"protocol_version"`
	Scope                string                  `json:"scope"`
	Challenge            InitialMovableChallenge `json:"challenge"`
	MovableReceiptSHA256 string                  `json:"movable_receipt_sha256"`
	HostStageSHA256      string                  `json:"host_stage_sha256"`
}

func (c InitialCompletion) Validate() error {
	if c.ProtocolVersion != 1 || c.Scope != InitialCompletionScope || c.Challenge.Validate() != nil || !digest(c.MovableReceiptSHA256) || !digest(c.HostStageSHA256) {
		return fmt.Errorf("original initial completion differs")
	}
	return nil
}
func (c InitialCompletion) MatchReceipt(r InitialMovableReceipt) error {
	h, e := r.SHA256()
	if e != nil || c.Validate() != nil || c.Challenge != r.Challenge || c.MovableReceiptSHA256 != h {
		return fmt.Errorf("initial completion differs from original movable receipt")
	}
	return nil
}
func wireSHA(v any) (string, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
func (r InitialPrebootReceipt) SHA256() (string, error) {
	if e := r.Validate(); e != nil {
		return "", e
	}
	return wireSHA(r)
}
func (r InitialMovableReceipt) SHA256() (string, error) {
	if e := r.Validate(); e != nil {
		return "", e
	}
	return wireSHA(r)
}
func (c *InitialMovableChallenge) UnmarshalJSON(raw []byte) error {
	type plain InitialMovableChallenge
	var p plain
	if e := control.UnmarshalObjectBounded(raw, &p, MaxFrameBytes, 512); e != nil {
		return e
	}
	*c = InitialMovableChallenge(p)
	return nil
}
func (r *InitialMovableReceipt) UnmarshalJSON(raw []byte) error {
	type plain InitialMovableReceipt
	var p plain
	if e := control.UnmarshalObjectBounded(raw, &p, MaxFrameBytes, 512); e != nil {
		return e
	}
	*r = InitialMovableReceipt(p)
	return nil
}
func (c *InitialCompletion) UnmarshalJSON(raw []byte) error {
	type plain InitialCompletion
	var p plain
	if e := control.UnmarshalObjectBounded(raw, &p, MaxFrameBytes, 512); e != nil {
		return e
	}
	*c = InitialCompletion(p)
	return nil
}
func WriteInitialMovableChallenge(w io.Writer, c InitialMovableChallenge) error {
	if e := c.Validate(); e != nil {
		return e
	}
	return writeFrame(w, 3, c)
}
func ReadInitialMovableChallenge(r io.Reader) (InitialMovableChallenge, error) {
	var c InitialMovableChallenge
	if e := readFrame(r, 3, &c); e != nil {
		return InitialMovableChallenge{}, e
	}
	if e := c.Validate(); e != nil {
		return InitialMovableChallenge{}, e
	}
	return c, nil
}
func WriteInitialMovableReceipt(w io.Writer, r InitialMovableReceipt) error {
	if e := r.Validate(); e != nil {
		return e
	}
	return writeFrame(w, 4, r)
}
func ReadInitialMovableReceipt(r io.Reader) (InitialMovableReceipt, error) {
	var c InitialMovableReceipt
	if e := readFrame(r, 4, &c); e != nil {
		return InitialMovableReceipt{}, e
	}
	if e := c.Validate(); e != nil {
		return InitialMovableReceipt{}, e
	}
	return c, nil
}
func WriteInitialCompletion(w io.Writer, c InitialCompletion) error {
	if e := c.Validate(); e != nil {
		return e
	}
	return writeFrame(w, 5, c)
}
func ReadInitialCompletion(r io.Reader) (InitialCompletion, error) {
	var c InitialCompletion
	if e := readFrame(r, 5, &c); e != nil {
		return InitialCompletion{}, e
	}
	if e := c.Validate(); e != nil {
		return InitialCompletion{}, e
	}
	return c, nil
}
