package resourceguard

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const arrayAuthorityScope = "compute_ctl:dsga:array-authority"
const arrayAuthorityMaximumBodyBytes = 16 * 1024

// signedArrayRequest属于专用JWT签名claims；不得从HTTP body复制后当签名事实。
type signedArrayRequest struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Method          string `json:"method"`
	Path            string `json:"path"`
	BodySHA256      string `json:"body_sha256"`
}

func (b *signedArrayRequest) UnmarshalJSON(raw []byte) error {
	type plain signedArrayRequest
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*b = signedArrayRequest(next)
	return nil
}
func arrayAuthorityPath(path string) bool {
	switch path {
	case "/dsg/v1/resource/array-authorize", "/dsg/v1/resource/array-observe", "/dsg/v1/resource/array-replay-original":
		return true
	}
	return false
}
func (b signedArrayRequest) matches(r *http.Request, raw []byte) bool {
	if b.ProtocolVersion != 1 || b.Method != http.MethodPost || !arrayAuthorityPath(b.Path) || r.Method != b.Method || r.URL.Path != b.Path || len(raw) == 0 || len(raw) > arrayAuthorityMaximumBodyBytes || len(b.BodySHA256) != 64 || b.BodySHA256 == strings.Repeat("0", 64) {
		return false
	}
	digest, err := hex.DecodeString(b.BodySHA256)
	return err == nil && hex.EncodeToString(digest) == b.BodySHA256 && b.BodySHA256 == physicalDigest(raw)
}

// ArrayAuthorityCommand由真实journal Start CAS后的executor产生。Root仍重读原
// resource WAL、Birth、预算和固定registry；Request/ResourceReference只是选取
// 已有原来源，不能凭此packet创建Birth、提高M/normal/arrays cap。
type ArrayAuthorityCommand struct {
	ProtocolVersion         uint32          `json:"protocol_version"`
	Identity                Identity        `json:"identity"`
	IssuerEpoch             control.Uint64  `json:"issuer_epoch"`
	ResourceReferenceSHA256 string          `json:"resource_reference_sha256"`
	Request                 control.Request `json:"request"`
}

func (c ArrayAuthorityCommand) Validate() error {
	if c.ProtocolVersion != 1 || c.Identity.Validate() != nil || c.Request.Validate() != nil || c.IssuerEpoch == 0 || c.IssuerEpoch != c.Request.Key.IssuerEpoch || c.Identity.VMUID != c.Request.Key.Instance.VMUID || c.Identity.ComputeInstanceID != c.Request.Key.Instance.ComputeInstanceID || !physicalDigestValid(c.ResourceReferenceSHA256) {
		return fmt.Errorf("invalid original array authority command")
	}
	return nil
}
func (c *ArrayAuthorityCommand) UnmarshalJSON(raw []byte) error {
	type plain ArrayAuthorityCommand
	var n plain
	if err := control.UnmarshalObject(raw, &n); err != nil {
		return err
	}
	next := ArrayAuthorityCommand(n)
	if err := next.Validate(); err != nil {
		return err
	}
	*c = next
	return nil
}

type ArrayAuthorityEvidence struct {
	ProtocolVersion uint32                `json:"protocol_version"`
	VMShrinkPermit  bool                  `json:"vm_shrink_permit"`
	Command         ArrayAuthorityCommand `json:"command"`
	CommandSHA256   string                `json:"command_sha256"`
	ResourceFloor   control.Uint64        `json:"resource_floor"`
	Recorded        bool                  `json:"recorded"`
}

func (e ArrayAuthorityEvidence) validateFor(c ArrayAuthorityCommand) error {
	raw, err := json.Marshal(c)
	if err != nil || c.Validate() != nil || e.ProtocolVersion != 1 || e.VMShrinkPermit || !e.Recorded || e.Command != c || e.CommandSHA256 != physicalDigest(raw) || e.ResourceFloor != c.IssuerEpoch {
		return fmt.Errorf("array original authority not confirmed")
	}
	return nil
}
func (e *ArrayAuthorityEvidence) UnmarshalJSON(raw []byte) error {
	type plain ArrayAuthorityEvidence
	var n plain
	if err := control.UnmarshalObject(raw, &n); err != nil {
		return err
	}
	*e = ArrayAuthorityEvidence(n)
	return nil
}

type arrayAuthorityBackend interface {
	AuthorizeArrays(context.Context, ArrayAuthorityCommand) (ArrayAuthorityEvidence, error)
	ObserveArrayAuthority(context.Context, ArrayAuthorityCommand) (ArrayAuthorityEvidence, error)
	ReplayArrayAuthority(context.Context, ArrayAuthorityCommand) (ArrayAuthorityEvidence, error)
}

// ArrayAuthorityReference只摘要原资源记录的稳定完整字段，不把一次current/RSS
// 混入来源选择。Root在同原flock内对真实record重算；客户端不能凭摘要自发授权。
func (v View) ArrayAuthorityReference() (string, error) {
	if _, err := v.ConfirmedLimits(); err != nil {
		return "", err
	}
	if v.Birth == nil {
		return "", fmt.Errorf("array original Birth missing")
	}
	return arrayAuthorityRecordReference(record{Version: 1, Identity: v.Identity, Floor: v.IssuerFloorEpoch, Original: v.Original, Held: v.Held, Applied: v.Applied, Cancelled: v.Cancelled, Bootstrap: &bootstrapRecord{Original: v.Bootstrap, Applied: v.BootstrapApplied, SystemPlacementObserved: v.SystemPlacementObserved}, Birth: v.Birth})
}
func arrayAuthorityRecordReference(r record) (string, error) {
	// Bootstrap来源字段(含PreSystem ref)单独来自真实record；对外View没有该私有
	// ref，故以原公开Bootstrap完整Command及已确认System标志形成同一投影。
	type reference struct {
		ProtocolVersion  uint32           `json:"protocol_version"`
		Identity         Identity         `json:"identity"`
		Floor            control.Uint64   `json:"floor"`
		Bootstrap        BootstrapCommand `json:"bootstrap"`
		BootstrapApplied bool             `json:"bootstrap_applied"`
		SystemObserved   bool             `json:"system_observed"`
		Original         *Command         `json:"original"`
		Held             *Command         `json:"held"`
		Applied          bool             `json:"applied"`
		Cancelled        *Command         `json:"cancelled"`
		Birth            *ProcessIdentity `json:"birth"`
	}
	if r.Bootstrap == nil || r.Birth == nil {
		return "", fmt.Errorf("array source is incomplete")
	}
	raw, err := json.Marshal(reference{1, r.Identity, r.Floor, r.Bootstrap.Original, r.Bootstrap.Applied, r.Bootstrap.SystemPlacementObserved, r.Original, r.Held, r.Applied, r.Cancelled, r.Birth})
	if err != nil {
		return "", err
	}
	return physicalDigest(raw), nil
}
