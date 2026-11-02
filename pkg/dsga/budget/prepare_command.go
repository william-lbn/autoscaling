package budget

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// PrepareCommand 保留创建 Outside 前的完整原命令；字段顺序与既有 wire 相同。
// 这只是有界持久化合同，资格认证和硬限写入仍由原生产者及 Root 完成。
type PrepareCommand struct {
	ProtocolVersion uint32              `json:"protocol_version"`
	Qualification   QualificationSource `json:"qualification"`
	Prepared        Prepared            `json:"prepared"`
	RootSHA256      string              `json:"root_sha256"`
	OutsideName     string              `json:"outside_name"`
}

// CanonicalOutsideName 与旧命令相同：名称自身清空后对全部字段内容寻址。
func (c PrepareCommand) CanonicalOutsideName() (string, error) {
	c.OutsideName = ""
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "dsga-o-" + hex.EncodeToString(sum[:])[:56], nil
}

func (c PrepareCommand) Validate() error {
	p := c.Prepared
	if c.ProtocolVersion != 1 || c.Qualification.Validate() != nil || p.ValidateFor(p.Request, p.Identity) != nil || p.OutsideClaim != nil || !validDigest(c.RootSHA256) {
		return fmt.Errorf("incomplete original prepare command")
	}
	prefix := fmt.Sprintf("qualified-%s-%d-", c.Qualification.SHA256, p.Request.TargetVMMemoryBytes)
	for _, item := range []struct {
		bound  Bound
		suffix string
	}{{p.SystemOutsidePG, "system"}, {p.Transition, "transition"}, {p.Safety, "safety"}} {
		if item.bound.Kind != CommittedUpper || item.bound.SourceID != prefix+item.suffix {
			return fmt.Errorf("prepare bounds differ from original qualification and target")
		}
	}
	name, err := c.CanonicalOutsideName()
	if err != nil || c.OutsideName != name {
		return fmt.Errorf("prepare Outside name differs from complete original command")
	}
	return nil
}

func (c *PrepareCommand) UnmarshalJSON(data []byte) error {
	type plain PrepareCommand
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	value := PrepareCommand(next)
	if err := value.Validate(); err != nil {
		return err
	}
	*c = value
	return nil
}
