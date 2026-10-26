package budget

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// SystemCharged是完整SystemOutsidePG的一个独立计费域组件，不能单独赋给完整
// 外部上界。PG聚合源不含本域；根组既有charge、未计费内核项等仍须独立来源。
// 本组件不随PG boot、运行资源revision或owner floor重造，也不产生VM许可。
// FromView/Match的调用者必须使用原Guest认证root通道；摘要不认证自报JSON。
type SystemCharged struct {
	ProtocolVersion uint32                         `json:"protocol_version"`
	VMShrinkPermit  bool                           `json:"vm_shrink_permit"`
	Scope           string                         `json:"scope"`
	Identity        resourceguard.Identity         `json:"identity"`
	Bootstrap       resourceguard.BootstrapCommand `json:"bootstrap"`
	Bound           Bound                          `json:"bound"`
}

func SystemChargedFromView(v resourceguard.View) (SystemCharged, error) {
	if _, err := v.ConfirmedLimits(); err != nil {
		return SystemCharged{}, err
	}
	s := SystemCharged{ProtocolVersion: 1, Scope: resourceguard.SystemChargedScope, Identity: v.Identity, Bootstrap: v.Bootstrap}
	s.Bootstrap.Limits.HugePools = append([]resourceguard.HugePool(nil), v.Bootstrap.Limits.HugePools...)
	bound, err := s.derive()
	if err != nil {
		return SystemCharged{}, err
	}
	s.Bound = bound
	return s, nil
}
func (s SystemCharged) derive() (Bound, error) {
	if s.ProtocolVersion != 1 || s.VMShrinkPermit || s.Scope != resourceguard.SystemChargedScope || s.Identity.Validate() != nil || s.Bootstrap.Validate() != nil || s.Bootstrap.Identity != s.Identity || s.Bootstrap.Limits.SystemMemoryMaxBytes == 0 {
		return Bound{}, fmt.Errorf("invalid system charged-domain source")
	}
	// 只摘要原身份/创建，排除易变的当前RSS、PG Birth/Held/floor；运行PG额度变化
	// 不替换同一个系统承诺。原创建额度本身更换则是另一来源，不能无声收养。
	raw, err := json.Marshal(struct {
		Scope     string                         `json:"scope"`
		Identity  resourceguard.Identity         `json:"identity"`
		Bootstrap resourceguard.BootstrapCommand `json:"bootstrap"`
	}{s.Scope, s.Identity, s.Bootstrap})
	if err != nil {
		return Bound{}, err
	}
	hash := sha256.Sum256(raw)
	return Bound{Kind: HardLimit, UpperBytes: s.Bootstrap.Limits.SystemMemoryMaxBytes, SourceID: "root-system-charged-" + hex.EncodeToString(hash[:])}, nil
}
func (s SystemCharged) Validate() error {
	b, err := s.derive()
	if err != nil {
		return err
	}
	if s.Bound != b {
		return fmt.Errorf("system charged-domain bound changed or included residual outside scope")
	}
	return nil
}
func (s SystemCharged) Match(v resourceguard.View) error {
	if err := s.Validate(); err != nil {
		return err
	}
	latest, err := SystemChargedFromView(v)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(s, latest) {
		return fmt.Errorf("system charged-domain original source replaced")
	}
	return nil
}
func (s *SystemCharged) UnmarshalJSON(b []byte) error {
	type plain SystemCharged
	var p plain
	if err := control.UnmarshalObject(b, &p); err != nil {
		return err
	}
	next := SystemCharged(p)
	if err := next.Validate(); err != nil {
		return err
	}
	*s = next
	return nil
}
