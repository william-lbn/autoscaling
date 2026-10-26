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

const rootAggregateScope = "pg-cgroup-and-entire-guest-huge-pool"

// RootAggregate是实际资源读回生产的PG聚合约束引用，不是完整VM预算或释放回执。
// Normal memory.max覆盖PG cgroup计费域中的主段/DSG/私有/DSM/LFC及内核成本，
// 整个Guest HugeTLB池另计一次。进程迁入不等于既有共享页收费迁入；完整外部
// 来源仍须覆盖其他计费域，不能由本源省略。OutsideUpperBytes仅声明外部预留，本生产者
// 明确排除它，绝不升级成已认证的系统上界，也不再叠加原生Shared或DSM成本。
//
// 生产调用方必须使用原身份认证的root InspectState；Validate只检查来源结构、
// 算术和摘要，不认证客户端自报内容。后续动作必须Match新的认证View，源限额
// 变更/未知时保留旧引用和预算，不能因PG boot/owner变化把未知清成0。
type RootAggregate struct {
	ProtocolVersion uint32                         `json:"protocol_version"`
	VMShrinkPermit  bool                           `json:"vm_shrink_permit"`
	Scope           string                         `json:"scope"`
	Identity        resourceguard.Identity         `json:"identity"`
	Bootstrap       resourceguard.BootstrapCommand `json:"bootstrap"`
	Birth           resourceguard.ProcessIdentity  `json:"birth"`
	Held            *resourceguard.Command         `json:"held,omitempty"`
	Bound           Bound                          `json:"bound"`
}

type rootAggregateReference struct {
	ProtocolVersion uint32                         `json:"protocol_version"`
	Scope           string                         `json:"scope"`
	Identity        resourceguard.Identity         `json:"identity"`
	Bootstrap       resourceguard.BootstrapCommand `json:"bootstrap"`
	Birth           resourceguard.ProcessIdentity  `json:"birth"`
	Held            *resourceguard.Command         `json:"held,omitempty"`
}

// RootAggregateFromView仅在root原Bootstrap/Held已确认且当前整组限额真实吻合时
// 生产。Actual占用只作内核读回有效性检查，绝不把一次RSS/current当未来承诺。
// 源引用不含瞬时占用或当前owner floor，约束未替换时接管仍引用原承诺。
func RootAggregateFromView(v resourceguard.View) (RootAggregate, error) {
	var result RootAggregate
	if _, err := v.ConfirmedLimits(); err != nil {
		return result, err
	}
	if v.Birth == nil || v.Birth.Validate() != nil || v.Birth.Identity != v.Identity || !v.Actual.CgroupPopulated {
		return result, fmt.Errorf("PG aggregate lacks original process birth or populated resource domain")
	}
	result = RootAggregate{ProtocolVersion: 1, Scope: rootAggregateScope, Identity: v.Identity, Bootstrap: v.Bootstrap, Birth: *v.Birth}
	// 拥有自己的数组/命令，不能让调用方复用View切片后改变已产生的来源。
	result.Bootstrap.Limits.HugePools = append([]resourceguard.HugePool(nil), v.Bootstrap.Limits.HugePools...)
	if v.Held != nil {
		held := *v.Held
		held.HugePools = append([]resourceguard.HugePool(nil), v.Held.HugePools...)
		result.Held = &held
	}
	bound, err := result.derive()
	if err != nil {
		return RootAggregate{}, err
	}
	result.Bound = bound
	return result, nil
}

func (a RootAggregate) derive() (Bound, error) {
	if a.ProtocolVersion != 1 || a.VMShrinkPermit || a.Scope != rootAggregateScope || a.Identity.Validate() != nil ||
		a.Bootstrap.Validate() != nil || a.Bootstrap.Identity != a.Identity || a.Birth.Validate() != nil || a.Birth.Identity != a.Identity {
		return Bound{}, fmt.Errorf("invalid original PG aggregate source")
	}
	limits := a.Bootstrap.Limits
	if a.Held != nil {
		if a.Held.Validate() != nil || a.Held.Identity != a.Identity || a.Held.IssuerEpoch < a.Bootstrap.IssuerEpoch ||
			a.Held.PidsMaxTasks != a.Bootstrap.Limits.PidsMaxTasks || a.Held.PostmasterPID != a.Birth.PostmasterPID || a.Held.PostmasterStartTicks != a.Birth.PostmasterStartTicks {
			return Bound{}, fmt.Errorf("PG aggregate held command differs from original identity/birth")
		}
		limits = resourceguard.Limits{MemoryMaxBytes: a.Held.MemoryMaxBytes, PidsMaxTasks: a.Held.PidsMaxTasks, OutsideUpperBytes: a.Held.OutsideUpperBytes, HugePools: a.Held.HugePools}
	}
	// TotalUpperBytes先做全部乘加溢出/规范检查。仅扣已明确包含的Outside份额，
	// HugeTLB reservation/已用/空闲池不相加，全部只用整池的一份持有限额。
	total, err := limits.TotalUpperBytes()
	if err != nil {
		return Bound{}, err
	}
	upper := control.Uint64(total - uint64(limits.OutsideUpperBytes))
	reference := rootAggregateReference{ProtocolVersion: a.ProtocolVersion, Scope: a.Scope, Identity: a.Identity, Bootstrap: a.Bootstrap, Birth: a.Birth, Held: a.Held}
	raw, err := json.Marshal(reference)
	if err != nil {
		return Bound{}, err
	}
	digest := sha256.Sum256(raw)
	return Bound{Kind: HardLimit, UpperBytes: upper, SourceID: "root-pg-aggregate-" + hex.EncodeToString(digest[:])}, nil
}

// Validate是不可变引用的格式/摘要校验；不能替代原root认证通道和最新Match。
func (a RootAggregate) Validate() error {
	expected, err := a.derive()
	if err != nil {
		return err
	}
	if a.Bound != expected {
		return fmt.Errorf("PG aggregate bound was changed or double counted")
	}
	return nil
}

// Match允许当前owner floor升高但原限额不变；任何新资源命令、partial/未知、
// Guest/cgroup重建或出生身份变化都不能把旧来源重新标成有效或改写其Upper。
func (a RootAggregate) Match(v resourceguard.View) error {
	if err := a.Validate(); err != nil {
		return err
	}
	latest, err := RootAggregateFromView(v)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(a, latest) {
		return fmt.Errorf("PG aggregate original source was replaced")
	}
	return nil
}

func (a *RootAggregate) UnmarshalJSON(data []byte) error {
	type plain RootAggregate
	var next plain
	if err := control.UnmarshalObject(data, &next); err != nil {
		return err
	}
	value := RootAggregate(next)
	if err := value.Validate(); err != nil {
		return err
	}
	*a = value
	return nil
}
