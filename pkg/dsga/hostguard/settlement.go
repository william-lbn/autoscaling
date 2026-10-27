package hostguard

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// Settlement是原VM已结清预算的持久高水位，不是新的VM执行许可。保留完整
// 原Request/CM UID/Runner来源和VM预算WAL摘要，终态历史淘汰后仍可拒绝旧
// Host重发。节点owner还须实际核原VM无Active、原Runner完成/无pending/实际
// 值及来源活性；本结构和纯模型不替代这些认证读取或平台物理回收资格。
type Settlement struct {
	Request         control.Request      `json:"request"`
	HostClaim       budget.ObjectRef     `json:"host_claim"`
	Runner          budget.RunnerSources `json:"runner"`
	BudgetSequence  control.Uint64       `json:"budget_sequence"`
	FinalizationSHA string               `json:"finalization_sha256"`
	// Through只扩展“之后无容量效果”的关闭水位，不替换当前成功预算。
	// 两项均零表示旧v4/v5语义；新值必须直接来自VM永久关闭证明，不能
	// 根据当前before、超时或原请求目标推算。含此证明的Node必须升级v6。
	ThroughKey      control.Key    `json:"through_key,omitempty"`
	ThroughSequence control.Uint64 `json:"through_sequence,omitempty"`
}

func (s Settlement) Validate() error {
	var emptyKey control.Key
	digest, err := hex.DecodeString(s.FinalizationSHA)
	if s.Request.Validate() != nil || s.HostClaim.Validate() != nil || s.Runner.Validate() != nil || s.BudgetSequence == 0 || err != nil || len(digest) != 32 || hex.EncodeToString(digest) != s.FinalizationSHA {
		return fmt.Errorf("invalid original Host settlement watermark")
	}
	if s.ThroughKey == emptyKey {
		if s.ThroughSequence != 0 {
			return fmt.Errorf("host closed-through sequence has no original key")
		}
	} else if s.ThroughKey.Validate() != nil || s.ThroughKey.Instance != s.Request.Key.Instance || compare(s.ThroughKey, s.Request.Key) <= 0 || s.ThroughSequence <= s.BudgetSequence {
		return fmt.Errorf("host closed-through watermark regressed or changed VM/PG budget")
	}
	return nil
}

// ClosedThrough区分预算所属Key与较新但已证明无容量效果的关闭Key。只能用于
// 永久拒绝旧派发/结清峰值，不能用于生成PG/Guest/Runner执行票据。
func (s Settlement) ClosedThrough() control.Key {
	if s.ThroughSequence != 0 {
		return s.ThroughKey
	}
	return s.Request.Key
}

func (s Settlement) sameBudget(other Settlement) bool {
	var emptyKey control.Key
	s.ThroughKey, other.ThroughKey = emptyKey, emptyKey
	s.ThroughSequence, other.ThroughSequence = 0, 0
	return s == other
}

// covers只允许同一原预算的关闭水位向前推进。新VM关闭证明可先于Node结清
// CAS出现，因此只读当前预算可核旧Node来源；反方向或同序列换Key一律拒绝。
func (s Settlement) covers(prior Settlement) bool {
	if s.Validate() != nil || prior.Validate() != nil || !s.sameBudget(prior) {
		return false
	}
	order := compare(s.ClosedThrough(), prior.ClosedThrough())
	return order > 0 && s.ThroughSequence > prior.ThroughSequence || order == 0 && s == prior
}

// 旧结清没有Through时保持原JSON字节形状；不能让Go的空struct omitempty把
// 零Key也输出，从而使原v4/v5账本无故写出新字段。新关闭证明不允许删字段降级。
func (s Settlement) MarshalJSON() ([]byte, error) {
	var emptyKey control.Key
	if s.ThroughKey == emptyKey && s.ThroughSequence == 0 {
		type legacy struct {
			Request         control.Request      `json:"request"`
			HostClaim       budget.ObjectRef     `json:"host_claim"`
			Runner          budget.RunnerSources `json:"runner"`
			BudgetSequence  control.Uint64       `json:"budget_sequence"`
			FinalizationSHA string               `json:"finalization_sha256"`
		}
		return json.Marshal(legacy{Request: s.Request, HostClaim: s.HostClaim, Runner: s.Runner, BudgetSequence: s.BudgetSequence, FinalizationSHA: s.FinalizationSHA})
	}
	type plain Settlement
	return json.Marshal(plain(s))
}

func (s *Settlement) UnmarshalJSON(raw []byte) error {
	type plain Settlement
	var v plain
	if err := control.UnmarshalObject(raw, &v); err != nil {
		return err
	}
	*s = Settlement(v)
	return s.Validate()
}

// SettlementFor只从原VM完整当前预算派生匹配值，不能由请求自报替代。这个值
// 可用于只读预算来源核对；产生容量释放仍须可信Settler逐项核原外部事实。
func SettlementFor(m journal.Model) (Settlement, error) {
	var none Settlement
	if m.Validate() != nil || m.ProtocolVersion < 4 || m.BudgetHeld == nil {
		return none, fmt.Errorf("host settlement requires current complete Runner-backed VM budget")
	}
	var c budget.FinalizationCommand
	h := m.BudgetHeld
	if json.Unmarshal(h.Finalization.Command, &c) != nil || c.Validate() != nil || c.Finalized.Sources == nil || c.Finalized.Sources.Runner == nil {
		return none, fmt.Errorf("host settlement lacks original finalization command and sources")
	}
	var emptyKey control.Key
	s := Settlement{Request: c.Finalized.Request, HostClaim: c.Finalized.Sources.HostClaim, Runner: *c.Finalized.Sources.Runner, BudgetSequence: h.RecordedSequence, FinalizationSHA: h.Finalization.CommandSHA256, ThroughKey: emptyKey, ThroughSequence: 0}
	if b := m.BudgetBoundary; b != nil && b.ThroughKey != s.Request.Key {
		// Model.Validate已认证原预算/所有可见历史/永久水位；这里不从
		// HighWater或当前Active自行补造“没有容量效果”的证据。
		s.ThroughKey, s.ThroughSequence = b.ThroughKey, b.ThroughSequence
	}
	return s, s.Validate()
}

func (m Model) settledPlacement(p Plan) (*Settlement, bool) {
	row, err := m.Placement(p.RunnerPodUID)
	if err != nil || row.Settled == nil {
		return nil, false
	}
	return row.Settled, compare(p.Request.Key, row.Settled.ClosedThrough()) <= 0
}

// settle只结清同一原Pod的当前成功预算：目标容量继续持有，旧峰值不再占账；
// 原请求CM仍固定，旧操作持有可收敛为当前一条。高水位不淘汰，任何旧Key均
// 禁止重新Reserve。新于结清水位的在途承诺完整保留，不能因这次CAS清掉。
func (m Model) settle(actor journal.Owner, podUID string, closed Settlement) (Model, []Hold, error) {
	if m.Validate() != nil || m.ProtocolVersion < 3 || actor != m.LeaseFloor || closed.Validate() != nil {
		return m, nil, fmt.Errorf("host settlement requires complete original domain/current owner")
	}
	p, err := m.Placement(podUID)
	if err != nil || !p.Managed || p.VMUID != closed.Request.Key.Instance.VMUID {
		return m, nil, fmt.Errorf("host settlement changed original Pod/VM")
	}
	if p.Settled != nil {
		order := compare(closed.Request.Key, p.Settled.Request.Key)
		if order < 0 || order == 0 && !closed.covers(*p.Settled) || order > 0 && (closed.BudgetSequence <= max(p.Settled.BudgetSequence, p.Settled.ThroughSequence) || compare(closed.Request.Key, p.Settled.ClosedThrough()) <= 0) {
			return m, nil, fmt.Errorf("host settlement regressed or changed original budget watermark")
		}
		if closed == *p.Settled {
			return m, []Hold{}, nil
		}
	}
	current := false
	for _, h := range m.Holds {
		if h.Plan.RunnerPodUID == podUID && h.Plan.Request == closed.Request && h.Claim != nil && *h.Claim == closed.HostClaim {
			current = true
		}
	}
	if !current || m.Sequence == control.Uint64(math.MaxUint64) {
		return m, nil, fmt.Errorf("host settlement has no original published hold or sequence exhausted")
	}
	next, removed := m.Clone(), []Hold{}
	next.ProtocolVersion, next.Sequence = max(m.ProtocolVersion, 4), next.Sequence+1
	if closed.ThroughSequence != 0 {
		next.ProtocolVersion = max(next.ProtocolVersion, 6)
	}
	next.Holds = slices.DeleteFunc(next.Holds, func(h Hold) bool {
		if h.Plan.RunnerPodUID == podUID && h.Plan.Request != closed.Request && compare(h.Plan.Request.Key, closed.ClosedThrough()) <= 0 {
			removed = append(removed, h)
			return true
		}
		return false
	})
	for i := range *next.Placements {
		row := &(*next.Placements)[i]
		if row.PodUID == podUID {
			// 目标本身永久保留，其他更晚操作的peak由Holds另计。跨对象
			// 并发资格由生产者在Node快照之后真实读VM确认，最后Node CAS。
			row.MemoryBytes, row.Settled = closed.Request.TargetVMMemoryBytes, &closed
		}
	}
	if err := next.Validate(); err != nil {
		return m, nil, err
	}
	return next, removed, nil
}
