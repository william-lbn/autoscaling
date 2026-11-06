package dsga

import (
	"encoding/json"
	"fmt"
)

// ServiceAdmission 将同一个 PG boot 的准入读回用于恢复日志。旧 boot 逐字保留
// 原生租约格式；硬预算 boot 保存实际能力和内存响应，不制造续租、期限或写回执。
// 读回本身不能重开 SQL，调用方仍须审计原预算并确认原写者、UID 和资源版本。
type ServiceAdmission struct {
	Timed  *AdmissionLeaseResponse
	Budget *BudgetAdmissionRead
}

type BudgetAdmissionRead struct {
	Capabilities Capabilities   `json:"capabilities"`
	Memory       MemoryResponse `json:"memory"`
}

// 有限租期的原生观察沿用旧账本表示。
func (r AdmissionLeaseResponse) ForServiceAdmission() ServiceAdmission {
	return ServiceAdmission{Timed: &r}
}

func (f ServiceAdmission) Snapshot() Snapshot {
	if f.Budget != nil && f.Timed == nil {
		return f.Budget.Memory.Observed
	}
	if f.Timed != nil && f.Budget == nil {
		return f.Timed.Observed
	}
	return Snapshot{}
}

func (f ServiceAdmission) BudgetBound() bool { return f.Budget != nil && f.Timed == nil }

// Timed 和 Budget 在同一 boot 互斥。旧格式的 observe、armed、epoch 及过期规则保留。
// allowExpired 仅供独立保护关闭使用，不能据它重开 SQL。
func (f ServiceAdmission) ValidRead(instance Instance, epoch Uint64, allowExpired bool) bool {
	if f.BudgetBound() {
		b := f.Budget
		bound, err := b.Capabilities.Bind(instance.VMUID, instance.ComputeInstanceID)
		return err == nil && bound == instance && b.Capabilities.AdmissionPolicy == AdmissionPolicyEnforcedBudget &&
			b.Memory.ValidateFor(instance) == nil && b.Memory.Observed.IssuerFloorEpoch == epoch
	}
	l := f.Timed
	return l != nil && f.Budget == nil && l.ValidateFor(l.Request, instance) == nil &&
		l.Request.Action == "observe" && l.Armed && (allowExpired || !l.Expired) &&
		l.Request.IssuerEpoch == epoch && l.Observed.IssuerFloorEpoch == epoch && l.LeaseEpoch <= epoch
}

// 同 boot 内的策略不得切换。有限租期比较真实 deadline；硬预算比较实际写者代次。
func (f ServiceAdmission) AtLeast(previous ServiceAdmission) bool {
	if f.BudgetBound() && previous.BudgetBound() {
		now, before := f.Snapshot(), previous.Snapshot()
		return now.Instance == before.Instance && now.IssuerFloorEpoch >= before.IssuerFloorEpoch
	}
	return f.Budget == nil && previous.Budget == nil && f.Timed != nil && previous.Timed != nil &&
		f.Timed.Observed.Instance == previous.Timed.Observed.Instance &&
		f.Timed.DeadlineNS >= previous.Timed.DeadlineNS && f.Timed.LeaseEpoch >= previous.Timed.LeaseEpoch
}

func (f ServiceAdmission) MarshalJSON() ([]byte, error) {
	if f.BudgetBound() {
		return json.Marshal(struct {
			Budget *BudgetAdmissionRead `json:"enforced_budget"`
		}{f.Budget})
	}
	if f.Timed == nil || f.Budget != nil {
		return nil, fmt.Errorf("service admission requires one native observation")
	}
	return json.Marshal(f.Timed)
}

func (f *ServiceAdmission) UnmarshalJSON(raw []byte) error {
	if len(raw) > 64*1024 {
		return fmt.Errorf("service admission exceeds native observation limit")
	}
	var kind struct {
		Budget json.RawMessage `json:"enforced_budget"`
	}
	if err := json.Unmarshal(raw, &kind); err != nil {
		return err
	}
	if kind.Budget != nil {
		var wire struct {
			Budget BudgetAdmissionRead `json:"enforced_budget"`
		}
		if err := UnmarshalObject(raw, &wire); err != nil {
			return err
		}
		candidate := ServiceAdmission{Budget: &wire.Budget}
		pg := candidate.Snapshot()
		if !candidate.ValidRead(pg.Instance, pg.IssuerFloorEpoch, false) {
			return fmt.Errorf("invalid boot-bound enforced budget observation")
		}
		*f = candidate
		return nil
	}
	var legacy AdmissionLeaseResponse
	if err := json.Unmarshal(raw, &legacy); err != nil {
		return err
	}
	*f = legacy.ForServiceAdmission()
	return nil
}
