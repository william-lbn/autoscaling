package journal

import (
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// CommittedServiceCoverage只覆盖已完整提交全部资源的最后准入步骤。
// 原Operation和Resume的Planned/Unknown/Settled逐字保留；这份独立证据
// 说明更高原生revision已恢复同一预算的服务，不声称原Resume曾成功。
// Origin是完成CAS的当前owner，Request保留原开放issuer；接管只审计旧
// 开放，不改其epoch重发。它不是VM/Host释放许可，可信来源仍单独认证。
// Outcome与本字段互相约束，删证据不能把原Unknown投影成普通成功。
type CommittedServiceCoverage struct {
	Origin             Owner                          `json:"origin"`
	FinalizationSHA256 string                         `json:"finalization_sha256"`
	Request            control.AdmissionRequest       `json:"request"`
	Receipt            control.AdmissionResponse      `json:"receipt"`
	Query              bool                           `json:"query"`
	Current            control.AdmissionLeaseResponse `json:"current"`
}

// committedResourceBudget不递归调用Model.Validate。只有全部资源及完整
// Finalize已Completed、最后Resume尚Planned或原结果未知/完成时可用；
// 取消、Cleanup和任何资源Unknown都拒绝，不能借准入覆盖退休资源效果。
func committedResourceBudget(op Operation) (budget.Finalized, error) {
	var none budget.Finalized
	if op.AbortRequested || op.Cleanup != nil || len(op.Steps) < 2 {
		return none, fmt.Errorf("service recovery cannot cover abort or missing resource domains")
	}
	for i, step := range op.Steps {
		if i == len(op.Steps)-1 {
			if step.Action != ResumeAdmission || step.Phase != Planned && step.Phase != Unconfirmed && (step.Phase != Settled || step.Observation == nil || step.Observation.Result != Completed) {
				return none, fmt.Errorf("service recovery cannot cover rejected final admission")
			}
		} else if step.Phase != Settled || step.Observation == nil || step.Observation.Result != Completed {
			return none, fmt.Errorf("service recovery cannot retire incomplete resource effect")
		}
	}
	return finalizedBudget(op, op.Steps[len(op.Steps)-2])
}

func (m Model) stableServiceRecovery() error {
	if m.Active == nil {
		return nil
	}
	v := m.serviceClosureVersion()
	if m.ServiceClosure == nil || v != 13 && v != 14 || m.BudgetHeld == nil {
		return fmt.Errorf("service recovery requires original stable finalized closure")
	}
	if _, err := committedResourceBudget(*m.Active); err != nil {
		return err
	}
	if !reflect.DeepEqual(m.BudgetHeld.Finalization, m.Active.Steps[len(m.Active.Steps)-2]) {
		return fmt.Errorf("service recovery changed retained finalization")
	}
	return nil
}

// CheckStableServiceRecovery供生产入口核对原WAL，不生成派发许可。
// 无Active继续既有稳定协议；有Active仅接受13/14及完整资源结算。
func (m Model) CheckStableServiceRecovery() error {
	if err := m.Validate(); err != nil {
		return err
	}
	return m.stableServiceRecovery()
}

func cloneCommittedCoverage(p *CommittedServiceCoverage) *CommittedServiceCoverage {
	if p == nil {
		return nil
	}
	n := *p
	n.Receipt = *cloneAdmissionResponse(&p.Receipt)
	n.Current = cloneLeaseResponse(p.Current)
	return &n
}

func (record Record) closingOutcome() (Outcome, error) {
	p := record.ServiceCoverage
	if p == nil {
		return record.Operation.closingOutcome()
	}
	f, err := committedResourceBudget(record.Operation)
	q := p.Request
	if err != nil || record.Outcome != RecoveredAfterResourceCommit || p.Origin.Validate() != nil || p.Origin.Epoch < record.Operation.Origin.Epoch || p.Origin.Epoch == record.Operation.Origin.Epoch && p.Origin != record.Operation.Origin || p.FinalizationSHA256 != record.Operation.Steps[len(record.Operation.Steps)-2].CommandSHA256 || q.Validate() != nil || !q.Open || q.Instance != record.Operation.Request.Key.Instance || q.IssuerEpoch < record.Operation.Origin.Epoch || q.IssuerEpoch > p.Origin.Epoch || q.BudgetCommitmentID != record.Operation.Request.BudgetCommitmentID || q.ExpectedCapacityGeneration != f.Shared.Observed.CapacityGeneration || q.Revision <= f.Shared.Observed.AdmissionRevision || !p.Receipt.Confirms() || p.Receipt.ValidateFor(q, q.Instance, p.Query) != nil || !validLiveReopen(p.Current, q, p.Origin) || p.Receipt.Observed.IssuerFloorEpoch > p.Origin.Epoch || p.Receipt.Observed.CapacityGeneration != q.ExpectedCapacityGeneration {
		return "", fmt.Errorf("resource commit lacks exact independent live admission coverage")
	}
	last := record.Operation.Steps[len(record.Operation.Steps)-1]
	if last.Phase != Planned {
		var original control.AdmissionRequest
		if json.Unmarshal(last.Command, &original) != nil || !original.Open || original.Instance != q.Instance || original.IssuerEpoch > q.IssuerEpoch || original.Revision >= q.Revision || original.ExpectedCapacityGeneration != q.ExpectedCapacityGeneration || original.BudgetCommitmentID != q.BudgetCommitmentID {
			return "", fmt.Errorf("independent admission coverage does not supersede original resume")
		}
	}
	return RecoveredAfterResourceCommit, nil
}

// completionRecord只从已确认的新开放及刚重审的有效期限派生独立结案。
// 调用者随后在同CAS保存完整Lifecycle上下文和永久预算关闭水位；原Active
// 在此之前始终被冻结。没有重放原Resume，没有补造该步骤的领域回执。
func (m Model) committedCompletionRecord(actor Owner, current control.AdmissionLeaseResponse, sequence control.Uint64) (*Record, error) {
	if m.Active == nil {
		return nil, nil
	}
	if err := m.stableServiceRecovery(); err != nil {
		return nil, err
	}
	r := m.ServiceClosure.Reopen
	if r == nil || r.Phase != Settled || r.Receipt == nil || r.Closing != nil {
		return nil, fmt.Errorf("committed service completion requires confirmed independent reopen")
	}
	op := cloneOperation(*m.Active)
	p := &CommittedServiceCoverage{Origin: actor, FinalizationSHA256: op.Steps[len(op.Steps)-2].CommandSHA256, Request: r.Request, Receipt: *cloneAdmissionResponse(r.Receipt), Query: r.Query, Current: cloneLeaseResponse(current)}
	record := &Record{Operation: op, Outcome: RecoveredAfterResourceCommit, ClosedSequence: sequence, ServiceCoverage: p}
	_, err := record.closingOutcome()
	return record, err
}

func (m Model) validateCommittedLifecycle(c Model, l *ServiceLifecycle) error {
	if c.Active == nil {
		return nil
	}
	want, err := c.committedCompletionRecord(l.Origin, l.Current, l.RecordedSequence)
	if err != nil {
		return err
	}
	// 完成本身必须同时建立永久预算关闭证明。后续Start可以撤销它，
	// 但HWM尚未推进且没有新Active时，删掉水位不能成为合法完成投影。
	if m.Active == nil && m.HighWater != nil && *m.HighWater == want.Operation.Request.Key && (m.BudgetBoundary == nil || m.BudgetBoundary.SucceededSequence != want.ClosedSequence) {
		return fmt.Errorf("committed service completion lost atomic closed budget boundary")
	}
	for _, record := range m.History {
		if record.Operation.Request.Key == want.Operation.Request.Key {
			if !reflect.DeepEqual(record, *want) {
				return fmt.Errorf("committed service lifecycle rewrote original operation or coverage")
			}
			return nil
		}
	}
	// 只有真正推进过历史淘汰计数，且HWM已经严格越过原Key，才允许
	// 最近普通结果缓存不再含它；完整原Unknown仍留在Lifecycle上下文。
	if m.ExpiredResults <= c.ExpiredResults || m.HighWater == nil || compareKeys(*m.HighWater, want.Operation.Request.Key) <= 0 {
		return fmt.Errorf("committed service lifecycle lost required terminal coverage")
	}
	return nil
}

func (p *CommittedServiceCoverage) UnmarshalJSON(raw []byte) error {
	type plain CommittedServiceCoverage
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxObservationBytes*2+4096, 16384); err != nil {
		return err
	}
	*p = CommittedServiceCoverage(next)
	return nil
}
