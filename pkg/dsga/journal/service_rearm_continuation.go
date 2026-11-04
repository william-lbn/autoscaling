package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ServiceRearmContinuation 是真实闭合服务的期限续接，不代表发生过业务
// 开放或再关闭。首次 Rearm 原文永久保留；只保存完整直接前驱及累计摘要，
// 复用既有有界历史载体。版本1严格解码，旧读者必须拒绝新字段后升级。
type ServiceRearmContinuation struct {
	ProtocolVersion uint32                   `json:"protocol_version"`
	Cycle           control.Uint64           `json:"cycle"`
	Rearm           ServiceRearm             `json:"rearm"`
	Retired         *ServiceReopenRetirement `json:"retired"`
}

// LatestRearm 只选择持久化的最近意图，不生成命令或发放派发许可。
func (c *ServiceClosure) LatestRearm() *ServiceRearm {
	if c.RearmContinuation != nil {
		return &c.RearmContinuation.Rearm
	}
	return c.Rearm
}

func cloneServiceRearmContinuation(v *ServiceRearmContinuation) *ServiceRearmContinuation {
	if v == nil {
		return nil
	}
	next := *v
	next.Rearm.Before = cloneLeaseResponse(next.Rearm.Before)
	if next.Rearm.Receipt != nil {
		receipt := *next.Rearm.Receipt
		receipt.Current = cloneLeaseResponse(receipt.Current)
		next.Rearm.Receipt = &receipt
	}
	if next.Retired != nil {
		retired := *next.Retired
		retired.Context = append([]byte(nil), retired.Context...)
		next.Retired = &retired
	}
	return &next
}

func (m Model) validateServiceRearmContinuation() error {
	c := m.ServiceClosure
	if c.RearmContinuation == nil {
		return nil
	}
	return m.validateClosedRearmRecord(c.RearmContinuation, false)
}

func (m Model) validateClosedRearmRecord(n *ServiceRearmContinuation, archived bool) error {
	c := m.ServiceClosure
	if n.ProtocolVersion != 1 || n.Cycle == 0 || c.Rearm == nil || c.Rearm.Phase != Settled || c.Rearm.Receipt == nil {
		return fmt.Errorf("closed rearm continuation lacks original confirmed recovery")
	}
	// 复用原恢复的完整 native/预算/关闭校验，不递归校验后继，也不改变原模型。
	carrier, closure := m, *c
	closure.Rearm, closure.RearmContinuation = &n.Rearm, nil
	carrier.ServiceClosure = &closure
	if err := carrier.validateServiceRearm(); err != nil {
		return err
	}
	previous := c.Rearm
	if archived {
		if n.Retired != nil {
			return fmt.Errorf("archived closed rearm embeds recursive history")
		}
	} else if n.Cycle == 1 {
		if n.Retired != nil {
			return fmt.Errorf("first closed rearm invented predecessor")
		}
	} else {
		h := n.Retired
		if h == nil || h.ContextSHA256 != rawDigest(h.Context) || h.EvidenceSHA256 != reopenRetirementDigest(h.Context, h.PreviousSHA256) || n.Cycle == 2 && h.PreviousSHA256 != "" || n.Cycle > 2 && !lifecycleDigest(h.PreviousSHA256) {
			return fmt.Errorf("closed rearm lost complete bounded predecessor")
		}
		type plain ServiceRearmContinuation
		var decoded plain
		if err := control.UnmarshalObjectBounded(h.Context, &decoded, MaxRawBytes/2, 16384); err != nil {
			return err
		}
		old := ServiceRearmContinuation(decoded)
		if old.Cycle == control.Uint64(math.MaxUint64) || old.Cycle+1 != n.Cycle {
			return fmt.Errorf("closed rearm cycle differs")
		}
		if err := m.validateClosedRearmRecord(&old, true); err != nil {
			return err
		}
		previous = &old.Rearm
	}
	b, r := n.Rearm.Before, n.Rearm
	if previous.Phase != Settled || previous.Receipt == nil || r.Origin.Epoch <= previous.Origin.Epoch || r.StartedSequence <= previous.RecordedSequence || b.DeadlineNS < previous.Receipt.AppliedDeadlineNS || b.LeaseEpoch < previous.Origin.Epoch || b.Observed.AdmissionRevision != previous.Before.Observed.AdmissionRevision || r.ClosureVersion != previous.ClosureVersion || r.ClosureSHA256 != previous.ClosureSHA256 || r.HeldSHA256 != previous.HeldSHA256 {
		return fmt.Errorf("closed rearm does not follow exact higher-owner expired recovery")
	}
	return nil
}

// ContinueServiceRearm 不接受调用方期限或原生命令；只接收最近已确认
// 恢复摘要和由正式客户端新读的 expired/closed。Unknown 永不被新选择退休。
func (m Model) ContinueServiceRearm(actor Owner, selector string, before control.AdmissionLeaseResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Rearm == nil || c.Reopen != nil || c.Pending() || m.stableServiceRecovery() != nil {
		return m, fmt.Errorf("closed rearm continuation requires original stable closed service")
	}
	old := c.LatestRearm()
	if old.Phase != Settled || old.Receipt == nil || selector != old.CommandSHA256 || actor.Epoch <= old.Origin.Epoch {
		return m, fmt.Errorf("closed rearm continuation requires latest settled recovery and higher owner")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	cycle := control.Uint64(1)
	var retired *ServiceReopenRetirement
	if prior := c.RearmContinuation; prior != nil {
		if prior.Cycle == control.Uint64(math.MaxUint64) {
			return m, fmt.Errorf("closed rearm cycle exhausted")
		}
		previous := *prior
		previous.Retired = nil
		raw, err := json.Marshal(previous)
		if err != nil {
			return m, err
		}
		retired = &ServiceReopenRetirement{Context: raw, ContextSHA256: rawDigest(raw)}
		if prior.Retired != nil {
			retired.PreviousSHA256 = prior.Retired.EvidenceSHA256
		}
		retired.EvidenceSHA256 = reopenRetirementDigest(raw, retired.PreviousSHA256)
		cycle = prior.Cycle + 1
	}
	q := before.Request
	q.Action, q.ExpectedDeadlineNS = "rearm", before.DeadlineNS
	q.ExpectedCapacityGeneration, q.ExpectedAdmissionRevision = before.Observed.CapacityGeneration, before.Observed.AdmissionRevision
	raw, err := json.Marshal(q)
	if err != nil {
		return m, err
	}
	next.ServiceClosure.RearmContinuation = &ServiceRearmContinuation{ProtocolVersion: 1, Cycle: cycle, Rearm: ServiceRearm{ClosureVersion: old.ClosureVersion, Origin: actor, StartedSequence: next.Sequence, ClosureSHA256: old.ClosureSHA256, HeldSHA256: old.HeldSHA256, Before: cloneLeaseResponse(before), Request: q, CommandSHA256: rawDigest(raw), Phase: Unconfirmed}, Retired: retired}
	return next, next.Validate()
}

// 同 UID/RV 写前 CAS 确认后才产生派发票据；写前丢回复保持 Unknown。
func (s *Store) ContinueServiceRearm(ctx context.Context, snapshot *Snapshot, actor Owner, selector string, before control.AdmissionLeaseResponse) (*Snapshot, *ServiceRearm, error) {
	if err := s.checked(snapshot); err != nil {
		return nil, nil, err
	}
	if !snapshot.Dispatchable() {
		return nil, nil, fmt.Errorf("closed rearm requires original running VM")
	}
	next, err := snapshot.model.ContinueServiceRearm(actor, selector, before)
	if err != nil {
		return nil, nil, err
	}
	result, err := s.apply(ctx, snapshot, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure.LatestRearm(), nil
}

func (n *ServiceRearmContinuation) UnmarshalJSON(raw []byte) error {
	type plain ServiceRearmContinuation
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*n = ServiceRearmContinuation(next)
	return nil
}
