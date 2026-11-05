package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"k8s.io/klog/v2"
)

type recoveryLeaseScopeKey struct{}

// 只记录同一次显式调用自己的成功原生回复，不缓存或替代任何来源审计。
// 不写WAL、不发放Owner；每次renew前仍要求完整审计刚成功及同UID/RV。
type recoveryLeaseScope struct {
	actor           journal.Owner
	instance        control.Instance
	closed          control.AdmissionRequest
	initialReopen   string
	initialSequence control.Uint64
	appliedRearm    *control.AdmissionLeaseRequest
	appliedOpen     *control.AdmissionRequest
	established     bool
	stopped         bool
}

func (w *PGWorker) scopeServiceRecoveryLease(ctx context.Context, command control.CoordinationCommand, s *journal.Snapshot) context.Context {
	if command.Action != control.CoordinationRecoverServiceLifecycle {
		return ctx
	}
	m := s.State()
	c := m.ServiceClosure
	if c == nil {
		return ctx
	}
	_, _, closed, selected := c.Selection()
	originalReopen := ""
	if c.Reopen != nil {
		originalReopen = c.Reopen.CommandSHA256
		if c.Reopen.Closing != nil {
			_, _, closed, selected = c.Reopen.Closing.Close.Selection()
		}
	}

	if command.Selector != selected {
		return ctx
	}
	instance, _ := w.Binding()
	return context.WithValue(ctx, recoveryLeaseScopeKey{}, &recoveryLeaseScope{actor: w.session.Actor(), instance: instance, closed: closed, initialReopen: originalReopen, initialSequence: m.Sequence})
}

func noteRecoveryRearmReply(ctx context.Context, fact control.AdmissionLeaseResponse) {
	s, _ := ctx.Value(recoveryLeaseScopeKey{}).(*recoveryLeaseScope)
	if s == nil {
		return
	}
	if !fact.Renewed || fact.Expired || !fact.Armed || fact.Request.Action != "rearm" {
		s.stopped = true
		return
	}
	q := fact.Request
	s.appliedRearm = &q
	s.established = true
}
func noteRecoveryOpenReply(ctx context.Context, fact control.AdmissionResponse, err error) {
	s, _ := ctx.Value(recoveryLeaseScopeKey{}).(*recoveryLeaseScope)
	if s == nil {
		return
	}
	if err != nil || !fact.Confirms() {
		s.stopped = true
		return
	}
	q := fact.Request
	s.appliedOpen = &q
}

// 仅在调用方完整来源审计返回nil后调用。durable serving证明同时承接完成后的
// 原Run latch审计，避免显式调用结束后再串联数轮审计耗尽同一个30秒窗口。
func (w *PGWorker) renewAuditedRecoveryLease(ctx context.Context, snapshot *journal.Snapshot) (err error) {
	scope, _ := ctx.Value(recoveryLeaseScopeKey{}).(*recoveryLeaseScope)
	m := snapshot.State()
	var completed *journal.ServiceClosure
	if scope == nil {
		if m.PGInstance == nil || m.ServiceClosure != nil || m.Active != nil || m.ServiceLifecycle == nil || m.ServiceLifecycle.SafetyStage != "serving" {
			return nil
		}
		var frozen struct {
			ServiceClosure *journal.ServiceClosure `json:"service_closure"`
		}
		if json.Unmarshal(m.ServiceLifecycle.Context, &frozen) != nil || frozen.ServiceClosure == nil || frozen.ServiceClosure.Reopen == nil {
			return ErrServiceSafety
		}
		completed = frozen.ServiceClosure
		scope = &recoveryLeaseScope{actor: w.session.Actor(), instance: *m.PGInstance, established: true}
	}
	if scope.stopped {
		return nil
	}
	stage := "authority"
	start := time.Now()
	defer func() {
		result := "confirmed"
		if err != nil {
			result = "stopped"
			scope.stopped = true
		}
		klog.InfoS("DSGA audited recovery lease", "stage", stage, "elapsed_ms", time.Since(start).Milliseconds(), "result", result, "error_type", fmt.Sprintf("%T", err))
	}()
	instance, _ := w.Binding()
	if scope.actor != w.session.Actor() || scope.instance != instance || m.Owner != scope.actor || !snapshot.Dispatchable() {
		return ErrLeadershipLost
	}
	if err := w.session.Check(ctx); err != nil {
		return err
	}
	c := m.ServiceClosure
	if completed != nil {
		c = completed
	}
	if c == nil {
		return ErrServiceSafety
	}
	// 原先未知的意图不能由当前数值变成已确认。只认持久settled或本调用真实回复。
	if r := c.LatestRearm(); r != nil {
		if r.Phase == journal.Settled && r.Origin == scope.actor {
			scope.established = true
		}
		if r.Phase == journal.Unconfirmed && (scope.appliedRearm == nil || *scope.appliedRearm != r.Request) {
			return nil
		}
	}
	if c.Reopen != nil && c.Reopen.Rearm != nil {
		r := c.Reopen.Rearm
		if r.Phase == journal.Settled && r.Origin == scope.actor {
			scope.established = true
		}
		if r.Phase == journal.Unconfirmed && (scope.appliedRearm == nil || *scope.appliedRearm != r.Request) {
			return nil
		}
	}
	if completed == nil {
		_, _, anchored, _ := c.Selection()
		if c.Reopen != nil {
			if c.Reopen.Closing != nil {
				_, _, anchored, _ = c.Reopen.Closing.Close.Selection()
			} else if c.Reopen.PriorClose != nil {
				anchored = *c.Reopen.PriorClose
			}
		}
		if anchored != scope.closed {
			return ErrServiceSafety
		}
	}
	stage = "observe"
	current, e := w.observeAdmissionLease(ctx)
	if e != nil {
		return e
	}
	if !current.Armed {
		return ErrServiceSafety
	}
	if current.Expired {
		// 原过期前置仍可由原higher-owner rearm恢复；已恢复后再次过期绝不复活。
		if !scope.established {
			return nil
		}
		return errNativeAdmissionExpired
	}
	if current.LeaseEpoch != scope.actor.Epoch || current.Observed.IssuerFloorEpoch != scope.actor.Epoch {
		return ErrServiceSafety
	}
	pg := current.Observed
	if pg.Operation != nil && pg.Operation.Phase == control.Preparing {
		return ErrServiceSafety
	}
	for _, a := range pg.Arrays {
		if a.AllocationUnknown {
			return ErrServiceSafety
		}
	}
	var gate control.AdmissionRequest
	if c.Reopen == nil {
		_, _, gate, _ = c.Selection()
	} else {
		reopen := c.Reopen
		if pg.AdmissionOpen {
			if reopen.Closing != nil || reopen.Rearm != nil {
				return ErrServiceSafety
			}
			if reopen.Phase != journal.Settled && (scope.appliedOpen == nil || *scope.appliedOpen != reopen.Request) {
				return nil
			}
			gate = reopen.Request
		} else if reopen.Closing != nil {
			if reopen.Closing.Close.Pending() {
				return nil
			}
			_, _, gate, _ = reopen.Closing.Close.Selection()
		} else {
			// 当前调用刚写入的开放意图尚未发送。旧Unknown必须先独立关门，不能续活。
			if reopen.Phase == journal.Unconfirmed && reopen.CommandSHA256 == scope.initialReopen {
				return nil
			}
			if reopen.Phase == journal.Unconfirmed && (reopen.Origin != scope.actor || reopen.StartedSequence <= scope.initialSequence) {
				return ErrServiceSafety
			}
			gate = control.AdmissionRequest{Instance: instance, Revision: reopen.Before.Observed.AdmissionRevision, ExpectedCapacityGeneration: reopen.Request.ExpectedCapacityGeneration, Open: false}
		}
	}
	if pg.Instance != instance || pg.AdmissionRevision != gate.Revision || pg.CapacityGeneration != gate.ExpectedCapacityGeneration || pg.AdmissionOpen != gate.Open {
		return ErrServiceSafety
	}
	stage = "same_snapshot"
	if err := w.session.Check(ctx); err != nil {
		return err
	}
	after, e := w.session.store.Load(ctx)
	if e != nil {
		return e
	}
	if after.ResourceVersion() != snapshot.ResourceVersion() || after.State().Owner != scope.actor || !after.Dispatchable() {
		return ErrServiceSafety
	}
	stage = "renew"
	q := current.Request
	q.Action = "renew"
	q.ExpectedDeadlineNS = current.DeadlineNS
	q.ExpectedCapacityGeneration = pg.CapacityGeneration
	q.ExpectedAdmissionRevision = pg.AdmissionRevision
	renewed, e := w.client.AdmissionLease(ctx, q)
	if e != nil {
		return errors.Join(ErrServiceSafety, e)
	}
	if !renewed.Renewed || renewed.Expired || renewed.DeadlineNS < current.DeadlineNS || renewed.LeaseEpoch != scope.actor.Epoch || renewed.Observed.Instance != instance || renewed.Observed.IssuerFloorEpoch != scope.actor.Epoch || renewed.Observed.CapacityGeneration != pg.CapacityGeneration || renewed.Observed.AdmissionRevision != pg.AdmissionRevision || renewed.Observed.AdmissionOpen != pg.AdmissionOpen {
		return ErrServiceSafety
	}
	scope.established = true
	return nil
}
