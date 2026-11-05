package owner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"k8s.io/klog/v2"
)

// 单个受信管理选择只组合原有恢复入口，不改变Run调度、原生期限或WAL。
// 每个低层方法仍完整审计来源并以原UID/RV落账；任何未知立即交还管理端。
// 复合调用之间没有周期审计插入，但单方法本身超出原期限仍会安全拒绝。
func (w *PGWorker) recoverServiceLifecycle(ctx context.Context, selector string) (progress pgexec.Progress, err error) {
	currentStage, stageStart := lifecyclePreflight, time.Now()
	defer func() {
		if err != nil {
			klog.InfoS("DSGA explicit service lifecycle stopped", "stage", string(currentStage), "elapsed_ms", time.Since(stageStart).Milliseconds(), "result", "stopped", "error_type", fmt.Sprintf("%T", err))
		}
	}()
	timed := func(stage serviceLifecycleStage, run func() error) error {
		currentStage, stageStart = stage, time.Now()
		return serviceLifecycleTimed(stage, run)
	}
	var s *journal.Snapshot
	load := func(stage serviceLifecycleStage) error {
		return timed(stage, func() error {
			s, err = w.loadServiceLifecycleSequence(ctx)
			return err
		})
	}
	if err := load(lifecyclePreflight); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := selectedLifecycleClosure(s, selector); err != nil {
		return pgexec.AwaitingFact, err
	}
	latest := s.State().ServiceClosure.LatestRearm()
	if latest == nil {
		if err := timed(lifecycleRearm, func() error { _, err := w.rearmServiceLease(ctx, selector); return err }); err != nil {
			return pgexec.AwaitingFact, err
		}
	} else {
		if latest.Phase != journal.Settled {
			return pgexec.AwaitingFact, ErrServiceSafety
		}
		// 这里只观察真实期限以选择原方法；不从墙钟、历史回执或调用方推断过期。
		var expired bool
		if err := timed(lifecycleObserveLease, func() error {
			fact, err := w.observeAdmissionLease(ctx)
			if err != nil {
				return err
			}
			if !fact.Armed || fact.Observed.AdmissionOpen || fact.Observed.IssuerFloorEpoch > w.session.Actor().Epoch {
				return ErrServiceSafety
			}
			expired = fact.Expired
			if !expired && (fact.LeaseEpoch != w.session.Actor().Epoch || fact.Observed.IssuerFloorEpoch != w.session.Actor().Epoch) {
				return ErrServiceSafety
			}
			return nil
		}); err != nil {
			return pgexec.AwaitingFact, err
		}
		if expired {
			if w.session.Actor().Epoch <= latest.Origin.Epoch {
				return pgexec.AwaitingFact, ErrServiceSafety
			}
			selected := latest.CommandSHA256
			if err := timed(lifecycleContinueRearm, func() error { _, err := w.continueServiceRearm(ctx, selected); return err }); err != nil {
				return pgexec.AwaitingFact, err
			}
		}
	}
	if err := load(lifecycleAfterRearm); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := selectedLifecycleClosure(s, selector); err != nil {
		return pgexec.AwaitingFact, err
	}
	latest = s.State().ServiceClosure.LatestRearm()
	if latest == nil || latest.Phase != journal.Settled || latest.Origin != w.session.Actor() {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	rearmSHA := latest.CommandSHA256
	if err := timed(lifecycleReopen, func() error { _, err := w.reopenService(ctx, selector); return err }); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := load(lifecycleAfterReopen); err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	c := m.ServiceClosure
	if c == nil || c.Pending() || c.Reopen == nil || c.Reopen.Phase != journal.Settled || c.Reopen.Closing != nil || c.Reopen.Rearm != nil || c.Reopen.Origin != w.session.Actor() || c.LatestRearm() == nil || c.LatestRearm().CommandSHA256 != rearmSHA {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	_, _, _, currentSelector := c.Selection()
	if currentSelector != selector {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	reopenSHA, beforeSequence := c.Reopen.CommandSHA256, m.Sequence
	if err := timed(lifecycleComplete, func() error { _, err := w.completeServiceReopen(ctx, reopenSHA); return err }); err != nil {
		return pgexec.AwaitingFact, err
	}
	if err := load(lifecycleAfterComplete); err != nil {
		return pgexec.AwaitingFact, err
	}
	m = s.State()
	l := m.ServiceLifecycle
	if m.ServiceClosure != nil || l == nil || l.SafetyStage != "serving" || l.Origin != w.session.Actor() || l.RecordedSequence <= beforeSequence {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	// Model.Load已严格验证完整冻结上下文；这里只核本次选择对应的准确摘要。
	var frozen struct {
		ServiceClosure *journal.ServiceClosure `json:"service_closure"`
	}
	if json.Unmarshal(l.Context, &frozen) != nil || frozen.ServiceClosure == nil || frozen.ServiceClosure.Reopen == nil || frozen.ServiceClosure.Reopen.CommandSHA256 != reopenSHA {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	_, _, _, completedSelector := frozen.ServiceClosure.Selection()
	if completedSelector != selector {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	return pgexec.Recorded, nil
}

func (w *PGWorker) loadServiceLifecycleSequence(ctx context.Context) (*journal.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := w.session.Check(ctx); err != nil {
		return nil, err
	}
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return nil, err
	}
	instance, _ := w.Binding()
	m := s.State()
	if !s.Dispatchable() || m.Owner != w.session.Actor() || m.PGInstance == nil || *m.PGInstance != instance {
		return nil, ErrLeadershipLost
	}
	return s, nil
}

func selectedLifecycleClosure(s *journal.Snapshot, selector string) error {
	c := s.State().ServiceClosure
	// 本次不接管已有开放/再关闭/未确认意图；这些仍使用原显式观察/重放入口。
	if c == nil || c.Pending() || c.Reopen != nil {
		return ErrServiceSafety
	}
	_, _, _, selected := c.Selection()
	if selected != selector {
		return fmt.Errorf("service lifecycle requires latest complete closure selector")
	}
	return nil
}

type serviceLifecycleStage string

const (
	lifecyclePreflight     serviceLifecycleStage = "preflight"
	lifecycleObserveLease  serviceLifecycleStage = "observe_lease"
	lifecycleRearm         serviceLifecycleStage = "rearm"
	lifecycleContinueRearm serviceLifecycleStage = "continue_rearm"
	lifecycleAfterRearm    serviceLifecycleStage = "after_rearm"
	lifecycleReopen        serviceLifecycleStage = "reopen"
	lifecycleAfterReopen   serviceLifecycleStage = "after_reopen"
	lifecycleComplete      serviceLifecycleStage = "complete"
	lifecycleAfterComplete serviceLifecycleStage = "after_complete"
)

// 仅固定stage、耗时和类型，不记录Error正文、选择摘要、HTTP正文或凭据。
func serviceLifecycleTimed(stage serviceLifecycleStage, run func() error) error {
	start := time.Now()
	err := run()
	result := "confirmed"
	if err != nil {
		result = "stopped"
	}
	klog.InfoS("DSGA explicit service lifecycle stage", "stage", string(stage), "elapsed_ms", time.Since(start).Milliseconds(), "result", result, "error_type", fmt.Sprintf("%T", err))
	return err
}
