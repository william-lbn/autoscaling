//go:build dsga_native_trusted_control && dsga_native_stock_preparation && linux && dsga_native_initial_lifecycle && dsga_native_memory_origin_wire && dsga_native_memory_commit2 && dsga_native_role_transfer2 && dsga_native_ordinary2

package vmguard_test

import (
	"context"
	"encoding/json"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"reflect"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

const trustedPG17ControllerTakeoverMode = "controller-takeover"

var errTrustedPG17YieldController = errors.New("DEV original controller stopped after settled grow")

// 两轮都只由原 New/Run 领取真实 journal epoch；不手造 Session、不改持有者或代次。
func trustedPG17RunControllers(ctx context.Context, f *vmguard.NativePG17TrustedHostFixture, mode string, run func(context.Context, *owner.Session, bool, *trustedPG17ControllerHandoff) error) error {
	rounds := 1
	if mode == trustedPG17ControllerTakeoverMode {
		rounds = 2
	}
	var previous *owner.Session
	var original journal.Model
	var handoff trustedPG17ControllerHandoff
	for round := 0; round < rounds; round++ {
		elector, err := owner.New(f.Core.CoordinationV1(), f.Store, owner.Options{LeaseDuration: 6 * time.Second, RenewDeadline: 4 * time.Second, RetryPeriod: time.Second, PodUID: "pod-uid"})
		if err != nil {
			return err
		}
		err = elector.Run(ctx, func(runCtx context.Context, session *owner.Session) error {
			if err := session.Check(runCtx); err != nil {
				return err
			}
			current, err := f.Store.Load(runCtx)
			if err != nil {
				return err
			}
			if round == 1 {
				if previous == nil || session.Actor().Epoch <= previous.Actor().Epoch || session.Actor().HolderID == previous.Actor().HolderID {
					return errors.New("DEV new controller did not claim a higher original owner")
				}
				oldErr := previous.Check(runCtx)
				if !errors.Is(oldErr, owner.ErrLeadershipLost) {
					return errors.New("DEV retired Session was not fenced")
				}
				model := current.State()
				if model.Owner != session.Actor() || !reflect.DeepEqual(model.History, original.History) || !reflect.DeepEqual(model.Active, original.Active) || !reflect.DeepEqual(model.BudgetHeld, original.BudgetHeld) || !reflect.DeepEqual(model.ResourceIdentity, original.ResourceIdentity) || !reflect.DeepEqual(model.PGInstance, original.PGInstance) {
					return errors.New("DEV controller claim rewrote original effects")
				}
				if err := trustedDriverSave("fault-controller-claimed", map[string]any{"old_epoch": previous.Actor().Epoch, "new_epoch": session.Actor().Epoch, "old_session_leadership_lost": true, "same_original_history_and_budget": true, "actual_new_holder": session.Actor().HolderID, "state": model, "Host_API_is_model": true, "E": 0}); err != nil {
					return err
				}
			}
			if round == 0 {
				previous = session
			}
			result := run(runCtx, session, round == 1, &handoff)
			if round == 0 && errors.Is(result, errTrustedPG17YieldController) {
				// 回调先由自身defer取消并join所有原Run/轮询，Elector.Run返回后才建立下一Elector。
				snap, err := f.Store.Load(ctx)
				if err != nil {
					return err
				}
				original = snap.State()
				if original.Active != nil || len(original.History) != 1 || original.History[0].Outcome != journal.Succeeded || original.BudgetHeld == nil {
					return errors.New("DEV controller yielded outside settled grow boundary")
				}
				if err := trustedDriverSave("fault-controller-retired", map[string]any{"state": original, "same_root_process_required": true, "E": 0}); err != nil {
					return err
				}
			}
			return result
		})
		if round == 0 && rounds == 2 && errors.Is(err, errTrustedPG17YieldController) {
			if !handoff.Ready {
				return errors.New("DEV original readonly handoff missing")
			}
			if err := trustedPG17AwaitOriginalExpiry(ctx, f, handoff, original); err != nil {
				return err
			}
			continue
		}
		return err
	}
	return errors.New("DEV controller takeover continuation not completed")
}

// 新owner只消费权威原生租约和原关闭WAL；确有过期才显式选原rearm/reopen，不延原TTL。
func trustedPG17AwaitControllerAdmission(ctx context.Context, f *vmguard.NativePG17TrustedHostFixture, session *owner.Session, pg *compute.Client, endpoint *owner.ControlEndpoint) error {
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for bounded.Err() == nil {
		snap, err := f.Store.Load(bounded)
		if err != nil {
			return err
		}
		model := snap.State()
		if model.Owner != session.Actor() || model.Active != nil {
			return errors.New("DEV takeover admission recovery changed original boundary")
		}
		memory, err := pg.Memory(bounded)
		if err != nil {
			return err
		}
		fact, err := pg.AdmissionLease(bounded, control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: memory.Observed.Instance, IssuerEpoch: session.Actor().Epoch, ExpectedCapacityGeneration: memory.Observed.CapacityGeneration, ExpectedAdmissionRevision: memory.Observed.AdmissionRevision, Action: "observe"})
		if err != nil {
			return err
		}
		if fact.TimeoutMS != 30000 {
			return errors.New("DEV takeover changed native admission TTL")
		}
		if !fact.Expired && fact.LeaseEpoch == session.Actor().Epoch && fact.Observed.AdmissionOpen && model.ServiceClosure == nil {
			if model.ServiceLifecycle == nil {
				return errors.New("DEV native reopen lacks completed original service lifecycle")
			}
			if err := trustedDriverSave("fault-controller-admission-ready", map[string]any{"fact": fact, "state": model, "E": 0}); err != nil {
				return err
			}
			return nil
		}
		c := model.ServiceClosure
		var action control.CoordinationAction
		var selector string
		if c != nil {
			if c.Rearm != nil && c.Rearm.Phase == journal.Unconfirmed {
				selector = c.Rearm.CommandSHA256
				action = control.CoordinationObserveServiceRearm
			} else if c.Reopen != nil && c.Reopen.Phase == journal.Unconfirmed {
				selector = c.Reopen.CommandSHA256
				action = control.CoordinationObserveServiceReopen
			} else if !c.Pending() && c.Rearm == nil {
				_, _, _, selector = c.Selection()
				action = control.CoordinationRearmService
			} else if !c.Pending() && c.Rearm.Phase == journal.Settled && c.Reopen == nil {
				_, _, _, selector = c.Selection()
				action = control.CoordinationReopenService
			} else if c.Reopen != nil && c.Reopen.Phase == journal.Settled && !fact.Expired && fact.LeaseEpoch == session.Actor().Epoch && fact.Observed.AdmissionOpen {
				selector = c.Reopen.CommandSHA256
				action = control.CoordinationCompleteServiceReopen
			}
		}
		if action != "" {
			if _, err = endpoint.Submit(bounded, control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(memory.Observed.Instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: action, Selector: selector}); err != nil {
				return err
			}
		}
		if err = trustedDriverPause(bounded); err != nil {
			return err
		}
	}
	return bounded.Err()
}

type trustedPG17ControllerHandoff struct {
	PG    compute.Config
	Root  resourceguard.ClientConfig
	Ready bool
}

// 原资格的UID/SHA只从成功Grow的原命令恢复，仍由生产Reader和新worker重审。
func trustedPG17RetainedQualification(model journal.Model) (budget.QualificationSource, error) {
	if model.Validate() != nil || model.Active != nil || len(model.History) != 1 || model.History[0].Outcome != journal.Succeeded {
		return budget.QualificationSource{}, errors.New("DEV retained qualification lacks settled original grow")
	}
	op := model.History[0].Operation
	for _, step := range op.Steps {
		if step.Action == journal.PrepareBudget && step.Phase == journal.Settled && step.Observation != nil && step.Observation.Result == journal.Completed {
			var cmd budget.PrepareCommand
			if json.Unmarshal(step.Command, &cmd) != nil || cmd.Prepared.Request != op.Request || cmd.Qualification.Validate() != nil {
				return budget.QualificationSource{}, errors.New("DEV original complete qualification command invalid")
			}
			return cmd.Qualification, nil
		}
	}
	return budget.QualificationSource{}, errors.New("DEV original qualification command missing")
}

// 旧Run/Elector都已join；这些新SDK仅做原epoch只读观察，等待真正原生过期，绝不续租。
func trustedPG17AwaitOriginalExpiry(ctx context.Context, f *vmguard.NativePG17TrustedHostFixture, h trustedPG17ControllerHandoff, original journal.Model) error {
	bounded, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	pg, err := compute.New(bounded, h.PG)
	if err != nil {
		return err
	}
	defer pg.CloseIdleConnections()
	rc := h.Root
	rc.Context = bounded
	root, err := resourceguard.NewClient(rc)
	if err != nil {
		return err
	}
	defer root.CloseIdleConnections()
	before, err := root.InspectState(bounded)
	if err != nil {
		return err
	}
	limits, err := before.ConfirmedLimits()
	if err != nil {
		return err
	}
	memory, err := pg.Memory(bounded)
	if err != nil {
		return err
	}
	var first *control.AdmissionLeaseResponse
	count := 0
	for bounded.Err() == nil {
		snap, err := f.Store.Load(bounded)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(snap.State(), original) {
			return errors.New("DEV expiry wait changed original journal")
		}
		view, err := root.InspectState(bounded)
		if err != nil {
			return err
		}
		current, err := view.ConfirmedLimits()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, limits) || !reflect.DeepEqual(view.Birth, before.Birth) || !reflect.DeepEqual(view.Held, before.Held) || !reflect.DeepEqual(view.Original, before.Original) {
			return errors.New("DEV expiry wait changed original N or held root")
		}
		pgState, err := pg.Memory(bounded)
		if err != nil {
			return err
		}
		if pgState.Observed.Instance != memory.Observed.Instance || pgState.Observed.CapacityGeneration != memory.Observed.CapacityGeneration || pgState.Observed.LowBuffers != memory.Observed.LowBuffers || pgState.Observed.HighBuffers != memory.Observed.HighBuffers {
			return errors.New("DEV expiry wait changed original PG capacity")
		}
		actual, err := f.ReadTrustedTakeoverMemory(bounded)
		if err != nil {
			return err
		}
		if actual.RequestedBytes != 8<<30 || actual.ActualBytes != 8<<30 {
			return errors.New("DEV expiry wait changed actual QMP memory")
		}
		fact, err := pg.AdmissionLease(bounded, control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: memory.Observed.Instance, IssuerEpoch: h.PG.IssuerEpoch, ExpectedCapacityGeneration: memory.Observed.CapacityGeneration, ExpectedAdmissionRevision: pgState.Observed.AdmissionRevision, Action: "observe"})
		if err != nil {
			return err
		}
		if !fact.Armed || fact.TimeoutMS != 30000 {
			return errors.New("DEV expiry wait changed original native lease contract")
		}
		if first == nil {
			value := fact
			first = &value
		}
		if fact.DeadlineNS != first.DeadlineNS || fact.LeaseEpoch != first.LeaseEpoch {
			return errors.New("DEV readonly expiry wait renewed original lease")
		}
		count++
		if fact.Expired {
			return trustedDriverSave("fault-controller-original-expired", map[string]any{"first_native": first, "expired_native": fact, "actual_samples": count, "original_state": original, "root_before": before, "root_after": view, "PG_before": memory, "PG_after": pgState, "actual_QMP": actual, "P_A_scope": "before/after original validation; not per-sample separate P/A readback", "E": 0})
		}
		select {
		case <-bounded.Done():
			return bounded.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return bounded.Err()
}
