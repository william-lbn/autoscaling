package owner

import (
	"context"
	"encoding/json"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// renewGuestServiceAudit只服务原Guest执行器同次调用的写前/完整写后出口。
// Shared不是许可：旧永久头、新Prepared、Host、Runner、Root及原PG事实都须
// 本次重新认证并在尾部复核。它不Start/Record、不重开、不重置期限或消除Unknown。
func (w *PGWorker) renewGuestServiceAudit(ctx context.Context, s *journal.Snapshot, shared control.SharedCostResponse) (auditErr error) {
	stage := "guest_service_boundary"
	defer func() { recordServiceSourceAuditError(stage, auditErr) }()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if s == nil || w.session == nil || w.qualification == nil || w.safety == nil || w.safety.claims == nil || w.safety.runner == nil || w.resourceOwner == nil || w.resourceOwner.session != w.session || ctx.Value(recoveryLeaseScopeKey{}) != nil {
		return ErrServiceSafety
	}
	m, actor := s.State(), w.session.Actor()
	if m.Validate() != nil || !s.Dispatchable() || m.Owner != actor || m.ResourceIdentity == nil || m.PGInstance == nil || m.Active == nil || m.Active.Origin != actor || m.Active.Request.Key.IssuerEpoch != actor.Epoch || *m.PGInstance != m.Active.Request.Key.Instance || m.Active.AbortRequested || m.Active.Cleanup != nil || m.ServiceClosure != nil {
		return ErrServiceSafety
	}
	prior := make(map[journal.Action]*journal.Step)
	var guest *journal.Step
	for i := range m.Active.Steps {
		step := &m.Active.Steps[i]
		if step.Action == journal.GuestReady {
			if guest != nil || (step.Phase != journal.Planned && step.Phase != journal.Unconfirmed) || step.Phase == journal.Unconfirmed && (step.DispatchedBy == nil || *step.DispatchedBy != actor) {
				return ErrServiceSafety
			}
			guest = step
		} else if guest == nil {
			if step.Phase != journal.Settled || step.Observation == nil || step.Observation.Result != journal.Completed {
				return ErrServiceSafety
			}
			prior[step.Action] = step
		} else if step.Phase != journal.Planned {
			return ErrServiceSafety
		}
	}
	if guest == nil || prior[journal.PrepareBudget] == nil || w.client.Pending() != nil || w.resourceOwner.client.Pending() != nil {
		return ErrServiceSafety
	}
	pending, closing := w.client.PendingAdmission()
	if pending != nil || closing != nil {
		return ErrServiceSafety
	}
	var p budget.Prepared
	prepared := prior[journal.PrepareBudget]
	if prepared.TargetID != m.PGInstance.ComputeInstanceID || json.Unmarshal(prepared.Observation.Data, &p) != nil || p.ValidateFor(m.Active.Request, *m.ResourceIdentity) != nil || p.ValidateCurrent(shared, actor.Epoch) != nil {
		return ErrServiceSafety
	}
	if p.Request.Kind == control.Shrink {
		// 原关闭与PG/Release须真正完成；当前数组仍由ValidateCurrent逐项核原Key、
		// 下一容量代及物理释放回执，绝不把阶段名或低buffers单独当成释放证明。
		q := prior[journal.Quiesce]
		var closed budget.Quiesced
		if q == nil || prior[journal.ResizePG] == nil || prior[journal.ReleasePG] == nil || json.Unmarshal(q.Observation.Data, &closed) != nil || closed.ValidateFor(p, actor.Epoch) != nil || shared.Observed.AdmissionOpen || shared.Observed.AdmissionRevision != closed.Close.Revision {
			return ErrServiceSafety
		}
	}
	stage = "guest_service_held_sources"
	// 旧Held只读捕获与尾部复核共用原读者；闭包不是预算或续租许可。
	recheckHeld, err := w.captureActiveHeldSources(ctx, s)
	if err != nil {
		return err
	}
	stage = "guest_service_prepared_sources"
	if err = w.safety.claims.CheckOutside(ctx, p); err != nil {
		return err
	}
	var host *budgetclaims.Observed
	var hostRef budget.ObjectRef
	var vmCommand vmguard.Command
	var vmFact budget.VMSettlement
	if p.Request.Kind == control.Grow {
		h, vm := prior[journal.ReserveHost], prior[journal.ResizeVM]
		var command struct {
			ProtocolVersion uint32           `json:"protocol_version"`
			Request         control.Request  `json:"request"`
			Claim           budget.ObjectRef `json:"claim"`
			PreparedSHA256  string           `json:"prepared_sha256"`
			HostSHA256      string           `json:"host_sha256"`
		}
		if h == nil || vm == nil || control.UnmarshalObject(h.Command, &command) != nil || command.ProtocolVersion != 1 || command.Request != p.Request || command.PreparedSHA256 != vmguard.SourceDigest(prepared.Observation.Data) || json.Unmarshal(vm.Command, &vmCommand) != nil || vmCommand.Validate() != nil || vmCommand.Request != p.Request || vmCommand.BeforeBytes != p.VMBeforeMemoryBytes || vmCommand.PreparedSHA256 != command.PreparedSHA256 || vmCommand.HostClaim != command.Claim || vmCommand.HostSHA256 != command.HostSHA256 || vmCommand.GuestSHA256 != "" || json.Unmarshal(vm.Observation.Data, &vmFact) != nil || vmFact.Runner == nil {
			return ErrServiceSafety
		}
		hostRef = command.Claim
		host, err = w.safety.claims.Read(ctx, s, p, hostRef)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(host.Host)
		if err != nil {
			return err
		}
		if command.HostSHA256 != vmguard.SourceDigest(raw) || h.TargetID != host.Host.SourceID || vmFact.Host != host.Host || vmFact.HostClaim != command.Claim {
			return ErrServiceSafety
		}
	} else {
		// Shrink无ReserveHost步骤，原Scheduler仍发布当前Request的Host引用。
		// 它与旧Held共用同一Node权威；两份来源都核验，不相加、不借旧引用冒新计划。
		hostRef, err = w.safety.claims.HostReference(ctx, p.Request)
		if err != nil {
			return err
		}
		host, err = w.safety.claims.Read(ctx, s, p, hostRef)
		if err != nil {
			return err
		}
	}
	stage = "guest_service_root"
	q, err := w.safety.claims.Qualification(ctx, *w.qualification)
	if err != nil {
		return err
	}
	view, err := w.resourceOwner.client.InspectState(ctx)
	if err != nil {
		return err
	}
	aggregate, err := budget.RootAggregateFromView(view)
	if err != nil {
		return err
	}
	process, err := w.resourceOwner.client.InspectProcess(ctx)
	if err != nil {
		return err
	}
	if view.Identity != *m.ResourceIdentity || view.IssuerFloorEpoch != actor.Epoch || view.Birth == nil || process != *view.Birth || q.Identity != view.Identity {
		return ErrServiceSafety
	}
	raw, err := json.Marshal(view.Bootstrap)
	if err != nil {
		return err
	}
	vm := s.VirtualMachine()
	expectedVM := p.VMBeforeMemoryBytes
	if p.Request.Kind == control.Grow {
		expectedVM = p.Request.TargetVMMemoryBytes
	}
	if vm.Status.MemorySize == nil {
		return ErrServiceSafety
	}
	actualVM, exact := vm.Status.MemorySize.AsInt64()
	if !exact || actualVM <= 0 || control.Uint64(actualVM) != expectedVM || vm.Status.PodName != p.RunnerPodName || vm.Status.RestartCount != p.RunnerRestartCount || q.RunnerPodName != vm.Status.PodName || q.NodeName != vm.Status.Node || q.RunnerRestartCount != vm.Status.RestartCount || q.RootBootstrapSHA256 != vmguard.SourceDigest(raw) || view.Actual.GuestMemTotalBytes < p.GuestUsableMemoryBytes {
		return ErrServiceSafety
	}
	if guest.Phase == journal.Planned {
		old, _, err := idleServiceBound(m)
		if err != nil {
			return err
		}
		if aggregate.Bound != old {
			return ErrServiceSafety
		}
	} else {
		var command resourceguard.Command
		if json.Unmarshal(guest.Command, &command) != nil || command.Validate() != nil || command.IssuerEpoch != actor.Epoch || command.Identity != p.Identity || command.BudgetCommitmentID != p.Request.BudgetCommitmentID || command.PostmasterPID != process.PostmasterPID || command.PostmasterStartTicks != process.PostmasterStartTicks || view.Original == nil || view.Held == nil || !reflect.DeepEqual(command, *view.Original) || !reflect.DeepEqual(command, *view.Held) || !view.Applied || view.Actual.ValidateFor(command) != nil {
			return ErrServiceSafety
		}
		limits, err := view.ConfirmedLimits()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(limits, p.Limits) {
			return ErrServiceSafety
		}
	}
	stage = "guest_service_runner"
	// Grow只有本Active原ResizeVM完整回执可证明target；Shrink尚未派发VM，
	// 须认证原永久头的before实际值；当前Shrink Host仍由上方原读者独立认证。
	readRunner := func() (vmguard.CurrentBudgetVM, error) {
		birth, ref, publication, err := w.safety.runner.Inspect(ctx, &q.RunnerBirth, actor)
		if err != nil {
			return vmguard.CurrentBudgetVM{}, err
		}
		raw, err := json.Marshal(birth.Identity)
		if err != nil {
			return vmguard.CurrentBudgetVM{}, err
		}
		if ref != q.RunnerBirth || birth.Identity.RunnerPodUID != q.RunnerPodUID || birth.Identity.NodeUID != q.NodeUID || vmguard.SourceDigest(raw) != q.RunnerIdentitySHA256 || publication.State == nil || publication.Actual == nil || publication.State.Pending != nil || publication.Actual.RequestedBytes != expectedVM || publication.Actual.ActualBytes != expectedVM {
			return vmguard.CurrentBudgetVM{}, ErrServiceSafety
		}
		if p.Request.Kind == control.Grow {
			proof, err := w.safety.runner.ObserveOriginal(ctx, vmCommand, prior[journal.ResizeVM].CommandSHA256, actor)
			if err != nil {
				return vmguard.CurrentBudgetVM{}, err
			}
			if vmFact.ValidateFor(p, aggregate, m.Incarnation) != nil || proof.Sources != *vmFact.Runner || !reflect.DeepEqual(proof.Birth, birth) {
				return vmguard.CurrentBudgetVM{}, ErrServiceSafety
			}
			return vmguard.CurrentBudgetVM{Budget: proof}, nil
		}
		if m.InitialHeld != nil {
			initial, err := m.InitialBudgetFor()
			if err != nil {
				return vmguard.CurrentBudgetVM{}, err
			}
			proof, err := w.safety.runner.InitialCapacityFor(ctx, initial, actor)
			return vmguard.CurrentBudgetVM{Initial: &proof}, err
		}
		return w.safety.runner.CheckActiveHeldBudget(ctx, m)
	}
	runner, err := readRunner()
	if err != nil {
		return err
	}
	stage = "guest_service_sources_recheck"
	if err = recheckHeld(ctx); err != nil {
		return err
	}
	if host != nil {
		if err = w.safety.claims.Recheck(ctx, s, p, hostRef, host); err != nil {
			return err
		}
	}
	if err = w.safety.claims.CheckOutside(ctx, p); err != nil {
		return err
	}
	latestQ, err := w.safety.claims.Qualification(ctx, *w.qualification)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(q, latestQ) {
		return ErrServiceSafety
	}
	stage = "guest_service_root_recheck"
	latestRoot, err := w.resourceOwner.client.InspectState(ctx)
	if err != nil {
		return err
	}
	latestProcess, err := w.resourceOwner.client.InspectProcess(ctx)
	if err != nil {
		return err
	}
	if aggregate.Match(latestRoot) != nil || latestRoot.IssuerFloorEpoch != actor.Epoch || latestRoot.Birth == nil || latestProcess != process || latestProcess != *latestRoot.Birth || !reflect.DeepEqual(view.Original, latestRoot.Original) || !reflect.DeepEqual(view.Cancelled, latestRoot.Cancelled) || view.Applied != latestRoot.Applied || view.Actual.GuestMemTotalBytes != latestRoot.Actual.GuestMemTotalBytes {
		return ErrServiceSafety
	}
	stage = "guest_service_runner_recheck"
	latestRunner, err := readRunner()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(runner, latestRunner) {
		return ErrServiceSafety
	}
	stage = "guest_service_pg_recheck"
	latestShared, err := w.client.SharedCost(ctx)
	if err != nil {
		return err
	}
	if p.ValidateCurrent(latestShared, actor.Epoch) != nil || !reflect.DeepEqual(shared, latestShared) {
		return ErrServiceSafety
	}
	// 原尾端仍fresh Observe并逐字段匹配Native，再独立Session.Check及末Store.Load
	// 的原VM UID/RV；最后只续同一Armed、未过期、原cap/revision/deadline的精确CAS。
	stage = "guest_service_renew"
	return w.renewCurrentServiceAudit(ctx, s, latestShared)
}

// captureActiveHeldSources只保留原Reader已读到的旧永久头来源，供同一次完整
// audit末尾复核；没有独立准入/释放/续租权限，也不要求伸缩中的实际VM等于旧值。
// VM/Root/PG的当前值仍由各原执行器单独认证，不能把此闭包当完整来源证明。
func (w *PGWorker) captureActiveHeldSources(ctx context.Context, s *journal.Snapshot) (func(context.Context) error, error) {
	if s == nil || w.safety == nil || w.safety.claims == nil {
		return nil, ErrServiceSafety
	}
	if s.State().InitialHeld != nil {
		initial, err := s.State().InitialBudgetFor()
		if err != nil {
			return nil, err
		}
		original, err := w.safety.claims.ReadInitialBudget(ctx, s, initial)
		if err != nil {
			return nil, err
		}
		return func(tail context.Context) error {
			return w.safety.claims.RecheckInitialBudget(tail, s, initial, original)
		}, nil
	}
	original, err := w.safety.claims.ReadActiveHeld(ctx, s)
	if err != nil {
		return nil, err
	}
	return func(tail context.Context) error { return w.safety.claims.RecheckActiveHeld(tail, s, original) }, nil
}
