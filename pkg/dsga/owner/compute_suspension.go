package owner

import (
	"context"
	"errors"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// suspendCompute runs inside the current Owner control loop. The operation
// selects an irreversible close of this boot. A replay observes the recorded
// close. It never derives a different revision from a missing response.
func (w *PGWorker) suspendCompute(ctx context.Context, snapshot *journal.Snapshot, operation string) (pgexec.Progress, error) {
	m := snapshot.State()
	instance, epoch := w.Binding()
	if !snapshot.Dispatchable() || m.Owner != w.session.Actor() || m.PGInstance == nil || *m.PGInstance != instance {
		return pgexec.AwaitingFact, ErrLeadershipLost
	}
	if c := m.ServiceClosure; c != nil {
		if c.SuspensionOperation != operation {
			return pgexec.AwaitingFact, ErrServiceSafety
		}
		if c.Pending() {
			origin, _, _, selector := c.Selection()
			if origin.Epoch < epoch {
				return w.continueServiceClosure(ctx, selector)
			}
			if c.Continuation != nil {
				_, progress, err := w.advanceServiceSafety(ctx, "")
				return progress, err
			}
			fact, err := w.client.AdmissionStatus(ctx, c.Request)
			if err != nil {
				return pgexec.AwaitingFact, err
			}
			if fact.Confirms() {
				_, err := w.session.store.RecordServiceClosure(ctx, snapshot, m.Owner, fact, true)
				return pgexec.Recorded, err
			}
			// The authorized suspension still selects this exact close. Replay
			// only after asking the original PG boot for the exact request and
			// observing that its immediately preceding revision is still live.
			if !mayReplayComputeSuspension(c.Request, instance, epoch, fact) {
				return pgexec.AwaitingFact, ErrServiceSafety
			}
			latest, err := w.session.store.Load(ctx)
			if err != nil {
				return pgexec.AwaitingFact, err
			}
			if !latest.Dispatchable() || latest.State().Sequence != m.Sequence || latest.State().Owner != m.Owner {
				return pgexec.AwaitingFact, ErrServiceSafety
			}
			return w.dispatchComputeSuspension(ctx, latest, c.Request)
		}
		// A settled native closure can survive leadership change. Rebind the
		// observation floor to the current Owner before accepting its drain.
		fact, err := w.client.SharedCost(ctx)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		_, _, closed, _ := c.Selection()
		pg := fact.Observed
		if pg.Instance != instance || pg.AdmissionOpen || pg.CapacityGeneration != closed.ExpectedCapacityGeneration ||
			pg.AdmissionRevision != closed.Revision || pg.IssuerFloorEpoch > epoch {
			return pgexec.AwaitingFact, ErrServiceSafety
		}
		if pg.IssuerFloorEpoch < epoch {
			fence, err := w.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch})
			if err != nil || fence.Disposition != control.Fenced {
				return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
			}
		}
		prepared, err := w.observeSuspensionDrain(ctx, snapshot, operation)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		w.suspensionDrain = prepared
		return pgexec.Reconciled, nil
	}
	if w.safety == nil || w.resourceOwner == nil || m.Active != nil || w.client.Pending() != nil ||
		w.resourceOwner.client.Pending() != nil || w.automatic != nil && w.automatic.pending != nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	if open, close := w.client.PendingAdmission(); open != nil || close != nil {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	auditErr := w.inspectIdleService(ctx, snapshot)
	if lag, ok := auditErr.(*idlePGFenceNeeded); ok {
		if err := w.fenceIdlePG(ctx, snapshot, lag.observed); err != nil {
			return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
		}
		auditErr = w.inspectIdleService(ctx, snapshot)
	}
	if auditErr != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, auditErr)
	}
	shared, err := w.client.SharedCost(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	pg := shared.Observed
	budget, err := m.ServiceBudgetID()
	if err != nil || pg.Instance != instance || pg.IssuerFloorEpoch != epoch || !pg.AdmissionOpen ||
		pg.AdmissionRevision == control.Uint64(math.MaxUint64) {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch,
		Revision: pg.AdmissionRevision + 1, ExpectedCapacityGeneration: pg.CapacityGeneration, Open: false, BudgetCommitmentID: budget}
	_, _, err = w.session.store.StartComputeSuspension(ctx, snapshot, m.Owner, request, operation)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	// Leave the native call for the next loop, with its own call deadline.
	// Reconciliation follows the same status-first path after takeover.
	return pgexec.Recorded, nil
}

func mayReplayComputeSuspension(request control.AdmissionRequest, instance control.Instance, epoch control.Uint64, fact control.AdmissionResponse) bool {
	pg := fact.Observed
	return !request.Open && request.IssuerEpoch == epoch && fact.Disposition == control.AdmissionNotFound &&
		fact.ValidateFor(request, instance, true) == nil && pg.IssuerFloorEpoch == epoch &&
		pg.CapacityGeneration == request.ExpectedCapacityGeneration && pg.AdmissionRevision == request.Revision-1
}

func (w *PGWorker) dispatchComputeSuspension(ctx context.Context, snapshot *journal.Snapshot, request control.AdmissionRequest) (pgexec.Progress, error) {
	if err := w.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	fact, err := w.client.SetAdmission(ctx, request)
	if err != nil || !fact.Confirms() {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	_, err = w.session.store.RecordServiceClosure(ctx, snapshot, w.session.Actor(), fact, false)
	return pgexec.Recorded, err
}

func (w *PGWorker) observeSuspensionDrain(ctx context.Context, snapshot *journal.Snapshot, operation string) (*control.PreparedResponse, error) {
	m := snapshot.State()
	if !snapshot.Dispatchable() || m.Owner != w.session.Actor() || m.ServiceClosure == nil ||
		m.ServiceClosure.SuspensionOperation != operation || m.ServiceClosure.Pending() {
		return nil, ErrServiceSafety
	}
	fact, err := w.client.PreparedState(ctx)
	if err != nil {
		return nil, err
	}
	latest, err := w.session.store.Load(ctx)
	if err != nil {
		return nil, err
	}
	current := latest.State()
	if !latest.Dispatchable() || current.Owner != m.Owner || current.Sequence != m.Sequence {
		return nil, ErrServiceSafety
	}
	if err := w.session.Check(ctx); err != nil {
		return nil, err
	}
	if err := m.CheckComputeSuspensionDrain(operation, fact); err != nil {
		// A valid native response can still report existing sessions. Keep
		// the same Owner and wait for them to drain without reopening admission.
		return nil, errors.Join(ErrServiceSafety, err)
	}
	return &fact, nil
}
