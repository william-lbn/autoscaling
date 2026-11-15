package owner

import (
	"context"

	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
)

// ObserveLogicalDemand uses the current Owner's authenticated read channel.
func (worker *PGWorker) ObserveLogicalDemand(ctx context.Context) (compute.LogicalDemandObservation, error) {
	if err := worker.session.Check(ctx); err != nil {
		return compute.LogicalDemandObservation{}, err
	}
	callCtx, cancel := worker.session.coupledContext(ctx)
	defer cancel()
	fact, err := worker.client.LogicalDemandObservation(callCtx)
	if err != nil {
		return compute.LogicalDemandObservation{}, err
	}
	if err := worker.session.Check(callCtx); err != nil {
		return compute.LogicalDemandObservation{}, err
	}
	return fact, nil
}
