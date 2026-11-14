package journal

import (
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// CheckComputeSuspensionDrain binds a fresh native observation to the exact
// accepted operation, closed generation and currently authenticated owner.
// Physical termination and Host settlement are separate observations.
func (m Model) CheckComputeSuspensionDrain(operation string, fact control.PreparedResponse) error {
	if err := m.Validate(); err != nil {
		return err
	}
	c := m.ServiceClosure
	if operation == "" || c == nil || c.SuspensionOperation != operation || c.Pending() || m.Active != nil || m.PGInstance == nil {
		return fmt.Errorf("original compute suspension is not closed")
	}
	_, _, closed, _ := c.Selection()
	connections, work := fact.Connections, fact.Connections.Work
	pg := work.Observed
	if fact.ValidateFor(*m.PGInstance) != nil || fact.PreparedSlots != 0 || !fact.RuntimePreparesBlocked ||
		!connections.PublicConnectionsBlocked || !connections.PrivateManagementReady ||
		!work.ObserverExcluded || !work.ForegroundIdle || work.ClientProcesses != 0 ||
		pg.AdmissionRevision != closed.Revision || pg.CapacityGeneration != closed.ExpectedCapacityGeneration ||
		pg.IssuerFloorEpoch != m.Owner.Epoch {
		return fmt.Errorf("compute suspension awaits the original closed and drained PostgreSQL instance")
	}
	return nil
}
