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
	return m.checkClosedPostgresDrain(fact)
}

// HasSettledIdleServiceClosure identifies an existing, fully confirmed safety
// close. It grants no suspension authority: callers must independently bind the
// current runtime selection, original VM/boot, Operation and authenticated Owner.
// Extended recovery or unknown execution remains on its existing recovery path.
func (m Model) HasSettledIdleServiceClosure() bool {
	c := m.ServiceClosure
	version := m.ClosureFormatVersion()
	return m.Validate() == nil && m.Active == nil && m.PGInstance != nil && c != nil &&
		(version == 10 || version == 11) && c.Reason == "idle_source_unconfirmed" &&
		c.SuspensionOperation == "" && !c.Pending() && c.Reopen == nil && c.Rearm == nil
}

// CheckClosedSafetyDrain only authenticates the original closed native boundary
// and its drain. Runtime selection separately authorizes suspension. This does
// not rewrite the safety close as a suspension, reopen admission or release Host
// capacity; physical termination and original Host settlement remain required.
func (m Model) CheckClosedSafetyDrain(fact control.PreparedResponse) error {
	if !m.HasSettledIdleServiceClosure() {
		return fmt.Errorf("original idle safety closure is not settled")
	}
	return m.checkClosedPostgresDrain(fact)
}

func (m Model) checkClosedPostgresDrain(fact control.PreparedResponse) error {
	c := m.ServiceClosure
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
