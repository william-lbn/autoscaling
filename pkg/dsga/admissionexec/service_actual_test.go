package admissionexec

import (
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 预算来源审计在开放前后都可达，但只认证同WAL准确选择的门禁，不能被
// 任意相同布尔值或更新revision放宽。这里只测纯合同，不冒充原生PG认证。
func TestServiceActualGateRequiresExactRetainedAdmissionSelection(t *testing.T) {
	_, open, full := completionAuditFixture(t)
	closed := open
	closed.Open, closed.Revision = false, open.Revision
	open.Revision++
	reclosed := closed
	reclosed.Revision = open.Revision + 1
	for _, mode := range []string{"original-closed", "late-capacity-still-closed", "foreign-closed", "unselected-open", "selected-open", "wrong-open-capacity", "foreign-open", "prior-close", "reclosed", "foreign-reclosed"} {
		t.Run(mode, func(t *testing.T) {
			var closure journal.ServiceClosure
			closure.Request = closed
			c := &closure
			current := full.Shared.Observed
			current.AdmissionOpen, current.AdmissionRevision = false, closed.Revision
			valid := false
			switch mode {
			case "original-closed":
				valid = true
			case "late-capacity-still-closed":
				// 原Resize晚到提交由独立PG预算证明负责；旧关闭的expected
				// 不改写，门禁核对仍只认该原revision。完整预算另核容量。
				current.CapacityGeneration++
				valid = true
			case "foreign-closed":
				current.AdmissionRevision++
			case "unselected-open":
				current.AdmissionOpen = true
			case "selected-open", "wrong-open-capacity", "foreign-open":
				var opening journal.ServiceReopen
				opening.Request = open
				c.Reopen = &opening
				current.AdmissionOpen, current.AdmissionRevision = true, open.Revision
				valid = mode == "selected-open"
				if mode == "wrong-open-capacity" {
					current.CapacityGeneration++
				}
				if mode == "foreign-open" {
					current.AdmissionRevision++
				}
			case "prior-close":
				var opening journal.ServiceReopen
				opening.Request, opening.PriorClose = open, &reclosed
				c.Reopen = &opening
				current.AdmissionRevision = reclosed.Revision
				valid = true
			case "reclosed", "foreign-reclosed":
				var closing journal.ServiceClosure
				closing.Request = reclosed
				var safety journal.ServiceReopenClosure
				safety.Close = &closing
				var opening journal.ServiceReopen
				opening.Request, opening.Closing = open, &safety
				c.Reopen = &opening
				current.AdmissionRevision = reclosed.Revision
				valid = mode == "reclosed"
				if !valid {
					current.AdmissionRevision++
				}
			}
			require.Equal(t, valid, serviceActualGate(c, current))
			changed := current
			changed.Instance.ComputeInstanceID += "-other"
			require.False(t, serviceActualGate(c, changed), "不能借同数值门禁跨原Compute实例")
		})
	}
	var empty control.Snapshot
	require.False(t, serviceActualGate(nil, empty))
}
