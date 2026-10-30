package budgetclaims

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 复用明确纯领域夹具，不创建第二套平台事实。本用例核来源选择的完整UID
// 与实际规格边界；真实对象寿命及Node峰值由生产组合中的固定Reader另测。
func TestServiceActualOutsideRequiresOriginalReferenceAndExactVMSize(t *testing.T) {
	raw, err := os.ReadFile("../admissionexec/testdata/reconciled-domain.json")
	require.NoError(t, err)
	var fixture struct {
		Prepared budget.Prepared `json:"prepared"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	p := fixture.Prepared
	require.NoError(t, p.ValidateFor(p.Request, p.Identity))
	require.NotNil(t, p.OutsideClaim)
	var original journal.Model
	usable, err := serviceActualOutside(original, p, p.Request.TargetVMMemoryBytes, *p.OutsideClaim)
	require.NoError(t, err)
	require.Equal(t, p.GuestUsableMemoryBytes, usable)
	for _, mode := range []string{"new-uid", "other-name", "old-vm", "unrelated-size"} {
		t.Run(mode, func(t *testing.T) {
			ref, memory := *p.OutsideClaim, p.Request.TargetVMMemoryBytes
			switch mode {
			case "new-uid":
				ref.UID += "-replacement"
			case "other-name":
				ref.Name += "-other"
			case "old-vm":
				memory = p.VMBeforeMemoryBytes
			case "unrelated-size":
				memory += control.Uint64(8 << 20)
			}
			_, err := serviceActualOutside(original, p, memory, ref)
			require.Error(t, err, "没有永久原来源时不能按数值补造另一VM规格资格")
		})
	}
}
