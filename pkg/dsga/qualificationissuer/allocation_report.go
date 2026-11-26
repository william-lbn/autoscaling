package qualificationissuer

import (
	"encoding/json"
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

const AllocationReportScope = "neon-dsga-linux-allocation-platform-report-v3"

// 生成仅做结构/完整Base一致性核验；真正发布仍要求原Config.ReportSHA256认证和实际capture。
// targets/transition不能来自普通API预算输入，只能来自受保护平台profile的确切候选。
func NewAllocationReport(parent Report, base budget.QualificationSource, allocation budget.AllocationQualification) (Report, error) {
	if parent.Validate() != nil || parent.ProtocolVersion != 2 || base.Validate() != nil || allocation.Base != base ||
		allocation.MatchBase(base, parent.Qualification) != nil {
		return Report{}, fmt.Errorf("allocation report must retain original certified Base")
	}
	raw, err := json.Marshal(allocation)
	if err != nil {
		return Report{}, err
	}
	var frozen budget.AllocationQualification
	if err = json.Unmarshal(raw, &frozen); err != nil {
		return Report{}, err
	}
	parent.ProtocolVersion = 3
	parent.Scope = AllocationReportScope
	parent.AllocationQualification = &frozen
	if err = parent.Validate(); err != nil {
		return Report{}, err
	}
	return parent, nil
}
