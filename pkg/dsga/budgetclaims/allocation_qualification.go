package budgetclaims

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

const AllocationQualificationKind = "platform-allocation-qualification-v2"

// QualificationRead 的分支由原只读入口固定。字段不公开，避免调用者把 v2
// 当作旧 PlatformQualification 送入现有自动动作。返回的候选只是原来源的
// 观察值；没有预算/Host 许可，没有旧引用释放判断，也没有任何资源写操作。
type QualificationRead struct {
	platform   *budget.PlatformQualification
	allocation *budget.AllocationObservation
}

func (v *QualificationRead) Platform() (budget.PlatformQualification, error) {
	if v == nil || v.platform == nil || v.allocation != nil {
		return budget.PlatformQualification{}, fmt.Errorf("allocation qualification cannot be projected into legacy platform target")
	}
	q := *v.platform
	q.Targets = append([]budget.QualifiedTarget(nil), q.Targets...)
	for i := range q.Targets {
		q.Targets[i].Limits.HugePools = append([]resourceguard.HugePool(nil), q.Targets[i].Limits.HugePools...)
	}
	return q, nil
}

func (v *QualificationRead) Allocation() (budget.AllocationObservation, error) {
	if v == nil || v.allocation == nil || v.platform != nil {
		return budget.AllocationObservation{}, fmt.Errorf("legacy qualification has no allocation observation")
	}
	a := *v.allocation
	a.Candidate.Limits.HugePools = append([]resourceguard.HugePool(nil), a.Candidate.Limits.HugePools...)
	a.RetainedTarget.Limits.HugePools = append([]resourceguard.HugePool(nil), a.RetainedTarget.Limits.HugePools...)
	return a, nil
}

// Qualification 是所有现有调用者保留的严格 v1 包装。Source 的旧 wire 不变，
// 新分配字段、kind 或 v2 文档不能经这个入口被“升级后降级”收养。正式读取
// 共用 ReadQualification；本切片不改变 RuntimeSelection/Owner 的选择或执行。
func (r *Reader) Qualification(ctx context.Context, source budget.QualificationSource) (budget.PlatformQualification, error) {
	value, err := r.ReadQualification(ctx, budget.QualificationSelection{ProtocolVersion: 1, Source: source})
	if err != nil {
		return budget.PlatformQualification{}, err
	}
	return value.Platform()
}

// ReadQualification 只使用构造器固定的 Kubernetes 客户端/namespace。新 v2
// 先读原分配对象，再读取其原 Base v1 UID/SHA，最后重读两份来源，要求持续
// immutable/保护 finalizer/原 VM owner/原内容。跨对象读取并非事务或执行许可；
// 未来实际动作仍须逐次新鲜认证并独立核完整 PG/Root 事实，不能缓存此值退款。
func (r *Reader) ReadQualification(ctx context.Context, selected budget.QualificationSelection) (*QualificationRead, error) {
	if err := selected.Validate(); err != nil {
		return nil, err
	}
	if selected.ProtocolVersion == 1 {
		q, err := r.readLegacyQualification(ctx, selected.Source)
		if err != nil {
			return nil, err
		}
		return &QualificationRead{platform: &q}, nil
	}
	// 选择来自调用者内存；网络读取前复制确切值，避免后续复用该指针改变
	// 本次已选择的 allocation。这里只固定请求，不代替外部来源认证。
	selector := *selected.Allocation
	cm, err := r.claim(ctx, selected.Source.Claim, AllocationQualificationKind)
	if err != nil {
		return nil, err
	}
	raw := []byte(cm.Data[ClaimKey])
	digest := sha256.Sum256(raw)
	var q budget.AllocationQualification
	if hex.EncodeToString(digest[:]) != selected.Source.SHA256 || json.Unmarshal(raw, &q) != nil || q.VMUID != string(r.binding.VMUID) || q.Incarnation != r.binding.Incarnation {
		return nil, fmt.Errorf("original allocation qualification content or VM identity changed")
	}
	base, err := r.readLegacyQualification(ctx, q.Base)
	if err != nil {
		return nil, err
	}
	observed, err := q.Observe(selected.Source, selector, base)
	if err != nil {
		return nil, err
	}
	latest, err := r.claim(ctx, selected.Source.Claim, AllocationQualificationKind)
	if err != nil {
		return nil, err
	}
	if latest.ResourceVersion != cm.ResourceVersion || !reflect.DeepEqual(latest.Data, cm.Data) {
		return nil, fmt.Errorf("original allocation changed during source reconciliation")
	}
	baseLatest, err := r.readLegacyQualification(ctx, q.Base)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(base, baseLatest) || q.MatchBase(q.Base, baseLatest) != nil {
		return nil, fmt.Errorf("original allocation parent changed during source reconciliation")
	}
	return &QualificationRead{allocation: &observed}, nil
}
