package plugin

import (
	"context"
	"slices"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/neondatabase/autoscaling/pkg/api"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostscheduler"
	"github.com/neondatabase/autoscaling/pkg/plugin/state"
)

func (s *PluginState) hostCallContext() (context.Context, context.CancelFunc) {
	base := s.hostContext
	if base == nil {
		base = context.Background()
	}
	// 完整Host事务包含多个原对象GET/Lease核验及Node CAS；单CRUD期限
	// 仍由原调用方维护，不把它误当整个事务的总期限。
	timeout := hostscheduler.HostOperationTimeout
	return context.WithTimeout(base, timeout)
}

// applyHostAllocation只影响本地规划视图，最终授予仍须Node CAS。Guest预算
// 不含Host开销；普通Pod按完整硬limit而非requests。容量不打折memory
// overcommit；受管VM不再经旧批准/自动迁移流程，防止两套写者相互越过许可。
func applyHostAllocation(p *state.Pod, allocation hostguard.Placement) {
	p.Mem.Reserved = api.Bytes(allocation.MemoryBytes)
	p.Mem.Overcommit = resource.NewQuantity(1, resource.DecimalSI)
	if allocation.VMUID == "" || allocation.Managed {
		p.Mem.Requested = p.Mem.Reserved
	}
	if allocation.Managed {
		p.Migratable, p.AlwaysMigrate, p.Migrating = false, false, false
	}
}

func (s *PluginState) reserveHost(ctx context.Context, pod *core.Pod, node string) (*core.Pod, *hostguard.Placement, error) {
	if s.host == nil || !s.host.HasNode(node) {
		return pod, nil, nil
	}
	// 已正式退休的普通终态事件不重新授予。只有完整关闭证明且无原保护时
	// 才走只读确认；nil allocation避免把伪零Placement写入本地计费视图。
	// 本地普通requests视图可保守多计，原Delete事件仍按既有流程清理。
	if (pod.Status.Phase == core.PodSucceeded || pod.Status.Phase == core.PodFailed) && pod.Annotations[hostguard.RetirementAnnotation] != "" && !slices.Contains(pod.Finalizers, hostguard.PodProtectionFinalizer) {
		canonical, err := s.host.ObserveRetiredOrdinary(ctx, node, pod)
		return canonical, nil, err
	}
	canonical, held, err := s.host.Reserve(ctx, node, pod, control.Uint64(0))
	if err != nil {
		return nil, nil, err
	}
	return canonical, &held, nil
}

// checkHost只读过滤明确Host域，nil/域外继续旧路径。它不修改评分或放置规则，
// 每次失败交调度器正常重试；Reserve仍是真正授予的唯一CAS入口。
func (s *PluginState) checkHost(ctx context.Context, pod *core.Pod, node string) error {
	if s.host == nil || !s.host.HasNode(node) {
		return nil
	}
	// 完整Host事务包含多个原对象GET/Lease核验及Node CAS；单CRUD期限
	// 仍由原调用方维护，不把它误当整个事务的总期限。
	timeout := hostscheduler.HostOperationTimeout
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.host.CheckPlacement(bounded, node, pod, control.Uint64(0))
}
