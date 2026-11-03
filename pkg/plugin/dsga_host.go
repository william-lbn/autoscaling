package plugin

import (
	"context"
	"time"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/neondatabase/autoscaling/pkg/api"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/plugin/state"
)

func (s *PluginState) hostCallContext() (context.Context, context.CancelFunc) {
	base := s.hostContext
	if base == nil {
		base = context.Background()
	}
	timeout := time.Duration(s.config.K8sCRUDTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
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
	timeout := time.Duration(s.config.K8sCRUDTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.host.CheckPlacement(bounded, node, pod, control.Uint64(0))
}
