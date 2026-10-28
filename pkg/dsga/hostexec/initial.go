package hostexec

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// checkInitial只核首次预算仍有原Node/Pod/Runner实际持有，不生成Resize
// Settlement。初始placement早于任何Resize，没有Host操作CM或VM命令；不能
// 为复用旧结清格式伪造它们，也不能把首次开放当任何后继操作的关闭证明。
// 因此本分支永不写容量/回收来源；有在途或历史峰值仍原样持有。PG/root的
// 持续核验由原Agent执行，本Host观察不赋开放或Guest/QMP动作权限。
func (p *Publisher) checkInitial(ctx context.Context, s *journal.Snapshot) error {
	m := s.State()
	i, err := m.InitialBudgetFor()
	if err != nil {
		return err
	}
	if !s.Dispatchable() || m.Active != nil || m.InitialHeld.Admission == nil || m.InitialHeld.Admission.Phase != journal.Settled || i.NodeName != p.node.Binding().NodeName || i.NodeUID != string(p.node.Binding().NodeUID) || i.HostOverheadBytes != p.overhead {
		return fmt.Errorf("initial Host observation lacks settled original VM/Node binding")
	}
	if err := p.currentOwner(ctx, s); err != nil {
		return err
	}
	sources, err := p.claims.ReadInitialBudget(ctx, s, i)
	if err != nil {
		return err
	}
	birth, ref, observed, err := p.runner.Inspect(ctx, &i.RunnerBirth, m.Owner)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(birth.Identity)
	if err != nil {
		return err
	}
	if ref != i.RunnerBirth || vmguard.SourceDigest(raw) != i.RunnerIdentitySHA256 || birth.Identity.RunnerPodUID != i.RunnerPodUID || birth.Identity.NodeUID != i.NodeUID || observed.State.Pending != nil || observed.Actual.RequestedBytes != i.VMMemoryBytes || observed.Actual.ActualBytes != i.VMMemoryBytes || s.VirtualMachine().Status.MemorySize == nil {
		return fmt.Errorf("initial Host observation changed original Runner or actual capacity")
	}
	actual, exact := s.VirtualMachine().Status.MemorySize.AsInt64()
	if !exact || actual <= 0 || uint64(actual) != uint64(i.VMMemoryBytes) {
		return fmt.Errorf("initial Host VM status differs from retained actual capacity")
	}
	if last := observed.State.Last; last != nil {
		// 只有原域终态能排除未知。相同数值、NotFound或超时均不能
		// 消除旧命令；这里也不删除其持久回执或释放任何Host峰值。
		proof, err := p.runner.ObserveAbortOriginal(ctx, last.Intent.Command, last.Intent.JournalSHA256, m.Owner)
		if err != nil {
			return err
		}
		if !proof.Receipt.Complete() && !proof.Receipt.CancelledBeforeSend() {
			return vmguard.ErrUnknown
		}
	}
	if err := p.claims.RecheckInitialBudget(ctx, s, i, sources); err != nil {
		return err
	}
	return p.unchanged(ctx, s)
}
