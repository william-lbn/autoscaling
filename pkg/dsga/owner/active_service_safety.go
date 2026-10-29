package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// inspectActiveService核不随正常VM/Guest过渡改变的原身份和来源。不能用旧
// 永久预算的容量/Root字节要求合法扩缩中间态；只接受原Held，或本Active已
// 写前登记的完整Guest命令。来源读失败/替换、硬约束漂移必须关已有准入。
// 网络不持本地锁；串行调用前Session已认证，真正关闭仍须独立UID/RV写前CAS。
func (w *PGWorker) inspectActiveService(ctx context.Context, s *journal.Snapshot) error {
	m := s.State()
	if w.resourceOwner == nil || m.ResourceIdentity == nil || w.qualification == nil || !s.Dispatchable() {
		return fmt.Errorf("active service lacks fixed original domains")
	}
	q, err := w.safety.claims.Qualification(ctx, *w.qualification)
	if err != nil {
		return err
	}
	view, err := w.resourceOwner.client.InspectState(ctx)
	if err != nil {
		return err
	}
	if view.Identity != *m.ResourceIdentity || q.Identity != view.Identity || view.IssuerFloorEpoch > m.Owner.Epoch || view.Birth == nil {
		return fmt.Errorf("active service changed Root identity or floor")
	}
	aggregate, aggregateErr := budget.RootAggregateFromView(view)
	process, err := w.resourceOwner.client.InspectProcess(ctx)
	if err != nil {
		return err
	}
	if process != *view.Birth {
		return fmt.Errorf("active service lost original Postmaster birth")
	}
	expected, _, err := idleServiceBound(m)
	held := err == nil && aggregateErr == nil && aggregate.Bound == expected
	for _, step := range m.Active.Steps {
		if step.Action == journal.GuestReady && step.Phase != journal.Planned {
			var command resourceguard.Command
			if json.Unmarshal(step.Command, &command) != nil {
				return fmt.Errorf("active Guest command changed")
			}
			if aggregateErr == nil && view.Held != nil && reflect.DeepEqual(*view.Held, command) {
				held = true
			}
			// Root可能已写到目标约束，但其完成fsync/回复尚未知。若完整
			// 原Original及当前实际约束恰好符合本Active的原命令，这仍是
			// 占住原峰值的合法未知；不能误当漂移，也不能补写Completed。
			if !view.Applied && view.Original != nil && reflect.DeepEqual(*view.Original, command) && view.Actual.ValidateFor(command) == nil && view.BootstrapApplied && (view.Bootstrap.Limits.SystemMemoryMaxBytes == 0 || view.SystemPlacementObserved) && process.PostmasterPID == command.PostmasterPID && process.PostmasterStartTicks == command.PostmasterStartTicks {
				held = true
			}
		}
	}
	if !held {
		return fmt.Errorf("active Root bound belongs to neither original budget nor original Guest command")
	}
	raw, err := json.Marshal(view.Bootstrap)
	if err != nil {
		return err
	}
	vm := s.VirtualMachine()
	if q.RootBootstrapSHA256 != vmguard.SourceDigest(raw) || q.RunnerPodName != vm.Status.PodName || q.NodeName != vm.Status.Node || q.RunnerRestartCount != vm.Status.RestartCount {
		return fmt.Errorf("active original qualification binding changed")
	}
	birth, ref, publication, err := w.safety.runner.Inspect(ctx, &q.RunnerBirth, m.Owner)
	// 接管时Runner floor尚未追上不等于丢来源；只容许具有完整原publication的
	// 这一暂态。没有State/Actual、Lease替换或坏来源均不能借ErrUnknown降级。
	if err != nil && (!errors.Is(err, vmguard.ErrUnknown) || publication.State == nil || publication.Actual == nil) {
		return err
	}
	raw, err = json.Marshal(birth.Identity)
	if err != nil {
		return err
	}
	if ref != q.RunnerBirth || birth.Identity.RunnerPodUID != q.RunnerPodUID || birth.Identity.NodeUID != q.NodeUID || vmguard.SourceDigest(raw) != q.RunnerIdentitySHA256 {
		return fmt.Errorf("active original Runner birth changed")
	}
	// 即使新Prepare/Host尚未产生，也必须继续认证原永久头的来源和
	// 原Pod/Node成本。这里只读原持有，不要求实际容量等于旧预算，故
	// 正常扩缩过渡不会被错判；不能只看新目标而遗漏仍在服务的旧承诺。
	if m.InitialHeld != nil {
		initial, err := m.InitialBudgetFor()
		if err != nil {
			return err
		}
		if _, err := w.safety.claims.ReadInitialBudget(ctx, s, initial); err != nil {
			return err
		}
	} else if _, err := w.safety.claims.ReadActiveHeld(ctx, s); err != nil {
		return err
	}
	// 原VM动作允许requested/actual暂不相等；那是仍占峰值的未知效果，不能
	// 按空闲规则判成已释放。这里只认证已经固定的来源，完整动作审计由原执行器做。
	var prepared *budget.Prepared
	for _, step := range m.Active.Steps {
		if step.Action == journal.PrepareBudget && step.Phase == journal.Settled && step.Observation != nil && step.Observation.Result == journal.Completed {
			var p budget.Prepared
			if json.Unmarshal(step.Observation.Data, &p) != nil || p.ValidateFor(m.Active.Request, *m.ResourceIdentity) != nil {
				return fmt.Errorf("active original Prepare changed")
			}
			if err := w.safety.claims.CheckOutside(ctx, p); err != nil {
				return err
			}
			prepared = &p
		}
	}
	if prepared != nil {
		for _, step := range m.Active.Steps {
			if step.Action == journal.ReserveHost && step.Phase != journal.Planned {
				// 原Host消费者的固定合同；只读原UID，不用当前同数值Host补来源。
				var command struct {
					ProtocolVersion uint32           `json:"protocol_version"`
					Request         control.Request  `json:"request"`
					Claim           budget.ObjectRef `json:"claim"`
					PreparedSHA256  string           `json:"prepared_sha256"`
					HostSHA256      string           `json:"host_sha256"`
				}
				if control.UnmarshalObject(step.Command, &command) != nil || command.Request != m.Active.Request {
					return fmt.Errorf("active original Host command changed")
				}
				if _, err := w.safety.claims.Read(ctx, s, *prepared, command.Claim); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// PG未Start时容量固定；已Start时只接受原代次/提交后下一代，原命令及效果
// 未知全部保留。VM/Root可能已有部分效果，但绝不据此释放/重发。若Quiesce
// 仍未知，逐字引用它并用close-original封住迟到写；否则独立关闭。
func (w *PGWorker) closeActiveService(ctx context.Context, s *journal.Snapshot) (bool, pgexec.Progress, error) {
	m := s.State()
	original, err := m.ActiveAdmissionForClosure()
	if err != nil {
		return false, pgexec.Idle, nil
	}
	pending, closing := w.client.PendingAdmission()
	if closing != nil || pending != nil && (original == nil || *pending != *original) {
		return true, pgexec.AwaitingFact, ErrServiceSafety
	}
	instance, epoch := w.Binding()
	fence, err := w.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch})
	if err != nil || fence.Disposition != control.Fenced {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	shared, err := w.client.SharedCost(ctx)
	if err != nil {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	pg := shared.Observed
	revision := pg.AdmissionRevision
	if original != nil {
		revision = max(revision, original.Revision)
	}
	if pg.Instance != instance || pg.IssuerFloorEpoch != epoch || !m.ActiveServiceCapacityAllowed(pg.CapacityGeneration) || revision == control.Uint64(math.MaxUint64) {
		return true, pgexec.AwaitingFact, ErrServiceSafety
	}
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch, Revision: revision + 1, ExpectedCapacityGeneration: pg.CapacityGeneration, Open: false, BudgetCommitmentID: m.Active.Request.BudgetCommitmentID}
	s, dispatch, err := w.session.store.StartActiveServiceClosure(ctx, s, m.Owner, request)
	if err != nil {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if dispatch.Original != nil {
		fact, err := w.client.CloseAdmissionOriginal(ctx, control.AdmissionCloseRequest{ProtocolVersion: 1, Original: *dispatch.Original, Close: dispatch.Request})
		if err != nil {
			return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
		}
		_, err = w.session.store.RecordActiveServiceClosure(ctx, s, m.Owner, fact, false)
		return true, pgexec.Recorded, err
	}
	fact, err := w.client.SetAdmission(ctx, dispatch.Request)
	if err != nil || !fact.Confirms() {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	_, err = w.session.store.RecordServiceClosure(ctx, s, m.Owner, fact, false)
	return true, pgexec.Recorded, err
}
