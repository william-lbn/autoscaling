package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// ErrServiceSafety表示保持原owner、预算和关闭WAL的安全等待。它不是排空、
// VM回收或NoEffect证明；源恢复也不能让Run自行重开门禁。
var ErrServiceSafety = errors.New("service safety retains original closed admission boundary")

type serviceSafety struct {
	claims *budgetclaims.Reader
	runner *vmguard.Reader
}

// inspectIdleService不写任何领域。Root原日志/硬约束、出生、资格和Runner
// 的任一读失败都不再视为“只停止建议仍服务”。只在无在途动作的永久预算
// 边界使用；不能把伸缩过程中的合法中间态误判为异常，或清旧SDK未知。
func (w *PGWorker) inspectIdleService(ctx context.Context, s *journal.Snapshot) error {
	m := s.State()
	if w.resourceOwner == nil || m.ResourceIdentity == nil || !s.Dispatchable() {
		return fmt.Errorf("idle service lost original resource or VM binding")
	}
	view, err := w.resourceOwner.client.InspectState(ctx)
	if err != nil {
		return err
	}
	if _, err := view.ConfirmedLimits(); err != nil {
		return err
	}
	process, err := w.resourceOwner.client.InspectProcess(ctx)
	if err != nil {
		return err
	}
	if view.Identity != *m.ResourceIdentity || view.Birth == nil || process != *view.Birth || view.IssuerFloorEpoch != w.session.Actor().Epoch {
		return fmt.Errorf("idle service changed original Root birth or floor")
	}
	root, err := budget.RootAggregateFromView(view)
	if err != nil {
		return err
	}
	expected, guestMinimum, err := idleServiceBound(m)
	if err != nil || expected != root.Bound || view.Actual.GuestMemTotalBytes < guestMinimum {
		// Root自身“已确认”不表示仍被外层原预算覆盖。SourceID摘要
		// 包含原出生、Bootstrap和Held完整命令，不能只比较总字节。
		return fmt.Errorf("idle service Root constraint differs from original permanent budget")
	}
	var source *budget.QualificationSource
	if w.qualification != nil {
		source = w.qualification
	} else if m.InitialHeld != nil {
		i, err := m.InitialBudgetFor()
		if err != nil {
			return err
		}
		source = &i.Qualification
	}
	if m.InitialHeld != nil {
		i, err := m.InitialBudgetFor()
		if err != nil {
			return err
		}
		if _, err := w.safety.claims.ReadInitialBudget(ctx, s, i); err != nil {
			return err
		}
	} else if _, err := w.safety.claims.ReadCurrent(ctx, s); err != nil {
		return err
	}
	// 没有可信资格的旧领域读者不补造资格；Root闭环仍有效，新的自动
	// 策略/预算也继续要求自己的完整来源。生产已配置资格则不得降级。
	if source == nil {
		return nil
	}
	q, err := w.safety.claims.Qualification(ctx, *source)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(view.Bootstrap)
	if err != nil {
		return err
	}
	vm := s.VirtualMachine()
	if q.Identity != view.Identity || vmguard.SourceDigest(raw) != q.RootBootstrapSHA256 || q.RunnerPodName != vm.Status.PodName || q.RunnerRestartCount != vm.Status.RestartCount || q.NodeName != vm.Status.Node {
		return fmt.Errorf("idle service qualification changed original Root or Runner")
	}
	birth, ref, published, err := w.safety.runner.Inspect(ctx, &q.RunnerBirth, w.session.Actor())
	if err != nil {
		return err
	}
	raw, err = json.Marshal(birth.Identity)
	if err != nil {
		return err
	}
	if ref != q.RunnerBirth || vmguard.SourceDigest(raw) != q.RunnerIdentitySHA256 || birth.Identity.RunnerPodUID != q.RunnerPodUID || birth.Identity.NodeUID != q.NodeUID || published.State.Pending != nil || published.Actual.ActualBytes != published.Actual.RequestedBytes || vm.Status.MemorySize == nil {
		return fmt.Errorf("idle service lacks original settled Runner capacity")
	}
	actual, exact := vm.Status.MemorySize.AsInt64()
	if !exact || actual <= 0 || control.Uint64(actual) != published.Actual.ActualBytes {
		return fmt.Errorf("idle service VM status differs from original Runner actual")
	}
	return nil
}

// 取永久头中已核验的原聚合来源及Guest最低保证，不从现有占用重算预算。
// 三种预算互斥由Model校验；旧分项预算无完整Root引用时明确拒绝自动维持。
func idleServiceBound(m journal.Model) (budget.Bound, control.Uint64, error) {
	if m.InitialHeld != nil {
		initial, err := m.InitialBudgetFor()
		return initial.PGAggregate.Bound, initial.GuestUsableMemoryBytes, err
	}
	if m.BudgetHeld != nil && m.BudgetHeld.Finalization.Observation != nil {
		var finalized budget.Finalized
		if json.Unmarshal(m.BudgetHeld.Finalization.Observation.Data, &finalized) == nil && finalized.Aggregate != nil {
			return *finalized.Aggregate, finalized.GuestUsableMemoryBytes, nil
		}
	}
	if m.CapacityHeld != nil {
		var command budget.ReconciledAbortCommand
		if json.Unmarshal(m.CapacityHeld.Cleanup.Command, &command) == nil {
			return command.Capacity.PGAggregate, command.Capacity.GuestUsableMemoryBytes, nil
		}
	}
	return budget.Bound{}, 0, fmt.Errorf("idle service has no exact retained Root aggregate")
}

// 本入口总在Run/Advance原串行goroutine内，并已执行Session.Check。网络不
// 持本地锁；首次命令必须由UID/RV写前CAS确认，随后发送同一SDK的关闭。
// 重建或丢回复只查询原命令。明确重放还须原派发owner与完整摘要均匹配，
// 不用“关闭幂等”作自动重发理由；旧命令可能仍在网络中，不能换代次覆盖。
func (w *PGWorker) advanceServiceSafety(ctx context.Context, replay string) (bool, pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return true, pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != w.session.Actor() {
		return true, pgexec.AwaitingFact, ErrLeadershipLost
	}
	if c := m.ServiceClosure; c != nil {
		origin, _, _, digest := c.Selection()
		if replay != "" && (digest != replay || origin != m.Owner || !c.Pending()) {
			return true, pgexec.AwaitingFact, fmt.Errorf("service closure replay differs from original owner/command")
		}
		if !c.Pending() {
			return true, pgexec.AwaitingFact, ErrServiceSafety
		}
		if n := c.Continuation; n != nil {
			// 普通轮询始终只查询最近选择；原始/前驱回执不能完成它。
			// 同owner明确重放也必须逐字使用WAL，不能重选revision。
			var fact control.AdmissionContinuationResponse
			if replay == "" {
				fact, err = w.client.AdmissionContinuationStatus(ctx, n.Request)
			} else {
				fact, err = w.client.ContinueAdmissionClosure(ctx, n.Request)
			}
			if err != nil {
				return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
			}
			_, err = w.session.store.RecordServiceClosureContinuation(ctx, s, m.Owner, fact, replay == "")
			return true, pgexec.Recorded, err
		}
		var fact control.AdmissionResponse
		if replay == "" {
			fact, err = w.client.AdmissionStatus(ctx, c.Request)
		} else {
			fact, err = w.client.SetAdmission(ctx, c.Request)
		}
		if err != nil || !fact.Confirms() {
			return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
		}
		_, err = w.session.store.RecordServiceClosure(ctx, s, m.Owner, fact, replay == "")
		return true, pgexec.Recorded, err
	}
	if replay != "" {
		return true, pgexec.AwaitingFact, fmt.Errorf("service closure WAL is absent")
	}
	if w.safety == nil || m.Active != nil || m.InitialHeld == nil && m.BudgetHeld == nil && m.CapacityHeld == nil {
		return false, pgexec.Idle, nil
	}
	if h := m.InitialHeld; h != nil && (h.Admission == nil || h.Admission.Phase != journal.Settled) {
		return false, pgexec.Idle, nil
	}
	if pending, closing := w.client.PendingAdmission(); pending != nil || closing != nil || w.client.Pending() != nil {
		return true, pgexec.AwaitingFact, ErrServiceSafety
	}
	if err := w.inspectIdleService(ctx, s); err == nil {
		return false, pgexec.Idle, nil
	}
	// 来源不可用时仍能独立认证PG的同boot、floor和当前准入代次。只从
	// 永久预算取ID，不重新承诺/释放；代次耗尽或PG不可达保留安全等待，
	// 不能宣称已经关闭。外层WAL损坏不在此路径，仍须独立关闭协议。
	shared, err := w.client.SharedCost(ctx)
	if err != nil {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	pg := shared.Observed
	instance, epoch := w.Binding()
	budgetID, err := m.ServiceBudgetID()
	if err != nil || pg.Instance != instance || pg.IssuerFloorEpoch != epoch || pg.AdmissionRevision == control.Uint64(math.MaxUint64) {
		return true, pgexec.AwaitingFact, ErrServiceSafety
	}
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch, Revision: pg.AdmissionRevision + 1, ExpectedCapacityGeneration: pg.CapacityGeneration, Open: false, BudgetCommitmentID: budgetID}
	s, dispatch, err := w.session.store.StartServiceClosure(ctx, s, m.Owner, request)
	if err != nil {
		// 旧预算若缺新格式所需的原Runner来源，不能投影补造后升级。
		// 仍保留原owner与异常事实等待恢复，不将一次拒绝变成反复换主。
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	fact, err := w.client.SetAdmission(ctx, dispatch.Request)
	if err != nil || !fact.Confirms() {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	_, err = w.session.store.RecordServiceClosure(ctx, s, m.Owner, fact, false)
	return true, pgexec.Recorded, err
}

// continueServiceClosure只由已认证的同一串行管理循环调用。先原生fence
// 当前owner，再观察全局准入revision，最后写前CAS并发出完整新关闭。
// fence不能证明已关闭，也不清旧未知；若旧关闭已应用则使用下一revision，
// 未投递则可复用其未消费revision。任何失败都保留原预算/旧工作，不自动
// 重开、释放或缩VM；再次接管必须选择最新摘要，不能从最初关闭跳跃。
func (w *PGWorker) continueServiceClosure(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	c := m.ServiceClosure
	if m.Owner != w.session.Actor() || c == nil || !c.Pending() {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	origin, _, previous, digest := c.Selection()
	instance, epoch := w.Binding()
	if selector != digest || m.Owner.Epoch <= origin.Epoch || c.Request.Instance != instance {
		return pgexec.AwaitingFact, fmt.Errorf("service continuation requires exact latest closure and higher current owner")
	}
	// PG的fence与后续setter还会独立认证当前签名owner；此处先封住
	// 旧setter再读revision，避免“先观察，旧请求迟到后仍按旧值派发”。
	fence, err := w.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch})
	if err != nil || fence.Disposition != control.Fenced {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	shared, err := w.client.SharedCost(ctx)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	pg := shared.Observed
	if pg.Instance != instance || pg.IssuerFloorEpoch != epoch || pg.CapacityGeneration != c.Request.ExpectedCapacityGeneration || pg.AdmissionRevision == control.Uint64(math.MaxUint64) || pg.AdmissionRevision+1 < previous.Revision {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	request := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: c.Request, PreviousClose: previous, Close: c.Request}
	request.Close.IssuerEpoch, request.Close.Revision = epoch, pg.AdmissionRevision+1
	s, dispatch, err := w.session.store.StartServiceClosureContinuation(ctx, s, m.Owner, selector, request)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	fact, err := w.client.ContinueAdmissionClosure(ctx, dispatch.Request)
	if err != nil {
		return pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	_, err = w.session.store.RecordServiceClosureContinuation(ctx, s, m.Owner, fact, false)
	return pgexec.Recorded, err
}
