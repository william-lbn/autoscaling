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
		if c.Reopen != nil {
			// 新开放/再关闭只能查自己的最新 WAL，不退回原关闭接口。
			// 普通轮询绝不重放 setter，原管理重放也不能借旧摘要越过它。
			if replay != "" {
				return true, pgexec.AwaitingFact, ErrServiceSafety
			}
			progress, err := w.advanceServiceReopen(ctx)
			return true, progress, err
		}
		if c.Rearm != nil && c.Rearm.Phase == journal.Unconfirmed && replay == "" {
			progress, err := w.advanceServiceRearm(ctx, c.Rearm.CommandSHA256, false)
			return true, progress, err
		}
		origin, _, _, digest := c.Selection()
		if replay != "" && (digest != replay || origin != m.Owner || !c.Pending()) {
			return true, pgexec.AwaitingFact, fmt.Errorf("service closure replay differs from original owner/command")
		}
		if !c.Pending() {
			if replay == "" && c.PGRecovery != nil {
				// 普通轮询只复核已持久PG证明，原Active未知及关闭保持。
				_, err := w.executor.RecoverServicePG(ctx, s, digest, false)
				if err != nil {
					return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
				}
				if c.ResourceRecovery == nil {
					return true, pgexec.Reconciled, ErrServiceSafety
				}
			}
			if replay == "" && c.ResourceRecovery != nil && w.resources != nil {
				// 只查已持久资源恢复，不在普通轮询中创建取消/重发Apply。
				_, progress, err := w.resources.RecoverServiceGuest(ctx, s, digest, false)
				return true, progress, errors.Join(ErrServiceSafety, err)
			}
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
				return w.recoverServiceClosureCapacity(ctx, m, err)
			}
			_, err = w.session.store.RecordServiceClosureContinuation(ctx, s, m.Owner, fact, replay == "")
			return true, pgexec.Recorded, err
		}
		if c.Original != nil {
			selection := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: *c.Original, Close: c.Request}
			var fact control.AdmissionClosure
			if replay == "" {
				fact, err = w.client.AdmissionClosureStatus(ctx, selection)
			} else {
				fact, err = w.client.CloseAdmissionOriginal(ctx, selection)
			}
			if err != nil {
				return w.recoverServiceClosureCapacity(ctx, m, err)
			}
			if m.ClosureFormatVersion() == 18 {
				_, err = w.session.store.RecordCleanupServiceClosure(ctx, s, m.Owner, fact, replay == "")
			} else if m.ClosureFormatVersion() == 16 || m.ClosureFormatVersion() == 17 {
				_, err = w.session.store.RecordActiveServiceClosure(ctx, s, m.Owner, fact, replay == "")
			} else if c.OperationSHA256 != "" {
				_, err = w.session.store.RecordResumeServiceClosure(ctx, s, m.Owner, fact, replay == "")
			} else {
				_, err = w.session.store.RecordInitialServiceClosure(ctx, s, m.Owner, fact, replay == "")
			}
			return true, pgexec.Recorded, err
		}
		var fact control.AdmissionResponse
		if replay == "" {
			fact, err = w.client.AdmissionStatus(ctx, c.Request)
		} else {
			fact, err = w.client.SetAdmission(ctx, c.Request)
		}
		if err != nil || !fact.Confirms() {
			return w.recoverServiceClosureCapacity(ctx, m, err)
		}
		_, err = w.session.store.RecordServiceClosure(ctx, s, m.Owner, fact, replay == "")
		return true, pgexec.Recorded, err
	}
	if replay != "" {
		return true, pgexec.AwaitingFact, fmt.Errorf("service closure WAL is absent")
	}
	// 原实际容量Cleanup在首次完整预算之前也可能已写前持久；不能因没有
	// 永久头跳过其来源检查。边界从原Cleanup取，绝不补造Initial/Finalized。
	if w.safety == nil || m.InitialHeld == nil && m.BudgetHeld == nil && m.CapacityHeld == nil && (m.Active == nil || m.Active.Cleanup == nil) {
		return false, pgexec.Idle, nil
	}
	if m.Active != nil {
		if m.Active.Cleanup != nil {
			boundary, err := m.CleanupServiceForClosure()
			if err != nil {
				return false, pgexec.Idle, nil
			}
			if err := w.inspectCleanupService(ctx, s); err == nil {
				return false, pgexec.Idle, nil
			}
			return w.closeCleanupService(ctx, s, boundary)
		}
		// 仅在所有先前领域完整结算、最后开放前后的稳定边界复用
		// 完整预算审计。其他Resize中间态不能按旧永久预算误判来源漂移。
		original, err := m.ResumeAdmissionForClosure()
		if err != nil {
			f, plannedErr := m.PlannedResumeBudgetForClosure()
			if plannedErr != nil {
				generation, pendingErr := m.PendingFinalizationGenerationForClosure()
				if pendingErr != nil || w.finalize == nil {
					if w.qualification == nil {
						return false, pgexec.Idle, nil
					}
					if _, boundaryErr := m.ActiveAdmissionForClosure(); boundaryErr != nil {
						return false, pgexec.Idle, nil
					}
					if err := w.inspectActiveService(ctx, s); err == nil {
						return false, pgexec.Idle, nil
					}
					return w.closeActiveService(ctx, s)
				}
				if err := w.finalize.CheckBeforeFinalization(ctx, s); err == nil {
					return false, pgexec.Idle, nil
				}
				return w.closePlannedResumeService(ctx, s, generation)
			}
			if err := w.admission.CheckRetainedService(ctx, s); err == nil {
				return false, pgexec.Idle, nil
			}
			return w.closePlannedResumeService(ctx, s, f.Shared.Observed.CapacityGeneration)
		}
		if err := w.admission.CheckRetainedService(ctx, s); err == nil {
			return false, pgexec.Idle, nil
		}
		return w.closeOriginalService(ctx, s, original, true)
	}
	if h := m.InitialHeld; h != nil {
		if h.Admission == nil {
			return false, pgexec.Idle, nil
		}
		if h.Admission.Phase == journal.Unconfirmed && !m.InitialAdmissionReady() {
			return w.advanceInitialServiceSafety(ctx, s)
		}
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

// 原关闭可能在取SharedCost和setter之间撞上原PG提交。只在v17、当前
// 同一owner且容量严格前进时，continueServiceClosure才允许新选择并先写前
// CAS；同代次/网络未知原样等待，不自动重放原setter，也不等待被迫换owner。
// 单个原Resize最多前进一次，因此该自动恢复有界，不会无穷消耗revision。
func (w *PGWorker) recoverServiceClosureCapacity(ctx context.Context, m journal.Model, originalErr error) (bool, pgexec.Progress, error) {
	if m.ClosureFormatVersion() == 17 && m.ServiceClosure != nil && m.ServiceClosure.Pending() {
		origin, _, _, digest := m.ServiceClosure.Selection()
		if origin == m.Owner {
			progress, err := w.continueServiceClosure(ctx, digest)
			if err == nil {
				return true, progress, nil
			}
			originalErr = errors.Join(originalErr, err)
		}
	}
	return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, originalErr)
}

// Resume仍为空Planned时没有可供CloseAdmissionOriginal选择的原开放。
// 先原生fence封住旧owner，再在同boot/完整Finalize容量下选择真实关闭；
// UID/RV写前CAS固定完整Active及预算后才派发SetAdmission(false)。Resize
// SDK可能仅待已Completed原回执对账，保留它而不阻塞独立关闭；另一未知
// 准入则拒绝收养。任何失败不Finish、不改Planned、不释放或缩VM。
func (w *PGWorker) closePlannedResumeService(ctx context.Context, s *journal.Snapshot, generation control.Uint64) (bool, pgexec.Progress, error) {
	if pending, closing := w.client.PendingAdmission(); pending != nil || closing != nil {
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
	m := s.State()
	if m.Active == nil || pg.Instance != instance || pg.IssuerFloorEpoch != epoch || pg.CapacityGeneration != generation || pg.AdmissionRevision == control.Uint64(math.MaxUint64) {
		return true, pgexec.AwaitingFact, ErrServiceSafety
	}
	// 资源前驱已完成但Finalize尚未落账时，只引用原Active的目标承诺ID，
	// 不以当前永久头的旧ID冒充新容量已结算；Start会固定完整原Active。
	budgetID := m.Active.Request.BudgetCommitmentID
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch, Revision: pg.AdmissionRevision + 1, ExpectedCapacityGeneration: generation, Open: false, BudgetCommitmentID: budgetID}
	s, dispatch, err := w.session.store.StartServiceClosure(ctx, s, w.session.Actor(), request)
	if err != nil {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	fact, err := w.client.SetAdmission(ctx, dispatch.Request)
	if err != nil || !fact.Confirms() {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	_, err = w.session.store.RecordServiceClosure(ctx, s, w.session.Actor(), fact, false)
	return true, pgexec.Recorded, err
}

// continueServiceClosure只由已认证的同一串行管理循环调用。先原生fence
// 当前owner，再观察全局准入revision，最后写前CAS并发出完整新关闭。
// fence不能证明已关闭，也不清旧未知；若旧关闭已应用则使用下一revision，
// 未投递则可复用其未消费revision。任何失败都保留原预算/旧工作，不自动
// 重开、释放或缩VM；再次接管必须选择最新摘要，不能从最初关闭跳跃。
// 已派发PG的容量在关闭前进一次时，当前同owner也可持久选择新关闭；严格
// 更高revision封住迟到旧关闭。该自动分支由原Resize有限容量集合约束。
func (w *PGWorker) continueServiceClosure(ctx context.Context, selector string) (pgexec.Progress, error) {
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	c := m.ServiceClosure
	if m.Owner != w.session.Actor() || c == nil || c.Reopen != nil || !c.Pending() {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	origin, _, previous, digest := c.Selection()
	instance, epoch := w.Binding()
	if selector != digest || m.Owner.Epoch < origin.Epoch || m.Owner.Epoch == origin.Epoch && m.Owner != origin || c.Request.Instance != instance {
		return pgexec.AwaitingFact, fmt.Errorf("service continuation requires exact latest closure and current owner")
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
	allowed := pg.CapacityGeneration == c.Request.ExpectedCapacityGeneration
	if m.ClosureFormatVersion() == 17 {
		allowed = m.ActiveServiceCapacityAllowed(pg.CapacityGeneration) && pg.CapacityGeneration >= previous.ExpectedCapacityGeneration
	}
	if pg.Instance != instance || pg.IssuerFloorEpoch != epoch || !allowed || pg.AdmissionRevision == control.Uint64(math.MaxUint64) {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	request := control.AdmissionContinuationRequest{ProtocolVersion: 1, Original: c.ProtectedAdmission(), PreviousClose: previous, Close: c.Request}
	if m.ClosureFormatVersion() == 17 {
		request.ProtocolVersion = 2
		request.Close.ExpectedCapacityGeneration = pg.CapacityGeneration
	}
	if m.Owner == origin && (m.ClosureFormatVersion() != 17 || pg.CapacityGeneration <= previous.ExpectedCapacityGeneration || previous.Revision == control.Uint64(math.MaxUint64)) {
		return pgexec.AwaitingFact, ErrServiceSafety
	}
	// 原首次开放及关闭都未投递时，PG水位可能仍为0；新关闭必须
	// 至少覆盖最近已持久的关闭，不能退到1与最初开放共享revision。
	request.Close.IssuerEpoch, request.Close.Revision = epoch, max(pg.AdmissionRevision+1, previous.Revision)
	if m.Owner == origin {
		request.Close.Revision = max(request.Close.Revision, previous.Revision+1)
	}
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

// 首次开放可能已执行、未发送或仍在网络中。先复核完整原预算；来源异常
// 时写独立关闭WAL，再关闭同boot/容量/预算下所有更低revision，保留原开放
// Unconfirmed。此处不调用原开放Status来清本地栅栏，也不以NotFound重发。
// 网络期间无本地锁，失租取消通过Session ctx及SDK凭据检查逐次传播。
func (w *PGWorker) advanceInitialServiceSafety(ctx context.Context, s *journal.Snapshot) (bool, pgexec.Progress, error) {
	m := s.State()
	if w.initial == nil {
		return true, pgexec.AwaitingFact, ErrServiceSafety
	}
	if err := w.initial.CheckRetained(ctx, s); err == nil {
		return false, pgexec.Idle, nil
	}
	var command budget.InitialAdmissionCommand
	if json.Unmarshal(m.InitialHeld.Admission.Command, &command) != nil {
		return true, pgexec.AwaitingFact, ErrServiceSafety
	}
	original := command.Request
	return w.closeOriginalService(ctx, s, original, false)
}

// 首次与最终开放共享同一原生关闭协议，但写入各自严格的外层WAL。
// resume只在完整Finalized已持有时为true，不借当前PG状态拼造预算。
// 已确认的Resize SDK pending可能仅待原终态对账；关闭可走独立观察池，
// 不清该未知。首次规则仍拒绝任何Resize pending，保持原兼容边界。
func (w *PGWorker) closeOriginalService(ctx context.Context, s *journal.Snapshot, original control.AdmissionRequest, resume bool) (bool, pgexec.Progress, error) {
	m := s.State()
	pending, closing := w.client.PendingAdmission()
	if closing != nil || pending != nil && *pending != original || !resume && w.client.Pending() != nil {
		return true, pgexec.AwaitingFact, ErrServiceSafety
	}
	instance, epoch := w.Binding()
	// 来源可能在observe到达PG前就失败；新owner也必须独立取得原生
	// fence，不能靠Root可达性决定是否封住旧owner的迟到开放。
	fence, err := w.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch})
	if err != nil || fence.Disposition != control.Fenced {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	shared, err := w.client.SharedCost(ctx)
	if err != nil {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	pg := shared.Observed
	// 初次开放只允许原容量，跨容量/boot必须走独立生命周期协议。
	// max后先核溢出再加一；未投递的原revision也必须被关闭水位覆盖。
	revision := max(pg.AdmissionRevision, original.Revision)
	if pg.Instance != instance || pg.IssuerFloorEpoch != epoch || pg.CapacityGeneration != original.ExpectedCapacityGeneration || revision == control.Uint64(math.MaxUint64) {
		return true, pgexec.AwaitingFact, ErrServiceSafety
	}
	closeRequest := original
	closeRequest.Open, closeRequest.IssuerEpoch, closeRequest.Revision = false, epoch, revision+1
	selection := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: original, Close: closeRequest}
	var dispatch *journal.ServiceClosure
	if resume {
		s, dispatch, err = w.session.store.StartResumeServiceClosure(ctx, s, m.Owner, selection)
	} else {
		s, dispatch, err = w.session.store.StartInitialServiceClosure(ctx, s, m.Owner, selection)
	}
	if err != nil {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	selection.Original, selection.Close = *dispatch.Original, dispatch.Request
	fact, err := w.client.CloseAdmissionOriginal(ctx, selection)
	if err != nil {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if resume {
		_, err = w.session.store.RecordResumeServiceClosure(ctx, s, m.Owner, fact, false)
	} else {
		_, err = w.session.store.RecordInitialServiceClosure(ctx, s, m.Owner, fact, false)
	}
	return true, pgexec.Recorded, err
}
