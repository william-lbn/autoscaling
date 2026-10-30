package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// Cleanup的实际/完整目标/旧容量各有原生产预算审计，不能按旧永久头把合法
// 部分容量误关；额外的资格校验只核固定身份/出生/Bootstrap，不重算成本。
// 无资格的旧领域构造器保持原预算严格门禁，生产配置了资格则每轮必须重读。
func (w *PGWorker) inspectCleanupService(ctx context.Context, s *journal.Snapshot) error {
	if err := w.admission.CheckCleanupService(ctx, s); err != nil {
		return err
	}
	return w.inspectCleanupQualification(ctx, s)
}

func (w *PGWorker) inspectCleanupQualification(ctx context.Context, s *journal.Snapshot) error {
	if w.qualification == nil {
		return nil
	}
	q, err := w.safety.claims.Qualification(ctx, *w.qualification)
	if err != nil {
		return err
	}
	view, err := w.resourceOwner.client.InspectState(ctx)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(view.Bootstrap)
	if err != nil {
		return err
	}
	vm := s.VirtualMachine()
	if s.State().ResourceIdentity == nil || view.Identity != *s.State().ResourceIdentity || q.Identity != view.Identity || q.RootBootstrapSHA256 != vmguard.SourceDigest(raw) || q.RunnerPodName != vm.Status.PodName || q.RunnerRestartCount != vm.Status.RestartCount || q.NodeName != vm.Status.Node {
		return fmt.Errorf("cleanup qualification changed original Root or Runner")
	}
	birth, ref, publication, err := w.safety.runner.Inspect(ctx, &q.RunnerBirth, s.State().Owner)
	if err != nil {
		return err
	}
	raw, err = json.Marshal(birth.Identity)
	if err != nil || ref != q.RunnerBirth || vmguard.SourceDigest(raw) != q.RunnerIdentitySHA256 || birth.Identity.RunnerPodUID != q.RunnerPodUID || birth.Identity.NodeUID != q.NodeUID || publication.State.Pending != nil || publication.Actual.ActualBytes != publication.Actual.RequestedBytes {
		return fmt.Errorf("cleanup qualification lost original settled Runner")
	}
	return nil
}

// 原Cleanup已把资源效果结清到固定代次，独立关闭不得跟随任意当前容量。
// 先原生fence再读revision；原开放即使未到达PG，也用更高水位封住迟到写。
// 只在原串行owner内调用，不持本地锁跨网络；写前CAS丢回复零派发。
func (w *PGWorker) closeCleanupService(ctx context.Context, s *journal.Snapshot, boundary journal.CleanupServiceBoundary) (bool, pgexec.Progress, error) {
	m := s.State()
	pending, closing := w.client.PendingAdmission()
	if closing != nil || pending != nil && (boundary.Admission == nil || *pending != *boundary.Admission) {
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
	if boundary.Admission != nil {
		revision = max(revision, boundary.Admission.Revision)
	}
	if pg.Instance != instance || pg.IssuerFloorEpoch != epoch || pg.CapacityGeneration != boundary.CapacityGeneration || revision == control.Uint64(math.MaxUint64) {
		return true, pgexec.AwaitingFact, ErrServiceSafety
	}
	request := control.AdmissionRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch, Revision: revision + 1, ExpectedCapacityGeneration: boundary.CapacityGeneration, Open: false, BudgetCommitmentID: boundary.BudgetCommitmentID}
	s, dispatch, err := w.session.store.StartCleanupServiceClosure(ctx, s, m.Owner, request)
	if err != nil {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	if dispatch.Original != nil {
		fact, err := w.client.CloseAdmissionOriginal(ctx, control.AdmissionCloseRequest{ProtocolVersion: 1, Original: *dispatch.Original, Close: dispatch.Request})
		if err != nil {
			return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
		}
		_, err = w.session.store.RecordCleanupServiceClosure(ctx, s, m.Owner, fact, false)
		return true, pgexec.Recorded, err
	}
	fact, err := w.client.SetAdmission(ctx, dispatch.Request)
	if err != nil || !fact.Confirms() {
		return true, pgexec.AwaitingFact, errors.Join(ErrServiceSafety, err)
	}
	_, err = w.session.store.RecordServiceClosure(ctx, s, m.Owner, fact, false)
	return true, pgexec.Recorded, err
}
