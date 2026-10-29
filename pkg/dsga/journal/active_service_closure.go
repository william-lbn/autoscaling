package journal

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// activeClosureBoundary只选择尚未进入最终开放的原操作，不将合法中间态
// 投影成已完成预算。完整Active（包括取消标记及全部未知）由关闭摘要固定。
// Cleanup可能另有开放，须由其独立恢复合同处理，此处明确拒绝而非丢弃。
// 返回的Quiesce是唯一可能在途的准入写；必须一起封住，不能清SDK栅栏绕过它。
func (model Model) activeClosureBoundary() (*control.AdmissionRequest, error) {
	op := model.Active
	if op == nil || op.Cleanup != nil || len(op.Steps) == 0 {
		return nil, fmt.Errorf("active closure lacks original resource operation")
	}
	var original *control.AdmissionRequest
	for _, step := range op.Steps {
		switch step.Action {
		case ResumeAdmission, FinalizeBudget:
			if step.Phase != Planned {
				return nil, fmt.Errorf("active closure cannot adopt final settlement or open")
			}
		case ResizePG:
			if step.Phase != Planned {
				// PG已派发仍须逐字保留原命令；不能按当前容量收养另一个
				// Resize。关闭不消费其回执，不把未知改成Completed/NoEffect。
				request, err := control.DecodeRequest(step.Command)
				if err != nil || request != op.Request || step.DispatchedBy == nil || step.TargetID != op.Request.Key.Instance.ComputeInstanceID {
					return nil, fmt.Errorf("active closure lost original dispatched PG command")
				}
			}
		case Quiesce:
			if step.Phase == Unconfirmed {
				var command budget.QuiesceCommand
				if json.Unmarshal(step.Command, &command) != nil || command.Request != op.Request || command.Close.Open || command.Close.Instance != op.Request.Key.Instance || step.DispatchedBy == nil || command.Close.IssuerEpoch != step.DispatchedBy.Epoch {
					return nil, fmt.Errorf("active closure lost original quiesce unknown")
				}
				saved := command.Close
				original = &saved
			}
		}
	}
	return original, nil
}

func (model Model) activePGDispatched() bool {
	if model.Active != nil {
		for _, step := range model.Active.Steps {
			if step.Action == ResizePG {
				return step.Phase != Planned
			}
		}
	}
	return false
}

// 单个原Resize只能保持原代次或提交后前进一次。不使用expected+1，避免
// u64最大值回绕；PG未派发时严格固定原代次。该集合不是完成/释放证明。
func (model Model) activeServiceCapacityAllowed(generation control.Uint64) bool {
	if model.Active == nil {
		return false
	}
	expected := model.Active.Request.ExpectedCapacityGeneration
	return generation == expected || model.activePGDispatched() && generation > expected && generation-expected == 1
}

// ActiveServiceCapacityAllowed供生产执行器在原生fence后的观察上核定有限
// 容量集合；必须先通过完整持久模型校验，不能拿SDK当前容量替换原Request。
func (model Model) ActiveServiceCapacityAllowed(generation control.Uint64) bool {
	return model.Validate() == nil && model.activeServiceCapacityAllowed(generation)
}

func (model Model) ActiveAdmissionForClosure() (*control.AdmissionRequest, error) {
	if err := model.Validate(); err != nil {
		return nil, err
	}
	original, err := model.activeClosureBoundary()
	return original, err
}

func (model Model) validateActiveServiceClosure() error {
	c := model.ServiceClosure
	original, err := model.activeClosureBoundary()
	digest, hashErr := operationClosureDigest(model.Active)
	reason, version := "active_source_unconfirmed", uint32(16)
	if model.activePGDispatched() {
		reason, version = "active_pg_source_unconfirmed", 17
	}
	if err != nil || hashErr != nil || model.ProtocolVersion != version || c.Reason != reason || c.OperationSHA256 != digest || (original == nil) != (c.Original == nil) || original != nil && *original != *c.Original {
		return fmt.Errorf("active closure changed original operation or admission unknown")
	}
	if !model.activeServiceCapacityAllowed(c.Request.ExpectedCapacityGeneration) {
		return fmt.Errorf("active closure capacity lies outside original resize")
	}
	if c.Receipt != nil && !model.activeServiceCapacityAllowed(c.Receipt.Observed.CapacityGeneration) || c.ClosureReceipt != nil && !model.activeServiceCapacityAllowed(c.ClosureReceipt.ClosedBy.Observed.CapacityGeneration) {
		return fmt.Errorf("active closure proof capacity lies outside original resize")
	}
	if original == nil {
		if c.ClosureReceipt != nil {
			return fmt.Errorf("active closure invented prior admission")
		}
		return nil
	}
	if c.Receipt != nil || c.Request.ClosesOver(*original) != nil {
		return fmt.Errorf("active closure did not fence original quiesce")
	}
	if c.Phase == Settled {
		selection := control.AdmissionCloseRequest{ProtocolVersion: 1, Original: *original, Close: c.Request}
		if c.ClosureReceipt == nil || c.ClosureReceipt.ValidateFor(selection, *model.PGInstance, c.Query) != nil {
			return fmt.Errorf("active closure lacks complete native proof")
		}
	}
	return nil
}

// 仅生成写前模型；没有Source健康或资源完成的含义。原预算/全部Active逐字
// 保留，原操作不能普通Record/Abort/Finish；更高owner仍复用完整关闭接续。
func (model Model) StartActiveServiceClosure(actor Owner, request control.AdmissionRequest) (Model, error) {
	if err := model.Validate(); err != nil {
		return model, err
	}
	if model.ServiceClosure != nil {
		return model, fmt.Errorf("active closure already retained")
	}
	original, err := model.activeClosureBoundary()
	if err != nil {
		return model, err
	}
	next, err := model.next(actor)
	if err != nil {
		return model, err
	}
	digest, err := request.ContextSHA256()
	if err != nil {
		return model, err
	}
	opDigest, err := operationClosureDigest(model.Active)
	if err != nil {
		return model, err
	}
	next.ProtocolVersion = 16
	next.ServiceClosure = &ServiceClosure{Origin: actor, StartedSequence: next.Sequence, Reason: "active_source_unconfirmed", Request: request, CommandSHA256: digest, Phase: Unconfirmed, RecordedSequence: 0, Query: false, Receipt: nil, Continuation: nil, Original: original, ClosureReceipt: nil, OperationSHA256: opDigest}
	if model.activePGDispatched() {
		next.ProtocolVersion, next.ServiceClosure.Reason = 17, "active_pg_source_unconfirmed"
	}
	return next, next.Validate()
}

// VM UID/RV写前CAS失败不返回派发票据。原WAL已落盘但回复丢失时，下轮只查
// 原关闭；不能按最新资源事实重新选择容量/revision以绕过未知。
func (store *Store) StartActiveServiceClosure(ctx context.Context, s *Snapshot, actor Owner, request control.AdmissionRequest) (*Snapshot, *ServiceClosure, error) {
	if err := store.checked(s); err != nil {
		return nil, nil, err
	}
	if !s.Dispatchable() {
		return nil, nil, fmt.Errorf("active closure requires original running VM")
	}
	next, err := s.model.StartActiveServiceClosure(actor, request)
	if err != nil {
		return nil, nil, err
	}
	result, err := store.apply(ctx, s, next)
	if err != nil {
		return nil, nil, err
	}
	return result, result.State().ServiceClosure, nil
}

func (store *Store) RecordActiveServiceClosure(ctx context.Context, s *Snapshot, actor Owner, fact control.AdmissionClosure, query bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	reason := "active_source_unconfirmed"
	if s.model.ProtocolVersion == 17 {
		reason = "active_pg_source_unconfirmed"
	}
	next, err := s.model.recordOriginalServiceClosure(actor, fact, query, reason)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}
