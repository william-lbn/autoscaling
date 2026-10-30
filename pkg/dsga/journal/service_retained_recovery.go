package journal

import (
	"context"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// RetainedRunnerCancellation是已经由固定Runner读者认证的原取消见证。
// VM序列与Runner本地序列不同域，绝不按数值互换；本结构不发送取消。
type RetainedRunnerCancellation struct {
	Owner             Owner          `json:"owner"`
	VMSequence        control.Uint64 `json:"vm_sequence"`
	VMResourceVersion string         `json:"vm_resource_version"`
}

// ServiceRetainedRecovery只覆盖早期Prepare：其余资源步骤从未派发。
// Source只有一个完整原永久叶子；不嵌旧Header.Recovery/Model/Lifecycle。
// 原Prepare Unknown、Abort及以Active目标ID发出的原关闭始终逐字保留。
// 当前来源必须由执行器认证；本类型只验证持久合同，不产生网络执行权。
type ServiceRetainedRecovery struct {
	Origin                Owner                       `json:"origin"`
	RecordedSequence      control.Uint64              `json:"recorded_sequence"`
	Incarnation           string                      `json:"incarnation"`
	OperationSHA256       string                      `json:"operation_sha256"`
	ClosedOrigin          Owner                       `json:"closed_origin"`
	Closed                control.AdmissionRequest    `json:"closed"`
	ClosedStartedSequence control.Uint64              `json:"closed_started_sequence"`
	ClosedSequence        control.Uint64              `json:"closed_sequence"`
	Source                RetainedBudgetSource        `json:"source"`
	Anchor                *ClosedBudgetBoundary       `json:"anchor"`
	Fence                 control.FenceResponse       `json:"issuer_fence"`
	Current               control.SharedCostResponse  `json:"current"`
	Root                  budget.RootAggregate        `json:"root"`
	NativeVM              *budget.NativeCapacity      `json:"native_vm"`
	InitialVM             *budget.InitialVMSources    `json:"initial_vm"`
	NativeVMCancellation  *RetainedRunnerCancellation `json:"native_vm_cancellation"`
	TailVM                *budget.NativeCapacity      `json:"tail_vm"`
	TailVMCancellation    *RetainedRunnerCancellation `json:"tail_vm_cancellation"`
	// Completed Prepare必须保存独立完整Node封存，不能借Host404或Planned
	// 放宽原分支。可选字段缺省保留旧JSON形状；旧严格读者拒绝新字段。
	PreparedHostBarrier *budget.HostReservationBarrier `json:"prepared_host_barrier,omitempty"`
	ClosedCommandSHA256 string                         `json:"closed_command_sha256,omitempty"`
}

func earlyRetainedOperation(op Operation) error {
	if op.Validate() != nil || op.Cleanup != nil || len(op.Steps) == 0 {
		return fmt.Errorf("retained recovery requires complete original early operation")
	}
	for _, s := range op.Steps {
		if s.Action == PrepareBudget {
			if s.Phase != Planned && s.Phase != Unconfirmed {
				return fmt.Errorf("retained recovery cannot cover completed or rejected Prepare")
			}
			continue
		}
		if s.Phase != Planned {
			return fmt.Errorf("retained recovery cannot cross dispatched resource action")
		}
	}
	return nil
}

// ValidateCurrent供来源collector在写Proof前及每次重开/完成前复核。
// 不要求metadata已落账；原预算容量、可用量和Root身份固定，floor可随接管
// 增高，门禁revision由各自原WAL核验，不能要求关闭之后仍等于旧开放快照。
func (p ServiceRetainedRecovery) ValidateCurrent(shared control.SharedCostResponse, root budget.RootAggregate, actor Owner, vmMemory, guestUsable control.Uint64) error {
	leaf, err := p.Source.validatedLeaf()
	if err != nil {
		return fmt.Errorf("retained current source changed original capacity, usable memory, owner or domain")
	}
	return p.validateCurrentWithLeaf(shared, root, actor, vmMemory, guestUsable, leaf)
}

// leaf仅来自本次public入口或ValidateFor的完整校验，不跨网络保存。
// Source身份/union/SHA和完整Step已核；此处仍独立核当前PG/Root/实际值。
func (p ServiceRetainedRecovery) validateCurrentWithLeaf(shared control.SharedCostResponse, root budget.RootAggregate, actor Owner, vmMemory, guestUsable control.Uint64, leaf retainedBudgetLeaf) error {
	facts := leaf.facts
	if actor.Validate() != nil || shared.ValidateFor(facts.instance) != nil || shared.Observed.IssuerFloorEpoch != actor.Epoch || root.Validate() != nil || root.Identity.VMUID != facts.instance.VMUID || root.Identity.ComputeInstanceID != facts.instance.ComputeInstanceID || vmMemory != facts.memory || guestUsable < facts.usable || shared.Observed.CapacityGeneration != facts.generation {
		return fmt.Errorf("retained current source changed original capacity, usable memory, owner or domain")
	}

	for _, a := range shared.Observed.Arrays {
		if a.AllocationUnknown {
			return fmt.Errorf("retained current PG allocation remains unknown")
		}
	}
	if shared.Observed.Operation != nil && shared.Observed.Operation.Phase != control.Committed && shared.Observed.Operation.Phase != control.Denied {
		return fmt.Errorf("retained current PG head remains in flight")
	}
	if i := p.Source.Initial; i != nil {
		return i.MatchCurrent(shared, root, actor.Epoch, vmMemory, guestUsable)
	}
	if c := p.Source.Capacity; c != nil {
		digest, err := budget.RootDigest(root)
		if err != nil || digest != c.RootSHA256 || root.Bound != c.PGAggregate {
			return fmt.Errorf("retained actual source changed original complete root")
		}
		return c.ValidateCurrent(shared, actor.Epoch, vmMemory)
	}
	c := leaf.finalization
	if c == nil || c.Finalized.Aggregate == nil || *c.Finalized.Aggregate != root.Bound {
		return fmt.Errorf("retained finalized source changed complete original root")
	}
	f := c.Finalized
	return f.ValidateFor(f.Request, shared, actor.Epoch, f.RunnerPodName, f.RunnerRestartCount, uint64(vmMemory))
}

func (p ServiceRetainedRecovery) validateCancellation(vm *budget.NativeCapacity, w *RetainedRunnerCancellation) error {
	if vm == nil {
		if w != nil {
			return fmt.Errorf("retained cancellation has no original VM source")
		}
		return nil
	}
	if vm.Validate() != nil || vm.Cancelled != (w != nil) {
		return fmt.Errorf("retained native completion/cancellation discriminator changed")
	}
	if w != nil && (w.Owner.Validate() != nil || !recoveryOwnerAt(p.Origin, w.Owner, p.Origin) || w.Owner.Epoch < vm.Request.Key.IssuerEpoch || w.VMSequence == 0 || w.VMSequence >= p.ClosedStartedSequence || control.ValidateID(w.VMResourceVersion, "retained original VM resource version") != nil) {
		return fmt.Errorf("retained native cancellation lost original VM witness")
	}
	return nil
}

func (p ServiceRetainedRecovery) validateVM(op Operation, leaf retainedBudgetLeaf) error {
	facts := leaf.facts
	if (p.NativeVM == nil) == (p.InitialVM == nil) || p.validateCancellation(p.NativeVM, p.NativeVMCancellation) != nil || p.validateCancellation(p.TailVM, p.TailVMCancellation) != nil {
		return fmt.Errorf("retained recovery lacks exclusive complete original VM source")
	}
	if v := p.InitialVM; v != nil {
		if p.TailVM != nil {
			return fmt.Errorf("retained initial VM cannot borrow cancellation tail")
		}
		if p.Source.Initial != nil {
			if v.MatchInitial(*p.Source.Initial) != nil {
				return fmt.Errorf("retained recovery changed initial VM source")
			}
		} else if c := p.Source.Capacity; c == nil || c.InitialVM == nil || c.InitialOutside == nil || *c.InitialVM != *v || v.MatchInitial(*c.InitialOutside) != nil {
			return fmt.Errorf("retained recovery invented initial VM source")
		}
		return nil
	}
	v := p.NativeVM
	if v.Request.Key.Instance != facts.instance || compareKeys(v.Request.Key, op.Request.Key) >= 0 || v.MemoryBytes() != facts.memory {
		return fmt.Errorf("retained native VM differs from original prior capacity")
	}
	if i := p.Source.Initial; i != nil {
		if v.Sources.BirthClaim != i.RunnerBirth || p.TailVM != nil {
			return fmt.Errorf("retained initial command changed original boot source")
		}
	} else if c := p.Source.Capacity; c != nil {
		if c.InitialVM != nil || *v != c.VM || p.TailVM != nil {
			return fmt.Errorf("retained actual VM replaced original native source")
		}
	} else {
		f := leaf.finalization
		if f == nil || f.Finalized.Sources == nil || f.Finalized.Sources.Runner == nil || v.Cancelled || v.Request != f.Finalized.Request || v.Sources != *f.Finalized.Sources.Runner || v.PreparedSHA256 != f.PreparedSHA256 || v.HostClaim != f.Finalized.Sources.HostClaim || v.Request.Kind == control.Shrink && v.GuestSHA256 != f.GuestSHA256 || v.Request.Kind == control.Grow && v.GuestSHA256 != "" {
			return fmt.Errorf("retained finalization changed full original VM source")
		}
	}
	if t := p.TailVM; t != nil {
		w := p.TailVMCancellation
		if p.Source.Finalization == nil || p.Anchor == nil || !t.Cancelled || t.Request.Key.Instance != facts.instance || compareKeys(v.Request.Key, t.Request.Key) >= 0 || compareKeys(t.Request.Key, p.Anchor.ThroughKey) > 0 || compareKeys(t.Request.Key, p.Anchor.ThroughKey) == 0 && t.Request.Key != p.Anchor.ThroughKey || t.BeforeBytes != facts.memory || t.Sources.BirthClaim != v.Sources.BirthClaim || t.Sources.StateClaim != v.Sources.StateClaim || w == nil || w.VMSequence <= p.Source.RecordedSequence || w.VMSequence >= p.Anchor.ThroughSequence {
			return fmt.Errorf("retained VM cancellation tail lies outside exact original closed chain")
		}
	}
	return nil
}

// ValidateFor核保存后的完整原操作/关闭/来源，不依赖原终态缓存或当前网络。
func (p ServiceRetainedRecovery) ValidateFor(op Operation) error {
	hash, err := operationClosureDigest(&op)
	leaf, factErr := p.Source.validatedLeaf()
	facts := leaf.facts
	if err != nil || factErr != nil || p.validatePreparedHost(op, leaf) != nil || hash != p.OperationSHA256 || !validIncarnation(p.Incarnation) || !recoveryOwnerAt(p.Origin, op.Origin, p.Origin) || !recoveryOwnerAt(p.Origin, p.ClosedOrigin, p.Origin) || p.Closed.Validate() != nil || p.Closed.Open || p.Closed.Instance != op.Request.Key.Instance || facts.instance != op.Request.Key.Instance || p.Closed.BudgetCommitmentID != op.Request.BudgetCommitmentID || p.Closed.IssuerEpoch != p.ClosedOrigin.Epoch || p.Closed.ExpectedCapacityGeneration != facts.generation || op.Request.ExpectedCapacityGeneration != facts.generation || p.Source.RecordedSequence >= p.ClosedStartedSequence || p.ClosedStartedSequence == 0 || p.ClosedSequence <= p.ClosedStartedSequence || p.RecordedSequence <= p.ClosedSequence {
		return fmt.Errorf("retained recovery changed original early operation, closed selection or prior budget")
	}
	if p.Fence.Disposition != control.Fenced || p.Fence.ValidateFor(control.FenceRequest{ProtocolVersion: 1, Instance: facts.instance, IssuerEpoch: p.Origin.Epoch}, facts.instance) != nil || !control.SameResizeRecoveryBoundary(p.Fence.Observed, p.Current.Observed) || p.Current.Observed.AdmissionOpen || p.Current.Observed.AdmissionRevision != p.Closed.Revision || p.validateCurrentWithLeaf(p.Current, p.Root, p.Origin, facts.memory, facts.usable, leaf) != nil {
		return fmt.Errorf("retained recovery lacks stable fenced complete current sources")
	}
	if head := p.Current.Observed.Operation; head != nil && compareKeys(head.Key, op.Request.Key) >= 0 {
		return fmt.Errorf("retained recovery observed native PG at undispatched active intent")
	}
	if p.Source.Initial != nil {
		if p.Anchor != nil || p.Source.Initial.Incarnation != p.Incarnation {
			return fmt.Errorf("initial retained recovery invented resize closure anchor")
		}
	} else {
		b := p.Anchor
		var key control.Key
		if p.Source.Capacity != nil {
			key = p.Source.Capacity.Request.Key
		} else {
			key = leaf.finalization.Finalized.Request.Key
		}
		if b == nil || b.ProtocolVersion != 1 || b.BudgetKey != key || b.FinalizationSHA256 != p.Source.SourceSHA256 || b.RecordedSequence != p.Source.RecordedSequence || b.SucceededSequence <= b.RecordedSequence || b.ThroughSequence < b.SucceededSequence || b.ThroughSequence >= p.ClosedStartedSequence || b.ThroughKey.Instance != facts.instance || compareKeys(b.ThroughKey, b.BudgetKey) < 0 || compareKeys(b.ThroughKey, op.Request.Key) >= 0 || compareKeys(b.ThroughKey, b.BudgetKey) == 0 && (b.ThroughKey != b.BudgetKey || b.ThroughSequence != b.SucceededSequence) {
			return fmt.Errorf("retained recovery lost exact original closed budget anchor")
		}
	}
	return p.validateVM(op, leaf)
}

func cloneServiceRetainedRecovery(p *ServiceRetainedRecovery) *ServiceRetainedRecovery {
	if p == nil {
		return nil
	}
	n := *p
	if p.PreparedHostBarrier != nil {
		b := p.PreparedHostBarrier.Clone()
		n.PreparedHostBarrier = &b
	}
	n.Source = *cloneRetainedBudgetSource(&p.Source)
	n.Current.Observed = cloneActualSnapshot(p.Current.Observed)
	n.Fence.Observed = cloneActualSnapshot(p.Fence.Observed)
	n.Root = cloneActualRoot(p.Root)
	if p.Anchor != nil {
		b := *p.Anchor
		n.Anchor = &b
	}
	if p.NativeVM != nil {
		v := *p.NativeVM
		n.NativeVM = &v
	}
	if p.InitialVM != nil {
		v := *p.InitialVM
		n.InitialVM = &v
	}
	if p.TailVM != nil {
		v := *p.TailVM
		n.TailVM = &v
	}
	if p.NativeVMCancellation != nil {
		v := *p.NativeVMCancellation
		n.NativeVMCancellation = &v
	}
	if p.TailVMCancellation != nil {
		v := *p.TailVMCancellation
		n.TailVMCancellation = &v
	}
	return &n
}

func (m Model) validateServiceRetainedRecovery() error {
	c := m.ServiceClosure
	if c == nil || c.RetainedRecovery == nil {
		return nil
	}
	p := c.RetainedRecovery
	origin, _, closed, _ := c.Selection()
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	anchor := m.BudgetBoundary
	if m.Active != nil && m.Active.PriorBudgetBoundary != nil {
		if anchor != nil {
			return fmt.Errorf("retained recovery mixed original anchors")
		}
		anchor = m.Active.PriorBudgetBoundary
	}
	if m.serviceClosureVersion() != 16 || c.Pending() || m.Active == nil || m.ResourceIdentity == nil || c.ActualRecovery != nil || c.Finalization != nil || c.CleanupRecovery != nil || c.ResourceRecovery != nil || c.PGRecovery != nil || c.PGRejection != nil || c.VMRecovery != nil || p.RecordedSequence > m.Sequence || p.Incarnation != m.Incarnation || p.Root.Identity != *m.ResourceIdentity || !recoveryOwnerAt(m.Owner, p.Origin, m.Owner) || p.ClosedOrigin != origin || p.Closed != closed || p.ClosedStartedSequence != c.StartedSequence || p.ClosedSequence != at || p.OperationSHA256 != c.OperationSHA256 || m.checkRetainedSource(p.Source) != nil || !reflect.DeepEqual(p.Anchor, anchor) {
		return fmt.Errorf("retained recovery replaced original confirmed closure or current permanent source")
	}
	if p.PreparedHostBarrier != nil {
		_, _, _, selected := c.Selection()
		if p.ClosedCommandSHA256 != selected || m.checkPreparedHostBarrier(*p.PreparedHostBarrier) != nil {
			return fmt.Errorf("retained Host barrier changed original selected closure")
		}
	}
	return p.ValidateFor(*m.Active)
}

// Record只落独立证明，metadata来自原Model与next CAS。外部可填资源来源，
// 不能自报另一个owner、关闭或未来序列；已保存Proof在接管后永不重写。
func (m Model) RecordServiceRetainedRecovery(actor Owner, selector string, input ServiceRetainedRecovery) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Pending() || c.RetainedRecovery != nil || c.Rearm != nil || c.Reopen != nil || m.Active == nil || m.ResourceIdentity == nil {
		return m, fmt.Errorf("retained recovery requires original confirmed early closure")
	}
	origin, _, closed, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("retained recovery selected another closed command")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	p := cloneServiceRetainedRecovery(&input)
	// 未落账输入必须没有自报metadata，避免静默接受相互矛盾的原命令引用。
	var noOwner Owner
	var noAdmission control.AdmissionRequest
	if p.RecordedSequence != 0 || p.Origin != noOwner || p.Incarnation != "" || p.OperationSHA256 != "" || p.ClosedSequence != 0 || p.ClosedStartedSequence != 0 || p.ClosedOrigin != noOwner || p.Closed != noAdmission || p.ClosedCommandSHA256 != "" {
		return m, fmt.Errorf("retained recovery metadata must come from original WAL")
	}
	p.Origin, p.RecordedSequence, p.Incarnation, p.OperationSHA256 = actor, next.Sequence, m.Incarnation, c.OperationSHA256
	p.ClosedOrigin, p.Closed, p.ClosedStartedSequence, p.ClosedSequence = origin, closed, c.StartedSequence, c.RecordedSequence
	if c.Continuation != nil {
		p.ClosedSequence = c.Continuation.RecordedSequence
	}
	if p.PreparedHostBarrier != nil {
		p.ClosedCommandSHA256 = selected
	}
	next.ServiceClosure.RetainedRecovery = p
	return next, next.Validate()
}

func (s *Store) RecordServiceRetainedRecovery(ctx context.Context, snapshot *Snapshot, actor Owner, selector string, p ServiceRetainedRecovery) (*Snapshot, error) {
	if err := s.checked(snapshot); err != nil {
		return nil, err
	}
	if !snapshot.Dispatchable() {
		return nil, fmt.Errorf("retained recovery requires original running VM")
	}
	next, err := snapshot.model.RecordServiceRetainedRecovery(actor, selector, p)
	if err != nil {
		return nil, err
	}
	return s.apply(ctx, snapshot, next)
}

// EffectiveServiceBudgetID只用于独立恢复的新开放；原关闭继续由
// ServiceBudgetID核Active目标，不能改写已派发原关闭或其selector。
func (m Model) EffectiveServiceBudgetID() (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	return m.effectiveServiceBudgetID()
}

func (m Model) effectiveServiceBudgetID() (string, error) {
	if m.ServiceClosure != nil && m.ServiceClosure.RetainedRecovery != nil {
		return m.ServiceClosure.RetainedRecovery.Source.BudgetCommitmentID()
	}
	if m.ServiceClosure == nil {
		return "", fmt.Errorf("no original closure")
	}
	_, _, q, _ := m.ServiceClosure.Selection()
	return q.BudgetCommitmentID, nil
}

func (p *ServiceRetainedRecovery) UnmarshalJSON(raw []byte) error {
	type plain ServiceRetainedRecovery
	var v plain
	if err := control.UnmarshalObjectBounded(raw, &v, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*p = ServiceRetainedRecovery(v)
	return nil
}

func (p *RetainedRunnerCancellation) UnmarshalJSON(raw []byte) error {
	type plain RetainedRunnerCancellation
	var v plain
	if err := control.UnmarshalObject(raw, &v); err != nil {
		return err
	}
	*p = RetainedRunnerCancellation(v)
	return nil
}
