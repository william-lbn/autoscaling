package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// ServiceActualRecovery是原资源在途操作的独立完整实际预算，不是Finalize或
// Cleanup回执。原Active、Unknown、Abort、原关闭及旧永久头保持；只有全部
// 当前来源认证、独立开放完成后的同一CAS，才安装本预算并退休原操作。
// 保存原Guest/VM封存证据，防止历史淘汰后丢掉旧命令不能再执行的依据。
type ServiceActualRecovery struct {
	Origin           Owner                      `json:"origin"`
	RecordedSequence control.Uint64             `json:"recorded_sequence"`
	Incarnation      string                     `json:"incarnation"`
	OperationSHA256  string                     `json:"operation_sha256"`
	ClosedOrigin     Owner                      `json:"closed_origin"`
	Closed           control.AdmissionRequest   `json:"closed"`
	ClosedSequence   control.Uint64             `json:"closed_sequence"`
	PreparedSHA256   string                     `json:"prepared_sha256"`
	Capacity         budget.ReconciledCapacity  `json:"capacity"`
	CapacitySHA256   string                     `json:"capacity_sha256"`
	Current          control.SharedCostResponse `json:"current"`
	Root             budget.RootAggregate       `json:"root"`
	Guest            *ServiceResourceRecovery   `json:"guest,omitempty"`
	VM               *ServiceVMRecovery         `json:"vm,omitempty"`
	PGRejection      *ServicePGRejection        `json:"pg_rejection,omitempty"`
}

// RecoveredResourceOrigin保留有界原操作及独立实际预算；没有递归旧Model/
// Lifecycle。原资源Unknown即使历史缓存淘汰仍在这里，不投影成业务目标成功。
type RecoveredResourceOrigin struct {
	Original Operation             `json:"original"`
	Proof    ServiceActualRecovery `json:"proof"`
}

func originalServicePrepared(op *Operation, id *resourceguard.Identity) (budget.Prepared, string, error) {
	var p budget.Prepared
	if op == nil || id == nil {
		return p, "", fmt.Errorf("actual service recovery lacks original resource operation")
	}
	for _, s := range op.Steps {
		if s.Action == PrepareBudget {
			if s.Phase != Settled || s.Observation == nil || s.Observation.Result != Completed || s.TargetID != op.Request.Key.Instance.ComputeInstanceID || json.Unmarshal(s.Observation.Data, &p) != nil || p.ValidateFor(op.Request, *id) != nil {
				return p, "", fmt.Errorf("actual service recovery lacks complete original Prepare")
			}
			return p, rawDigest(s.Observation.Data), nil
		}
	}
	return p, "", fmt.Errorf("actual service recovery has no original Prepare")
}

// 返回原Prepared副本，不把尚未完成的Prepare或目标配置重建为预算。
func (m Model) OriginalServicePrepared() (budget.Prepared, string, error) {
	return originalServicePrepared(m.Active, m.ResourceIdentity)
}

// basis只供永久Proof结构校验，不是可派发Model，不执行Validate/网络。
// 最近关闭逐字保存；当前实际容量可以是原PG动作下一代，但不能改旧关闭。
func (p ServiceActualRecovery) basis(op Operation) Model {
	id, instance, key := p.Root.Identity, op.Request.Key.Instance, op.Request.Key
	var m Model
	m.ProtocolVersion, m.VMUID, m.Incarnation, m.Sequence = 16, instance.VMUID, p.Incarnation, p.RecordedSequence
	m.Owner, m.Active, m.HighWater, m.ResourceIdentity, m.PGInstance = p.Origin, &op, &key, &id, &instance
	if m.activePGDispatched() {
		m.ProtocolVersion = 17
	}
	var closed ServiceClosure
	closed.Origin, closed.Request, closed.Phase, closed.RecordedSequence = p.ClosedOrigin, p.Closed, Settled, p.ClosedSequence
	closed.OperationSHA256, closed.ResourceRecovery, closed.VMRecovery = p.OperationSHA256, p.Guest, p.VM
	m.ServiceClosure = &closed
	return m
}

func (p ServiceActualRecovery) validate(op Operation) error {
	prepared, preparedHash, err := originalServicePrepared(&op, &p.Root.Identity)
	opHash, hashErr := operationClosureDigest(&op)
	raw, marshalErr := json.Marshal(p.Capacity)
	rootHash, rootErr := budget.RootDigest(p.Root)
	if err != nil || op.Validate() != nil || op.Cleanup != nil || hashErr != nil || opHash != p.OperationSHA256 || p.PreparedSHA256 != preparedHash || marshalErr != nil || p.CapacitySHA256 != rawDigest(raw) || !validIncarnation(p.Incarnation) || !recoveryOwnerAt(p.Origin, op.Origin, p.Origin) || !recoveryOwnerAt(p.ClosedOrigin, op.Origin, p.Origin) || p.RecordedSequence <= p.ClosedSequence || p.ClosedSequence == 0 || p.Closed.Validate() != nil || p.Closed.Open || p.Closed.Instance != op.Request.Key.Instance || p.Closed.BudgetCommitmentID != op.Request.BudgetCommitmentID || p.Closed.IssuerEpoch != p.ClosedOrigin.Epoch || p.Root.Validate() != nil || rootErr != nil || p.Capacity.ProtocolVersion != 4 || p.Capacity.Validate() != nil || p.Capacity.Request != op.Request || p.Capacity.IssuerEpoch != p.Origin.Epoch || p.Capacity.VMBeforeMemoryBytes != prepared.VMBeforeMemoryBytes || p.Capacity.RunnerPodName != prepared.RunnerPodName || p.Capacity.RunnerRestartCount != prepared.RunnerRestartCount || p.Capacity.RootSHA256 != rootHash || p.Capacity.PGAggregate != p.Root.Bound || p.Current.Observed.AdmissionOpen || p.Current.Observed.AdmissionRevision != p.Closed.Revision || p.Capacity.ValidateCurrent(p.Current, p.Origin.Epoch, p.Capacity.VMMemoryBytes) != nil {
		return fmt.Errorf("independent actual service budget lost original operation, closure or complete current resources")
	}
	base := p.basis(op)
	if p.Capacity.CapacityGeneration < p.Closed.ExpectedCapacityGeneration {
		return fmt.Errorf("actual service capacity regressed confirmed closed generation")
	}
	if i := p.Capacity.InitialOutside; i != nil && (i.Incarnation != p.Incarnation || i.PGAggregate.Identity != p.Root.Identity) {
		return fmt.Errorf("actual service initial outside belongs to another original boot or resource domain")
	}
	systemLimit := p.Root.Bootstrap.Limits.SystemMemoryMaxBytes
	if p.Root.Held != nil {
		systemLimit = p.Root.Held.SystemMemoryMaxBytes
	}
	if systemLimit == 0 || systemLimit > p.Capacity.SystemOutsidePG.UpperBytes {
		return fmt.Errorf("actual service system hard limit exceeds retained outside commitment")
	}
	if !base.activeServiceCapacityAllowed(p.Capacity.CapacityGeneration) || !base.activeServiceCapacityAllowed(p.Closed.ExpectedCapacityGeneration) {
		return fmt.Errorf("actual service capacity lies outside original PG action")
	}
	var pg, vm, guest *Step
	for i := range op.Steps {
		s := &op.Steps[i]
		switch s.Action {
		case FinalizeBudget, ResumeAdmission:
			if s.Phase != Planned {
				return fmt.Errorf("actual service budget cannot replace dispatched final settlement or admission")
			}
		case ResizePG:
			pg = s
		case ResizeVM:
			vm = s
		case GuestReady:
			guest = s
		}
	}
	if pg == nil || vm == nil || guest == nil {
		return fmt.Errorf("actual service budget lost original domain plan")
	}
	if pg.Phase == Planned {
		if p.Capacity.RecoveryPG != nil || p.Capacity.UnchangedPG == nil || !reflect.DeepEqual(*p.Capacity.UnchangedPG, prepared.Shared) {
			return fmt.Errorf("undispatched PG actual budget changed original stable baseline")
		}
	} else if p.Capacity.RecoveryPG == nil || p.Capacity.UnchangedPG != nil {
		return fmt.Errorf("dispatched PG actual budget omitted native terminal or isolation proof")
	}
	if pg.Phase != Planned {
		_, original, nativeErr := op.OriginalServicePG()
		_, rejectionErr := op.OriginalPGRejection()
		if nativeErr != nil && rejectionErr != nil {
			return fmt.Errorf("actual budget lost full original dispatched PG command or audit")
		}
		if original != nil && p.Capacity.RecoveryPG.Status.Receipt.Result != nil && !control.SameResizeRecoveryTerminal(*original.Receipt.Result, *p.Capacity.RecoveryPG.Status.Receipt.Result) {
			return fmt.Errorf("actual budget replaced immutable original native PG result")
		}
		if pg.Phase == Settled && pg.Observation != nil {
			if pg.Observation.Result == Completed && (p.Capacity.CapacityGeneration <= op.Request.ExpectedCapacityGeneration || p.Capacity.CapacityGeneration-op.Request.ExpectedCapacityGeneration != 1 || p.Capacity.Buffers != op.Request.TargetBuffers) || pg.Observation.Result == Rejected && p.Capacity.CapacityGeneration != op.Request.ExpectedCapacityGeneration {
				return fmt.Errorf("actual capacity contradicts immutable original PG terminal")
			}
		}
	}
	if _, rejected := op.OriginalPGRejection(); rejected == nil && p.PGRejection == nil {
		return fmt.Errorf("actual budget omitted original explicit rejection barrier")
	}
	if p.PGRejection != nil {
		r := p.PGRejection
		if r.validate(op) != nil || r.RecordedSequence >= p.RecordedSequence || r.Closed != p.Closed || r.ClosedSequence != p.ClosedSequence || !recoveryOwnerAt(p.Origin, r.Origin, p.Origin) || p.Capacity.RecoveryPG == nil || p.Capacity.RecoveryPG.Status.Receipt.Result != nil || !control.SameResizeRecoveryBoundary(r.Status.Observed, p.Capacity.RecoveryPG.Status.Observed) {
			return fmt.Errorf("actual budget changed original independent PG rejection barrier")
		}
	}
	if vm.Phase == Planned {
		if p.VM != nil || p.Capacity.VMMemoryBytes != prepared.VMBeforeMemoryBytes || p.Capacity.InitialVM == nil && p.Capacity.VM.Request.Key == op.Request.Key {
			return fmt.Errorf("undispatched VM actual budget borrowed a new native action")
		}
	} else if vm.Phase == Unconfirmed || vm.Phase == Settled && vm.Observation != nil && vm.Observation.Result == Completed {
		if p.VM == nil || p.VM.Phase != Settled || p.VM.RecordedSequence >= p.RecordedSequence || p.VM.Capacity == nil || p.Capacity.InitialVM != nil || *p.VM.Capacity != p.Capacity.VM || base.validateServiceVMRecovery() != nil {
			return fmt.Errorf("actual budget cannot cross unresolved original VM effect")
		}
	} else if vm.Phase != Settled || vm.Observation == nil || vm.Observation.Result != NoEffect || p.Capacity.InitialVM != nil || !p.Capacity.VM.Cancelled || p.Capacity.VM.Request != op.Request || p.Capacity.VM.Sources.JournalSHA256 != vm.CommandSHA256 {
		return fmt.Errorf("actual budget changed original sealed VM cancellation")
	} else {
		// 原NoEffect已正式落账也不能丢掉其原来源，或用同目标的新回执
		// 替换它。完整不可变Receipt的认证仍由固定Runner读者承担。
		var audit struct {
			ProtocolVersion uint32               `json:"protocol_version"`
			Sources         budget.RunnerSources `json:"sources"`
		}
		var command serviceVMProjection
		n := p.Capacity.VM
		if json.Unmarshal(vm.Observation.Data, &audit) != nil || audit.ProtocolVersion != 1 || audit.Sources != n.Sources || control.UnmarshalObject(vm.Command, &command) != nil {
			return fmt.Errorf("actual budget lost original VM cancellation source audit")
		}
		raw, err := json.Marshal(command)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(append([]byte("neon-dsga-runner-memory-v1\x00"), raw...))
		if n.Request != command.Request || n.BeforeBytes != command.BeforeBytes || n.PreparedSHA256 != command.PreparedSHA256 || n.GuestSHA256 != command.GuestSHA256 || n.HostClaim != command.HostClaim || n.Sources.BirthClaim != command.BindingClaim || n.Sources.CommandSHA256 != hex.EncodeToString(digest[:]) {
			return fmt.Errorf("actual budget VM cancellation changed original native command")
		}
	}
	if guest.Phase == Unconfirmed {
		if p.Guest == nil || p.Guest.Guest == nil || p.Guest.Guest.Phase != Settled || p.Guest.Guest.RecordedSequence >= p.RecordedSequence || base.validateServiceResourceRecovery() != nil {
			return fmt.Errorf("actual budget cannot retire unresolved original Guest effect")
		}
		root, err := budget.RootAggregateFromView(*p.Guest.Guest.State)
		if err != nil || !reflect.DeepEqual(root, p.Root) {
			return fmt.Errorf("actual budget Guest barrier differs from current root constraints")
		}
	} else if p.Guest != nil {
		return fmt.Errorf("actual budget invented independent Guest selection")
	}
	if guest.Phase == Settled && guest.Observation != nil {
		var root budget.RootAggregate
		switch guest.Observation.Result {
		case Completed:
			var audit struct {
				PGAggregate budget.RootAggregate `json:"pg_aggregate"`
			}
			if json.Unmarshal(guest.Observation.Data, &audit) != nil {
				return fmt.Errorf("actual budget lost original completed Guest source")
			}
			root = audit.PGAggregate
			var command resourceguard.Command
			l := prepared.Limits
			if json.Unmarshal(guest.Command, &command) != nil || command.Validate() != nil || guest.DispatchedBy == nil || command.IssuerEpoch != guest.DispatchedBy.Epoch || command.Identity != p.Root.Identity || command.BudgetCommitmentID != op.Request.BudgetCommitmentID || p.Root.Held == nil || !reflect.DeepEqual(*p.Root.Held, command) || command.MemoryMaxBytes != l.MemoryMaxBytes || command.PidsMaxTasks != l.PidsMaxTasks || command.AddressSpaceMaxBytes != l.AddressSpaceMaxBytes || command.SystemMemoryMaxBytes != l.SystemMemoryMaxBytes || command.OutsideUpperBytes != l.OutsideUpperBytes || !reflect.DeepEqual(command.HugePools, l.HugePools) {
				return fmt.Errorf("actual budget lost complete original applied Guest constraints")
			}
		case NoEffect:
			var audit struct {
				Cancellation resourceguard.Cancellation `json:"cancellation"`
			}
			if json.Unmarshal(guest.Observation.Data, &audit) != nil {
				return fmt.Errorf("actual budget lost original Guest cancellation")
			}
			var command resourceguard.Command
			if json.Unmarshal(guest.Command, &command) != nil || audit.Cancellation.State.IssuerFloorEpoch > p.Origin.Epoch || audit.Cancellation.ValidateFor(command, audit.Cancellation.State.IssuerFloorEpoch) != nil {
				return fmt.Errorf("actual budget changed original Guest cancellation barrier")
			}
			root, err = budget.RootAggregateFromView(audit.Cancellation.State)
		default:
			return fmt.Errorf("actual budget cannot adopt rejected Guest without a sealed source")
		}
		if err != nil || !reflect.DeepEqual(root, p.Root) {
			return fmt.Errorf("actual budget current root differs from original settled Guest")
		}
	}
	return nil
}

// 复用既有完整Snapshot深拷贝，不重新校验或把错误夹具归零。
func cloneActualSnapshot(snapshot control.Snapshot) control.Snapshot {
	var temporary control.AdmissionResponse
	temporary.Observed = snapshot
	return cloneAdmissionResponse(&temporary).Observed
}

func cloneActualRoot(r budget.RootAggregate) budget.RootAggregate {
	r.Bootstrap.Limits.HugePools = append(r.Bootstrap.Limits.HugePools[:0:0], r.Bootstrap.Limits.HugePools...)
	if r.Held != nil {
		held := *r.Held
		held.HugePools = append(held.HugePools[:0:0], held.HugePools...)
		r.Held = &held
	}
	return r
}

func cloneActualCapacity(c budget.ReconciledCapacity) budget.ReconciledCapacity {
	if c.PGOperation != nil {
		op := *c.PGOperation
		c.PGOperation = &op
	}
	if c.InitialVM != nil {
		v := *c.InitialVM
		c.InitialVM = &v
	}
	if c.InitialOutside != nil {
		i := *c.InitialOutside
		i.PGAggregate = cloneActualRoot(i.PGAggregate)
		i.Shared.Observed = cloneActualSnapshot(i.Shared.Observed)
		c.InitialOutside = &i
	}
	if c.UnchangedPG != nil {
		s := *c.UnchangedPG
		s.Observed = cloneActualSnapshot(s.Observed)
		c.UnchangedPG = &s
	}
	if c.RecoveryPG != nil {
		p := *c.RecoveryPG
		p.Fence.Observed = cloneActualSnapshot(p.Fence.Observed)
		p.Status.Observed = cloneActualSnapshot(p.Status.Observed)
		if p.Status.Receipt.Result != nil {
			result := cloneActualSnapshot(*p.Status.Receipt.Result)
			p.Status.Receipt.Result = &result
		}
		c.RecoveryPG = &p
	}
	return c
}

func cloneServiceActualRecovery(p *ServiceActualRecovery) *ServiceActualRecovery {
	if p == nil {
		return nil
	}
	n := *p
	n.Capacity = cloneActualCapacity(p.Capacity)
	n.Current.Observed = cloneActualSnapshot(p.Current.Observed)
	n.Root, n.Guest, n.VM = cloneActualRoot(p.Root), cloneServiceResourceRecovery(p.Guest), cloneServiceVMRecovery(p.VM)
	n.PGRejection = cloneServicePGRejection(p.PGRejection)
	return &n
}

func cloneRecoveredResource(p *RecoveredResourceOrigin) *RecoveredResourceOrigin {
	if p == nil {
		return nil
	}
	return &RecoveredResourceOrigin{Original: cloneOperation(p.Original), Proof: *cloneServiceActualRecovery(&p.Proof)}
}

func (m Model) validateServiceActualRecovery() error {
	c := m.ServiceClosure
	p := c.ActualRecovery
	if p == nil {
		return nil
	}
	origin, _, closed, _ := c.Selection()
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	version := m.serviceClosureVersion()
	if version != 16 && version != 17 || c.Pending() || m.Active == nil || p.RecordedSequence > m.Sequence || !recoveryOwnerAt(p.Origin, origin, m.Owner) || p.ClosedOrigin != origin || p.Closed != closed || p.ClosedSequence != at || p.Incarnation != m.Incarnation || p.OperationSHA256 != c.OperationSHA256 || !reflect.DeepEqual(p.Guest, c.ResourceRecovery) || !reflect.DeepEqual(p.VM, c.VMRecovery) || !reflect.DeepEqual(p.PGRejection, c.PGRejection) {
		return fmt.Errorf("actual service recovery replaced original confirmed closure or domain proofs")
	}
	if source := c.PGRecovery; source != nil && (p.Capacity.RecoveryPG == nil || !reflect.DeepEqual(p.Capacity.RecoveryPG.Fence, source.Fence) || !reflect.DeepEqual(p.Capacity.RecoveryPG.Status, source.Status)) {
		return fmt.Errorf("actual service recovery replaced retained original PG proof")
	}
	if source := c.PGRejection; source != nil && (p.Capacity.RecoveryPG == nil || !reflect.DeepEqual(p.Capacity.RecoveryPG.Fence, source.Fence) || !reflect.DeepEqual(p.Capacity.RecoveryPG.Status, source.Status)) {
		return fmt.Errorf("actual service recovery replaced retained original PG rejection proof")
	}
	return p.validate(*m.Active)
}

func (m Model) RecordServiceActualRecovery(actor Owner, selector string, capacity budget.ReconciledCapacity, current control.SharedCostResponse, root budget.RootAggregate) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Pending() || c.ActualRecovery != nil || c.Rearm != nil || c.Reopen != nil || m.Active == nil || m.ResourceIdentity == nil {
		return m, fmt.Errorf("actual recovery requires original confirmed resource closure")
	}
	origin, _, closed, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("actual recovery selected another closure")
	}
	_, preparedHash, err := m.OriginalServicePrepared()
	if err != nil {
		return m, err
	}
	raw, err := json.Marshal(capacity)
	if err != nil {
		return m, err
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	p := &ServiceActualRecovery{Origin: actor, RecordedSequence: next.Sequence, Incarnation: m.Incarnation, OperationSHA256: c.OperationSHA256, ClosedOrigin: origin, Closed: closed, ClosedSequence: at, PreparedSHA256: preparedHash, Capacity: capacity, CapacitySHA256: rawDigest(raw), Current: current, Root: root, Guest: c.ResourceRecovery, VM: c.VMRecovery, PGRejection: c.PGRejection}
	next.ServiceClosure.ActualRecovery = cloneServiceActualRecovery(p)
	if err := m.checkInitialOutside(capacity); err != nil {
		return m, err
	}
	return next, next.Validate()
}

// 同VM UID/RV条件写保存完整实际预算。没有原开放发送票据，不安装永久头，
// 丢回复只可重读；原始Unknown和所有来源/峰值在后继完成CAS前一直保留。
func (s *Store) RecordServiceActualRecovery(ctx context.Context, snapshot *Snapshot, actor Owner, selector string, capacity budget.ReconciledCapacity, current control.SharedCostResponse, root budget.RootAggregate) (*Snapshot, error) {
	if err := s.checked(snapshot); err != nil {
		return nil, err
	}
	if !snapshot.Dispatchable() {
		return nil, fmt.Errorf("actual service recovery requires original running VM")
	}
	next, err := snapshot.model.RecordServiceActualRecovery(actor, selector, capacity, current, root)
	if err != nil {
		return nil, err
	}
	return s.apply(ctx, snapshot, next)
}

func (p *ServiceActualRecovery) UnmarshalJSON(raw []byte) error {
	type plain ServiceActualRecovery
	var n plain
	if err := control.UnmarshalObjectBounded(raw, &n, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*p = ServiceActualRecovery(n)
	return nil
}

func (p *RecoveredResourceOrigin) UnmarshalJSON(raw []byte) error {
	type plain RecoveredResourceOrigin
	var n plain
	if err := control.UnmarshalObjectBounded(raw, &n, MaxRawBytes/2, 16384); err != nil {
		return err
	}
	*p = RecoveredResourceOrigin(n)
	return nil
}
