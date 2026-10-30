package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// ServiceVMRecovery固定服务关闭后的独立Runner选择及其原生容量来源。
// 原Active/Abort/Unknown、预算与关闭逐字保留；只有原Runner在自己的flock
// 内证明完成或永久封住未发送，才可另行落账。本域单独完成不授予开放、
// 普通Finish或Host释放。普通轮询可以观察已保存选择，不得创建新选择。
type ServiceVMRecovery struct {
	Origin           Owner                    `json:"origin"`
	StartedSequence  control.Uint64           `json:"started_sequence"`
	OperationSHA256  string                   `json:"operation_sha256"`
	Closed           control.AdmissionRequest `json:"closed"`
	ClosedSequence   control.Uint64           `json:"closed_sequence"`
	CommandSHA256    string                   `json:"command_sha256"`
	CancelUnsent     bool                     `json:"cancel_unsent"`
	Phase            Phase                    `json:"phase"`
	RecordedSequence control.Uint64           `json:"recorded_sequence"`
	ObservedBy       *Owner                   `json:"observed_by"`
	Capacity         *budget.NativeCapacity   `json:"capacity"`
	Cancellation     *ServiceVMCancellation   `json:"cancellation"`
}

// ServiceVMCancellation只是已认证原回执中的见证摘要，不是取消许可。
// VMResourceVersion不作数值比较；真实UID/RV条件读写由Store与Runner认证。
// Capacity的不可变Receipt引用仍须固定Reader逐次重读完整回执及实际物理值。
type ServiceVMCancellation struct {
	Owner             Owner          `json:"owner"`
	VMSequence        control.Uint64 `json:"vm_sequence"`
	VMResourceVersion string         `json:"vm_resource_version"`
}

// serviceVMProjection仅交叉绑定既有Runner协议v1中的不变量，不能发送命令。
// journal不能反向依赖vmguard；完整几何、原出生、命令域摘要、取消flock终态
// 与当前QMP实际值仍由固定vmguard.Reader认证。这里绝不把摘要当来源认证。
type serviceVMProjection struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Identity        struct {
		VMUID        string         `json:"vm_uid"`
		Incarnation  string         `json:"incarnation"`
		RunnerPodUID string         `json:"runner_pod_uid"`
		RunnerBootID string         `json:"runner_boot_id"`
		NodeUID      string         `json:"node_uid"`
		MinimumBytes control.Uint64 `json:"minimum_bytes"`
		MaximumBytes control.Uint64 `json:"maximum_bytes"`
		BlockBytes   control.Uint64 `json:"block_bytes"`
	} `json:"identity"`
	BindingClaim   budget.ObjectRef `json:"binding_claim"`
	HostClaim      budget.ObjectRef `json:"host_claim"`
	Request        control.Request  `json:"request"`
	BeforeBytes    control.Uint64   `json:"before_bytes"`
	PreparedSHA256 string           `json:"prepared_sha256"`
	HostSHA256     string           `json:"host_sha256"`
	GuestSHA256    string           `json:"guest_sha256"`
}

// OriginalServiceVM只返回原ResizeVM及完整Prepared副本；允许原ResizeVM已
// 完成而ObserveVM/后继资源未知，绝不据此改写原步骤。所有前驱必须已完成。
func (m Model) OriginalServiceVM() (Step, budget.Prepared, error) {
	var p budget.Prepared
	if m.Active == nil || m.ResourceIdentity == nil || m.PGInstance == nil {
		return Step{}, p, fmt.Errorf("VM service recovery lacks original bound operation")
	}
	preparedHash, guestHash := "", ""
	for _, step := range m.Active.Steps {
		if step.Action == ResizeVM {
			var c serviceVMProjection
			if preparedHash == "" || step.Phase != Unconfirmed && step.Phase != Settled || step.DispatchedBy == nil || step.Phase == Settled && (step.Observation == nil || step.Observation.Result != Completed) || control.UnmarshalObject(step.Command, &c) != nil || c.ProtocolVersion != 1 || c.Request != m.Active.Request || c.BeforeBytes != p.VMBeforeMemoryBytes || c.PreparedSHA256 != preparedHash || c.GuestSHA256 != guestHash || !lifecycleDigest(c.HostSHA256) || c.BindingClaim.Validate() != nil || c.HostClaim.Validate() != nil || c.Identity.VMUID != m.VMUID || c.Identity.Incarnation != m.Incarnation || c.Identity.RunnerPodUID != step.TargetID || c.Request.Key.IssuerEpoch != step.DispatchedBy.Epoch {
				return Step{}, p, fmt.Errorf("VM service recovery lost complete original ResizeVM sources")
			}
			return cloneStep(step), p, nil
		}
		if step.Phase != Settled || step.Observation == nil || step.Observation.Result != Completed {
			return Step{}, p, fmt.Errorf("VM service recovery has unfinished original predecessor")
		}
		if step.Action == PrepareBudget {
			if json.Unmarshal(step.Observation.Data, &p) != nil || p.ValidateFor(m.Active.Request, *m.ResourceIdentity) != nil {
				return Step{}, p, fmt.Errorf("VM service recovery lost original complete Prepared")
			}
			preparedHash = rawDigest(step.Observation.Data)
		}
		if step.Action == GuestReady {
			guestHash = rawDigest(step.Observation.Data)
		}
	}
	return Step{}, p, fmt.Errorf("VM service recovery has no original dispatched ResizeVM")
}

func recoveryOwnerAt(owner, lower, upper Owner) bool {
	return owner.Validate() == nil && owner.Epoch >= lower.Epoch && (owner.Epoch != lower.Epoch || owner == lower) && owner.Epoch <= upper.Epoch && (owner.Epoch != upper.Epoch || owner == upper)
}

func (m Model) validateServiceVMRecovery() error {
	p := m.ServiceClosure.VMRecovery
	if p == nil {
		return nil
	}
	c := m.ServiceClosure
	step, _, err := m.OriginalServiceVM()
	version := m.serviceClosureVersion()
	if err != nil || version != 16 && version != 17 || c.Pending() || !recoveryOwnerAt(p.Origin, *step.DispatchedBy, m.Owner) || p.OperationSHA256 != c.OperationSHA256 || p.CommandSHA256 != step.CommandSHA256 || p.StartedSequence <= p.ClosedSequence || p.StartedSequence > m.Sequence || p.ClosedSequence == 0 || p.CancelUnsent && step.Phase != Unconfirmed {
		return fmt.Errorf("VM service recovery changed original closed command or selection")
	}
	closedOrigin, _, closed, _ := c.Selection()
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	if p.Closed != closed || p.ClosedSequence != at || !recoveryOwnerAt(p.Origin, closedOrigin, m.Owner) {
		return fmt.Errorf("VM service recovery replaced latest complete closure")
	}
	if p.Phase == Unconfirmed {
		if p.RecordedSequence != 0 || p.ObservedBy != nil || p.Capacity != nil || p.Cancellation != nil {
			return fmt.Errorf("unknown VM recovery contains completion")
		}
		return nil
	}
	if p.Phase != Settled || p.RecordedSequence <= p.StartedSequence || p.RecordedSequence > m.Sequence || p.ObservedBy == nil || !recoveryOwnerAt(*p.ObservedBy, p.Origin, m.Owner) || p.Capacity == nil || p.Capacity.Validate() != nil {
		return fmt.Errorf("VM service recovery lacks independently recorded original capacity")
	}
	var command serviceVMProjection
	if control.UnmarshalObject(step.Command, &command) != nil {
		return fmt.Errorf("VM service recovery lost original command")
	}
	n := p.Capacity
	// v1投影逐字段保留原协议，规范JSON次序与Runner一致。双域摘要一起
	// 固定，防止同目标、同Host或同外层摘要掩盖被替换的原生命令。
	raw, err := json.Marshal(command)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(append([]byte("neon-dsga-runner-memory-v1\x00"), raw...))
	if n.Request != command.Request || n.BeforeBytes != command.BeforeBytes || n.PreparedSHA256 != command.PreparedSHA256 || n.GuestSHA256 != command.GuestSHA256 || n.HostClaim != command.HostClaim || n.Sources.BirthClaim != command.BindingClaim || n.Sources.JournalSHA256 != step.CommandSHA256 || n.Sources.CommandSHA256 != hex.EncodeToString(hash[:]) {
		return fmt.Errorf("VM service recovery substituted original capacity sources")
	}
	if !n.Cancelled {
		if p.Cancellation != nil {
			return fmt.Errorf("completed VM recovery contains cancellation")
		}
		return nil
	}
	w := p.Cancellation
	if step.Phase != Unconfirmed || (!p.CancelUnsent && !m.Active.AbortRequested) || w == nil || !recoveryOwnerAt(w.Owner, *step.DispatchedBy, *p.ObservedBy) || w.VMSequence == 0 || w.VMSequence >= p.RecordedSequence || control.ValidateID(w.VMResourceVersion, "original VM resource version") != nil || !m.Active.AbortRequested && (w.VMSequence < p.StartedSequence || !recoveryOwnerAt(w.Owner, p.Origin, *p.ObservedBy)) {
		return fmt.Errorf("VM service recovery lacks selected durable unsent cancellation witness")
	}
	return nil
}

func cloneServiceVMRecovery(p *ServiceVMRecovery) *ServiceVMRecovery {
	if p == nil {
		return nil
	}
	n := *p
	if p.ObservedBy != nil {
		owner := *p.ObservedBy
		n.ObservedBy = &owner
	}
	if p.Capacity != nil {
		capacity := *p.Capacity
		n.Capacity = &capacity
	}
	if p.Cancellation != nil {
		witness := *p.Cancellation
		n.Cancellation = &witness
	}
	return &n
}

func (m Model) StartServiceVMRecovery(actor Owner, selector string, cancelUnsent bool) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Pending() || c.VMRecovery != nil || c.Rearm != nil || c.Reopen != nil {
		return m, fmt.Errorf("VM recovery requires original unchanged confirmed closure")
	}
	step, _, err := m.OriginalServiceVM()
	if err != nil {
		return m, err
	}
	_, _, closed, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("VM recovery selected another closure")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	at := c.RecordedSequence
	if c.Continuation != nil {
		at = c.Continuation.RecordedSequence
	}
	next.ServiceClosure.VMRecovery = &ServiceVMRecovery{Origin: actor, StartedSequence: next.Sequence, OperationSHA256: c.OperationSHA256, Closed: closed, ClosedSequence: at, CommandSHA256: step.CommandSHA256, CancelUnsent: cancelUnsent, Phase: Unconfirmed, RecordedSequence: 0, ObservedBy: nil, Capacity: nil, Cancellation: nil}
	return next, next.Validate()
}

func (m Model) RecordServiceVMRecovery(actor Owner, selector string, capacity budget.NativeCapacity, witness *ServiceVMCancellation) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.VMRecovery == nil || c.VMRecovery.Phase != Unconfirmed || c.Rearm != nil || c.Reopen != nil {
		return m, fmt.Errorf("VM recovery is not original independent unknown")
	}
	_, _, _, selected := c.Selection()
	if selector != selected {
		return m, fmt.Errorf("VM recovery record selected another closure")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	p := next.ServiceClosure.VMRecovery
	p.Phase, p.RecordedSequence, p.ObservedBy, p.Capacity = Settled, next.Sequence, &actor, &capacity
	if witness != nil {
		copied := *witness
		p.Cancellation = &copied
	}
	return next, next.Validate()
}

// 写前选择及落账各自使用同VM UID/RV CAS；丢回复返回nil快照。Runner独立
// GET看到已持久选择后，仍须在自己的原flock内认证并封存，不消费调用方票据。
func (store *Store) StartServiceVMRecovery(ctx context.Context, s *Snapshot, actor Owner, selector string, cancelUnsent bool) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	if !s.Dispatchable() {
		return nil, fmt.Errorf("VM recovery requires original running VM")
	}
	next, err := s.model.StartServiceVMRecovery(actor, selector, cancelUnsent)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (store *Store) RecordServiceVMRecovery(ctx context.Context, s *Snapshot, actor Owner, selector string, capacity budget.NativeCapacity, witness *ServiceVMCancellation) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	if !s.Dispatchable() {
		return nil, fmt.Errorf("VM recovery requires original running VM")
	}
	next, err := s.model.RecordServiceVMRecovery(actor, selector, capacity, witness)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (p *ServiceVMRecovery) UnmarshalJSON(raw []byte) error {
	type plain ServiceVMRecovery
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, MaxObservationBytes, 2048); err != nil {
		return err
	}
	*p = ServiceVMRecovery(next)
	return nil
}

func (p *ServiceVMCancellation) UnmarshalJSON(raw []byte) error {
	type plain ServiceVMCancellation
	var next plain
	if err := control.UnmarshalObjectBounded(raw, &next, 2048, 64); err != nil {
		return err
	}
	*p = ServiceVMCancellation(next)
	return nil
}
