package vmguard

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"

	coordination "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func (r *Runner) readLease(ctx context.Context) (*coordination.Lease, error) {
	binding := r.store.Binding()
	lease, err := r.coord.Leases(binding.Namespace).Get(ctx, journal.LeaseName(binding), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := journal.ValidateLease(lease, binding, journal.LeaseName(binding), ""); err != nil {
		return nil, err
	}
	// expiry只限制新的认证观测，不撤销floor/原意图/持有。这里不把系统时钟
	// 当fence；最终串行fence仍在本地WAL，时钟异常只会拒绝而非放行过期写。
	now := time.Now()
	if lease.Spec.RenewTime.After(now.Add(5*time.Second)) || !now.Before(lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds)*time.Second)) {
		return nil, fmt.Errorf("Runner owner Lease is expired or timestamp is in the future")
	}
	return lease, nil
}

func (r *Runner) authorize(ctx context.Context) (authorization, error) {
	var a authorization
	first, err := r.readLease(ctx)
	if err != nil {
		return a, err
	}
	s, err := r.store.Load(ctx)
	if err != nil {
		return a, err
	}
	model := s.State()
	last, err := r.readLease(ctx)
	if err != nil {
		return a, err
	}
	if first.UID != last.UID || *first.Spec.HolderIdentity != model.Owner.HolderID || *last.Spec.HolderIdentity != model.Owner.HolderID {
		return a, fmt.Errorf("Runner owner changed across Lease/journal/Lease reads")
	}
	cm, err := readClaim(ctx, r.core, r.store.Binding(), r.engine.binding, bindingKind, true)
	if err != nil {
		return a, err
	}
	var birth Birth
	if json.Unmarshal([]byte(cm.Data[ClaimKey]), &birth) != nil || !sameBirth(birth, r.birth) {
		return a, fmt.Errorf("Runner original Birth changed")
	}
	stateCM, err := readClaim(ctx, r.core, r.store.Binding(), r.birth.StateClaim, stateKind, false)
	if err != nil {
		return a, err
	}
	var published Publication
	if json.Unmarshal([]byte(stateCM.Data[ClaimKey]), &published) != nil || initialPublicationMatches(published, r.birth, &r.engine.binding, true) != nil || published.Identity != r.birth.Identity || published.BindingClaim != nil && *published.BindingClaim != r.engine.binding {
		return a, fmt.Errorf("Runner original publication source changed before dispatch")
	}
	pod, err := r.core.Pods(r.store.Binding().Namespace).Get(ctx, r.pod, metav1.GetOptions{})
	if err != nil {
		return a, err
	}
	if err := originalPod(pod, r.store.Binding(), r.pod, r.birth.Identity.RunnerPodUID, r.node); err != nil {
		return a, err
	}
	node, err := r.core.Nodes().Get(ctx, r.node, metav1.GetOptions{})
	if err != nil {
		return a, err
	}
	if node == nil || string(node.UID) != r.birth.Identity.NodeUID || node.ResourceVersion == "" || node.DeletionTimestamp != nil {
		return a, fmt.Errorf("Runner original Node changed")
	}
	a.Owner, a.LeaseUID = model.Owner, string(last.UID)
	op := model.Active
	if op == nil {
		return a, nil
	}
	prior := map[journal.Action]*journal.Step{}
	var vmStep *journal.Step
	for i := range op.Steps {
		step := &op.Steps[i]
		if step.Phase == journal.Settled {
			if step.Observation == nil || step.Observation.Result != journal.Completed {
				return a, nil
			}
			prior[step.Action] = step
			continue
		}
		if step.Action == journal.ResizeVM && step.Phase == journal.Unconfirmed {
			vmStep = step
		}
		break
	}
	if vmStep == nil {
		return a, nil
	}
	var c Command
	if json.Unmarshal(vmStep.Command, &c) != nil || c.Validate() != nil || c.Identity != r.birth.Identity || c.BindingClaim != r.engine.binding || c.Request != op.Request || vmStep.TargetID != c.Identity.RunnerPodUID || vmStep.DispatchedBy == nil || vmStep.DispatchedBy.Epoch != c.Request.Key.IssuerEpoch {
		a.Denied = fmt.Errorf("Runner original ResizeVM command is incomplete or replaced")
		return a, nil
	}
	a.Command, a.JournalSHA256 = &c, vmStep.CommandSHA256
	selectedCancel := false
	if closed := model.ServiceClosure; closed != nil && closed.VMRecovery != nil {
		choice := closed.VMRecovery
		selectedCancel = choice.Phase == journal.Unconfirmed && choice.CancelUnsent && choice.CommandSHA256 == vmStep.CommandSHA256
	}
	if (op.AbortRequested || selectedCancel) && s.Dispatchable() {
		// 只认证原取消，或同VM UID/RV已持久的独立未发送封存选择；二者
		// 均逐字绑定原派发者/命令，不授予预算或新QMP写。独立选择不改
		// 冻结Active.Abort，接管后见证仍记录本次真实owner/Sequence/RV。
		// 即使Host/PG暂失联，Runner仍在原flock内证明从未跨发送屏障；
		// 不能由Denied、单纯关闭或普通not_sent推导取消。
		a.Abort = &cancellationAuthority{Origin: *vmStep.DispatchedBy, Witness: Cancellation{ProtocolVersion: 1, Owner: model.Owner, VMSequence: model.Sequence, VMResourceVersion: s.ResourceVersion()}}
	}
	if model.ServiceClosure != nil {
		// 独立关闭写前点冻结原Active，来源恢复不能把原Unknown重新当成新
		// 派发或消费旧not_sent重放选择。先保留完整原Command和明确Abort，
		// 使engine仍可在原flock内只读对账may_have_sent，或封存原取消；
		// 关闭本身不表示未发送，也不产生取消、预算释放或新QMP写许可。
		a.Denied = fmt.Errorf("Runner service closure blocks new original ResizeVM dispatch or replay")
		return a, nil
	}
	if op.AbortRequested || op.Origin != model.Owner || *vmStep.DispatchedBy != model.Owner || !s.Dispatchable() {
		a.Denied = fmt.Errorf("Runner original ResizeVM is cancelled, old-owner or not dispatchable")
		return a, nil
	}
	if err := r.verifySources(ctx, s, c, prior); err != nil {
		// 缺源阻止新写，不挡住engine对已经may_have_sent的旧动作只读恢复。
		a.Denied = err
		return a, nil
	}
	a.ReplayNonce, a.ReplayReceipt, err = r.replaySelection(ctx, s, c, vmStep.CommandSHA256, published)
	if err != nil {
		a.Denied = err
		return a, nil
	}
	a.Dispatch = true
	return a, nil
}

func (r *Runner) verifySources(ctx context.Context, s *journal.Snapshot, c Command, prior map[journal.Action]*journal.Step) error {
	prepared := prior[journal.PrepareBudget]
	if prepared == nil || prepared.Observation == nil || SourceDigest(prepared.Observation.Data) != c.PreparedSHA256 {
		return fmt.Errorf("Runner original complete PrepareBudget source missing")
	}
	var p budget.Prepared
	model := s.State()
	if json.Unmarshal(prepared.Observation.Data, &p) != nil || model.ResourceIdentity == nil || model.PGInstance == nil || *model.PGInstance != c.Request.Key.Instance || p.ValidateFor(c.Request, *model.ResourceIdentity) != nil || p.VMBeforeMemoryBytes != c.BeforeBytes || p.RunnerPodName != r.pod || p.RunnerRestartCount != 0 {
		return fmt.Errorf("Runner prepared original PG/Guest/VM baseline differs")
	}
	observed, err := r.claims.Read(ctx, s, p, c.HostClaim)
	if err != nil {
		return err
	}
	rawHost, err := json.Marshal(observed.Host)
	if err != nil || SourceDigest(rawHost) != c.HostSHA256 || observed.Host.RunnerPodUID != c.Identity.RunnerPodUID || observed.Host.NodeUID != c.Identity.NodeUID {
		return fmt.Errorf("Runner original Host commitment differs")
	}
	held := max(c.BeforeBytes, c.Request.TargetVMMemoryBytes)
	if uint64(observed.Host.HostOverheadBytes) > math.MaxUint64-uint64(held) || observed.Host.ReservedMemoryBytes < held+observed.Host.HostOverheadBytes {
		return fmt.Errorf("host must retain both original and target memory plus overhead until VM settlement")
	}
	if c.Request.Kind == control.Grow {
		step := prior[journal.ReserveHost]
		var host budget.HostReservation
		if step == nil || step.Observation == nil || json.Unmarshal(step.Observation.Data, &host) != nil || host != observed.Host || step.TargetID != host.SourceID {
			return fmt.Errorf("Runner grow lacks original completed Host hold")
		}
		return r.claims.Recheck(ctx, s, p, c.HostClaim, observed)
	}
	guest := prior[journal.GuestReady]
	if prior[journal.Quiesce] == nil || prior[journal.ResizePG] == nil || prior[journal.ReleasePG] == nil || guest == nil || guest.Observation == nil || SourceDigest(guest.Observation.Data) != c.GuestSHA256 {
		return fmt.Errorf("Runner shrink lacks original complete pre-VM steps")
	}
	var audit resourceexec.Audit
	var command resourceguard.Command
	// VM最终写者同样要求原AS逐字固定；不能只核normal/任务数而遗漏出生约束。
	if json.Unmarshal(guest.Command, &command) != nil || json.Unmarshal(guest.Observation.Data, &audit) != nil || audit.ProtocolVersion != 2 || !reflect.DeepEqual(audit.Command, command) || command.Identity != p.Identity || command.BudgetCommitmentID != p.Request.BudgetCommitmentID || command.IssuerEpoch != p.Request.Key.IssuerEpoch || command.MemoryMaxBytes != p.Limits.MemoryMaxBytes || command.PidsMaxTasks != p.Limits.PidsMaxTasks || command.AddressSpaceMaxBytes != p.Limits.AddressSpaceMaxBytes || command.SystemMemoryMaxBytes != p.Limits.SystemMemoryMaxBytes || command.OutsideUpperBytes != p.Limits.OutsideUpperBytes || !reflect.DeepEqual(command.HugePools, p.Limits.HugePools) || audit.Evidence.ValidateFor(command) != nil || audit.PGAggregate.Validate() != nil || audit.PGAggregate.Identity != p.Identity || audit.PGAggregate.Held == nil || !reflect.DeepEqual(*audit.PGAggregate.Held, command) || audit.RunnerPodName != r.pod || audit.RunnerRestarts != p.RunnerRestartCount || audit.VMMemoryBytes != c.BeforeBytes || p.ValidateCurrent(audit.Shared, c.Request.Key.IssuerEpoch) != nil {
		return fmt.Errorf("Runner original shrink PG release/Guest limits audit differs")
	}
	if audit.Fence.ValidateFor(control.FenceRequest{ProtocolVersion: 1, Instance: c.Request.Key.Instance, IssuerEpoch: c.Request.Key.IssuerEpoch}, c.Request.Key.Instance) != nil || audit.Fence.Disposition != control.Fenced || audit.Shared.Observed.CapacityGeneration < audit.Fence.Observed.CapacityGeneration {
		return fmt.Errorf("Runner original Guest audit lacks complete native issuer fence")
	}
	return r.claims.Recheck(ctx, s, p, c.HostClaim, observed)
}
