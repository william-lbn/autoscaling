package admissionexec

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// RecoverServiceRetained只在明确管理选择中恢复原完整预算。Prepare尚未完成，
// Host/Guest/VM/PG均未派发；已经在网络中的Outside CREATE仍可能提交，所以
// 不按404或关闭推导NoEffect，不补Prepared、不新建Host、不重发资源命令。
// 独立Proof的CAS只是保存来源；后继显式重开和完成CAS才解除关闭。
func (e *Executor) RecoverServiceRetained(ctx context.Context, s *journal.Snapshot, selector string) error {
	if s == nil || s.State().ServiceClosure == nil {
		return fmt.Errorf("retained service recovery lacks original closure")
	}
	c := s.State().ServiceClosure
	_, _, _, selected := c.Selection()
	if selector != selected {
		return fmt.Errorf("retained service recovery selected another closure")
	}
	if c.RetainedRecovery != nil {
		return e.CheckServiceRetained(ctx, s)
	}
	proof, err := e.checkServiceRetained(ctx, s, nil)
	if err != nil {
		return err
	}
	_, err = e.store.RecordServiceRetainedRecovery(ctx, s, e.actor, selector, proof)
	return err
}

// CheckServiceRetained每次重新认证当前全部来源，不把曾经保存的证明当活性
// 许可。接管沿用原Proof/预算ID；当前floor可以合法推进，原来源不得重选。
// 它既不清SDK未知，也不开放、结清Host、回收任何对象。
func (e *Executor) CheckServiceRetained(ctx context.Context, s *journal.Snapshot) error {
	if s == nil || s.State().ServiceClosure == nil || s.State().ServiceClosure.RetainedRecovery == nil {
		return fmt.Errorf("retained service check lacks independent saved budget proof")
	}
	_, err := e.checkServiceRetained(ctx, s, s.State().ServiceClosure.RetainedRecovery)
	return err
}

// retainedRunnerFacts只保留认证读取得到的原不可变来源和取消见证；不缓存
// Runner当前State或把当前数值变成一条新命令。主预算与较新取消尾分开，
// 无命令的出生来源也另列，避免将Initial冒充普通Resize完成。
type retainedRunnerFacts struct {
	native       *budget.NativeCapacity
	initial      *budget.InitialVMSources
	cancellation *journal.RetainedRunnerCancellation
	tail         *budget.NativeCapacity
	tailCancel   *journal.RetainedRunnerCancellation
	identity     vmguard.Identity
}

func retainedCancellation(proof vmguard.Confirmed) *journal.RetainedRunnerCancellation {
	if proof.Receipt.Cancellation == nil {
		return nil
	}
	c := proof.Receipt.Cancellation
	return &journal.RetainedRunnerCancellation{Owner: c.Owner, VMSequence: c.VMSequence, VMResourceVersion: c.VMResourceVersion}
}

func retainedConfirmedFacts(proof vmguard.Confirmed) (retainedRunnerFacts, error) {
	var facts retainedRunnerFacts
	native, err := vmguard.CapacitySource(proof)
	if err != nil {
		return facts, err
	}
	facts.native, facts.cancellation, facts.identity = &native, retainedCancellation(proof), proof.Birth.Identity
	return facts, nil
}

func retainedKeyBefore(left, right control.Key) bool {
	return left.Instance == right.Instance && (left.IssuerEpoch < right.IssuerEpoch || left.IssuerEpoch == right.IssuerEpoch && left.Revision < right.Revision)
}

// Initial的长期读者本就允许此前已有明确Completed/Cancelled的原命令。
// 有Last时必须重新核不可变回执和当前原State；只有从未有任何命令时才
// 使用InitialCapacityFor，不能为省事抹掉旧取消或制造一个启动Resize。
func (e *Executor) initialRetainedRunner(ctx context.Context, m journal.Model, initial budget.Initial) (retainedRunnerFacts, error) {
	var facts retainedRunnerFacts
	birth, ref, published, err := e.runner.Inspect(ctx, &initial.RunnerBirth, e.actor)
	if err != nil {
		return facts, err
	}
	raw, err := json.Marshal(birth.Identity)
	if err != nil {
		return facts, err
	}
	if published.State == nil || published.Actual == nil || ref != initial.RunnerBirth || vmguard.SourceDigest(raw) != initial.RunnerIdentitySHA256 || birth.Identity.RunnerPodUID != initial.RunnerPodUID || birth.Identity.NodeUID != initial.NodeUID || published.State.Pending != nil || published.Actual.RequestedBytes != initial.VMMemoryBytes || published.Actual.ActualBytes != initial.VMMemoryBytes {
		return facts, fmt.Errorf("retained initial Runner changed birth, state or actual capacity")
	}
	if last := published.State.Last; last != nil {
		if m.Active == nil || !retainedKeyBefore(last.Intent.Command.Request.Key, m.Active.Request.Key) {
			return facts, fmt.Errorf("retained initial Runner command is outside original prior operation")
		}
		proof, err := e.runner.ObserveAbortOriginal(ctx, last.Intent.Command, last.Intent.JournalSHA256, e.actor)
		if err != nil {
			return facts, err
		}
		return retainedConfirmedFacts(proof)
	}
	proof, err := e.runner.InitialCapacityFor(ctx, initial, e.actor)
	if err != nil {
		return facts, err
	}
	facts.initial, facts.identity = &proof.Sources, proof.Birth.Identity
	return facts, nil
}

func (e *Executor) retainedRunner(ctx context.Context, m journal.Model, source journal.RetainedBudgetSource) (retainedRunnerFacts, error) {
	if source.Initial != nil {
		return e.initialRetainedRunner(ctx, m, *source.Initial)
	}
	var facts retainedRunnerFacts
	proof, err := e.runner.CheckActiveHeldBudget(ctx, m)
	if err != nil {
		return facts, err
	}
	if proof.Initial != nil {
		facts.initial, facts.identity = &proof.Initial.Sources, proof.Initial.Birth.Identity
		return facts, nil
	}
	facts, err = retainedConfirmedFacts(proof.Budget)
	if err != nil {
		return facts, err
	}
	if proof.Cancellation != nil {
		tail, err := vmguard.CapacitySource(*proof.Cancellation)
		if err != nil {
			return facts, err
		}
		facts.tail, facts.tailCancel = &tail, retainedCancellation(*proof.Cancellation)
	}
	return facts, nil
}

func (e *Executor) checkServiceRetained(ctx context.Context, s *journal.Snapshot, existing *journal.ServiceRetainedRecovery) (journal.ServiceRetainedRecovery, error) {
	var proof journal.ServiceRetainedRecovery
	if s == nil || !s.Dispatchable() || e.resources == nil || e.claims == nil || e.runner == nil {
		return proof, fmt.Errorf("retained service recovery requires original running production domains")
	}
	m, vm := s.State(), s.VirtualMachine()
	if m.Owner != e.actor || m.PGInstance == nil || *m.PGInstance != e.instance || m.ResourceIdentity == nil || m.ServiceClosure == nil || m.ServiceClosure.Pending() || e.client.Pending() != nil || e.resources.Pending() != nil {
		return proof, fmt.Errorf("retained service recovery changed owner, closure or pending resource domain")
	}
	source, err := m.RetainedServiceSource()
	if err != nil {
		return proof, err
	}
	if existing != nil && !reflect.DeepEqual(source, existing.Source) {
		return proof, fmt.Errorf("saved retained service source is no longer the original current budget")
	}
	fence, err := e.client.Fence(ctx, control.FenceRequest{ProtocolVersion: 1, Instance: e.instance, IssuerEpoch: e.actor.Epoch})
	if err != nil {
		return proof, err
	}
	if fence.Disposition != control.Fenced {
		return proof, fmt.Errorf("retained service PG serialized fence remains pending")
	}
	shared, err := e.client.SharedCost(ctx)
	if err != nil {
		return proof, err
	}
	if !serviceActualGate(m.ServiceClosure, shared.Observed) || !control.SameResizeRecoveryBoundary(fence.Observed, shared.Observed) {
		return proof, fmt.Errorf("retained service PG differs from selected native gate")
	}
	root, usable, err := e.reconciledRoot(ctx, s)
	if err != nil {
		return proof, err
	}
	runner, err := e.retainedRunner(ctx, m, source)
	if err != nil {
		return proof, err
	}
	if vm.Status.MemorySize == nil {
		return proof, fmt.Errorf("retained service lacks current VM actual status")
	}
	actual, exact := vm.Status.MemorySize.AsInt64()
	if !exact || actual <= 0 {
		return proof, fmt.Errorf("retained service VM actual status is not exact positive bytes")
	}
	// 后面的完整ValidateCurrent一次核原预算实际值；固定claims读者
	// 另核VM status中的原Pod/重启数及当前Node。不能为每个标量再次
	// 解析整份Finalize命令/回执，也不把本次已校验叶子跨轮次缓存。
	memory := control.Uint64(actual)
	proof.Source, proof.Fence, proof.Current, proof.Root = source, fence, shared, root
	if source.Initial == nil {
		anchor := m.BudgetBoundary
		if m.Active.PriorBudgetBoundary != nil {
			anchor = m.Active.PriorBudgetBoundary
		}
		if anchor == nil {
			return proof, fmt.Errorf("retained service budget lost original closed anchor")
		}
		copyAnchor := *anchor
		proof.Anchor = &copyAnchor
	}
	proof.NativeVM, proof.InitialVM = runner.native, runner.initial
	proof.NativeVMCancellation, proof.TailVM, proof.TailVMCancellation = runner.cancellation, runner.tail, runner.tailCancel
	if err := proof.ValidateCurrent(shared, root, e.actor, memory, usable); err != nil {
		return proof, err
	}
	if existing != nil {
		// 不比较随owner/floor或实际释放合法变化的当前PG观察；完整来源、
		// 原Root和两个不同Runner意图必须逐字不变，接管不能重选便宜来源。
		if !reflect.DeepEqual(proof.Source, existing.Source) || !reflect.DeepEqual(proof.Anchor, existing.Anchor) || !reflect.DeepEqual(proof.Root, existing.Root) || !reflect.DeepEqual(proof.NativeVM, existing.NativeVM) || !reflect.DeepEqual(proof.InitialVM, existing.InitialVM) || !reflect.DeepEqual(proof.NativeVMCancellation, existing.NativeVMCancellation) || !reflect.DeepEqual(proof.TailVM, existing.TailVM) || !reflect.DeepEqual(proof.TailVMCancellation, existing.TailVMCancellation) {
			return proof, fmt.Errorf("retained service changed immutable original recovery sources")
		}
		if err := existing.ValidateCurrent(shared, root, e.actor, memory, usable); err != nil {
			return proof, err
		}
	}
	var initialSources *budgetclaims.InitialSources
	var heldSources *budgetclaims.Current
	if source.Initial != nil {
		initialSources, err = e.claims.ReadInitialBudget(ctx, s, *source.Initial)
		if err == nil && (runner.identity.RunnerPodUID != source.Initial.RunnerPodUID || runner.identity.NodeUID != source.Initial.NodeUID) {
			err = fmt.Errorf("retained initial Runner and Host placement domains differ")
		}
	} else {
		heldSources, err = e.claims.ReadActiveHeld(ctx, s)
		if err == nil && (runner.identity.RunnerPodUID != heldSources.Host.RunnerPodUID || runner.identity.NodeUID != heldSources.Host.NodeUID) {
			err = fmt.Errorf("retained Runner and original Host source domains differ")
		}
	}
	if err != nil {
		return proof, err
	}
	// 双读包围跨域网络；只允许PG成本实际下降，不允许容量、准入选择、
	// Root、原生Runner/取消见证或来源UID变化。最后仍以同VM UID/RV落账。
	latest, err := e.client.SharedCost(ctx)
	if err != nil {
		return proof, err
	}
	if !serviceActualGate(m.ServiceClosure, latest.Observed) || latest.Observed.AdmissionRevision != shared.Observed.AdmissionRevision || latest.Observed.AdmissionOpen != shared.Observed.AdmissionOpen || proof.ValidateCurrent(latest, root, e.actor, memory, usable) != nil {
		return proof, fmt.Errorf("retained service PG changed during source verification")
	}
	againRoot, againUsable, err := e.reconciledRoot(ctx, s)
	if err != nil || !reflect.DeepEqual(againRoot, root) || proof.ValidateCurrent(latest, againRoot, e.actor, memory, againUsable) != nil {
		return proof, fmt.Errorf("retained service Root changed during source verification")
	}
	againRunner, err := e.retainedRunner(ctx, m, source)
	if err != nil || !reflect.DeepEqual(againRunner, runner) {
		return proof, fmt.Errorf("retained service original Runner sources changed during verification")
	}
	if source.Initial != nil {
		err = e.claims.RecheckInitialBudget(ctx, s, *source.Initial, initialSources)
	} else {
		err = e.claims.RecheckActiveHeld(ctx, s, heldSources)
	}
	if err != nil {
		return proof, err
	}
	// source来自本次整模校验后的深副本，m和它在网络期间都不修改。
	// 末尾重新Load并核同UID/RV；不能以重复验证同一旧本地m代替此屏障。
	if err := e.sameSnapshot(ctx, s); err != nil {
		return proof, err
	}
	return proof, nil
}
