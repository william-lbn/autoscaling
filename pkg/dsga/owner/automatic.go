package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 采样失败只冻结新自动建议，不终止健康owner、不关闭服务、不丢在途操作。
// 已有恢复/管理调用继续走Run的原串行通道。配置错误在启动前直接拒绝。
var ErrPolicyObservation = errors.New("automatic memory policy awaits original observation")

type automaticMemory struct {
	config    policy.Config
	engine    *policy.Engine
	claims    *budgetclaims.Reader
	runner    *vmguard.Reader
	source    budget.QualificationSource
	next      time.Time
	pending   *control.Request
	pendingRV string
}

// ConfigureAutomaticMemory仅在Run前由可信Agent配置调用。它不开放旧WS/LFC/
// cgroup写者，不创建另一SDK/owner或后台执行队列；持有原worker的资源栅栏。
func (w *PGWorker) ConfigureAutomaticMemory(c policy.Config, core coreclient.CoreV1Interface) error {
	if w.running.Load() || w.automatic != nil || w.prepare == nil || w.resourceOwner == nil || w.vm == nil || w.finalize == nil || core == nil || w.qualification == nil || w.qualification.Validate() != nil {
		return fmt.Errorf("automatic memory policy requires unstarted complete production worker")
	}
	engine, err := policy.New(c)
	if err != nil {
		return err
	}
	claims, err := budgetclaims.New(core, w.session.binding)
	if err != nil {
		return err
	}
	runner, err := vmguard.NewReader(core, w.session.binding, string(w.session.leaseUID))
	if err != nil {
		return err
	}
	c.Targets = append([]policy.Target(nil), c.Targets...)
	w.automatic = &automaticMemory{config: c, engine: engine, claims: claims, runner: runner, source: *w.qualification, next: time.Time{}, pending: nil, pendingRV: ""}
	return nil
}

// sampleAutomatic读取的是策略输入，绝不将瞬时占用升级为未来硬上界。完整原
// 资格和Runner几何用于限制建议集合，真正Prepare/Quiesce/Guest/VM仍会再次
// 认证所有领域。采样全程不发送floor/setter，不持本地锁跨网络；用同VM RV
// 拒绝期间的日志/owner/Runner变化，不能拼接新旧boot或借当前值清未知SDK。
func (w *PGWorker) sampleAutomatic(ctx context.Context, s *journal.Snapshot) (policy.Sample, error) {
	var result policy.Sample
	d := w.automatic
	m, vm := s.State(), s.VirtualMachine()
	instance, epoch := w.Binding()
	rootClient := w.resourceOwner.client
	if !s.Dispatchable() || m.Owner != w.session.Actor() || m.Active != nil || m.PGInstance == nil || *m.PGInstance != instance || m.ResourceIdentity == nil || w.client.Pending() != nil || rootClient.Pending() != nil || m.InitialHeld == nil && m.BudgetHeld == nil && m.CapacityHeld == nil {
		return result, fmt.Errorf("automatic observation requires idle original bound domains and retained budget")
	}
	if pending, closeIntent := w.client.PendingAdmission(); pending != nil || closeIntent != nil {
		return result, fmt.Errorf("automatic observation cannot cross original admission unknown")
	}
	started := time.Now()
	q, err := d.claims.Qualification(ctx, d.source)
	if err != nil {
		return result, err
	}
	view, err := rootClient.InspectState(ctx)
	if err != nil {
		return result, err
	}
	limits, err := view.ConfirmedLimits()
	if err != nil {
		return result, err
	}
	process, err := rootClient.InspectProcess(ctx)
	if err != nil {
		return result, err
	}
	bootstrap, err := json.Marshal(view.Bootstrap)
	if err != nil {
		return result, err
	}
	if view.Identity != *m.ResourceIdentity || q.Identity != view.Identity || view.IssuerFloorEpoch != epoch || view.Birth == nil || process != *view.Birth || vmguard.SourceDigest(bootstrap) != q.RootBootstrapSHA256 || q.RunnerPodName != vm.Status.PodName || q.RunnerRestartCount != vm.Status.RestartCount || q.NodeName != vm.Status.Node {
		return result, fmt.Errorf("automatic observation changed original Root or qualified Runner")
	}
	birth, birthRef, publication, err := d.runner.Inspect(ctx, &q.RunnerBirth, w.session.Actor())
	if err != nil {
		return result, err
	}
	runnerIdentity, err := json.Marshal(birth.Identity)
	if err != nil {
		return result, err
	}
	if birthRef != q.RunnerBirth || vmguard.SourceDigest(runnerIdentity) != q.RunnerIdentitySHA256 || birth.Identity.RunnerPodUID != q.RunnerPodUID || birth.Identity.NodeUID != q.NodeUID || publication.State.Pending != nil || publication.Actual.RequestedBytes != publication.Actual.ActualBytes || vm.Status.MemorySize == nil {
		return result, fmt.Errorf("automatic observation lacks original settled native Runner")
	}
	vmBytes, exact := vm.Status.MemorySize.AsInt64()
	if !exact || vmBytes <= 0 || control.Uint64(vmBytes) != publication.Actual.ActualBytes {
		return result, fmt.Errorf("automatic VM status differs from native actual capacity")
	}
	work, err := w.client.WorkState(ctx)
	if err != nil {
		return result, err
	}
	pg := work.Observed
	if pg.IssuerFloorEpoch != epoch || pg.LowBuffers != pg.HighBuffers || pg.Operation != nil && pg.Operation.Phase == control.Preparing {
		return result, fmt.Errorf("automatic observation requires stable original PG capacity and floor")
	}
	for _, array := range pg.Arrays {
		if array.AllocationUnknown {
			return result, fmt.Errorf("automatic observation cannot ignore native unknown allocation")
		}
	}
	eligible := make([]bool, len(d.config.Targets))
	for i, target := range d.config.Targets {
		qualified, err := q.Target(target.VMMemoryBytes)
		if err != nil || target.Buffers > pg.MaximumBuffers || target.VMMemoryBytes < birth.Identity.MinimumBytes || target.VMMemoryBytes > birth.Identity.MaximumBytes || (target.VMMemoryBytes-birth.Identity.MinimumBytes)%birth.Identity.BlockBytes != 0 {
			return result, fmt.Errorf("automatic target lacks exact original qualification or native geometry")
		}
		// 安全降normal尚须专属执行协议，不能由策略低占用绕过Prepare的
		// 拒绝。资格是不可变来源，不能临时按比例修改成可派发的额度。
		eligible[i] = qualified.Limits.MemoryMaxBytes >= limits.MemoryMaxBytes && qualified.Limits.PidsMaxTasks == limits.PidsMaxTasks && qualified.Limits.SystemMemoryMaxBytes == limits.SystemMemoryMaxBytes && len(qualified.Limits.HugePools) == len(limits.HugePools)
		if eligible[i] {
			for j := range limits.HugePools {
				if qualified.Limits.HugePools[j].PageSizeKB != limits.HugePools[j].PageSizeKB {
					eligible[i] = false
				}
			}
		}
	}
	latest, err := w.session.store.Load(ctx)
	if err != nil {
		return result, err
	}
	if latest.ResourceVersion() != s.ResourceVersion() || latest.State().Owner != m.Owner || !latest.Dispatchable() {
		return result, fmt.Errorf("automatic VM or original journal changed during observation")
	}
	return policy.Sample{At: started, Instance: instance, CapacityGeneration: pg.CapacityGeneration, Current: policy.Target{VMMemoryBytes: control.Uint64(vmBytes), Buffers: pg.HighBuffers}, NormalCurrent: view.Actual.MemoryCurrentBytes, NormalMax: limits.MemoryMaxBytes, ClientProcesses: work.ClientProcesses, AdmissionOpen: pg.AdmissionOpen, Eligible: eligible}, nil
}

// advanceAutomatic仅由Run在普通Advance空闲时调用。唯一自动写入是原Begin
// 的UID/RV CAS，物理动作随后由既有步骤执行；管理入口与它不能并行写SDK。
// Pending保存完整Begin：回复丢失先读同一WAL，不重新取样、生成ID/期限或目标。
// 该本地栅栏只涉及“提交意图”，绝不能清PG/Root/Runner的独立执行未知。
func (w *PGWorker) advanceAutomatic(ctx context.Context) (pgexec.Progress, error) {
	// Pending恢复会先于普通Advance运行，故必须独立执行同一Session检查，
	// 不能因为日志owner未变就绕过Lease失效；全部读写也受原Session取消。
	if err := w.session.Check(ctx); err != nil {
		return pgexec.AwaitingFact, err
	}
	callCtx, cancel := w.session.coupledContext(ctx)
	defer cancel()
	return w.advanceAutomaticChecked(callCtx)
}

func (w *PGWorker) advanceAutomaticChecked(ctx context.Context) (pgexec.Progress, error) {
	d, now := w.automatic, time.Now()
	if d.pending == nil && now.Before(d.next) {
		return pgexec.Idle, nil
	}
	d.next = now.Add(time.Duration(d.config.SampleIntervalSeconds) * time.Second)
	s, err := w.session.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, fmt.Errorf("%w: %w", ErrPolicyObservation, err)
	}
	m := s.State()
	if m.Owner != w.session.Actor() || !s.Dispatchable() {
		return pgexec.AwaitingFact, ErrLeadershipLost
	}
	if d.pending != nil {
		// 完整原日志已含此Key，或已由更高/冲突水位取代时，只解除本地
		// Begin投递栅栏。原效果/终态继续由日志各域负责，不能宣称NoEffect。
		if m.Lookup(*d.pending) != journal.Stale {
			d.pending, d.pendingRV = nil, ""
			d.engine.Accepted(now)
			return pgexec.Reconciled, nil
		}
		if d.pendingRV != "" && s.ResourceVersion() != d.pendingRV {
			// 最后一次发送固定旧RV；最新完整日志既未接收该Key，且RV已
			// 前进，任何延迟旧CAS都必然失败。仅清本地投递意图，不改
			// PG/Root/VM执行未知，也不发布NoEffect领域回执。
			d.pending, d.pendingRV = nil, ""
			d.engine.Reset()
			return pgexec.Reconciled, nil
		}
		if m.Active != nil {
			return pgexec.AwaitingFact, nil
		}
	} else {
		sample, err := w.sampleAutomatic(ctx, s)
		if err != nil {
			d.engine.Reset()
			return pgexec.AwaitingFact, fmt.Errorf("%w: %w", ErrPolicyObservation, err)
		}
		target, selected, err := d.engine.Evaluate(time.Now(), sample)
		if err != nil {
			return pgexec.AwaitingFact, fmt.Errorf("%w: %w", ErrPolicyObservation, err)
		}
		if !selected {
			return pgexec.Idle, nil
		}
		id, err := control.NewOperationID()
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		revision := control.Uint64(1)
		if m.HighWater != nil && m.HighWater.IssuerEpoch == m.Owner.Epoch {
			if m.HighWater.Revision == control.Uint64(math.MaxUint64) {
				return pgexec.AwaitingFact, fmt.Errorf("automatic memory revision exhausted")
			}
			revision = m.HighWater.Revision + 1
		}
		kind := control.Grow
		if target.VMMemoryBytes < sample.Current.VMMemoryBytes || target.Buffers < sample.Current.Buffers {
			kind = control.Shrink
		}
		deadline := time.Now().Add(time.Duration(d.config.RequestDeadlineSeconds) * time.Second).UnixMilli()
		if deadline <= 0 {
			return pgexec.AwaitingFact, fmt.Errorf("invalid automatic memory wall clock")
		}
		request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: sample.Instance, IssuerEpoch: m.Owner.Epoch, Revision: revision, OperationID: id}, Kind: kind, ExpectedCapacityGeneration: sample.CapacityGeneration, TargetBuffers: target.Buffers, TargetVMMemoryBytes: target.VMMemoryBytes, BudgetCommitmentID: fmt.Sprintf("automatic-%016x%016x", id.High, id.Low), DeadlineUnixMillis: control.Uint64(deadline)}
		if err := request.Validate(); err != nil {
			return pgexec.AwaitingFact, err
		}
		d.pending = &request
	}
	// 过期并不证明旧Begin未发生。仅在原Key仍未接收时，用原RV做
	// 独立日志序列CAS屏障；成功或后续读到RV已前进才排除延迟旧CAS。
	// 回复丢失继续保留原请求与最后发送RV，不能换Key或延长原期限。
	millis := time.Now().UnixMilli()
	if millis <= 0 {
		return pgexec.AwaitingFact, fmt.Errorf("%w: invalid automatic wall clock", ErrPolicyObservation)
	}
	if uint64(millis) >= uint64(d.pending.DeadlineUnixMillis) {
		_, err := w.session.store.FenceUnacceptedBegin(ctx, s, w.session.Actor(), *d.pending, uint64(millis))
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		d.pending, d.pendingRV = nil, ""
		d.engine.Reset()
		return pgexec.Reconciled, nil
	}
	d.pendingRV = s.ResourceVersion()
	_, disposition, err := w.session.store.Begin(ctx, s, w.session.Actor(), *d.pending, uint64(millis))
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if disposition == journal.Accepted || disposition == journal.Replayed {
		d.pending, d.pendingRV = nil, ""
		d.engine.Accepted(time.Now())
		return pgexec.Recorded, nil
	}
	return pgexec.AwaitingFact, fmt.Errorf("%w: original Begin not accepted", ErrPolicyObservation)
}
