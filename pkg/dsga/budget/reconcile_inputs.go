package budget

import (
	"fmt"
	"math"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ReconcileInputs必须由原实例的认证读者逐项提供，不能直接从公开请求JSON构造。
// Prepared仍保留原目标；Outside可取本请求或旧完整预算，但其目标VM必须准确
// 等于当前实际VM，以免拿“小VM系统资格”替仍大的Guest覆盖内核/系统成本。
type ReconcileInputs struct {
	Prepared               Prepared
	Shared                 control.SharedCostResponse
	Root                   RootAggregate
	Outside                OutsideReservation
	OutsideClaim           ObjectRef
	Host                   HostReservation
	HostClaim              ObjectRef
	VM                     NativeCapacity
	InitialVM              *InitialVMSources
	GuestUsableMemoryBytes control.Uint64
	IssuerEpoch            control.Uint64
}

// Reconcile只生产自洽、去重的实际容量预算，不产生执行权或释放任何旧来源。
// 真正协调器须在Start/发送/Record/Finish前后重读原来源，并由同VM UID/RV
// CAS线性化；此纯函数不跨网络加锁，也不将结构验证冒充平台资格认证。
func Reconcile(i ReconcileInputs) (ReconciledCapacity, error) {
	return reconcile(i, nil)
}

func reconcile(i ReconcileInputs, initial *Initial) (ReconciledCapacity, error) {
	var c ReconciledCapacity
	p, s, root, out, host := i.Prepared, i.Shared, i.Root, i.Outside, i.Host
	vmBytes, instance := i.VM.MemoryBytes(), i.VM.Request.Key.Instance
	var noNative NativeCapacity
	if i.InitialVM != nil {
		if initial == nil || i.VM != noNative || i.InitialVM.MatchInitial(*initial) != nil {
			return c, fmt.Errorf("initial VM inputs cannot mix native commands or lose boot budget")
		}
		vmBytes, instance = i.InitialVM.MemoryBytes, i.InitialVM.Instance
	} else if i.VM.Validate() != nil {
		return c, fmt.Errorf("reconciled original native capacity is invalid")
	}
	incarnation := out.Incarnation
	system, transition, safety := out.SystemOutsidePG, out.Transition, out.Safety
	profile := out.ResidualProfileID
	var noOutside OutsideReservation
	var noRef ObjectRef
	if initial == nil {
		if out.ValidateSource() != nil || out.VMShrinkPermit || !out.HoldUntilExplicitSettlement || out.Identity != p.Identity || control.ValidateIncarnation(incarnation) != nil {
			return c, fmt.Errorf("reconciled outside lost original qualified operation")
		}
	} else {
		// 固定仍被原Initial实际覆盖的Guest，不以同数值root上限猜测
		// 原出生/命令相同；MatchCurrent逐项绑定完整原聚合与PG容量。
		if out != noOutside || i.OutsideClaim != noRef || initial.PGAggregate.Identity != p.Identity || initial.MatchCurrent(s, root, i.IssuerEpoch, vmBytes, i.GuestUsableMemoryBytes) != nil {
			return c, fmt.Errorf("reconciled initial inputs differ from original live boot budget")
		}
		incarnation = initial.Incarnation
		system, transition, safety = initial.SystemOutsidePG, initial.Transition, initial.Safety
		profile = "qualified-" + initial.Qualification.SHA256
	}
	if p.ValidateFor(p.Request, p.Identity) != nil || s.ValidateFor(p.Request.Key.Instance) != nil || root.Validate() != nil || root.Identity != p.Identity ||
		i.IssuerEpoch < p.Request.Key.IssuerEpoch || s.Observed.IssuerFloorEpoch != i.IssuerEpoch || s.Observed.LowBuffers != s.Observed.HighBuffers ||
		host.ValidateFor(p, incarnation) != nil || host.ProtocolVersion != 2 || host.HostOverheadBytes == 0 ||
		instance != p.Request.Key.Instance || s.Observed.MaximumBuffers != p.Shared.Observed.MaximumBuffers || s.StartupMainUpperBytes != p.Shared.StartupMainUpperBytes || s.StartupArraysUpperBytes != p.Shared.StartupArraysUpperBytes {
		return c, fmt.Errorf("reconciled inputs lost original native, Guest or reservation context")
	}
	if s.Observed.CapacityGeneration == p.Request.ExpectedCapacityGeneration && s.Observed.HighBuffers != p.Shared.Observed.HighBuffers {
		return c, fmt.Errorf("reconciled unchanged PG generation changed original buffer prefix")
	}
	limits := root.Bootstrap.Limits
	if root.Held != nil {
		limits.MemoryMaxBytes, limits.OutsideUpperBytes, limits.HugePools = root.Held.MemoryMaxBytes, root.Held.OutsideUpperBytes, root.Held.HugePools
	}
	outside := uint64(0)
	for _, b := range []Bound{system, transition, safety} {
		if uint64(b.UpperBytes) > math.MaxUint64-outside {
			return c, fmt.Errorf("reconciled outside commitments overflow")
		}
		outside += uint64(b.UpperBytes)
	}
	if outside != uint64(limits.OutsideUpperBytes) {
		return c, fmt.Errorf("reconciled current root limits cannot contain original full commitments")
	}
	// Shared可能含由Guest HugeTLB池承担的数组，不把整份Shared再压入
	// normal memory.max。最终以Root聚合上界覆盖并只计整池一次；真实
	// 内核归属仍由Root确认，不能靠这里的预算运算证明物理行为。
	peak := max(p.VMBeforeMemoryBytes, p.Request.TargetVMMemoryBytes, vmBytes)
	if uint64(host.HostOverheadBytes) > math.MaxUint64-uint64(peak) || host.ReservedMemoryBytes < peak+host.HostOverheadBytes {
		return c, fmt.Errorf("reconciled Host hold does not cover original peak and actual capacity")
	}
	digest, err := RootDigest(root)
	if err != nil {
		return c, err
	}
	c = ReconciledCapacity{
		ProtocolVersion: 1, VMShrinkPermit: false, Request: p.Request, IssuerEpoch: i.IssuerEpoch,
		RunnerPodName: p.RunnerPodName, RunnerRestartCount: p.RunnerRestartCount,
		VMBeforeMemoryBytes: p.VMBeforeMemoryBytes, VMMemoryBytes: vmBytes, GuestUsableMemoryBytes: i.GuestUsableMemoryBytes,
		CapacityGeneration: s.Observed.CapacityGeneration, Buffers: s.Observed.HighBuffers, MaximumBuffers: s.Observed.MaximumBuffers,
		StartupMainUpperBytes: s.StartupMainUpperBytes, StartupArraysUpperBytes: s.StartupArraysUpperBytes, RetainedSharedUpperBytes: s.CurrentMainAndArraysUpperBytes, PGOperation: nil,
		PGAggregate: root.Bound, RootSHA256: digest, OutsideClaim: i.OutsideClaim, OutsideRequest: out.Request, ResidualProfileID: profile,
		SystemOutsidePG: system, Transition: transition, Safety: safety, HostClaim: i.HostClaim, HostReservationID: host.SourceID, VM: i.VM, InitialOutside: initial, InitialVM: nil,
	}
	if initial != nil {
		c.ProtocolVersion = 2
		c.GuestUsableMemoryBytes = initial.GuestUsableMemoryBytes
	}
	if i.InitialVM != nil {
		owned := *i.InitialVM
		c.ProtocolVersion, c.InitialVM = 3, &owned
	}
	if s.Observed.Operation != nil {
		operation := *s.Observed.Operation
		c.PGOperation = &operation
	}
	if err := c.Validate(); err != nil {
		return ReconciledCapacity{}, err
	}
	if err := c.ValidateCurrent(s, i.IssuerEpoch, c.VMMemoryBytes); err != nil {
		return ReconciledCapacity{}, err
	}
	return c, nil
}
