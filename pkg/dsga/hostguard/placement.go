package hostguard

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"

	core "k8s.io/api/core/v1"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

const MaxPlacements = 256

// Profile是可信启动配置中固定的Host开销/调度域，不是请求自报的资格证明。
// ID须对应目标平台实际认证；本代码认证持有和算术，不从ID猜测物理上界。
// v3的OutsideBytes只覆盖固定系统域，其余普通Pod/旧VM/DSGA VM全部入Placements。
type Profile struct {
	ID               string         `json:"id"`
	SchedulerName    string         `json:"scheduler_name"`
	VMOverheadBytes  control.Uint64 `json:"vm_overhead_bytes"`
	PodOverheadBytes control.Uint64 `json:"pod_overhead_bytes"`
}

func (p Profile) Validate() error {
	if control.ValidateID(p.ID, "qualified Host profile") != nil || control.ValidateID(p.SchedulerName, "Host scheduler") != nil || p.VMOverheadBytes == 0 || p.PodOverheadBytes == 0 {
		return fmt.Errorf("invalid fixed Host allocation profile")
	}
	return nil
}

func (p *Profile) UnmarshalJSON(raw []byte) error {
	type plain Profile
	var v plain
	if err := control.UnmarshalObject(raw, &v); err != nil {
		return err
	}
	*p = Profile(v)
	return p.Validate()
}

// Placement独立于PG boot/首个Resize。原Pod UID第一次获准调度前就持久保留；
// 重启从原Node账本恢复，Pod/VM传播注解不能降低它。普通Pod的MemoryBytes必须
// 来自完整硬限额，VM来自原VM的Use/已批准/实际值与本次目标最大值，不打折overcommit。
type Placement struct {
	Namespace         string         `json:"namespace"`
	PodName           string         `json:"pod_name"`
	PodUID            string         `json:"pod_uid"`
	VMName            string         `json:"vm_name"`
	VMUID             string         `json:"vm_uid"`
	Incarnation       string         `json:"incarnation"`
	Managed           bool           `json:"managed"`
	MemoryBytes       control.Uint64 `json:"memory_bytes"`
	HostOverheadBytes control.Uint64 `json:"host_overhead_bytes"`
	// 仅v4原owner结清时设置；普通容量写者不可构造或清除它。
	Settled *Settlement `json:"settled,omitempty"`
	// Barrier由Node原owner封存。普通容量写者不得构造、替换或清空；
	// 它独立于结清水位，原Hold退休后仍永久阻止旧Key执行。
	Barrier *budget.HostReservationBarrier `json:"barrier,omitempty"`
}

func (p Placement) Bytes() (control.Uint64, error) {
	for _, s := range []string{p.Namespace, p.PodName, p.PodUID} {
		if control.ValidateID(s, "original Pod allocation") != nil {
			return 0, fmt.Errorf("invalid original Pod allocation identity")
		}
	}
	if (p.VMName == "") != (p.VMUID == "") || p.VMUID != "" && (control.ValidateID(p.VMName, "VM name") != nil || control.ValidateID(p.VMUID, "VM UID") != nil) {
		return 0, fmt.Errorf("invalid original Pod VM owner")
	}
	if p.Managed && (p.VMUID == "" || control.ValidateIncarnation(p.Incarnation) != nil) || !p.Managed && p.Incarnation != "" {
		return 0, fmt.Errorf("invalid original managed VM incarnation")
	}
	if p.MemoryBytes == 0 || p.HostOverheadBytes == 0 || p.HostOverheadBytes > control.Uint64(math.MaxUint64)-p.MemoryBytes {
		return 0, fmt.Errorf("pod memory plus Host overhead is zero or overflows")
	}
	if p.Settled != nil && (!p.Managed || p.Settled.Validate() != nil || p.Settled.Request.Key.Instance.VMUID != p.VMUID || p.MemoryBytes < p.Settled.MemoryBytes()) {
		return 0, fmt.Errorf("pod placement changed original closed budget watermark")
	}
	if p.Barrier != nil && (!p.Managed || p.Barrier.Validate() != nil || p.Barrier.Namespace != p.Namespace || p.Barrier.VMName != p.VMName || p.Barrier.RunnerPodUID != p.PodUID || p.Barrier.Request.Key.Instance.VMUID != p.VMUID || p.Barrier.Incarnation != p.Incarnation || p.Barrier.HostOverheadBytes != p.HostOverheadBytes) {
		return 0, fmt.Errorf("pod placement changed original prepared barrier domain")
	}
	return p.MemoryBytes + p.HostOverheadBytes, nil
}

func (p *Placement) UnmarshalJSON(raw []byte) error {
	type plain Placement
	var v plain
	if err := control.UnmarshalObject(raw, &v); err != nil {
		return err
	}
	*p = Placement(v)
	_, err := p.Bytes()
	return err
}

func (p Placement) sameOriginal(q Placement) bool {
	p.MemoryBytes, q.MemoryBytes = 0, 0
	p.Settled, q.Settled = nil, nil
	p.Barrier, q.Barrier = nil, nil
	return p == q
}

func (m Model) validatePlacements() error {
	if m.ProtocolVersion < 3 {
		if m.Profile != nil || m.Placements != nil {
			return fmt.Errorf("legacy Host ledger cannot project new allocation domains")
		}
		return nil
	}
	if m.Profile == nil || m.Profile.Validate() != nil || m.OutsideBytes == 0 || m.Placements == nil || *m.Placements == nil || len(*m.Placements) > MaxPlacements {
		return fmt.Errorf("host allocation ledger requires complete v3 profile/placements")
	}
	for i, p := range *m.Placements {
		if err := m.validatePreparedBarrier(p); err != nil {
			return err
		}
		var noInitial budget.InitialVMSources
		if p.Settled != nil && p.Settled.InitialVM != noInitial && m.ProtocolVersion < 8 {
			return fmt.Errorf("initial-source settlement requires Node v8")
		}
		if p.Settled != nil && m.ProtocolVersion < 4 {
			return fmt.Errorf("settled Host capacity requires explicit v4 semantics")
		}
		if p.Settled != nil && p.Settled.ThroughSequence != 0 && m.ProtocolVersion < 6 {
			return fmt.Errorf("closed-through Host capacity requires explicit v6 semantics")
		}
		if p.Settled != nil && p.Settled.ActualMemoryBytes != 0 && m.ProtocolVersion < 7 {
			return fmt.Errorf("actual-capacity settlement requires Node v7")
		}
		if _, err := p.Bytes(); err != nil {
			return err
		}
		overhead := m.Profile.PodOverheadBytes
		if p.VMUID != "" {
			overhead = m.Profile.VMOverheadBytes
		}
		if p.HostOverheadBytes != overhead {
			return fmt.Errorf("pod allocation changed fixed qualified Host overhead")
		}
		for _, old := range (*m.Placements)[:i] {
			if old.PodUID == p.PodUID {
				return fmt.Errorf("duplicate original Pod UID in allocation ledger")
			}
			if old.VMUID != "" && old.VMUID == p.VMUID && (old.Namespace != p.Namespace || old.VMName != p.VMName || old.Managed != p.Managed || old.Incarnation != p.Incarnation) {
				return fmt.Errorf("original VM allocation changed name/domain/incarnation")
			}
		}
		if p.Settled != nil {
			matched := false
			for _, h := range m.Holds {
				if h.Plan.RunnerPodUID != p.PodUID {
					continue
				}
				order := compare(h.Plan.Request.Key, p.Settled.Request.Key)
				if order < 0 || order > 0 && compare(h.Plan.Request.Key, p.Settled.ClosedThrough()) <= 0 {
					return fmt.Errorf("settled Host ledger retains a stale executable hold")
				}
				if order == 0 {
					if h.Plan.Request != p.Settled.Request || h.Claim == nil || *h.Claim != p.Settled.HostClaim {
						return fmt.Errorf("settled Host source changed original claim/plan")
					}
					if p.Settled.MemoryBytes() != h.Plan.BeforeBytes && p.Settled.MemoryBytes() != h.Plan.Request.TargetVMMemoryBytes {
						return fmt.Errorf("settled actual memory is outside original Host capacity endpoints")
					}
					matched = true
				}
			}
			if !matched {
				return fmt.Errorf("settled Pod has no original current Host claim")
			}
		}
	}
	for _, h := range m.Holds {
		p, err := m.Placement(h.Plan.RunnerPodUID)
		if err != nil || !p.Managed || p.VMUID != h.Plan.Request.Key.Instance.VMUID || p.VMName != h.Plan.VMName || p.Namespace != h.Plan.Namespace || p.Incarnation != h.Plan.Incarnation || p.HostOverheadBytes != h.Plan.HostOverheadBytes {
			return fmt.Errorf("host operation has no matching original Pod placement")
		}
	}
	return nil
}

func (m Model) Placement(podUID string) (Placement, error) {
	if m.ProtocolVersion >= 3 && m.Placements != nil {
		for _, p := range *m.Placements {
			if p.PodUID == podUID {
				if p.Settled != nil {
					copySettlement := *p.Settled
					p.Settled = &copySettlement
				}
				if p.Barrier != nil {
					barrier := p.Barrier.Clone()
					p.Barrier = &barrier
				}
				return p, nil
			}
		}
	}
	return Placement{}, fmt.Errorf("original Pod placement is not durably held")
}

// reservePlacement只增加同一原Pod的持有高水位。它是多写者容量CAS，不授Host
// 来源发布/释放资格；因此不借全局Scheduler leader和节点owner恰好同副本才能
// 调度。Actor/Node由Store跨网络核验，只有原UID/RV写确认后才能返回调度成功。
func (m Model) reservePlacement(p Placement, capacity control.Uint64) (Model, error) {
	return m.reservePlacementFor(p, nil, capacity)
}

// key只由可信Scheduler从本次真实原VM Active派生，数值目标不是新请求授权。
// nil保留旧入口，但在已有永久水位的Managed行上仅允许保留旧容量。
func (m Model) reservePlacementFor(p Placement, key *control.Key, capacity control.Uint64) (Model, error) {
	if m.Validate() != nil || m.ProtocolVersion < 3 || p.Settled != nil || p.Barrier != nil {
		return m, fmt.Errorf("pod allocation requires original complete v3 Host domain")
	}
	if _, err := p.Bytes(); err != nil {
		return m, err
	}
	if key != nil && (key.Validate() != nil || !p.Managed || key.Instance.VMUID != p.VMUID) {
		return m, fmt.Errorf("keyed placement requires complete original managed VM request")
	}
	if used, err := m.UsedBytes(); err != nil || used > capacity {
		return m, fmt.Errorf("existing Host allocations exceed current Node capacity")
	}
	next := m.Clone()
	found := false
	for i, old := range *next.Placements {
		if old.PodUID != p.PodUID {
			continue
		}
		if !old.sameOriginal(p) {
			return m, fmt.Errorf("same Pod UID changed original allocation context")
		}
		found = true
		// 检查必须先于同数值幂等返回：旧Key不能通过目标未变化重获资格；
		// 无Key只允许保留旧额度，不能清空或跨越Barrier/Through。
		if err := old.checkPlacementKey(key, p.MemoryBytes); err != nil {
			return m, err
		}
		if p.MemoryBytes <= old.MemoryBytes {
			return m, nil
		}
		(*next.Placements)[i].MemoryBytes = p.MemoryBytes
		break
	}
	if !found {
		if len(*next.Placements) == MaxPlacements {
			return m, fmt.Errorf("pod allocation bound exhausted; explicit settlement required")
		}
		*next.Placements = append(*next.Placements, p)
	}
	if next.Sequence == control.Uint64(math.MaxUint64) {
		return m, fmt.Errorf("host allocation sequence exhausted")
	}
	next.Sequence++
	if err := next.Validate(); err != nil {
		return m, err
	}
	used, err := next.UsedBytes()
	if err != nil || used > capacity {
		return m, fmt.Errorf("insufficient Node capacity for complete Pod allocation")
	}
	return next, nil
}

func clonePlacements(p *[]Placement) *[]Placement {
	if p == nil {
		return nil
	}
	copyRows := slices.Clone(*p)
	for i := range copyRows {
		if copyRows[i].Barrier != nil {
			barrier := copyRows[i].Barrier.Clone()
			copyRows[i].Barrier = &barrier
		}
		if copyRows[i].Settled != nil {
			copySettlement := *copyRows[i].Settled
			copyRows[i].Settled = &copySettlement
		}
	}
	return &copyRows
}

// InspectInitialVM供最终Runner在启动QEMU前使用。调用者须以自己的可信
// 客户端真实GET Node，不能接收请求带来的账本。首次启动尚无PG boot/Resize，
// 仍必须已有原Pod/VM/incarnation的完整容量；这不是后续伸缩动作许可。
func InspectInitialVM(node *core.Node, vm journal.Binding, podName, podUID string, memory control.Uint64) error {
	if node == nil || memory == 0 {
		return fmt.Errorf("initial Runner requires original Node and positive memory")
	}
	var m Model
	if err := json.Unmarshal([]byte(node.Annotations[LedgerAnnotation]), &m); err != nil {
		return fmt.Errorf("initial Runner Host ledger is unavailable: %w", err)
	}
	if _, err := inspect(node, m.Binding); err != nil || m.ProtocolVersion < 3 || node.DeletionTimestamp != nil {
		return fmt.Errorf("initial Runner requires protected complete v3 Host domain")
	}
	p, err := m.Placement(podUID)
	if err != nil || !p.Managed || p.Namespace != vm.Namespace || p.PodName != podName || p.VMName != vm.Name || p.VMUID != string(vm.VMUID) || p.Incarnation != vm.Incarnation || p.MemoryBytes < memory {
		return fmt.Errorf("initial Runner memory is not held for its original Pod/VM")
	}
	used, usedErr := m.UsedBytes()
	available, capErr := capacity(node)
	if usedErr != nil || capErr != nil || used > available {
		return fmt.Errorf("initial Runner Host domain exceeds current Node capacity")
	}
	return nil
}
