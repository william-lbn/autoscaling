package budget

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const (
	maxRecoveryCapacityBytes  = 128 * 1024
	maxRecoveryCapacityValues = 8192
)

// RecoveryPG保留原PG执行的独立串行对账来源，不改写原Unknown。完整终态和
// 更高floor隔离由既有原生合同区分；Fence/Status只能由固定认证Reader取得。
// 它不表示Guest/VM/Host完成，不赋予任何重发、开放或物理释放权限。
type RecoveryPG struct {
	Fence  control.FenceResponse   `json:"issuer_fence"`
	Status control.ControlResponse `json:"status"`
}

func (p *RecoveryPG) UnmarshalJSON(raw []byte) error {
	type plain RecoveryPG
	var value plain
	if err := control.UnmarshalObjectBounded(raw, &value, maxRecoveryCapacityBytes, maxRecoveryCapacityValues); err != nil {
		return err
	}
	*p = RecoveryPG(value)
	return nil
}

// v4的两个原PG来源互斥：已派发只能用完整独立对账；未派发只能固定原
// Prepared.Shared，不能凭当前数值把已派发Unknown投影成从未执行。原WAL
// 是否确为Planned由journal交叉验证，本包只核完整来源和预算不变量。
func (c ReconciledCapacity) validateServicePG() error {
	if c.ProtocolVersion != 4 || (c.RecoveryPG == nil) == (c.UnchangedPG == nil) {
		return fmt.Errorf("service actual capacity requires one explicit original PG source")
	}
	if c.RecoveryPG != nil {
		if c.PGOperation != nil {
			return fmt.Errorf("independent PG recovery cannot borrow an ordinary terminal head")
		}
		p := c.RecoveryPG
		if _, err := control.ValidateResizeRecovery(c.Request, p.Fence, p.Status); err != nil {
			return err
		}
		if c.IssuerEpoch < p.Fence.RequestedEpoch || !c.sameRecoveryCapacity(p.Status.Observed) || capacityKeyAlias(p.Status.Observed.Operation, c.Request.Key) {
			return fmt.Errorf("service actual capacity changed retained independent PG boundary")
		}
		return c.validateRecoveryCurrent(p.Status.Observed)
	}
	s := c.UnchangedPG
	if s.ValidateFor(c.Request.Key.Instance) != nil || s.Observed.IssuerFloorEpoch != c.Request.Key.IssuerEpoch || c.CapacityGeneration != c.Request.ExpectedCapacityGeneration ||
		!c.sameRecoveryCapacity(s.Observed) || s.Observed.LowBuffers != s.Observed.HighBuffers || c.StartupMainUpperBytes != s.StartupMainUpperBytes || c.StartupArraysUpperBytes != s.StartupArraysUpperBytes ||
		!reflect.DeepEqual(c.PGOperation, s.Observed.Operation) || c.PGOperation != nil && (c.PGOperation.Key == c.Request.Key || capacityKeyAlias(c.PGOperation, c.Request.Key) || compareCapacityKey(c.PGOperation.Key, c.Request.Key) > 0 || !terminalCapacityOperation(c.PGOperation)) {
		return fmt.Errorf("service unchanged PG source lost original undispatched capacity")
	}
	for _, array := range s.Observed.Arrays {
		if array.AllocationUnknown {
			return fmt.Errorf("service unchanged PG source contains unknown allocation")
		}
	}
	return nil
}

// 同一boot内固定启动主段/数组总量与稳定逻辑容量；不固定门禁或当前floor。
// 分项由SharedCostResponse的完整校验认证，纯持久合同仍单独防止乘加回绕。
func (c ReconciledCapacity) sameRecoveryCapacity(s control.Snapshot) bool {
	main, arrays := uint64(c.StartupMainUpperBytes), uint64(c.StartupArraysUpperBytes)
	return arrays <= math.MaxUint64-main && main+arrays == uint64(s.StartupSharedUpperBytes) &&
		s.Instance == c.Request.Key.Instance && s.CapacityGeneration == c.CapacityGeneration && s.LowBuffers == c.Buffers && s.HighBuffers == c.Buffers && s.MaximumBuffers == c.MaximumBuffers
}

func terminalCapacityOperation(op *control.OperationFact) bool {
	return op != nil && (op.Phase == control.Committed || op.Phase == control.Denied)
}

func capacityKeyAlias(op *control.OperationFact, key control.Key) bool {
	return op != nil && op.Key.IssuerEpoch == key.IssuerEpoch && op.Key.Revision == key.Revision && op.Key != key
}

func sameCapacityHead(a, b *control.OperationFact) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	x, y := *a, *b
	x.Replayed, y.Replayed = false, false
	return x == y
}

// 历史独立Proof保持不变；当前原生读者可因接管/再开放改变floor和准入。
// 容量不变不代表任意head均合法：原head不能回退或同位置换ID/上下文，
// 隔离的原Key也不能重现。仅允许严格更新且已终止的同容量后继head；
// 当前PREPARING始终拒绝，即使暂时Low==High。来源认证仍属于生产执行器。
func (c ReconciledCapacity) validateRecoveryCurrent(current control.Snapshot) error {
	if c.RecoveryPG == nil {
		if !reflect.DeepEqual(current.Operation, c.PGOperation) {
			return fmt.Errorf("undispatched original PG head changed")
		}
		return nil
	}
	p := c.RecoveryPG
	effect, err := control.ValidateResizeRecovery(c.Request, p.Fence, p.Status)
	if err != nil || !c.sameRecoveryCapacity(current) {
		return fmt.Errorf("current PG changed independently retained capacity")
	}
	op, originalHead := current.Operation, p.Status.Observed.Operation
	if op == nil {
		if originalHead != nil {
			return fmt.Errorf("current PG lost retained native head")
		}
		return nil
	}
	if !terminalCapacityOperation(op) || capacityKeyAlias(op, c.Request.Key) || originalHead != nil && capacityKeyAlias(op, originalHead.Key) {
		return fmt.Errorf("current PG head is in flight or aliases retained intent")
	}
	if op.Key == c.Request.Key {
		if effect == control.ResizeRecoveredIsolated || p.Status.Receipt.Result == nil || !control.SameResizeRecoveryTerminal(*p.Status.Receipt.Result, current) {
			return fmt.Errorf("current PG changed or resurrected original recovered terminal")
		}
	}
	if originalHead != nil {
		order := compareCapacityKey(op.Key, originalHead.Key)
		if order < 0 || order == 0 && !sameCapacityHead(op, originalHead) {
			return fmt.Errorf("current PG regressed or replaced retained native head")
		}
		if order == 0 {
			return nil
		}
	}
	if compareCapacityKey(op.Key, c.Request.Key) <= 0 {
		return fmt.Errorf("current PG successor did not advance original intent")
	}
	return nil
}

// 新构造器显式选择独立实际预算，旧Reconcile/ReconcileInitial继续生产原
// v1–3取消合同。所有输入先深拷贝保存来源；函数不访问网络、不调整限额。
func ReconcileRecovery(inputs ReconcileInputs, proof RecoveryPG) (ReconciledCapacity, error) {
	return reconcileService(inputs, nil, &proof, nil)
}

func ReconcileInitialRecovery(inputs ReconcileInputs, initial Initial, proof RecoveryPG) (ReconciledCapacity, error) {
	return reconcileService(inputs, &initial, &proof, nil)
}

func ReconcileServiceUnchanged(inputs ReconcileInputs, original control.SharedCostResponse) (ReconciledCapacity, error) {
	return reconcileService(inputs, nil, nil, &original)
}

func ReconcileInitialServiceUnchanged(inputs ReconcileInputs, initial Initial, original control.SharedCostResponse) (ReconciledCapacity, error) {
	return reconcileService(inputs, &initial, nil, &original)
}

func reconcileService(inputs ReconcileInputs, initial *Initial, proof *RecoveryPG, unchanged *control.SharedCostResponse) (ReconciledCapacity, error) {
	// JSON往返复用严格有界协议解码，独立拥有三份原生Snapshot中的数组、
	// ReleaseKey、Operation和历史Result；不依赖浅拷贝共享可变指针。
	sources := struct {
		Initial   *Initial                    `json:"initial"`
		Recovery  *RecoveryPG                 `json:"recovery"`
		Unchanged *control.SharedCostResponse `json:"unchanged"`
	}{Initial: initial, Recovery: proof, Unchanged: unchanged}
	raw, err := json.Marshal(sources)
	if err != nil {
		return ReconciledCapacity{}, err
	}
	var owned struct {
		Initial   *Initial                    `json:"initial"`
		Recovery  *RecoveryPG                 `json:"recovery"`
		Unchanged *control.SharedCostResponse `json:"unchanged"`
	}
	if err := control.UnmarshalObjectBounded(raw, &owned, maxRecoveryCapacityBytes, maxRecoveryCapacityValues); err != nil {
		return ReconciledCapacity{}, err
	}
	return reconcile(inputs, owned.Initial, owned.Recovery, owned.Unchanged)
}
