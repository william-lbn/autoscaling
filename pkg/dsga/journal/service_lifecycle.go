package journal

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// ServiceLifecycle是一次业务重开的完整完成证据，不是当前活性租约。Context
// 保存原关闭/接续/rearm/新开放、原预算/首次未知/身份/水位的完整冻结原文；
// 禁止嵌入前一个Lifecycle，因此空间与运行轮数无关。Current只接受完成前
// 重新审计后的同一原生开放和有效有限期限，不能用历史Applied或较新open替代。
//
// 下一轮完成时只退休已经被严格更高原生revision覆盖的旧准入选择，保存
// Previous水位与完整原生边界；不把旧Unknown改成成功、NoEffect或资源释放。
// 首次未知另在InitialHeld.Covered保存覆盖证明，直到正常预算协议原子替换
// 原预算。13/14依赖完整资源结算，15另须完整独立结算证明；同CAS保存
// 原Operation和覆盖终态。18取消另保存准确预算复核与独立取消结果，原
// Cleanup不改；16/17资源仍在途时不能借本归档裁掉未知。
type ServiceLifecycle struct {
	// 必填门禁阶段独立于可选当前关闭，防止删除新关闭字段后投影为
	// 上一周期已经恢复服务。只有新关闭写前或完整完成CAS可以改变。
	SafetyStage           string                         `json:"safety_stage"`
	Cycle                 control.Uint64                 `json:"cycle"`
	Origin                Owner                          `json:"origin"`
	RecordedSequence      control.Uint64                 `json:"recorded_sequence"`
	Context               []byte                         `json:"context"`
	ContextSHA256         string                         `json:"context_sha256"`
	Current               control.AdmissionLeaseResponse `json:"current"`
	Previous              *ServiceRetirement             `json:"previous"`
	ContextRetainedSHA256 string                         `json:"context_retained_sha256,omitempty"`
}

type ServiceRetirement struct {
	Cycle            control.Uint64                 `json:"cycle"`
	RecordedSequence control.Uint64                 `json:"recorded_sequence"`
	EvidenceSHA256   string                         `json:"evidence_sha256"`
	Current          control.AdmissionLeaseResponse `json:"current"`
}

// InitialCoverage与原InitialAdmission分开，原Phase/命令/回执永不回写。
// 它仅证明旧开放的准入revision已被另一个准确成功开放覆盖；原预算仍
// 逐字持有，不能从此推导Root/Host释放、连接排空或VM回收许可。
type InitialCoverage struct {
	RecordedSequence control.Uint64            `json:"recorded_sequence"`
	Request          control.AdmissionRequest  `json:"request"`
	Receipt          control.AdmissionResponse `json:"receipt"`
	Query            bool                      `json:"query"`
}

// ClosureFormatVersion返回当前关闭的完整格式（包括rearm/reopen外层），
// 不把生命周期外层21当关闭类别。没有关闭时返回0；普通预算版本不依赖它。
func (m Model) ClosureFormatVersion() uint32 {
	if m.ServiceClosure == nil {
		return 0
	}
	if m.ProtocolVersion >= 21 {
		return m.ServiceClosure.FormatVersion
	}
	return m.ProtocolVersion
}

func (m *Model) setClosureFormatVersion(version uint32) {
	if m.ServiceLifecycle != nil {
		m.ProtocolVersion = 21
		if m.RetainedCoverage != nil {
			m.ProtocolVersion = 22
		}
		m.ServiceClosure.FormatVersion = version
		m.ServiceLifecycle.SafetyStage = "closing"
	} else {
		m.ProtocolVersion = version
	}
}

func recoveryContext(raw []byte) (Model, error) {
	// 避免调用Model.UnmarshalJSON的顶层版本要求；这是去掉生命周期的
	// 冻结上下文，只允许格式20。仍使用同一严格、有界JSON解码器和完整
	// core校验，不能借此读当前VM或跳过旧关闭/预算不变量。
	type plain Model
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, MaxRawBytes/2, 16384); err != nil {
		return Model{}, err
	}
	m := Model(p)
	if m.ServiceLifecycle != nil || m.ProtocolVersion != 20 || m.ServiceClosure == nil || m.ServiceClosure.FormatVersion != 0 {
		return m, fmt.Errorf("recovery context must preserve one nonrecursive original reopen")
	}
	return m, m.validateCore()
}

// reopenCompletionCapacity在派发票据产生前预留冻结上下文的原生回执空间。
// 压缩大小不能代替原始JSON界限；大量可压缩历史同样不得先开放、再发现
// 无法永久保存完整恢复证据。这里不截断原预算/未知，不生成领域成功事实。
func (m Model) reopenCompletionCapacity() error {
	frozen := m.Clone()
	frozen.ProtocolVersion, frozen.ServiceLifecycle = 20, nil
	if frozen.RetainedCoverage != nil {
		raw, err := json.Marshal(frozen.RetainedCoverage)
		if err != nil {
			return err
		}
		frozen.RetainedContextSHA256 = rawDigest(raw)
	}
	frozen.RetainedCoverage = nil
	frozen.ServiceClosure.FormatVersion = 0
	raw, err := json.Marshal(frozen)
	if err != nil {
		return err
	}
	if len(raw)+MaxObservationBytes+4096 > MaxRawBytes/2 {
		return fmt.Errorf("service reopen cannot retain complete bounded lifecycle evidence")
	}
	if m.Active != nil {
		// 13～15完成CAS同时保留完整冻结上下文、原Operation终态和
		// 独立覆盖。按未来原回执上限预留JSON及base64膨胀，不能只看
		// 可压缩字节，或先开放再发现完整原Unknown无法永久归档。
		future := len(raw) + MaxObservationBytes + 4096
		extra := 2*MaxObservationBytes + 4096
		if frozen.ServiceClosure.Finalization != nil {
			original, err := json.Marshal(frozen.Active)
			if err != nil {
				return err
			}
			// 独立结算完成后，永久预算也保留完整原操作及恢复证据。
			// 它与结果/生命周期各负其责，预留不能只按历史原Active一份。
			extra += len(original) + 3*MaxObservationBytes + MaxCommandBytes
		}
		if p := frozen.ServiceClosure.CleanupRecovery; p != nil {
			original, err := json.Marshal(frozen.Active)
			if err != nil {
				return err
			}
			proof, err := json.Marshal(p)
			if err != nil {
				return err
			}
			// 取消覆盖另存原Operation与独立证明；实际容量永久头还保存
			// 原未知及同一证明。开放前预留，不能完成时截断原资源证据。
			extra += len(original) + 2*len(proof)
		}
		if p := frozen.ServiceClosure.RetainedRecovery; p != nil {
			proof, err := json.Marshal(p)
			if err != nil {
				return err
			}
			original, err := json.Marshal(frozen.Active)
			if err != nil {
				return err
			}
			extra += len(original) + 2*len(proof)
		}
		if p := frozen.ServiceClosure.ActualRecovery; p != nil {
			original, err := json.Marshal(frozen.Active)
			if err != nil {
				return err
			}
			proof, err := json.Marshal(p)
			if err != nil {
				return err
			}
			// 原操作、完整实际预算、永久头和覆盖各自承担责任，预留
			// 原文及base64开销，不能先开放再截断原Unknown或来源证明。
			extra += len(original) + 2*len(proof)
		}
		if future+base64.StdEncoding.EncodedLen(future)+extra > MaxRawBytes {
			return fmt.Errorf("committed service cannot retain atomic operation coverage")
		}
	}
	return nil
}

func validLiveReopen(b control.AdmissionLeaseResponse, q control.AdmissionRequest, actor Owner) bool {
	return b.ValidateFor(b.Request, q.Instance) == nil && b.Request.Action == "observe" && b.Request.IssuerEpoch == actor.Epoch &&
		b.Armed && !b.Expired && b.Observed.Instance == q.Instance && b.Observed.IssuerFloorEpoch == actor.Epoch &&
		b.LeaseEpoch <= actor.Epoch && b.Observed.AdmissionOpen && b.Observed.AdmissionRevision == q.Revision && b.Observed.CapacityGeneration == q.ExpectedCapacityGeneration
}

func (m Model) validateServiceLifecycle() error {
	l := m.ServiceLifecycle
	if (m.ProtocolVersion >= 21) != (l != nil) {
		return fmt.Errorf("journal must retain required service lifecycle evidence")
	}
	if l == nil {
		// 永久恢复预算在当前账本必须有已完成生命周期，禁止删掉
		// Lifecycle并降级格式冒充普通预算。冻结上下文有意不递归
		// 保存旧Lifecycle，只由recoveryContext调用validateCore校验；
		// 不能把这个当前服务要求放进永久预算的结构自洽检查。
		if m.BudgetHeld != nil && m.BudgetHeld.Recovery != nil || m.CapacityHeld != nil && (m.CapacityHeld.Recovery != nil || m.CapacityHeld.ActiveRecovery != nil) {
			return fmt.Errorf("recovered budget requires retained service lifecycle")
		}
		for _, record := range m.History {
			if record.ServiceCoverage != nil {
				return fmt.Errorf("covered resource completion requires retained service lifecycle")
			}
		}
		if m.ServiceClosure != nil && m.ServiceClosure.FormatVersion != 0 || m.InitialHeld != nil && m.InitialHeld.Covered != nil {
			return fmt.Errorf("legacy journal cannot project lifecycle fields")
		}
		return nil
	}
	if m.ServiceClosure == nil && l.SafetyStage != "serving" || m.ServiceClosure != nil && l.SafetyStage != "closing" {
		return fmt.Errorf("service lifecycle lost required current safety stage")
	}
	c, err := recoveryContext(l.Context)
	if err != nil {
		return err
	}
	r := c.ServiceClosure.Reopen
	if m.RetainedContextSHA256 != "" || c.RetainedContextSHA256 != l.ContextRetainedSHA256 {
		return fmt.Errorf("retained lifecycle context lost exact frozen slot retirement digest")
	}
	if c.ServiceClosure.RetainedRecovery != nil && m.RetainedCoverage == nil {
		return fmt.Errorf("retained lifecycle lost permanent original coverage")
	}
	if err := m.validateContextRetained(l); err != nil {
		return err
	}
	if l.Cycle == 0 || l.Origin.Validate() != nil || l.Origin != c.Owner || l.Origin.Epoch > m.Owner.Epoch || l.Origin.Epoch == m.Owner.Epoch && l.Origin != m.Owner || l.RecordedSequence <= c.Sequence || l.RecordedSequence > m.Sequence || l.ContextSHA256 != rawDigest(l.Context) || c.VMUID != m.VMUID || c.Incarnation != m.Incarnation || c.PGInstance == nil || m.PGInstance == nil || *c.PGInstance != *m.PGInstance || !reflect.DeepEqual(c.ResourceIdentity, m.ResourceIdentity) || c.stableServiceRecovery() != nil || r == nil || r.Phase != Settled || r.Closing != nil || !validLiveReopen(l.Current, r.Request, l.Origin) || l.Current.DeadlineNS < r.Before.DeadlineNS || l.Current.LeaseEpoch < r.Before.LeaseEpoch {
		return fmt.Errorf("service lifecycle lacks exact audited original completion")
	}
	if err := m.validateCommittedLifecycle(c, l); err != nil {
		return err
	}
	if p := l.Previous; p != nil {
		// 完整旧原生边界与累积摘要保留，较新成功关闭/开放才可退休旧
		// 准入选择。同PG容量只能前进，不能退到另一boot或较旧revision。
		if p.Cycle == 0 || p.Cycle == control.Uint64(math.MaxUint64) || l.Cycle != p.Cycle+1 || p.RecordedSequence == 0 || p.RecordedSequence >= c.ServiceClosure.StartedSequence || !lifecycleDigest(p.EvidenceSHA256) || p.Current.ValidateFor(p.Current.Request, *m.PGInstance) != nil || p.Current.Request.Action != "observe" || !p.Current.Armed || p.Current.Expired || !p.Current.Observed.AdmissionOpen || p.Current.Request.IssuerEpoch > l.Origin.Epoch || p.Current.Observed.IssuerFloorEpoch > l.Origin.Epoch || p.Current.Observed.AdmissionRevision >= c.ServiceClosure.Request.Revision || p.Current.Observed.CapacityGeneration > c.ServiceClosure.Request.ExpectedCapacityGeneration {
			return fmt.Errorf("service retirement regressed original native boundary")
		}
	} else if l.Cycle != 1 {
		return fmt.Errorf("service lifecycle lost required retirement watermark")
	}
	if m.ServiceClosure != nil {
		if m.ServiceClosure.FormatVersion < 10 || m.ServiceClosure.FormatVersion > 20 || m.ServiceClosure.StartedSequence <= l.RecordedSequence || m.ServiceClosure.Request.Revision <= l.Current.Observed.AdmissionRevision || m.ServiceClosure.Request.ExpectedCapacityGeneration < l.Current.Observed.CapacityGeneration {
			return fmt.Errorf("current closure predates completed service lifecycle")
		}
	}
	// 当前预算可由后继完整协议替换，但不能回退到完成前的旧头或旧HWM。
	if c.HighWater != nil && (m.HighWater == nil || compareKeys(*m.HighWater, *c.HighWater) < 0 || compareKeys(*m.HighWater, *c.HighWater) == 0 && *m.HighWater != *c.HighWater) || m.ExpiredResults < c.ExpiredResults {
		return fmt.Errorf("service lifecycle lost retained high water")
	}
	if m.InitialHeld != nil && c.InitialHeld != nil && (!reflect.DeepEqual(m.InitialHeld.Budget, c.InitialHeld.Budget) || !reflect.DeepEqual(m.InitialHeld.Admission, c.InitialHeld.Admission)) {
		return fmt.Errorf("service completion rewrote original first admission or budget")
	}
	if m.InitialHeld != nil && m.InitialHeld.Admission != nil && m.InitialHeld.Admission.Phase == Unconfirmed {
		p := m.InitialHeld.Covered
		if p == nil || c.InitialHeld == nil {
			return fmt.Errorf("completed service lost independent original first admission coverage")
		}
		if c.InitialHeld.Covered != nil {
			if !reflect.DeepEqual(p, c.InitialHeld.Covered) {
				return fmt.Errorf("service lifecycle rewrote original first admission coverage")
			}
		} else if p.RecordedSequence != l.RecordedSequence || p.Request != r.Request || p.Query != r.Query || !reflect.DeepEqual(p.Receipt, *r.Receipt) {
			return fmt.Errorf("first admission coverage differs from exact completed service reopen")
		}
	}
	// 普通业务完成不替换预算；15仅按冻结Proof安装独立预算。后继
	// Finalize/Cleanup须有更高落账序列；完整旧头也不能重新成为当前预算。
	var heldAt control.Uint64
	if h := m.InitialHeld; h != nil {
		heldAt = h.RecordedSequence
	}
	if h := m.BudgetHeld; h != nil {
		heldAt = h.RecordedSequence
	}
	if h := m.CapacityHeld; h != nil {
		heldAt = h.RecordedSequence
	}
	if heldAt <= l.RecordedSequence {
		if p := c.ServiceClosure.ActualRecovery; p != nil {
			expected, err := p.held(*c.Active)
			if err != nil || !reflect.DeepEqual(m.CapacityHeld, expected) || m.InitialHeld != nil || m.BudgetHeld != nil {
				return fmt.Errorf("actual completion lost atomic independent budget installation")
			}
			return nil
		}
		if p := c.ServiceClosure.CleanupRecovery; p != nil && p.Kind == "actual" {
			expected, err := p.held(*c.Active)
			if err != nil || !reflect.DeepEqual(m.CapacityHeld, expected) || m.InitialHeld != nil || m.BudgetHeld != nil {
				return fmt.Errorf("cancelled completion lost exact independently recovered actual budget")
			}
			return nil
		}
		if p := c.ServiceClosure.Finalization; p != nil {
			expected, err := p.held(c)
			if err != nil || !reflect.DeepEqual(m.BudgetHeld, expected) || m.InitialHeld != nil || m.CapacityHeld != nil {
				return fmt.Errorf("service completion lost exact independently recovered budget")
			}
			return nil
		}
		initial := cloneInitial(m.InitialHeld)
		if initial != nil && c.InitialHeld != nil {
			initial.Covered = c.InitialHeld.Covered
		}
		if !reflect.DeepEqual(initial, c.InitialHeld) || !reflect.DeepEqual(m.BudgetHeld, c.BudgetHeld) || !reflect.DeepEqual(m.CapacityHeld, c.CapacityHeld) {
			return fmt.Errorf("service lifecycle replaced retained budget without later atomic finalization")
		}
	}
	return nil
}

func (m Model) validateInitialCoverage() error {
	h := m.InitialHeld
	if h == nil || h.Covered == nil {
		return nil
	}
	p := h.Covered
	var original budget.InitialAdmissionCommand
	if h.Admission == nil || json.Unmarshal(h.Admission.Command, &original) != nil || p.RecordedSequence <= h.Admission.StartedSequence || p.RecordedSequence > m.Sequence || p.Request.Instance != *m.PGInstance || !p.Request.Open || p.Request.IssuerEpoch < original.Request.IssuerEpoch || p.Request.IssuerEpoch > m.Owner.Epoch || p.Request.Revision <= original.Request.Revision || p.Request.ExpectedCapacityGeneration != original.Request.ExpectedCapacityGeneration || p.Request.BudgetCommitmentID != original.Request.BudgetCommitmentID || !p.Receipt.Confirms() || p.Receipt.ValidateFor(p.Request, *m.PGInstance, p.Query) != nil || p.Receipt.Observed.IssuerFloorEpoch > m.Owner.Epoch || p.Receipt.Observed.CapacityGeneration != p.Request.ExpectedCapacityGeneration {
		return fmt.Errorf("first admission coverage lacks exact superseding native open")
	}
	return nil
}

// InitialAdmissionReady仅用于已经通过Validate的模型。Settled与被严格
// 更高原生revision覆盖分开表示；调用方仍须复核当前全部来源和有效期限。
func (m Model) InitialAdmissionReady() bool {
	h := m.InitialHeld
	return h != nil && h.Admission != nil && (h.Admission.Phase == Settled || h.Covered != nil && m.validateInitialCoverage() == nil)
}

// CompleteServiceReopen只处理原新开放已确认且未再关闭的稳定边界：无
// Active沿用既有规则；有Active接受13/14完整结算、15独立结算或18独立
// 取消预算证明。取消仍按真实容量结案，不投影成新目标成功。
// 调用方必须刚完成全部生产来源审计，再提供当前准确observe；同一UID/RV
// 原子保存完整证据、独立覆盖/预算水位并撤销关闭。失败原输入不变。
// 只读查询不会调用本入口，完成写丢回复只能重读，不能再次开放或续租。
func (m Model) CompleteServiceReopen(actor Owner, selector string, current control.AdmissionLeaseResponse) (Model, error) {
	if err := m.Validate(); err != nil {
		return m, err
	}
	c := m.ServiceClosure
	if c == nil || c.Reopen == nil || c.Reopen.CommandSHA256 != selector || c.Reopen.Phase != Settled || c.Reopen.Closing != nil || m.stableServiceRecovery() != nil || !validLiveReopen(current, c.Reopen.Request, actor) {
		return m, fmt.Errorf("service completion requires exact confirmed live original reopen")
	}
	next, err := m.next(actor)
	if err != nil {
		return m, err
	}
	contextModel := m.Clone()
	contextModel.ProtocolVersion = m.ClosureFormatVersion()
	contextModel.ServiceClosure.FormatVersion = 0
	contextModel.ServiceLifecycle = nil
	if contextModel.RetainedCoverage != nil {
		prior, err := json.Marshal(contextModel.RetainedCoverage)
		if err != nil {
			return m, err
		}
		contextModel.RetainedContextSHA256 = rawDigest(prior)
	}
	contextModel.RetainedCoverage = nil
	raw, err := json.Marshal(contextModel)
	if err != nil {
		return m, err
	}
	l := &ServiceLifecycle{SafetyStage: "serving", Cycle: 1, Origin: actor, RecordedSequence: next.Sequence, Context: raw, ContextSHA256: rawDigest(raw), Current: cloneLeaseResponse(current), Previous: nil}
	if old := m.ServiceLifecycle; old != nil {
		if old.Cycle == control.Uint64(math.MaxUint64) {
			return m, fmt.Errorf("service lifecycle counter exhausted")
		}
		prior, err := json.Marshal(old)
		if err != nil {
			return m, err
		}
		l.Cycle = old.Cycle + 1
		l.Previous = &ServiceRetirement{Cycle: old.Cycle, RecordedSequence: old.RecordedSequence, EvidenceSHA256: rawDigest(prior), Current: cloneLeaseResponse(old.Current)}
	}
	if m.RetainedCoverage != nil {
		priorRaw, err := json.Marshal(m.RetainedCoverage)
		if err != nil {
			return m, err
		}
		l.ContextRetainedSHA256 = rawDigest(priorRaw)
	}
	next.ProtocolVersion, next.ServiceLifecycle, next.ServiceClosure = 21, l, nil
	if next.RetainedCoverage != nil {
		next.ProtocolVersion = 22
	}
	if record, err := m.committedCompletionRecord(actor, current, next.Sequence); err != nil {
		return m, err
	} else if record != nil {
		if p := m.ServiceClosure.ActualRecovery; p != nil {
			held, err := p.held(*m.Active)
			if err != nil {
				return m, err
			}
			next.CapacityHeld, next.BudgetHeld, next.InitialHeld, next.BudgetBoundary = held, nil, nil, nil
		}
		if p := m.ServiceClosure.CleanupRecovery; p != nil && p.Kind == "actual" {
			held, err := p.held(*m.Active)
			if err != nil {
				return m, err
			}
			// 原实际容量Cleanup未知保存在新永久头，独立预算和原取消
			// 终态同CAS安装；不先投影成旧Cleanup Completed再调用Finish。
			next.CapacityHeld, next.BudgetHeld, next.InitialHeld, next.BudgetBoundary = held, nil, nil, nil
		}
		if p := m.ServiceClosure.Finalization; p != nil {
			held, err := p.held(m)
			if err != nil {
				return m, err
			}
			// 独立预算首次转为当前永久头，与完整原操作终态、覆盖及
			// 关闭水位同CAS。旧来源仍由自己的持有账本显式结清。
			next.BudgetHeld, next.InitialHeld, next.CapacityHeld, next.BudgetBoundary = held, nil, nil, nil
		}
		// 完整原Active、独立覆盖、永久预算关闭水位和业务完成必须同CAS。
		// 不调用普通Finish去伪造原Resume成功，失败也不清原关闭或预算。
		if p := m.ServiceClosure.RetainedRecovery; p != nil {
			if err := next.closeRetainedBudget(*p); err != nil {
				return m, err
			}
			if err := next.installRetainedCoverage(*record); err != nil {
				return m, err
			}
			next.ProtocolVersion = 22
		} else if p := m.ServiceClosure.CleanupRecovery; p != nil {
			if err := next.closeCancelledBudget(*p); err != nil {
				return m, err
			}
		} else {
			if err := next.closeBudgetBoundary(record.Outcome); err != nil {
				return m, err
			}
		}
		next.History = append(next.History, *record)
		next.Active = nil
		for len(next.History) > HistoryLimit {
			if err := next.expireOldest(); err != nil {
				return m, err
			}
		}
	}
	if next.InitialHeld != nil && next.InitialHeld.Admission != nil && next.InitialHeld.Admission.Phase == Unconfirmed && next.InitialHeld.Covered == nil {
		r := c.Reopen
		next.InitialHeld.Covered = &InitialCoverage{RecordedSequence: next.Sequence, Request: r.Request, Receipt: *cloneAdmissionResponse(r.Receipt), Query: r.Query}
	}
	return next, next.Validate()
}

func (store *Store) CompleteServiceReopen(ctx context.Context, s *Snapshot, actor Owner, selector string, current control.AdmissionLeaseResponse) (*Snapshot, error) {
	if err := store.checked(s); err != nil {
		return nil, err
	}
	if !s.Dispatchable() {
		return nil, fmt.Errorf("service completion requires original running VM")
	}
	next, err := s.model.CompleteServiceReopen(actor, selector, current)
	if err != nil {
		return nil, err
	}
	return store.apply(ctx, s, next)
}

func (l *ServiceLifecycle) UnmarshalJSON(raw []byte) error {
	type plain ServiceLifecycle
	var p plain
	if err := control.UnmarshalObjectBounded(raw, &p, MaxRawBytes, 16384); err != nil {
		return err
	}
	*l = ServiceLifecycle(p)
	return nil
}

func (r *ServiceRetirement) UnmarshalJSON(raw []byte) error {
	type plain ServiceRetirement
	var p plain
	if err := control.UnmarshalObject(raw, &p); err != nil {
		return err
	}
	*r = ServiceRetirement(p)
	return nil
}

func (p *InitialCoverage) UnmarshalJSON(raw []byte) error {
	type plain InitialCoverage
	var next plain
	if err := control.UnmarshalObject(raw, &next); err != nil {
		return err
	}
	*p = InitialCoverage(next)
	return nil
}

func lifecycleDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
