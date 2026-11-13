package owner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
)

const automaticObservationLimit = 16

// AutomaticDecision是原Owner内即时捕获的只读诊断，不写入执行WAL、不发放
// 预算或恢复许可。丢回复只更新提交观测；原sample/window/source/request不重算。
// 有界历史由Agent同时输出结构化日志，重启后不把当前样本伪造为旧决策。
type AutomaticDecision struct {
	ProtocolVersion uint32                       `json:"protocol_version"`
	Sequence        uint64                       `json:"sequence"`
	RecordedAt      time.Time                    `json:"recorded_at"`
	Request         control.Request              `json:"request"`
	Source          AutomaticPolicySource        `json:"source"`
	Sample          AutomaticSample              `json:"sample"`
	Window          policy.EvaluationObservation `json:"window"`
	Outcome         string                       `json:"outcome"`
}

type AutomaticPolicySource struct {
	Kind                string                     `json:"kind"`
	SelectionGeneration control.Uint64             `json:"selection_generation"`
	SelectionSHA256     string                     `json:"selection_sha256,omitempty"`
	AutomaticSHA256     string                     `json:"automatic_sha256"`
	Configuration       policy.Config              `json:"configuration"`
	Qualification       budget.QualificationSource `json:"qualification"`
}

type AutomaticSample struct {
	At                   time.Time        `json:"at"`
	Instance             control.Instance `json:"instance"`
	CapacityGeneration   control.Uint64   `json:"capacity_generation"`
	Current              policy.Target    `json:"current"`
	NormalCurrent        control.Uint64   `json:"normal_current_bytes"`
	NormalMax            control.Uint64   `json:"normal_max_bytes"`
	ClientProcesses      uint32           `json:"client_processes"`
	IdleClients          uint32           `json:"idle_clients"`
	RetainedTransactions uint32           `json:"retained_transactions"`
	SupportsOnlineShrink bool             `json:"supports_online_shrink"`
	AdmissionOpen        bool             `json:"admission_open"`
	Eligible             []bool           `json:"eligible"`
}

func (w *PGWorker) captureAutomaticDecision(request control.Request, sample policy.Sample) {
	d := w.automatic
	config := d.config
	config.Targets = append([]policy.Target(nil), config.Targets...)
	raw, _ := json.Marshal(config)
	sum := sha256.Sum256(raw)
	source := AutomaticPolicySource{Kind: "static_configuration", AutomaticSHA256: hex.EncodeToString(sum[:]), Configuration: config, Qualification: d.source}
	if w.runtimeStatus != nil {
		// 采用当时已应用的选择，不拿Desired冒充实际用于Evaluate的策略。
		source.Kind, source.SelectionGeneration, source.SelectionSHA256 = "runtime_selection", w.runtimeStatus.AppliedGeneration, w.runtimeStatus.AppliedSHA256
	}
	d.pendingDecision = &AutomaticDecision{ProtocolVersion: 1, Request: request, Source: source,
		Sample: AutomaticSample{At: sample.At, Instance: sample.Instance, CapacityGeneration: sample.CapacityGeneration,
			Current: sample.Current, NormalCurrent: sample.NormalCurrent, NormalMax: sample.NormalMax,
			ClientProcesses: sample.ClientProcesses, IdleClients: sample.IdleClients, RetainedTransactions: sample.RetainedTransactions,
			SupportsOnlineShrink: sample.SupportsOnlineShrink, AdmissionOpen: sample.AdmissionOpen, Eligible: append([]bool(nil), sample.Eligible...)},
		Window: d.engine.Observation()}
	w.recordAutomaticOutcome("selected")
}

func cloneAutomaticDecision(value AutomaticDecision) AutomaticDecision {
	value.Sample.Eligible = append([]bool(nil), value.Sample.Eligible...)
	value.Source.Configuration.Targets = append([]policy.Target(nil), value.Source.Configuration.Targets...)
	return value
}

func (w *PGWorker) recordAutomaticOutcome(outcome string) {
	// 固定码、不格式化任意错误；缺少原本地采样（例如旧版本恢复）不补造诊断。
	d := w.automatic
	if d == nil || d.pendingDecision == nil || d.pendingDecision.Outcome == outcome {
		return
	}
	w.automaticDecisionSequence++
	d.pendingDecision.Sequence, d.pendingDecision.RecordedAt, d.pendingDecision.Outcome = w.automaticDecisionSequence, time.Now().UTC(), outcome
	value := cloneAutomaticDecision(*d.pendingDecision)
	for i := range w.automaticDecisions {
		if w.automaticDecisions[i].Request.Key == value.Request.Key {
			w.automaticDecisions = append(w.automaticDecisions[:i], w.automaticDecisions[i+1:]...)
			break
		}
	}
	w.automaticDecisions = append(w.automaticDecisions, value)
	if len(w.automaticDecisions) > automaticObservationLimit {
		w.automaticDecisions = w.automaticDecisions[len(w.automaticDecisions)-automaticObservationLimit:]
	}
}

func (w *PGWorker) automaticDecisionSnapshot() []AutomaticDecision {
	result := make([]AutomaticDecision, len(w.automaticDecisions))
	for i, value := range w.automaticDecisions {
		result[i] = cloneAutomaticDecision(value)
	}
	return result
}
