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

type AutomaticSample = policy.Sample

// AutomaticEvaluation 记录候选策略对照所需的原 Owner 输入。失效读取仅记录
// 窗口重置，尝试前的目标不代表 Begin 或任何物理结果。
type AutomaticEvaluation struct {
	ProtocolVersion uint32                        `json:"protocol_version"`
	Instance        control.Instance              `json:"instance"`
	IssuerEpoch     control.Uint64                `json:"issuer_epoch"`
	At              time.Time                     `json:"at"`
	Source          AutomaticPolicySource         `json:"source"`
	Sample          *AutomaticSample              `json:"sample,omitempty"`
	Window          *policy.EvaluationObservation `json:"window,omitempty"`
	Proposed        *policy.Target                `json:"proposed,omitempty"`
	ResetReason     string                        `json:"reset_reason,omitempty"`
	Accepted        bool                          `json:"accepted,omitempty"`
}

func (w *PGWorker) automaticPolicySource() AutomaticPolicySource {
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
	return source
}

func (w *PGWorker) recordAutomaticEvaluation(at time.Time, sample *policy.Sample, proposed *policy.Target, resetReason string, accepted bool) {
	if w.observeAutomaticEvaluation == nil {
		return
	}
	instance, epoch := w.Binding()
	trace := AutomaticEvaluation{ProtocolVersion: 1, Instance: instance, IssuerEpoch: epoch,
		At: at, Source: w.automaticPolicySource(), Proposed: proposed, ResetReason: resetReason, Accepted: accepted}
	if sample != nil {
		copy := *sample
		copy.Eligible = append([]bool(nil), sample.Eligible...)
		window := w.automatic.engine.Observation()
		trace.Sample, trace.Window = &copy, &window
	}
	w.observeAutomaticEvaluation(trace)
}

func (w *PGWorker) captureAutomaticDecision(request control.Request, sample policy.Sample) {
	d := w.automatic
	sample.Eligible = append([]bool(nil), sample.Eligible...)
	d.pendingDecision = &AutomaticDecision{ProtocolVersion: 1, Request: request, Source: w.automaticPolicySource(),
		Sample: sample, Window: d.engine.Observation()}
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
