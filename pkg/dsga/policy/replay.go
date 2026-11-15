package policy

import (
	"fmt"
	"time"
)

type ReplayTrace struct {
	ProtocolVersion uint32        `json:"protocol_version"`
	PolicyRevision  string        `json:"policy_revision"`
	Configuration   Config        `json:"configuration"`
	Events          []ReplayEvent `json:"events"`
}

type ReplayEvent struct {
	Kind   string    `json:"kind"`
	At     time.Time `json:"at"`
	Sample *Sample   `json:"sample,omitempty"`
}

type ReplayEvaluation struct {
	Event       int                   `json:"event"`
	Target      *Target               `json:"target,omitempty"`
	Observation EvaluationObservation `json:"observation"`
}

type ReplayResult struct {
	ProtocolVersion uint32             `json:"protocol_version"`
	PolicyRevision  string             `json:"policy_revision"`
	Evaluations     []ReplayEvaluation `json:"evaluations"`
	Pending         bool               `json:"pending"`
}

// Replay 使用原 Owner 的窗口决策器，显式传入复现所需的时钟和结算事件。
// 原资格目录、样本和 accepted 事实由调用方冻结；输出仅用于离线比较。
func Replay(trace ReplayTrace) (ReplayResult, error) {
	if trace.ProtocolVersion != 1 || trace.PolicyRevision == "" || len(trace.Events) == 0 {
		return ReplayResult{}, fmt.Errorf("replay needs a versioned policy and observations")
	}
	engine, err := New(trace.Configuration)
	if err != nil {
		return ReplayResult{}, err
	}
	result := ReplayResult{ProtocolVersion: trace.ProtocolVersion, PolicyRevision: trace.PolicyRevision,
		Evaluations: make([]ReplayEvaluation, 0, len(trace.Events))}
	var lastTime time.Time
	for i, event := range trace.Events {
		if event.At.IsZero() || event.At.Before(lastTime) || (event.Kind == "evaluate") != (event.Sample != nil) {
			return ReplayResult{}, fmt.Errorf("invalid replay clock or event shape at %d", i)
		}
		lastTime = event.At
		switch event.Kind {
		case "evaluate":
			if result.Pending {
				return ReplayResult{}, fmt.Errorf("evaluation overlaps pending request at %d", i)
			}
			target, selected, _ := engine.Evaluate(event.At, *event.Sample)
			evaluation := ReplayEvaluation{Event: i, Observation: engine.Observation()}
			if selected {
				evaluation.Target, result.Pending = &target, true
			}
			result.Evaluations = append(result.Evaluations, evaluation)
		case "accepted", "discarded":
			if !result.Pending {
				return ReplayResult{}, fmt.Errorf("original request is absent at %d", i)
			}
			if event.Kind == "accepted" {
				engine.Accepted(event.At)
			} else {
				engine.Reset()
			}
			result.Pending = false
		case "reset":
			if result.Pending {
				return ReplayResult{}, fmt.Errorf("reset cannot resolve a pending request at %d", i)
			}
			engine.Reset()
		default:
			return ReplayResult{}, fmt.Errorf("unsupported replay event at %d", i)
		}
	}
	if len(result.Evaluations) == 0 {
		return ReplayResult{}, fmt.Errorf("replay contains no evaluation")
	}
	return result, nil
}
