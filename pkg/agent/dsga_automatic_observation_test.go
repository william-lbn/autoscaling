package agent

import (
	"encoding/json"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestDSGAAutomaticDecisionDumpAndLogPreserveOutcomeTransitions(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	logger := zap.New(core)
	var diagnostic dsgaTickDiagnostic
	// Dump解码必须沿原严格Request/Instance读者，不能用全零身份的伪请求
	// 测观察字段后反过来要求生产解码放宽。这里绑定完整有效的原模型请求。
	instance := control.Instance{VMUID: "original-vm", ComputeInstanceID: "original-compute", PGBootID: 7}
	request := control.Request{ProtocolVersion: 1,
		Key:  control.Key{Instance: instance, IssuerEpoch: 2, Revision: 3, OperationID: control.OperationID{High: 4, Low: 5}},
		Kind: control.Grow, ExpectedCapacityGeneration: 6, TargetBuffers: 1024,
		TargetVMMemoryBytes: 1 << 30, BudgetCommitmentID: "automatic-original", DeadlineUnixMillis: 123456789}
	require.NoError(t, request.Validate())
	decision := owner.AutomaticDecision{ProtocolVersion: 1, Sequence: 1, Outcome: "begin_unconfirmed", Request: request,
		Sample: owner.AutomaticSample{Instance: instance}}
	tick := owner.Tick{Progress: pgexec.AwaitingFact, AutomaticDecisions: []owner.AutomaticDecision{decision}}
	diagnostic.observe(logger, tick)
	diagnostic.observe(logger, tick)
	require.Len(t, observed.FilterMessage("DSGA automatic memory decision").All(), 1)
	decision.Sequence, decision.Outcome = 2, "reconciled_replayed"
	tick.AutomaticDecisions = []owner.AutomaticDecision{decision}
	diagnostic.observe(logger, tick)
	require.Len(t, observed.FilterMessage("DSGA automatic memory decision").All(), 2)
	require.Equal(t, pgexec.AwaitingFact, tick.Progress, "日志不能改原执行结果")
	state := DSGARunnerState{AutomaticObservationVersion: 1, AutomaticDecisions: tick.AutomaticDecisions}
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	var decoded struct {
		Version   uint32                    `json:"automaticObservationVersion"`
		Decisions []owner.AutomaticDecision `json:"automaticDecisions"`
	}
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.EqualValues(t, 1, decoded.Version)
	require.Equal(t, tick.AutomaticDecisions, decoded.Decisions)
}
