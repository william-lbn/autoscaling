package owner

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
	"github.com/neondatabase/autoscaling/pkg/dsga/runtimeselection"
	"github.com/stretchr/testify/require"
)

func TestAutomaticDecisionKeepsOriginalPolicySampleAndUnknownResult(t *testing.T) {
	c := automaticConfig(policy.Target{VMMemoryBytes: 1 << 30, Buffers: 512}, policy.Target{VMMemoryBytes: 2 << 30, Buffers: 1024})
	e, err := policy.New(c)
	require.NoError(t, err)
	sample := policy.Sample{At: time.Now().UTC(), Instance: control.Instance{VMUID: "vm", ComputeInstanceID: "compute", PGBootID: 8},
		CapacityGeneration: 7, Current: c.Targets[1], NormalCurrent: 100, NormalMax: 1000,
		SupportsOnlineShrink: true, ClientProcesses: 2, IdleClients: 2, AdmissionOpen: true, Eligible: []bool{true, true}}
	_, _, err = e.Evaluate(sample.At, sample)
	require.NoError(t, err)
	sample.At = sample.At.Add(2 * time.Second)
	_, selected, err := e.Evaluate(sample.At, sample)
	require.NoError(t, err)
	require.True(t, selected)
	w := &PGWorker{automatic: &automaticMemory{config: c, engine: e}, runtimeStatus: &runtimeselection.Status{
		AppliedGeneration: 3, AppliedSHA256: strings.Repeat("a", 64), DesiredGeneration: 4, DesiredSHA256: strings.Repeat("b", 64)}}
	request := control.Request{Key: control.Key{Instance: sample.Instance, IssuerEpoch: 2, Revision: 9}, Kind: control.Shrink,
		ExpectedCapacityGeneration: sample.CapacityGeneration, TargetVMMemoryBytes: c.Targets[0].VMMemoryBytes, TargetBuffers: c.Targets[0].Buffers}
	w.captureAutomaticDecision(request, sample)
	first := w.automaticDecisionSnapshot()[0]
	require.EqualValues(t, 3, first.Source.SelectionGeneration)
	require.Equal(t, strings.Repeat("a", 64), first.Source.SelectionSHA256)
	require.Equal(t, "runtime_selection", first.Source.Kind)
	require.Equal(t, "low_normal_idle_window", first.Window.Reason)
	w.runtimeStatus.AppliedGeneration, w.runtimeStatus.AppliedSHA256 = 4, strings.Repeat("b", 64)
	sample.Eligible[0], c.Targets[0].Buffers = false, 99
	w.recordAutomaticOutcome("begin_unconfirmed")
	unknown := w.automaticDecisionSnapshot()[0]
	require.Equal(t, first.Request, unknown.Request)
	require.Equal(t, first.Source, unknown.Source)
	require.Equal(t, first.Sample, unknown.Sample)
	require.Equal(t, first.Window, unknown.Window)
	require.Equal(t, "begin_unconfirmed", unknown.Outcome)
	w.recordAutomaticOutcome("begin_unconfirmed")
	require.Equal(t, unknown.Sequence, w.automaticDecisionSnapshot()[0].Sequence, "重复Unknown不制造新观测")
	w.recordAutomaticOutcome("reconciled_replayed")
	confirmed := w.automaticDecisionSnapshot()[0]
	require.Greater(t, confirmed.Sequence, unknown.Sequence)
	require.Equal(t, first.Source, confirmed.Source)
	require.Equal(t, first.Sample, confirmed.Sample)
	confirmed.Source.Configuration.Targets[0].Buffers = 42
	confirmed.Sample.Eligible[0] = false
	require.Equal(t, first.Source, w.automaticDecisionSnapshot()[0].Source, "对外诊断深拷贝，不能反向改变原策略")
	require.Equal(t, first.Sample, w.automaticDecisionSnapshot()[0].Sample)
	raw, err := json.Marshal(w.automaticDecisionSnapshot())
	require.NoError(t, err)
	require.NotContains(t, string(raw), "Desired")
}

func TestAutomaticDecisionHistoryIsBoundedAndNeverInventsMissingSample(t *testing.T) {
	w := &PGWorker{automatic: &automaticMemory{}}
	w.recordAutomaticOutcome("begin_unconfirmed")
	require.Empty(t, w.automaticDecisionSnapshot())
	for i := 0; i < automaticObservationLimit+5; i++ {
		w.automatic.pendingDecision = &AutomaticDecision{Request: control.Request{Key: control.Key{Revision: control.Uint64(i + 1)}}}
		w.recordAutomaticOutcome("accepted")
	}
	require.Len(t, w.automaticDecisionSnapshot(), automaticObservationLimit)
	require.EqualValues(t, 6, w.automaticDecisionSnapshot()[0].Request.Key.Revision)
}
