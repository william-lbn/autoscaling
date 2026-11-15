package policy

import (
	"encoding/json"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

func TestReplayPreservesOriginalWindowsAndPendingIntent(t *testing.T) {
	start := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	low := Target{VMMemoryBytes: 7 << 30, Buffers: 256}
	high := Target{VMMemoryBytes: 8 << 30, Buffers: 512}
	trace := ReplayTrace{
		ProtocolVersion: 1,
		PolicyRevision:  "fixture-revision",
		Configuration: Config{
			Targets: []Target{low, high}, SampleIntervalSeconds: 1, MaximumSampleGapSeconds: 3,
			GrowWindowSeconds: 1, ShrinkWindowSeconds: 2, CooldownSeconds: 4, RequestDeadlineSeconds: 120,
			ShrinkThresholdPermille: 200, GrowThresholdPermille: 800, EmergencyThresholdPermille: 950,
		},
	}
	sample := func(second int, current Target, working uint64) ReplayEvent {
		at := start.Add(time.Duration(second) * time.Second)
		return ReplayEvent{Kind: "evaluate", At: at, Sample: &Sample{
			At: at, Instance: control.Instance{VMUID: "vm", ComputeInstanceID: "compute", PGBootID: 12},
			CapacityGeneration: 3, Current: current, NormalCurrent: control.Uint64(working),
			NormalMax: 100, AdmissionOpen: true, SupportsOnlineShrink: true, Eligible: []bool{true, true},
		}}
	}
	trace.Events = []ReplayEvent{
		sample(0, high, 10),
		sample(1, high, 10),
		sample(2, high, 10),
		{Kind: "accepted", At: start.Add(2 * time.Second)},
		sample(3, low, 96),
		{Kind: "discarded", At: start.Add(3 * time.Second)},
		// A repeated observation cannot trigger a fresh action after the original request was discarded.
		sample(3, low, 96),
		sample(4, low, 96),
	}
	result, err := Replay(trace)
	require.NoError(t, err)
	require.Len(t, result.Evaluations, 6)
	require.Nil(t, result.Evaluations[0].Target)
	require.Nil(t, result.Evaluations[1].Target)
	require.Equal(t, &low, result.Evaluations[2].Target)
	require.Equal(t, control.Shrink, result.Evaluations[2].Observation.Kind)
	require.Equal(t, &high, result.Evaluations[3].Target)
	require.True(t, result.Evaluations[3].Observation.Emergency)
	require.False(t, result.Evaluations[4].Observation.Valid)
	require.Nil(t, result.Evaluations[4].Target)
	require.Equal(t, &high, result.Evaluations[5].Target)
	require.True(t, result.Pending)

	first, err := json.Marshal(result)
	require.NoError(t, err)
	again, err := Replay(trace)
	require.NoError(t, err)
	second, err := json.Marshal(again)
	require.NoError(t, err)
	require.Equal(t, first, second)

	trace.Events = append(trace.Events, sample(5, low, 96))
	_, err = Replay(trace)
	require.ErrorContains(t, err, "overlaps pending")
	trace.Events = trace.Events[:len(trace.Events)-1]

	trace.Events[3].At = start.Add(-time.Second)
	_, err = Replay(trace)
	require.ErrorContains(t, err, "invalid replay clock")
}
