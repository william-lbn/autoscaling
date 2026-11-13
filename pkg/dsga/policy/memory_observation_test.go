package policy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryPolicyObservationUsesActualWindowWithoutChangingDecision(t *testing.T) {
	c := testConfig()
	e, err := New(c)
	require.NoError(t, err)
	s := testSample(c)
	s.NormalCurrent, s.ClientProcesses, s.IdleClients, s.SupportsOnlineShrink = 100, 2, 2, true
	start := s.At
	evaluate(t, e, s, Target{}, false)
	require.Equal(t, "low_normal_idle_window", e.Observation().Reason)
	require.Equal(t, start, e.Observation().WindowStartedAt)
	s.At = s.At.Add(time.Duration(c.ShrinkWindowSeconds) * time.Second)
	evaluate(t, e, s, c.Targets[0], true)
	before := e.Observation()
	require.True(t, before.Valid)
	require.True(t, before.Selected)
	require.Equal(t, start, before.WindowStartedAt)
	require.Equal(t, s.At, before.SampleAt)
	require.Equal(t, c.ShrinkWindowSeconds, before.WindowSeconds)
	copy := e.Observation()
	copy.Reason, copy.WindowStartedAt = "caller-change", time.Time{}
	require.Equal(t, before, e.Observation(), "只读值副本不能改变策略窗口")
	e.Accepted(s.At)
	require.Equal(t, before, e.Observation(), "Accepted不能回写原已捕获决策")
	s.At = s.At.Add(time.Second)
	s.NormalCurrent = 950
	evaluate(t, e, s, c.Targets[2], true)
	require.Equal(t, "emergency_normal", e.Observation().Reason)
	require.True(t, e.Observation().Emergency)
	require.Equal(t, before.SampleAt, e.Observation().LastAcceptedAt)
	_, _, err = e.Evaluate(s.At, s)
	require.Error(t, err)
	require.False(t, e.Observation().Valid)
	require.False(t, e.Observation().Selected)
	require.Equal(t, "invalid_sample", e.Observation().Reason)
}

func TestMemoryPolicyObservationDistinguishesFixedMinimumFromRealPressure(t *testing.T) {
	c := testConfig()
	c.MinimumVMMemoryBytes, c.MaximumVMMemoryBytes = c.Targets[2].VMMemoryBytes, c.Targets[2].VMMemoryBytes
	e, err := New(c)
	require.NoError(t, err)
	s := testSample(c)
	s.NormalCurrent = 100
	evaluate(t, e, s, Target{}, false)
	s.At = s.At.Add(time.Duration(c.GrowWindowSeconds) * time.Second)
	evaluate(t, e, s, c.Targets[2], true)
	require.Equal(t, "below_minimum_window", e.Observation().Reason)
	require.False(t, e.Observation().Emergency)
}
