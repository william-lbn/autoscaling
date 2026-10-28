package policy

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func testConfig() Config {
	return Config{Targets: []Target{{VMMemoryBytes: 512 << 20, Buffers: 512}, {VMMemoryBytes: 1 << 30, Buffers: 1024}, {VMMemoryBytes: 2 << 30, Buffers: 2048}}, SampleIntervalSeconds: 1, MaximumSampleGapSeconds: 5, GrowWindowSeconds: 2, ShrinkWindowSeconds: 4, CooldownSeconds: 6, RequestDeadlineSeconds: 120, ShrinkThresholdPermille: 200, GrowThresholdPermille: 800, EmergencyThresholdPermille: 950}
}

func testSample(c Config) Sample {
	return Sample{At: time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC), Instance: control.Instance{VMUID: "vm-1", ComputeInstanceID: "compute-1", PGBootID: 1}, CapacityGeneration: 1, Current: c.Targets[1], NormalCurrent: 800, NormalMax: 1000, ClientProcesses: 0, AdmissionOpen: true, Eligible: []bool{true, true, true}}
}

func evaluate(t *testing.T, e *Engine, s Sample, want Target, selected bool) {
	t.Helper()
	target, ok, err := e.Evaluate(s.At, s)
	require.NoError(t, err)
	require.Equal(t, selected, ok)
	require.Equal(t, want, target)
}

func TestMemoryPolicyRejectsAmbiguousConfiguration(t *testing.T) {
	for name, change := range map[string]func(*Config){
		"missing-targets":    func(c *Config) { c.Targets = nil },
		"one-target":         func(c *Config) { c.Targets = c.Targets[:1] },
		"duplicate":          func(c *Config) { c.Targets[1] = c.Targets[0] },
		"dimension-reversal": func(c *Config) { c.Targets[1].Buffers = c.Targets[0].Buffers - 1 },
		"zero-size":          func(c *Config) { c.Targets[0].VMMemoryBytes = 0 },
		"size-overflow":      func(c *Config) { c.Targets[2].VMMemoryBytes = math.MaxUint64 },
		"buffer-bound":       func(c *Config) { c.Targets[2].Buffers = math.MaxUint32 },
		"no-interval":        func(c *Config) { c.SampleIntervalSeconds = 0 },
		"gap-too-short":      func(c *Config) { c.MaximumSampleGapSeconds = 0 },
		"no-hysteresis":      func(c *Config) { c.ShrinkWindowSeconds = c.GrowWindowSeconds },
		"threshold-order":    func(c *Config) { c.GrowThresholdPermille = c.ShrinkThresholdPermille },
		"threshold-overflow": func(c *Config) { c.EmergencyThresholdPermille = 1001 },
		"short-deadline":     func(c *Config) { c.RequestDeadlineSeconds = 59 },
	} {
		t.Run(name, func(t *testing.T) {
			c := testConfig()
			change(&c)
			_, err := New(c)
			require.Error(t, err)
		})
	}
	c := testConfig()
	c.Targets[1].VMMemoryBytes = c.Targets[0].VMMemoryBytes
	require.NoError(t, c.Validate(), "允许同VM仅调整Buffer的明确规格")
}

// 使用显式时刻推进，既验证持续性又避免真实sleep放大Mac开发耗时。
func TestMemoryPolicyWindowsClientsCooldownAndEmergency(t *testing.T) {
	c := testConfig()
	e, err := New(c)
	require.NoError(t, err)
	s := testSample(c)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.At = s.At.Add(time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.At = s.At.Add(time.Second)
	evaluate(t, e, s, c.Targets[2], true)
	e.Accepted(s.At)
	s.NormalCurrent = 100
	s.At = s.At.Add(time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.At = s.At.Add(4 * time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false) // 缩容窗口到，冷却未到。
	s.ClientProcesses = 1                                          // idle/startup客户端也必须重置缩容连续窗口。
	s.At = s.At.Add(time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.ClientProcesses = 0
	s.At = s.At.Add(time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.At = s.At.Add(4 * time.Second)
	evaluate(t, e, s, c.Targets[0], true)
	e.Accepted(s.At)
	s.NormalCurrent = 950
	s.At = s.At.Add(time.Second)
	evaluate(t, e, s, c.Targets[2], true) // 紧急高压可越过窗口和冷却。
	s.AdmissionOpen = false
	s.At = s.At.Add(time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false) // 紧急也不能自动重开闭门实例。
}

func TestMemoryPolicyResetsOnChangedOrInterruptedFacts(t *testing.T) {
	for name, change := range map[string]func(*Sample){
		"new-boot":         func(s *Sample) { s.Instance.PGBootID++ },
		"new-generation":   func(s *Sample) { s.CapacityGeneration++ },
		"new-cap":          func(s *Sample) { s.NormalMax++; s.NormalCurrent++ },
		"eligible-changed": func(s *Sample) { s.Eligible[0] = false },
		"sample-gap":       func(s *Sample) { s.At = s.At.Add(6 * time.Second) },
		"current-size":     func(s *Sample) { s.Current = testConfig().Targets[0] },
	} {
		t.Run(name, func(t *testing.T) {
			c := testConfig()
			e, err := New(c)
			require.NoError(t, err)
			s := testSample(c)
			evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
			s.At = s.At.Add(2 * time.Second)
			change(&s)
			evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
			s.At = s.At.Add(2 * time.Second)
			want := c.Targets[2]
			if name == "current-size" {
				want = c.Targets[1]
			}
			evaluate(t, e, s, want, true)
		})
	}
}

func TestMemoryPolicyRejectsStaleContradictoryAndRepeatedSamples(t *testing.T) {
	for name, change := range map[string]func(*Sample, *time.Time){
		"future":              func(s *Sample, now *time.Time) { s.At = now.Add(time.Second) },
		"stale":               func(s *Sample, now *time.Time) { s.At = now.Add(-6 * time.Second) },
		"unknown-instance":    func(s *Sample, _ *time.Time) { s.Instance.PGBootID = 0 },
		"unknown-generation":  func(s *Sample, _ *time.Time) { s.CapacityGeneration = 0 },
		"no-normal-bound":     func(s *Sample, _ *time.Time) { s.NormalMax = 0 },
		"current-over-bound":  func(s *Sample, _ *time.Time) { s.NormalCurrent = s.NormalMax + 1 },
		"unconfigured-size":   func(s *Sample, _ *time.Time) { s.Current.Buffers++ },
		"missing-eligibility": func(s *Sample, _ *time.Time) { s.Eligible = nil },
		"unqualified-current": func(s *Sample, _ *time.Time) { s.Eligible[1] = false },
	} {
		t.Run(name, func(t *testing.T) {
			c := testConfig()
			e, err := New(c)
			require.NoError(t, err)
			s := testSample(c)
			now := s.At
			change(&s, &now)
			_, selected, err := e.Evaluate(now, s)
			require.Error(t, err)
			require.False(t, selected)
		})
	}
	c := testConfig()
	e, err := New(c)
	require.NoError(t, err)
	s := testSample(c)
	s.NormalCurrent = 990
	evaluate(t, e, s, c.Targets[2], true)
	_, selected, err := e.Evaluate(s.At, s)
	require.Error(t, err)
	require.False(t, selected, "不能用重复紧急采样再次绕过窗口")
	_, selected, err = e.Evaluate(s.At, s)
	require.Error(t, err)
	require.False(t, selected, "拒绝后的Reset也必须保留时间水位")
	e.Accepted(s.At.Add(time.Minute))
	_, _, err = e.Evaluate(s.At, s)
	require.Error(t, err, "时钟倒退到接受之前必须拒绝")
}

func TestMemoryPolicyExactBoundsAndSliceIsolation(t *testing.T) {
	c := testConfig()
	e, err := New(c)
	require.NoError(t, err)
	original := c.Targets[2]
	c.Targets[2].Buffers++
	s := testSample(testConfig())
	s.NormalMax, s.NormalCurrent = math.MaxUint64, math.MaxUint64
	evaluate(t, e, s, original, true)
	s.Current = original
	s.At = s.At.Add(time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false) // 已达最大规格，不越界。
	e.Reset()
	newTime := s.At.Add(time.Second)
	s = testSample(testConfig())
	s.At = newTime
	s.NormalCurrent = 100
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.Eligible[0] = false // Engine必须复制采样，而不是共享调用者可变切片。
	s.At = s.At.Add(4 * time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.Eligible[0] = true
	s.At = s.At.Add(4 * time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false) // 资格再次变化重新累计。
	s.At = s.At.Add(4 * time.Second)
	evaluate(t, e, s, testConfig().Targets[0], true)
	e.Reset()
	s.At = s.At.Add(time.Second)
	s.Current = testConfig().Targets[0]
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.At = s.At.Add(4 * time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false) // 最小规格不越界。
	require.EqualValues(t, 1, threshold(1, 950, true))
	require.Zero(t, threshold(1, 200, false))
	require.EqualValues(t, uint64(math.MaxUint64), threshold(math.MaxUint64, 1000, true))
}

func TestMemoryPolicySelectsNearestEligibleAndRestartsAfterNeutralLoad(t *testing.T) {
	c := testConfig()
	e, err := New(c)
	require.NoError(t, err)
	s := testSample(c)
	s.Current = c.Targets[0]
	s.NormalCurrent, s.ClientProcesses = 950, 4
	s.Eligible[1] = false
	evaluate(t, e, s, c.Targets[2], true) // 紧急扩容可跳过不支持目标，并允许活动客户端。
	s.At = s.At.Add(time.Second)
	s.Eligible[2] = false
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.Eligible[1], s.Eligible[2] = true, true
	s.NormalCurrent = 800
	s.At = s.At.Add(time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.NormalCurrent = 500 // 回到中性区要清窗口，不能拼接两段高压。
	s.At = s.At.Add(2 * time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.NormalCurrent = 800
	s.At = s.At.Add(time.Second)
	evaluate(t, e, s, Target{VMMemoryBytes: 0, Buffers: 0}, false)
	s.At = s.At.Add(2 * time.Second)
	evaluate(t, e, s, c.Targets[1], true)
}
