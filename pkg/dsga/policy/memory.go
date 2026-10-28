// Package policy只产生伸缩建议，不签发预算、排空或VM执行许可。
// 指标是策略估计；真正执行仍由唯一owner的原WAL和各领域认证决定。
package policy

import (
	"fmt"
	"slices"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// Target必须来自可信配置的明确规格，不能由VM普通注解、自报WSS或比例外推。
// 相邻规格的VM与Buffer均不减少，至少一项增加，允许同VM只调整Buffer。
type Target struct {
	VMMemoryBytes control.Uint64 `json:"vmMemoryBytes"`
	Buffers       uint32         `json:"buffers"`
}

// Config不提供推测的生产阈值。所有时间有界，缩容窗口长于扩容窗口，且跨
// 采样间断/实例或容量变化重新累计；nil配置保留默认关闭。阈值为千分比整数，
// 避免浮点NaN/Inf、舍入及乘法溢出影响边界。Linux验收须校准实际阈值与规格。
type Config struct {
	Targets                    []Target `json:"targets"`
	SampleIntervalSeconds      uint     `json:"sampleIntervalSeconds"`
	MaximumSampleGapSeconds    uint     `json:"maximumSampleGapSeconds"`
	GrowWindowSeconds          uint     `json:"growWindowSeconds"`
	ShrinkWindowSeconds        uint     `json:"shrinkWindowSeconds"`
	CooldownSeconds            uint     `json:"cooldownSeconds"`
	RequestDeadlineSeconds     uint     `json:"requestDeadlineSeconds"`
	ShrinkThresholdPermille    uint16   `json:"shrinkThresholdPermille"`
	GrowThresholdPermille      uint16   `json:"growThresholdPermille"`
	EmergencyThresholdPermille uint16   `json:"emergencyThresholdPermille"`
}

func (c Config) Validate() error {
	if len(c.Targets) < 2 || len(c.Targets) > 32 || c.SampleIntervalSeconds == 0 || c.SampleIntervalSeconds > 60 || c.MaximumSampleGapSeconds < c.SampleIntervalSeconds || c.MaximumSampleGapSeconds > 300 || c.GrowWindowSeconds < c.SampleIntervalSeconds || c.GrowWindowSeconds > 3600 || c.ShrinkWindowSeconds <= c.GrowWindowSeconds || c.ShrinkWindowSeconds > 86400 || c.CooldownSeconds < c.SampleIntervalSeconds || c.CooldownSeconds > 86400 || c.RequestDeadlineSeconds < 60 || c.RequestDeadlineSeconds > 3600 || c.ShrinkThresholdPermille == 0 || c.ShrinkThresholdPermille >= c.GrowThresholdPermille || c.GrowThresholdPermille >= c.EmergencyThresholdPermille || c.EmergencyThresholdPermille > 1000 {
		return fmt.Errorf("invalid explicit memory policy windows or thresholds")
	}
	for i, t := range c.Targets {
		if t.VMMemoryBytes == 0 || t.VMMemoryBytes > 1<<63-1 || t.Buffers < 16 || t.Buffers > uint32((1<<31-1)/2) {
			return fmt.Errorf("invalid exact memory policy target")
		}
		if i > 0 {
			p := c.Targets[i-1]
			if t.VMMemoryBytes < p.VMMemoryBytes || t.Buffers < p.Buffers || t == p {
				return fmt.Errorf("memory policy targets must increase without reversing a dimension")
			}
		}
	}
	return nil
}

// Sample由正式只读生产者提供，At为采样开始时间，调用者另给完成时刻。
// Eligible是当前原资格/能力支持的确切目标，不是VM许可；例如安全降normal
// 尚未实现时不得推荐它制造永久Pending。未来补齐能力后同一目标才可启用。
// ClientProcesses必须包含启动和idle会话；仅前台快照Idle不足以触发自动缩容。
type Sample struct {
	At                 time.Time
	Instance           control.Instance
	CapacityGeneration control.Uint64
	Current            Target
	NormalCurrent      control.Uint64
	NormalMax          control.Uint64
	ClientProcesses    uint32
	AdmissionOpen      bool
	Eligible           []bool
}

// Engine只由同一owner串行循环持有，不另开goroutine或锁跨网络。重建策略
// 不丢任何原操作：所有已接受/未知请求由外层原WAL及SDK负责恢复。策略窗口
// 是可丢的性能状态，重建必须重新等待缩容窗口，紧急增长仍可优先选择。
type Engine struct {
	config   Config
	last     *Sample
	since    time.Time
	kind     control.Kind
	accepted time.Time
	lastAt   time.Time
}

func New(c Config) (*Engine, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	c.Targets = append([]Target(nil), c.Targets...)
	return &Engine{config: c, last: nil, since: time.Time{}, kind: "", accepted: time.Time{}, lastAt: time.Time{}}, nil
}

// Reset只清可丢窗口，保留采样时间水位和已接受的冷却事实；坏样本之后
// 重送同一紧急样本也不能再次触发。新Engine才从空水位开始。
func (e *Engine) Reset() { e.last, e.since, e.kind = nil, time.Time{}, "" }

func (e *Engine) Accepted(now time.Time) { e.Reset(); e.accepted = now }

// threshold的两个乘积均不超过原分母与1e6；不用current*1000，MaxInt64
// 字节上限附近也不会回绕。grow取ceil、shrink取floor，边界保持保守。
func threshold(total control.Uint64, permille uint16, ceil bool) control.Uint64 {
	q, r := uint64(total)/1000, uint64(total)%1000*uint64(permille)
	if ceil {
		r += 999
	}
	return control.Uint64(q*uint64(permille) + r/1000)
}

func (e *Engine) Evaluate(now time.Time, s Sample) (Target, bool, error) {
	var none Target
	index := -1
	for i, t := range e.config.Targets {
		if t == s.Current {
			index = i
		}
	}
	if now.IsZero() || s.At.IsZero() || s.At.After(now) || now.Sub(s.At) > time.Duration(e.config.MaximumSampleGapSeconds)*time.Second || s.Instance.Validate() != nil || s.CapacityGeneration == 0 || s.NormalMax == 0 || s.NormalCurrent > s.NormalMax || index < 0 || len(s.Eligible) != len(e.config.Targets) || !s.Eligible[index] {
		e.Reset()
		return none, false, fmt.Errorf("memory policy sample lacks fresh exact current facts")
	}
	// 同一采样不能重复累计窗口，也不能用时钟倒退绕过冷却期。拒绝
	// 后重新建立窗口；紧急增长只对真正的新鲜采样开放。
	if !e.lastAt.IsZero() && !s.At.After(e.lastAt) || !e.accepted.IsZero() && now.Before(e.accepted) {
		e.Reset()
		return none, false, fmt.Errorf("memory policy sample or acceptance clock did not advance")
	}
	e.lastAt = s.At
	if !s.AdmissionOpen {
		e.Reset()
		return none, false, nil
	}
	continuous := e.last != nil && s.At.After(e.last.At) && s.At.Sub(e.last.At) <= time.Duration(e.config.MaximumSampleGapSeconds)*time.Second && e.last.Instance == s.Instance && e.last.CapacityGeneration == s.CapacityGeneration && e.last.Current == s.Current && e.last.NormalMax == s.NormalMax && slices.Equal(e.last.Eligible, s.Eligible)
	if !continuous {
		e.Reset()
	}
	kind := control.Kind("")
	emergency := s.NormalCurrent >= threshold(s.NormalMax, e.config.EmergencyThresholdPermille, true)
	if s.NormalCurrent >= threshold(s.NormalMax, e.config.GrowThresholdPermille, true) {
		kind = control.Grow
	} else if s.ClientProcesses == 0 && s.NormalCurrent <= threshold(s.NormalMax, e.config.ShrinkThresholdPermille, false) {
		kind = control.Shrink
	}
	if kind != e.kind || e.since.IsZero() {
		e.since = s.At
	}
	e.kind = kind
	copySample := s
	copySample.Eligible = append([]bool(nil), s.Eligible...)
	e.last = &copySample
	if kind == "" {
		return none, false, nil
	}
	window := time.Duration(e.config.GrowWindowSeconds) * time.Second
	if kind == control.Shrink {
		window = time.Duration(e.config.ShrinkWindowSeconds) * time.Second
	}
	if !emergency && (s.At.Sub(e.since) < window || !e.accepted.IsZero() && now.Sub(e.accepted) < time.Duration(e.config.CooldownSeconds)*time.Second) {
		return none, false, nil
	}
	delta := 1
	if kind == control.Shrink {
		delta = -1
	}
	for next := index + delta; next >= 0 && next < len(e.config.Targets); next += delta {
		if s.Eligible[next] {
			return e.config.Targets[next], true, nil
		}
	}
	return none, false, nil
}
