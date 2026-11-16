package agent

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/neondatabase/autoscaling/pkg/agent/core"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"go.uber.org/zap"
)

var shadowStorageID = regexp.MustCompile(`^[0-9a-f]{32}$`)

// DSGACacheShadowConfig selects one verified storage branch and the original
// window estimator. The observation goroutine exports diagnostics only.
type DSGACacheShadowConfig struct {
	TenantID                string  `json:"tenantId"`
	TimelineID              string  `json:"timelineId"`
	SampleIntervalSeconds   uint    `json:"sampleIntervalSeconds"`
	MaximumSampleAgeSeconds uint    `json:"maximumSampleAgeSeconds"`
	MinimumWindowMinutes    int     `json:"minimumWindowMinutes"`
	SlopeWindowMinutes      int     `json:"slopeWindowMinutes"`
	MaximumSlopeIncrease    float64 `json:"maximumSlopeIncrease"`
}

func (cfg DSGACacheShadowConfig) validate() error {
	if !shadowStorageID.MatchString(cfg.TenantID) || !shadowStorageID.MatchString(cfg.TimelineID) ||
		cfg.SampleIntervalSeconds < 5 || cfg.SampleIntervalSeconds > 60 ||
		cfg.MaximumSampleAgeSeconds < cfg.SampleIntervalSeconds || cfg.MaximumSampleAgeSeconds > 300 ||
		cfg.SlopeWindowMinutes < 2 || cfg.SlopeWindowMinutes > 30 ||
		cfg.MinimumWindowMinutes < cfg.SlopeWindowMinutes || cfg.MinimumWindowMinutes > 60 ||
		cfg.MinimumWindowMinutes+cfg.SlopeWindowMinutes-1 > 60 ||
		math.IsInf(cfg.MaximumSlopeIncrease, 0) || math.IsNaN(cfg.MaximumSlopeIncrease) || cfg.MaximumSlopeIncrease <= 1 {
		return fmt.Errorf("invalid cache shadow scope or window configuration")
	}
	return nil
}

func (cfg DSGACacheShadowConfig) estimate(instance control.Instance, now time.Time, fact compute.LogicalDemandObservation) (uint64, error) {
	if err := cfg.validate(); err != nil {
		return 0, err
	}
	pages, err := fact.WorkingSetPages(instance, cfg.TenantID, cfg.TimelineID, now, time.Duration(cfg.MaximumSampleAgeSeconds)*time.Second)
	if err != nil {
		return 0, err
	}
	if len(pages) < cfg.MinimumWindowMinutes+cfg.SlopeWindowMinutes-1 {
		return 0, fmt.Errorf("cache shadow has no complete estimator window")
	}
	estimatedPages := core.EstimateTrueWorkingSetSize(pages, core.WssEstimatorConfig{
		MaxAllowedIncreaseFactor: cfg.MaximumSlopeIncrease,
		InitialOffset:            cfg.MinimumWindowMinutes - 1,
		WindowSize:               cfg.SlopeWindowMinutes,
	})
	return uint64(estimatedPages) * uint64(fact.Sample.Demand.BlockBytes), nil
}

func startDSGACacheShadow(
	parent context.Context, logger *zap.Logger, instance control.Instance,
	config DSGACacheShadowConfig, timeout time.Duration,
	read func(context.Context) (compute.LogicalDemandObservation, error),
) func() {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Duration(config.SampleIntervalSeconds) * time.Second)
		defer ticker.Stop()
		var lastSample control.Uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				readCtx, finish := context.WithTimeout(ctx, timeout)
				fact, err := read(readCtx)
				finish()
				if err != nil {
					logger.Debug("DSGA cache shadow observation unavailable")
					continue
				}
				evaluatedAt := time.Now()
				estimated, err := config.estimate(instance, evaluatedAt, fact)
				if err != nil {
					logger.Debug("DSGA cache shadow sample outside policy window")
					continue
				}
				if fact.Sample.SampledAtUnixMicros <= lastSample {
					logger.Debug("DSGA cache shadow sample clock did not advance")
					continue
				}
				lastSample = fact.Sample.SampledAtUnixMicros
				logger.Info("DSGA cache shadow working set",
					zap.Uint64("estimated_bytes", estimated),
					zap.Time("evaluated_at", evaluatedAt),
					zap.Any("observation", fact),
					zap.Uint32("block_bytes", fact.Sample.Demand.BlockBytes),
					zap.Uint32("covered_seconds", fact.Sample.Demand.CoveredSeconds))
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
