package compute

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const logicalDemandWindowSeconds = 60
const maxLogicalDemandSeconds = 3600

type LogicalDemandWindow struct {
	Seconds         uint32 `json:"seconds"`
	DistinctPages   uint64 `json:"distinct_pages"`
	SharedHitPages  uint64 `json:"shared_hit_pages"`
	SharedMissPages uint64 `json:"shared_miss_pages"`
}

func (w *LogicalDemandWindow) UnmarshalJSON(raw []byte) error {
	type wire LogicalDemandWindow
	return control.UnmarshalObject(raw, (*wire)(w))
}

type LogicalDemandSeries struct {
	Version        uint32                `json:"version"`
	Scope          string                `json:"scope"`
	BlockBytes     uint32                `json:"block_bytes"`
	CoveredSeconds uint32                `json:"covered_seconds"`
	Windows        []LogicalDemandWindow `json:"windows"`
}

func (d *LogicalDemandSeries) UnmarshalJSON(raw []byte) error {
	type wire LogicalDemandSeries
	return control.UnmarshalObject(raw, (*wire)(d))
}

type LogicalDemandSample struct {
	SampledAtUnixMicros control.Uint64      `json:"sampled_at_unix_micros"`
	TenantID            string              `json:"tenant_id"`
	TimelineID          string              `json:"timeline_id"`
	Demand              LogicalDemandSeries `json:"demand"`
}

func (s *LogicalDemandSample) UnmarshalJSON(raw []byte) error {
	type wire LogicalDemandSample
	return control.UnmarshalObject(raw, (*wire)(s))
}

// LogicalDemandObservation contains approximate distinct demand pages in
// complete minute windows of one Postmaster. Hit and miss pages can overlap.
type LogicalDemandObservation struct {
	ProtocolVersion uint32              `json:"protocol_version"`
	Instance        control.Instance    `json:"instance"`
	Sample          LogicalDemandSample `json:"sample"`
}

func (s *LogicalDemandObservation) UnmarshalJSON(raw []byte) error {
	type wire LogicalDemandObservation
	return control.UnmarshalObject(raw, (*wire)(s))
}

func (s LogicalDemandObservation) validFor(expected control.Instance) bool {
	demand := s.Sample.Demand
	if s.ProtocolVersion != 1 || expected.Validate() != nil || s.Instance != expected ||
		s.Sample.SampledAtUnixMicros == 0 || s.Sample.SampledAtUnixMicros > math.MaxInt64 ||
		!cacheStorageID.MatchString(s.Sample.TenantID) || !cacheStorageID.MatchString(s.Sample.TimelineID) ||
		demand.Version != 1 || demand.Scope != "postmaster_shared_relation_demand" ||
		demand.BlockBytes < 512 || demand.BlockBytes > 32768 || bits.OnesCount32(demand.BlockBytes) != 1 ||
		demand.CoveredSeconds > maxLogicalDemandSeconds || demand.Windows == nil ||
		len(demand.Windows) != int(demand.CoveredSeconds/logicalDemandWindowSeconds) {
		return false
	}
	for index, window := range demand.Windows {
		if window.Seconds != uint32(index+1)*logicalDemandWindowSeconds ||
			window.DistinctPages > 1<<32 || window.SharedHitPages > 1<<32 || window.SharedMissPages > 1<<32 {
			return false
		}
	}
	return true
}

func (client *Client) LogicalDemandObservation(ctx context.Context) (LogicalDemandObservation, error) {
	var observation LogicalDemandObservation
	if err := client.read(ctx, "/dsg/v1/cache/logical-demand", &observation); err != nil {
		return LogicalDemandObservation{}, err
	}
	if !observation.validFor(client.instance) {
		return LogicalDemandObservation{}, &CallError{MayHaveExecuted: true, Code: "invalid_logical_demand_observation"}
	}
	return observation, nil
}

// WorkingSetPages returns complete, fresh union windows for shadow policy.
// The caller supplies its original expected storage and instance identity.
func (s LogicalDemandObservation) WorkingSetPages(instance control.Instance,
	tenantID, timelineID string, now time.Time, maximumAge time.Duration) ([]float64, error) {
	sampled := time.UnixMicro(int64(s.Sample.SampledAtUnixMicros))
	if !s.validFor(instance) || s.Sample.TenantID != tenantID || s.Sample.TimelineID != timelineID ||
		now.IsZero() || maximumAge <= 0 || sampled.After(now) || now.Sub(sampled) > maximumAge ||
		len(s.Sample.Demand.Windows) == 0 {
		return nil, fmt.Errorf("logical demand window unavailable")
	}
	pages := make([]float64, len(s.Sample.Demand.Windows))
	for i, window := range s.Sample.Demand.Windows {
		pages[i] = float64(window.DistinctPages)
	}
	return pages, nil
}
