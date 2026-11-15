package compute

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const maxCacheDatabases = 128

var cacheStorageID = regexp.MustCompile(`^[0-9a-f]{32}$`)

type CacheDatabaseCounters struct {
	DatabaseOID            uint32          `json:"database_oid"`
	StatsResetAtUnixMicros *control.Uint64 `json:"stats_reset_at_unix_micros"`
	BufferHits             control.Uint64  `json:"buffer_hits"`
	BufferReads            control.Uint64  `json:"buffer_reads"`
}

func (c *CacheDatabaseCounters) UnmarshalJSON(raw []byte) error {
	type wire CacheDatabaseCounters
	return control.UnmarshalObject(raw, (*wire)(c))
}

type CacheCounters struct {
	SampledAtUnixMicros control.Uint64          `json:"sampled_at_unix_micros"`
	TenantID            string                  `json:"tenant_id"`
	TimelineID          string                  `json:"timeline_id"`
	Databases           []CacheDatabaseCounters `json:"databases"`
}

func (c *CacheCounters) UnmarshalJSON(raw []byte) error {
	type wire CacheCounters
	return control.UnmarshalObject(raw, (*wire)(c))
}

// CacheObservation is scoped to one Postmaster and storage branch.
type CacheObservation struct {
	ProtocolVersion uint32           `json:"protocol_version"`
	Instance        control.Instance `json:"instance"`
	Counters        CacheCounters    `json:"counters"`
}

func (s *CacheObservation) UnmarshalJSON(raw []byte) error {
	type wire CacheObservation
	return control.UnmarshalObject(raw, (*wire)(s))
}

func (s CacheObservation) validFor(instance control.Instance) bool {
	c := s.Counters
	if s.ProtocolVersion != 1 || s.Instance != instance || instance.Validate() != nil ||
		c.SampledAtUnixMicros == 0 || c.SampledAtUnixMicros > math.MaxInt64 ||
		!cacheStorageID.MatchString(c.TenantID) || !cacheStorageID.MatchString(c.TimelineID) ||
		len(c.Databases) == 0 || len(c.Databases) > maxCacheDatabases {
		return false
	}
	var previous uint32
	for _, d := range c.Databases {
		if d.DatabaseOID <= previous || d.BufferHits > math.MaxUint64-d.BufferReads ||
			d.StatsResetAtUnixMicros != nil && (*d.StatsResetAtUnixMicros == 0 || *d.StatsResetAtUnixMicros > c.SampledAtUnixMicros) {
			return false
		}
		previous = d.DatabaseOID
	}
	return true
}

func (client *Client) CacheObservation(ctx context.Context) (CacheObservation, error) {
	var sample CacheObservation
	if err := client.read(ctx, "/dsg/v1/cache/observation", &sample); err != nil {
		return CacheObservation{}, err
	}
	if !sample.validFor(client.instance) {
		return CacheObservation{}, &CallError{MayHaveExecuted: true, Code: "invalid_cache_observation"}
	}
	return sample, nil
}

// CacheDemand measures logical accesses and the reads that passed the shared
// buffer cache over a continuous interval. Repeated accesses count repeatedly.
type CacheDemand struct {
	BufferHits  control.Uint64 `json:"bufferHits"`
	BufferReads control.Uint64 `json:"bufferReads"`
}

// DemandSince requires the same database set and statistics epochs on both
// ends of the interval. A reset, restart or missing sample breaks continuity.
func (s CacheObservation) DemandSince(previous CacheObservation, maximumGap time.Duration) (CacheDemand, error) {
	var demand CacheDemand
	first, last := previous.Counters, s.Counters
	if !s.validFor(s.Instance) || !previous.validFor(s.Instance) ||
		first.TenantID != last.TenantID || first.TimelineID != last.TimelineID ||
		last.SampledAtUnixMicros <= first.SampledAtUnixMicros || maximumGap < time.Microsecond ||
		last.SampledAtUnixMicros-first.SampledAtUnixMicros > control.Uint64(maximumGap.Microseconds()) ||
		len(first.Databases) != len(last.Databases) {
		return CacheDemand{}, fmt.Errorf("cache observation window is discontinuous")
	}
	for i, database := range last.Databases {
		before := first.Databases[i]
		if database.DatabaseOID != before.DatabaseOID ||
			!sameStatisticsEpoch(before.StatsResetAtUnixMicros, database.StatsResetAtUnixMicros) ||
			database.BufferHits < before.BufferHits || database.BufferReads < before.BufferReads {
			return CacheDemand{}, fmt.Errorf("cache database counters changed epoch")
		}
		hits, reads := database.BufferHits-before.BufferHits, database.BufferReads-before.BufferReads
		if demand.BufferHits > math.MaxUint64-hits || demand.BufferReads > math.MaxUint64-reads {
			return CacheDemand{}, fmt.Errorf("cache database counters overflow")
		}
		demand.BufferHits += hits
		demand.BufferReads += reads
	}
	if demand.BufferHits > math.MaxUint64-demand.BufferReads {
		return CacheDemand{}, fmt.Errorf("cache logical access counters overflow")
	}
	return demand, nil
}

func sameStatisticsEpoch(a, b *control.Uint64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
