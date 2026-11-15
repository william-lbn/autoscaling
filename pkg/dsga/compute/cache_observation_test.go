package compute

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func TestCacheDemandRequiresContinuousNativeCounters(t *testing.T) {
	before := CacheObservation{ProtocolVersion: 1, Instance: control.Instance{VMUID: "vm", ComputeInstanceID: "compute", PGBootID: 1},
		Counters: CacheCounters{SampledAtUnixMicros: 1000000, TenantID: strings.Repeat("a", 32), TimelineID: strings.Repeat("b", 32),
			Databases: []CacheDatabaseCounters{{DatabaseOID: 10, BufferHits: 200, BufferReads: 10}}}}
	next := before
	next.Counters.SampledAtUnixMicros += 1000000
	next.Counters.Databases = []CacheDatabaseCounters{{DatabaseOID: 10, BufferHits: 1737, BufferReads: 10}}
	demand, err := next.DemandSince(before, 2*time.Second)
	if err != nil || demand != (CacheDemand{BufferHits: 1537, BufferReads: 0}) {
		t.Fatalf("warm trace: %+v %v", demand, err)
	}
	for name, change := range map[string]func(*CacheObservation){
		"boot":     func(s *CacheObservation) { s.Instance.PGBootID++ },
		"timeline": func(s *CacheObservation) { s.Counters.TimelineID = strings.Repeat("c", 32) },
		"database": func(s *CacheObservation) { s.Counters.Databases[0].DatabaseOID++ },
		"reset": func(s *CacheObservation) {
			reset := s.Counters.SampledAtUnixMicros
			s.Counters.Databases[0].StatsResetAtUnixMicros = &reset
		},
		"counter_decreased": func(s *CacheObservation) { s.Counters.Databases[0].BufferHits = 0 },
		"sample_missing":    func(s *CacheObservation) { s.Counters.Databases = nil },
		"gap":               func(s *CacheObservation) { s.Counters.SampledAtUnixMicros += 2000000 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := next
			changed.Counters.Databases = append([]CacheDatabaseCounters(nil), next.Counters.Databases...)
			change(&changed)
			if _, err := changed.DemandSince(before, 2*time.Second); err == nil {
				t.Fatal("discontinuous sample accepted")
			}
		})
	}
}

func TestCacheSampleWirePreservesZeroAndRejectsMissingFields(t *testing.T) {
	const raw = `{"database_oid":10,"stats_reset_at_unix_micros":null,"buffer_hits":"1537","buffer_reads":"0"}`
	var counters CacheDatabaseCounters
	if err := json.Unmarshal([]byte(raw), &counters); err != nil || counters.BufferReads != 0 {
		t.Fatalf("explicit zero: %+v %v", counters, err)
	}
	for _, altered := range []string{
		strings.Replace(raw, `,"buffer_reads":"0"`, "", 1),
		strings.Replace(raw, `"buffer_reads":"0"`, `"buffer_reads":0`, 1),
		strings.Replace(raw, `"buffer_reads":"0"`, `"buffer_reads":"0","buffer_reads":"1"`, 1),
		strings.Replace(raw, `"stats_reset_at_unix_micros":null,`, "", 1),
	} {
		if err := json.Unmarshal([]byte(altered), &counters); err == nil {
			t.Fatalf("invalid cache counter accepted: %s", altered)
		}
	}
}
