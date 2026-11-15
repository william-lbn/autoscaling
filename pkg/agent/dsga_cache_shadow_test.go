package agent

import (
	"strings"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
)

func TestDSGACacheShadowUsesCompleteBoundWindows(t *testing.T) {
	now := time.Unix(1770000000, 0)
	instance := control.Instance{VMUID: "vm", ComputeInstanceID: "compute", PGBootID: 12}
	cfg := DSGACacheShadowConfig{
		TenantID: strings.Repeat("a", 32), TimelineID: strings.Repeat("b", 32),
		SampleIntervalSeconds: 10, MaximumSampleAgeSeconds: 30,
		MinimumWindowMinutes: 2, SlopeWindowMinutes: 2, MaximumSlopeIncrease: 3,
	}
	fact := compute.LogicalDemandObservation{
		ProtocolVersion: 1, Instance: instance,
		Sample: compute.LogicalDemandSample{
			SampledAtUnixMicros: control.Uint64(now.Add(-time.Second).UnixMicro()),
			TenantID:            cfg.TenantID, TimelineID: cfg.TimelineID,
			Demand: compute.LogicalDemandSeries{
				Version: 1, Scope: "postmaster_shared_relation_demand", BlockBytes: 4096, CoveredSeconds: 180,
				Windows: []compute.LogicalDemandWindow{
					{Seconds: 60, DistinctPages: 10},
					{Seconds: 120, DistinctPages: 20},
					{Seconds: 180, DistinctPages: 130},
				},
			},
		},
	}
	value, err := cfg.estimate(instance, now, fact)
	if err != nil || value != 20*4096 {
		t.Fatalf("estimate=%d err=%v", value, err)
	}
	cases := map[string]func(*compute.LogicalDemandObservation){
		"old boot":       func(s *compute.LogicalDemandObservation) { s.Instance.PGBootID-- },
		"wrong timeline": func(s *compute.LogicalDemandObservation) { s.Sample.TimelineID = strings.Repeat("c", 32) },
		"stale": func(s *compute.LogicalDemandObservation) {
			s.Sample.SampledAtUnixMicros = control.Uint64(now.Add(-time.Minute).UnixMicro())
		},
		"clock ahead": func(s *compute.LogicalDemandObservation) {
			s.Sample.SampledAtUnixMicros = control.Uint64(now.Add(time.Second).UnixMicro())
		},
		"incomplete window": func(s *compute.LogicalDemandObservation) {
			s.Sample.Demand.CoveredSeconds = 120
			s.Sample.Demand.Windows = s.Sample.Demand.Windows[:2]
		},
		"missing window":     func(s *compute.LogicalDemandObservation) { s.Sample.Demand.Windows = nil },
		"invalid block size": func(s *compute.LogicalDemandObservation) { s.Sample.Demand.BlockBytes++ },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			changed := fact
			change(&changed)
			if _, err := cfg.estimate(instance, now, changed); err == nil {
				t.Fatal("invalid shadow sample produced an estimate")
			}
		})
	}
}
