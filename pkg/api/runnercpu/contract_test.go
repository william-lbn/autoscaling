package runnercpu

import (
	"math"
	"strings"
	"testing"
)

func validCPUObservation() Observation {
	return Observation{ProtocolVersion: 1, Scope: Scope, Identity: Identity{VMUID: "vm-one",
		ComputeInstanceID: "compute-one", Incarnation: strings.Repeat("1", 32), RunnerPodUID: "pod-one",
		RunnerBootID: strings.Repeat("2", 32), NodeUID: "node-one"}, Generation: 7,
		CgroupDevice: 1, CgroupInode: 2, QEMUPID: 42, QEMUStartTicks: 7, QuotaUS: 50000, PeriodUS: 100000, Weight: 100,
		OnlineVCPUs: 1, MaximumVCPUs: 2, MaximumMillicores: 2000, CommittedMillicores: 2000, TargetMillicores: 500}
}

func TestFractionalCPUIsExactKernelBandwidthAndIntegerTopology(t *testing.T) {
	observed := validCPUObservation()
	if err := observed.Validate(); err != nil {
		t.Fatal(err)
	}
	cpu, err := observed.Millicores()
	if err != nil || cpu != 500 {
		t.Fatalf("CPU=%d err=%v", cpu, err)
	}
	observed.Pending = true
	observed.OnlineVCPUs = 2
	if err := observed.Validate(); err != nil {
		t.Fatal("valid partial topology must remain observable:", err)
	}
	observed.Pending = false
	if observed.Validate() == nil {
		t.Fatal("partial topology was called complete")
	}
}

func TestCPUObservationRejectsUnboundedInexactForeignAndOverflowSources(t *testing.T) {
	cases := map[string]func(*Observation){
		"lost-commitment":        func(o *Observation) { o.CommittedMillicores = 1000 },
		"maximum-not-topology":   func(o *Observation) { o.MaximumMillicores = 500 },
		"missing-maximum":        func(o *Observation) { o.MaximumMillicores = 0 },
		"version":                func(o *Observation) { o.ProtocolVersion++ },
		"scope":                  func(o *Observation) { o.Scope = "guest-vcpu-only" },
		"zero-period":            func(o *Observation) { o.PeriodUS = 0 },
		"period-small":           func(o *Observation) { o.PeriodUS = 999 },
		"period-large":           func(o *Observation) { o.PeriodUS = 1000001 },
		"quota-zero":             func(o *Observation) { o.QuotaUS = 0 },
		"quota-overflow":         func(o *Observation) { o.QuotaUS = math.MaxUint64 },
		"period-overflow":        func(o *Observation) { o.PeriodUS = math.MaxUint64 },
		"fraction-not-millicore": func(o *Observation) { o.QuotaUS = 50001 },
		"cpu-over-topology":      func(o *Observation) { o.QuotaUS = 300000; o.Pending = true },
		"logical-not-actual":     func(o *Observation) { o.TargetMillicores = 750 },
		"zero-cgroup":            func(o *Observation) { o.CgroupInode = 0 },
		"no-source":              func(o *Observation) { o.Identity.RunnerPodUID = "" },
		"zero-boot":              func(o *Observation) { o.Identity.RunnerBootID = strings.Repeat("0", 32) },
		"weight":                 func(o *Observation) { o.Weight = 10001 },
		"half-vcpu":              func(o *Observation) { o.OnlineVCPUs = 0 },
		"topology-envelope":      func(o *Observation) { o.MaximumVCPUs = 65 },
		"negative-generation":    func(o *Observation) { o.Generation = -1 },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			observed := validCPUObservation()
			change(&observed)
			if observed.Validate() == nil {
				t.Fatal("invalid CPU source accepted")
			}
		})
	}
}
