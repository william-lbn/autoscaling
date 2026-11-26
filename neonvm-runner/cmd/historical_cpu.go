package main

import (
	"context"
	"fmt"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"go.uber.org/zap"
	"net/http"
)

// 历史合同只读原Guest真实整数CPU，不把旧dummy/lastValue升级成分数权益资格。
func historicalCPUCallbacks(spec *vmv1.VirtualMachineSpec, runner *vmguard.Runner) cpuServerCallbacks {
	source := func() (*api.VCPUCgroup, int, error) {
		if spec == nil || spec.DSGABootstrap == nil || spec.DSGABootstrap.RuntimeContractVersion != 0 || runner == nil {
			return nil, http.StatusServiceUnavailable, fmt.Errorf("historical CPU contract is not selected")
		}
		birth, err := runner.OriginalIdentity()
		if err != nil {
			return nil, http.StatusServiceUnavailable, err
		}
		var pid uint32
		var ticks uint64
		if err = runner.WithOriginalCPUProcess(func(p int, start uint64) error { pid, ticks = uint32(p), start; return nil }); err != nil {
			return nil, http.StatusServiceUnavailable, err
		}
		cpu, _, err := getNeonvmDaemonCPU()
		if err != nil || cpu == 0 || cpu%1000 != 0 {
			return nil, http.StatusServiceUnavailable, fmt.Errorf("actual historical Guest CPU topology unavailable: %v", err)
		}
		observed := &runnercpu.Historical{ProtocolVersion: runnercpu.ProtocolVersion, Scope: runnercpu.HistoricalScope,
			Identity: runnercpu.Identity{VMUID: birth.VMUID, ComputeInstanceID: spec.DSGABootstrap.ComputeInstanceID,
				Incarnation: birth.Incarnation, RunnerPodUID: birth.RunnerPodUID, RunnerBootID: birth.RunnerBootID, NodeUID: birth.NodeUID},
			OnlineVCPUs: uint32(cpu) / 1000, MaximumVCPUs: spec.Guest.CPUs.Max.RoundedUp(), QEMUPID: pid, QEMUStartTicks: ticks}
		if err = observed.Validate(); err != nil {
			return nil, http.StatusServiceUnavailable, err
		}
		return &api.VCPUCgroup{VCPUs: cpu, HistoricalGuest: observed}, http.StatusOK, nil
	}
	return cpuServerCallbacks{
		get: func(*zap.Logger) (*api.VCPUCgroup, int, error) { return source() },
		set: func(context.Context, *zap.Logger, api.VCPUChange) (int, error) {
			return http.StatusConflict, fmt.Errorf("historical CPU observation does not authorize a new entitlement or topology write")
		},
		ready: func(*zap.Logger) bool {
			observed, _, err := source()
			return err == nil && observed.HistoricalGuest.OnlineVCPUs == spec.Guest.CPUs.Use.RoundedUp()
		},
	}
}
