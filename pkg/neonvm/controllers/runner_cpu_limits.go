package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
)

func setRunnerCPULimits(ctx context.Context, vm *vmv1.VirtualMachine, cpu vmv1.MilliCPU) error {
	if vm.Spec.DSGABootstrap != nil {
		return fmt.Errorf("managed CPU change requires the original Birth identity")
	}
	return sendRunnerCPUChange(ctx, vm, api.VCPUChange{VCPUs: cpu})
}

func sendRunnerCPUChange(ctx context.Context, vm *vmv1.VirtualMachine, update api.VCPUChange) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	url := fmt.Sprintf("http://%s:%d/cpu_change", vm.Status.PodIP, vm.Spec.RunnerPort)

	data, err := json.Marshal(update)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("setRunnerCgroup: unexpected status %s", resp.Status)
	}
	return nil
}

var errHistoricalCPUUnobserved = errors.New("historical Runner has no original Guest topology observation")

func getRunnerCPULimits(ctx context.Context, vm *vmv1.VirtualMachine) (*api.VCPUCgroup, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	url := fmt.Sprintf("http://%s:%d/cpu_current", vm.Status.PodIP, vm.Spec.RunnerPort)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	// CPU失败会持续重查，错误状态也必须关闭body，避免每轮泄漏连接。
	// 回包只有一个CPU数值，4KiB为协议防御上界，不能对故障peer无限读入。
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("getRunnerCgroup: unexpected status %s", resp.Status)
	}

	const maxCPUResponseBytes = 4 * 1024
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCPUResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxCPUResponseBytes {
		return nil, fmt.Errorf("getRunnerCgroup: CPU response exceeds limit")
	}
	var result api.VCPUCgroup
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	if vm.Spec.DSGABootstrap != nil && vm.Spec.DSGABootstrap.RuntimeContractVersion == 0 {
		if result.Managed != nil {
			return nil, fmt.Errorf("historical Runtime cannot self-upgrade a managed CPU receipt")
		}
		observed := result.HistoricalGuest
		if observed == nil {
			return nil, errHistoricalCPUUnobserved
		}
		b := vm.Spec.DSGABootstrap
		if observed.Validate() != nil || result.VCPUs != vmv1.MilliCPU(observed.OnlineVCPUs*1000) ||
			observed.MaximumVCPUs != vm.Spec.Guest.CPUs.Max.RoundedUp() ||
			observed.Identity.VMUID != string(vm.UID) || observed.Identity.ComputeInstanceID != b.ComputeInstanceID ||
			observed.Identity.Incarnation != b.Incarnation {
			return nil, fmt.Errorf("historical CPU receipt is not original actual Guest topology")
		}
		return &result, nil
	}
	if vm.Spec.DSGABootstrap != nil {
		maximum, committed, budgetErr := vm.Spec.DSGARuntimeCPUBudget()
		if budgetErr != nil {
			return nil, budgetErr
		}
		if result.HistoricalGuest != nil || result.Managed == nil ||
			result.Managed.MaximumMillicores != maximum || result.Managed.CommittedMillicores != committed {
			return nil, fmt.Errorf("original CPU fixed Pod commitment differs")
		}
		if result.Managed == nil || result.Managed.Validate() != nil {
			return nil, fmt.Errorf("original CPU kernel observation unavailable")
		}
		current, err := result.Managed.Millicores()
		b := vm.Spec.DSGABootstrap
		id := result.Managed.Identity
		if err != nil || uint32(result.VCPUs) != current || id.VMUID != string(vm.UID) ||
			id.Incarnation != b.Incarnation || id.ComputeInstanceID != b.ComputeInstanceID {
			return nil, fmt.Errorf("original CPU observation identity or kernel quota differs")
		}
	}
	return &result, nil
}

func originalRunnerCPUIdentity(vm *vmv1.VirtualMachine, podUID, bootID, nodeUID string) runnercpu.Identity {
	b := vm.Spec.DSGABootstrap
	return runnercpu.Identity{VMUID: string(vm.UID), ComputeInstanceID: b.ComputeInstanceID,
		Incarnation: b.Incarnation, RunnerPodUID: podUID, RunnerBootID: bootID, NodeUID: nodeUID}
}
