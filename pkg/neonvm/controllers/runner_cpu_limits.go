package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
)

func setRunnerCPULimits(ctx context.Context, vm *vmv1.VirtualMachine, cpu vmv1.MilliCPU) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	url := fmt.Sprintf("http://%s:%d/cpu_change", vm.Status.PodIP, vm.Spec.RunnerPort)

	update := api.VCPUChange{VCPUs: cpu}

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

	return &result, nil
}
