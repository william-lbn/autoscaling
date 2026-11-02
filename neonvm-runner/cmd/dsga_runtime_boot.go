package main

import (
	"encoding/json"
	"fmt"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"go.uber.org/zap"
)

// 只派生计划；原VM/Secret绑定已由前置DecodeRuntimeAssets验证。没有资产就是
// unmanaged；失败先清旧计划，不能遗留上一份配置。这里不产生实际启动证据。
func configureRuntimeBootRequirements(cfg *Config, assets map[string][]byte) error {
	if cfg == nil {
		return fmt.Errorf("Runner configuration required")
	}
	cfg.runtimeBoot = nil
	cfg.initialBootstrap = nil
	if len(assets) == 0 {
		return nil
	}
	plan, err := resourceguard.DeriveRuntimeBootRequirements(assets[resourceguard.RuntimeResourceAssetName], assets)
	if err != nil {
		return err
	}
	if plan.Enabled() {
		base, err := initialBaseForRuntime(assets[resourceguard.RuntimeResourceAssetName])
		if err != nil {
			return err
		}
		if plan.BootMemoryUpperBoundBytes() < base {
			return fmt.Errorf("fixed initial base exceeds original boot upper")
		}
		cfg.initialBootstrap = &vmguard.InitialBootstrapConfig{RuntimeJSON: append([]byte(nil), assets[resourceguard.RuntimeResourceAssetName]...), OriginalBaseBytes: base}
	}
	cfg.runtimeBoot = &plan
	return nil
}

func checkedRuntimeKernelCmdline(cfg *Config, logger *zap.Logger, spec *vmv1.VirtualMachineSpec, status *vmv1.VirtualMachineStatus, hostname string) (string, error) {
	if cfg == nil || spec == nil || status == nil {
		return "", fmt.Errorf("original Runner boot inputs required")
	}
	if spec.DSGABootstrap != nil && cfg.runtimeBoot == nil {
		return "", fmt.Errorf("managed Runner lacks original boot requirements")
	}
	if spec.DSGABootstrap == nil && cfg.runtimeBoot != nil {
		return "", fmt.Errorf("runtime boot requirements on unmanaged Runner")
	}
	if cfg.runtimeBoot != nil && cfg.runtimeBoot.Enabled() {
		if spec.DSGABootstrap == nil || cfg.architecture != architectureAmd64 {
			return "", fmt.Errorf("normal-split boot requires original managed amd64 VM")
		}
	}
	cmdline := makeKernelCmdline(cfg, logger, spec, status, hostname)
	if cfg.runtimeBoot != nil {
		if err := cfg.runtimeBoot.ValidateKernelCommandLine(cmdline); err != nil {
			return "", err
		}
	}
	return cmdline, nil
}

// 调用方已派生完整原资产计划；最终QEMU入口还先核原raw SHA。这里复用
// RuntimeDocument.Source严格校验，只选择受支持部署模型的两个固定常量，
// 不新增用户可配base，不把计划值或模型字符串签成实际QMP出生事实。
func initialBaseForRuntime(raw []byte) (uint64, error) {
	var document resourceguard.RuntimeDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		return 0, err
	}
	source, err := document.Source()
	if err != nil {
		return 0, err
	}
	if source.Config.ExecutionModel == resourceguard.TrustedControllerExecutionModel {
		return trustedControllerInitialBaseBytes, nil
	}
	return managedInitialBaseBytes, nil
}
