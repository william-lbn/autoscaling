package main

import (
	"fmt"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 再从原整组raw assets派生，不能由调用者bool/空command冒充固定模式。
func fixedRuntimeISOSelected(assets map[string][]byte) (bool, error) {
	if len(assets) == 0 {
		return false, nil
	}
	req, e := resourceguard.DeriveRuntimeBootRequirements(assets[resourceguard.RuntimeResourceAssetName], assets)
	if e != nil {
		return false, e
	}
	return req.Enabled(), nil
}

// 静态新镜像的argv/env/持久孔在构建计划固定；拒未实现的动态挂载/脚本合同，
// 不悄悄丢弃调用者配置，也不通过mounts.sh恢复任意root shell。此检查在Birth前。
func validateFixedRuntimeVMSpec(cfg *Config, spec *vmv1.VirtualMachineSpec) error {
	if cfg == nil || spec == nil {
		return fmt.Errorf("original fixed runtime inputs missing")
	}
	if cfg.initialBootstrap == nil {
		if cfg.runtimeBoot != nil && cfg.runtimeBoot.Enabled() {
			return fmt.Errorf("fixed boot lost original initial mode")
		}
		return nil
	}
	if cfg.runtimeBoot == nil || !cfg.runtimeBoot.Enabled() || spec.DSGABootstrap == nil {
		return fmt.Errorf("fixed runtime mode has no original managed source")
	}
	if len(spec.Guest.Command) != 0 || len(spec.Guest.Args) != 0 || len(spec.Guest.Env) != 0 || len(spec.Disks) != 0 || spec.TLS != nil || spec.InitScript != "" || spec.EnableSSH != nil && *spec.EnableSSH {
		return fmt.Errorf("fixed runtime forbids legacy command/env/init scripts, SSH and undeclared data mounts")
	}
	if spec.Guest.Settings != nil && (len(spec.Guest.Settings.Sysctl) != 0 || spec.Guest.Settings.Swap != nil) {
		return fmt.Errorf("fixed runtime forbids legacy sysctl/swap overrides")
	}
	return nil
}
