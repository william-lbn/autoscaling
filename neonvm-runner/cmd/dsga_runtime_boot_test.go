package main

import (
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"go.uber.org/zap"
	"strings"
	"testing"
)

func runnerBootModelSpec() *vmv1.VirtualMachineSpec {
	return &vmv1.VirtualMachineSpec{DSGABootstrap: &vmv1.DSGABootstrap{ProtocolVersion: 1, Ready: true, ConfigSecretUID: "model-secret", VMUID: "model-vm", ComputeInstanceID: "model-compute", Incarnation: "1234567890abcdef1234567890abcdef"}}
}
func runnerBootModelConfig() *Config {
	return &Config{architecture: architectureAmd64, cpuScalingMode: vmv1.CpuScalingModeQMP, autoMovableRatio: "80"}
}

// 模型只核生产Runner静态接线，不启动QEMU，不认证实际Guest/base/资源来源。
func TestRunnerRuntimeBootFinalCommandLine(t *testing.T) {
	for _, version := range []uint32{1, 2} {
		name := "v1"
		if version == 2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) {
			cfg := runnerBootModelConfig()
			assets := runtimeISOFixture(t, version)
			if err := configureRuntimeBootRequirements(cfg, assets); err != nil {
				t.Fatal(err)
			}
			cmdline, err := checkedRuntimeKernelCmdline(cfg, zap.NewNop(), runnerBootModelSpec(), &vmv1.VirtualMachineStatus{}, "model-guest")
			if err != nil {
				t.Fatal(err)
			}
			managed := 0
			for _, arg := range strings.Fields(cmdline) {
				if arg == "neonvm.dsga=1" {
					managed++
				}
			}
			if managed != 1 {
				t.Fatalf("managed marker count %d", managed)
			}
			if version == 2 {
				if !cfg.runtimeBoot.Enabled() || strings.Contains(cmdline, "auto-movable") || strings.Contains(cmdline, "auto_movable_ratio") {
					t.Fatal("new boot mixed old memory policy")
				}
				if !strings.Contains(cmdline, "memhp_default_state=online_movable") || !strings.Contains(cmdline, "neonvm.pid_max_limit=512") {
					t.Fatal("new boot lost required early policy")
				}
				if strings.Contains(cmdline, "kernelcore=") || strings.Contains(cmdline, "mem=") {
					t.Fatal("planning upper bound was turned into physical layout")
				}
			} else if cfg.runtimeBoot.Enabled() || !strings.Contains(cmdline, "memory_hotplug.online_policy=auto-movable") {
				t.Fatal("v1 boot selection changed")
			}
		})
	}
	cfg := runnerBootModelConfig()
	if err := configureRuntimeBootRequirements(cfg, nil); err != nil {
		t.Fatal(err)
	}
	cmdline, err := checkedRuntimeKernelCmdline(cfg, zap.NewNop(), &vmv1.VirtualMachineSpec{}, &vmv1.VirtualMachineStatus{}, "")
	if err != nil || strings.Contains(cmdline, "neonvm.dsga=") {
		t.Fatal("unmanaged boot gained managed policy", err)
	}
}

func TestRunnerRuntimeBootRejectsFinalOverrides(t *testing.T) {
	for _, extra := range []string{"memhp_default_state=online", "memhp-default-state=online_movable", "neonvm.pid_max_limit=512", "memory_hotplug.online_policy=auto-movable", "neonvm.dsga.normal_split=split-v1", "lsm=none", "-- neonvm.pid_max_limit=512", strings.Repeat("x", 2100)} {
		t.Run(extra, func(t *testing.T) {
			cfg := runnerBootModelConfig()
			if err := configureRuntimeBootRequirements(cfg, runtimeISOFixture(t, 2)); err != nil {
				t.Fatal(err)
			}
			cfg.appendKernelCmdline = extra
			line, err := checkedRuntimeKernelCmdline(cfg, zap.NewNop(), runnerBootModelSpec(), &vmv1.VirtualMachineStatus{}, "")
			if err == nil || line != "" {
				t.Fatal("conflicting final argv accepted")
			}
		})
	}
	cfg := runnerBootModelConfig()
	if err := configureRuntimeBootRequirements(cfg, runtimeISOFixture(t, 2)); err != nil {
		t.Fatal(err)
	}
	cfg.architecture = architectureArm64
	if _, err := checkedRuntimeKernelCmdline(cfg, zap.NewNop(), runnerBootModelSpec(), &vmv1.VirtualMachineStatus{}, ""); err == nil {
		t.Fatal("amd64 normal profile borrowed by arm64")
	}
	cfg.architecture = architectureAmd64
	if _, err := checkedRuntimeKernelCmdline(cfg, zap.NewNop(), &vmv1.VirtualMachineSpec{}, &vmv1.VirtualMachineStatus{}, ""); err == nil {
		t.Fatal("new profile on unmanaged spec")
	}
	cfg = runnerBootModelConfig()
	if err := configureRuntimeBootRequirements(cfg, runtimeISOFixture(t, 1)); err != nil {
		t.Fatal(err)
	}
	cfg.appendKernelCmdline = "neonvm.dsga.normal_split=split-v1"
	if _, err := checkedRuntimeKernelCmdline(cfg, zap.NewNop(), runnerBootModelSpec(), &vmv1.VirtualMachineStatus{}, ""); err == nil {
		t.Fatal("v1 append silently enrolled new profile")
	}
}

func TestRunnerRuntimeBootFailureCannotFallBack(t *testing.T) {
	cfg := runnerBootModelConfig()
	if err := configureRuntimeBootRequirements(cfg, runtimeISOFixture(t, 2)); err != nil {
		t.Fatal(err)
	}
	assets := runtimeISOFixture(t, 2)
	delete(assets, "dsga-native-birth.json")
	if err := configureRuntimeBootRequirements(cfg, assets); err == nil {
		t.Fatal("partial boot assets accepted")
	}
	if cfg.runtimeBoot != nil {
		t.Fatal("failed configuration retained old plan")
	}
	if _, err := checkedRuntimeKernelCmdline(cfg, zap.NewNop(), runnerBootModelSpec(), &vmv1.VirtualMachineStatus{}, ""); err == nil {
		t.Fatal("failed managed source fell back")
	}
	if err := configureRuntimeBootRequirements(cfg, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := checkedRuntimeKernelCmdline(cfg, zap.NewNop(), &vmv1.VirtualMachineSpec{}, &vmv1.VirtualMachineStatus{}, ""); err != nil {
		t.Fatal(err)
	}
	if err := configureRuntimeBootRequirements(nil, assets); err == nil {
		t.Fatal("nil config accepted")
	}
}
