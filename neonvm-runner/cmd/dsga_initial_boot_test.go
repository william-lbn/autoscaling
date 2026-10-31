package main

import (
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"k8s.io/apimachinery/pkg/api/resource"
	"strings"
	"testing"
)

func initialRunnerModel(t *testing.T) (*Config, *vmv1.VirtualMachineSpec) {
	t.Helper()
	c := runnerBootModelConfig()
	c.cpuScalingMode = vmv1.CpuScalingModeSysfs
	if e := configureRuntimeBootRequirements(c, runtimeISOFixture(t, 2)); e != nil {
		t.Fatal(e)
	}
	s := runnerBootModelSpec()
	s.Guest.MemorySlotSize = resource.MustParse("512Mi")
	s.Guest.MemorySlots = vmv1.MemorySlots{Min: 2, Use: 2, Max: 8}
	return c, s
}

// Min是业务下限，绝不能成为内核出生base；原最大容量则完整留给virtio区域。
func TestRunnerInitialBaseUsesOriginalRuntimeAndSeparateBusinessMinimum(t *testing.T) {
	c, s := initialRunnerModel(t)
	args, e := initialBootMemoryArguments(c, s)
	if e != nil {
		t.Fatal(e)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{"size=268435456b,slots=1,maxmem=4294967296b", "memory-backend-ram,id=vmem0,size=4026531840b", "virtio-mem-pci,id=vm0,memdev=vmem0,node=0,block-size=8M,requested-size=0"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("initial geometry missing %q: %s", want, joined)
		}
	}
	if strings.Contains(joined, "size=1073741824b,") {
		t.Fatal("business Min became original base")
	}
	raw := append([]byte(nil), c.initialBootstrap.RuntimeJSON...)
	assets := runtimeISOFixture(t, 2)
	if e = configureRuntimeBootRequirements(c, assets); e != nil {
		t.Fatal(e)
	}
	assets[resourceguard.RuntimeResourceAssetName][0] ^= 1
	if string(c.initialBootstrap.RuntimeJSON) != string(raw) {
		t.Fatal("caller changed original launch source after selection")
	}
}

func TestRunnerInitialBaseRejectsLostOrMixedSourceAndGeometry(t *testing.T) {
	cases := map[string]func(*Config, *vmv1.VirtualMachineSpec){
		"lost-initial": func(c *Config, s *vmv1.VirtualMachineSpec) { c.initialBootstrap = nil },
		"lost-plan":    func(c *Config, s *vmv1.VirtualMachineSpec) { c.runtimeBoot = nil },
		"changed-raw":  func(c *Config, s *vmv1.VirtualMachineSpec) { c.initialBootstrap.RuntimeJSON[0] ^= 1 },
		"changed-base": func(c *Config, s *vmv1.VirtualMachineSpec) { c.initialBootstrap.OriginalBaseBytes = 512 << 20 },
		"unmanaged":    func(c *Config, s *vmv1.VirtualMachineSpec) { s.DSGABootstrap = nil },
		"arm":          func(c *Config, s *vmv1.VirtualMachineSpec) { c.architecture = architectureArm64 },
		"qmp-cpu":      func(c *Config, s *vmv1.VirtualMachineSpec) { c.cpuScalingMode = vmv1.CpuScalingModeQMP },
		"below-base": func(c *Config, s *vmv1.VirtualMachineSpec) {
			s.Guest.MemorySlotSize = resource.MustParse("128Mi")
			s.Guest.MemorySlots.Min = 1
		},
		"off-grid": func(c *Config, s *vmv1.VirtualMachineSpec) { s.Guest.MemorySlotSize = resource.MustParse("268435457") },
		"overflow": func(c *Config, s *vmv1.VirtualMachineSpec) {
			s.Guest.MemorySlotSize = resource.MustParse("9223372036854775807")
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c, s := initialRunnerModel(t)
			mutate(c, s)
			args, e := initialBootMemoryArguments(c, s)
			if e == nil || args != nil {
				t.Fatal("mixed source silently fell back or emitted QEMU args")
			}
		})
	}
}

func TestRunnerInitialBaseLegacyAndFailedReconfigure(t *testing.T) {
	c, s := initialRunnerModel(t)
	if e := configureRuntimeBootRequirements(c, runtimeISOFixture(t, 1)); e != nil {
		t.Fatal(e)
	}
	if c.initialBootstrap != nil {
		t.Fatal("v1 retained initial mode")
	}
	if args, e := initialBootMemoryArguments(c, s); e != nil || args != nil {
		t.Fatal("v1 memory selection changed", e)
	}
	c, s = initialRunnerModel(t)
	bad := runtimeISOFixture(t, 2)
	delete(bad, "dsga-native-birth.json")
	if e := configureRuntimeBootRequirements(c, bad); e == nil {
		t.Fatal("partial Source accepted")
	}
	if c.initialBootstrap != nil || c.runtimeBoot != nil {
		t.Fatal("failed Source retained a previous plan")
	}
	// 最终kernel入口仍独立拒绝缺来源受管模式；nil不是旧模式回退许可。
	if _, e := checkedRuntimeKernelCmdline(c, nil, s, &vmv1.VirtualMachineStatus{}, ""); e == nil {
		t.Fatal("lost plan reached final managed command")
	}
}
