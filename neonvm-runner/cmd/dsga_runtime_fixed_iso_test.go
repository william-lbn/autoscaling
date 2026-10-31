package main

import (
	"bytes"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"os"
	"path/filepath"
	"testing"
)

func TestRunnerFixedRuntimeISORejectsLegacyInputsBeforeTouchingOutput(t *testing.T) {
	for _, name := range []string{"command", "args", "sysctl", "env", "disk", "ssh", "swap", "shm"} {
		t.Run(name, func(t *testing.T) {
			var command, args, sysctl []string
			var env []vmv1.EnvVar
			var disks []vmv1.Disk
			ssh := false
			var swap, shm *resource.Quantity
			switch name {
			case "command":
				command = []string{"/bin/sh"}
			case "args":
				args = []string{"-c", "id"}
			case "sysctl":
				sysctl = []string{"kernel.core_pattern=core"}
			case "env":
				env = []vmv1.EnvVar{{Name: "PATH", Value: "/tmp"}}
			case "disk":
				disks = []vmv1.Disk{{Name: "data"}}
			case "ssh":
				ssh = true
			case "swap":
				v := resource.MustParse("0")
				swap = &v
			case "shm":
				v := resource.MustParse("1Mi")
				shm = &v
			}
			path := filepath.Join(t.TempDir(), "runtime.iso")
			sentinel := []byte("original bytes")
			if e := os.WriteFile(path, sentinel, 0600); e != nil {
				t.Fatal(e)
			}
			if e := createISO9660runtime(path, command, args, sysctl, env, disks, ssh, swap, shm, runtimeISOFixture(t, 2)); e == nil {
				t.Fatal("legacy input accepted")
			}
			b, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(b, sentinel) {
				t.Fatal("rejection changed original output", e)
			}
		})
	}
}

func TestRunnerFixedRuntimeSpecRejectsOverridesBeforeBirth(t *testing.T) {
	mutations := map[string]func(*vmv1.VirtualMachineSpec){
		"command": func(s *vmv1.VirtualMachineSpec) { s.Guest.Command = []string{"/bin/sh"} },
		"args":    func(s *vmv1.VirtualMachineSpec) { s.Guest.Args = []string{"x"} },
		"env":     func(s *vmv1.VirtualMachineSpec) { s.Guest.Env = []vmv1.EnvVar{{Name: "X", Value: "x"}} },
		"disk":    func(s *vmv1.VirtualMachineSpec) { s.Disks = []vmv1.Disk{{Name: "data"}} },
		"init":    func(s *vmv1.VirtualMachineSpec) { s.InitScript = "true" },
		"ssh":     func(s *vmv1.VirtualMachineSpec) { v := true; s.EnableSSH = &v },
		"sysctl":  func(s *vmv1.VirtualMachineSpec) { s.Guest.Settings = &vmv1.GuestSettings{Sysctl: []string{"a=b"}} },
		"swap": func(s *vmv1.VirtualMachineSpec) {
			v := resource.MustParse("0")
			s.Guest.Settings = &vmv1.GuestSettings{Swap: &v}
		},
	}
	c, s := initialRunnerModel(t)
	if e := validateFixedRuntimeVMSpec(c, s); e != nil {
		t.Fatal(e)
	}
	for n, mutate := range mutations {
		t.Run(n, func(t *testing.T) {
			c, s := initialRunnerModel(t)
			mutate(s)
			if validateFixedRuntimeVMSpec(c, s) == nil {
				t.Fatal("unbound runtime override accepted")
			}
		})
	}
	if e := configureRuntimeBootRequirements(c, runtimeISOFixture(t, 1)); e != nil {
		t.Fatal(e)
	}
	s.InitScript = "legacy retained"
	s.Guest.Command = []string{"/bin/sh"}
	if e := validateFixedRuntimeVMSpec(c, s); e != nil {
		t.Fatal("v1 selection changed", e)
	}
}
