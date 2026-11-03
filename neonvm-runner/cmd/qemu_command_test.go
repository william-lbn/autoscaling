package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 只构造生产命令，不Start、不接Kube、不生成Runner/Guest身份或容量证据。
func TestQEMUProcessCommandOriginalCgroupPath(t *testing.T) {
	dir := t.TempDir()
	qemu := filepath.Join(dir, qemuBinX8664)
	if err := os.WriteFile(qemu, []byte("unit executable fixture; never run"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	args := []string{"-runas", "qemu", "-monitor", "none", "-smp", "cpus=2,maxcpus=2,sockets=1,cores=2,threads=1", "-m", "size=512M,slots=1,maxmem=8192M", "-device", "virtio-mem-pci,id=vm0,memdev=vmem0,node=0,block-size=8M,requested-size=0"}
	before := append([]string(nil), args...)
	bin, argv, err := qemuProcessCommand(&Config{architecture: architectureAmd64}, "/original-pod/neonvm-qemu-original", args)
	if err != nil {
		t.Fatal(err)
	}
	expected := append([]string{"-g", "cpu:/original-pod/neonvm-qemu-original", qemu}, args...)
	if bin != "cgexec" || !filepath.IsAbs(argv[2]) || !reflect.DeepEqual(argv, expected) {
		t.Fatalf("original wrapper/argv changed: %q %q", bin, argv)
	}
	if !reflect.DeepEqual(args, before) {
		t.Fatal("mutated original QEMU arguments")
	}
}

func TestQEMUProcessCommandMissingQEMURefuses(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	bin, argv, err := qemuProcessCommand(&Config{architecture: architectureAmd64}, "/original", []string{"-monitor", "none"})
	if err == nil || bin != "" || argv != nil {
		t.Fatalf("missing original executable accepted: %q %q %v", bin, argv, err)
	}
}

func TestQEMUProcessCommandRelativePATHRefusesDot(t *testing.T) {
	// Go的ErrDot门不得被Abs变换绕过；仅LookPath真正成功才规范化路径。
	dir, err := os.MkdirTemp(".", ".qemu-command-unit-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.WriteFile(filepath.Join(dir, qemuBinX8664), []byte("unit executable fixture; never run"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	_, _, err = qemuProcessCommand(&Config{architecture: architectureAmd64}, "/original", nil)
	if !errors.Is(err, exec.ErrDot) {
		t.Fatalf("relative PATH must preserve ErrDot: %v", err)
	}
}

func TestQEMUProcessCommandLegacySkipUnchanged(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	args := []string{"-monitor", "none"}
	bin, argv, err := qemuProcessCommand(&Config{architecture: architectureAmd64, skipCgroupManagement: true}, "", args)
	if err != nil || bin != qemuBinX8664 || !reflect.DeepEqual(argv, args) {
		t.Fatalf("legacy command changed: %q %q %v", bin, argv, err)
	}
}

// PG17-U-QEMU-MEMCG-001：只构造命令，不启动进程；真实共同硬限由后继现场读回验收。
func TestQEMUProcessCommandDSGAInheritsOriginalPodCgroup(t *testing.T) {
	dir := t.TempDir()
	qemu := filepath.Join(dir, qemuBinX8664)
	if err := os.WriteFile(qemu, []byte("unit executable fixture; never run"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	args := []string{"-runas", "qemu", "-monitor", "none", "-smp", "cpus=2,maxcpus=2,sockets=1,cores=2,threads=1", "-m", "size=512M,slots=1,maxmem=8192M"}
	before := append([]string(nil), args...)
	for _, skip := range []bool{false, true} {
		cfg := &Config{architecture: architectureAmd64, skipCgroupManagement: skip, managedMemory: new(vmguard.Runner)}
		if cfg.usesLegacyQEMUCgroup() {
			t.Fatal("managed launch must not create an external CPU/memory group")
		}
		bin, argv, err := qemuProcessCommand(cfg, "/must-not-use-external-group", args)
		if err != nil || bin != qemu || !filepath.IsAbs(bin) || !reflect.DeepEqual(argv, before) {
			t.Fatalf("managed original argv differs, skip=%v: %q %q %v", skip, bin, argv, err)
		}
		if !reflect.DeepEqual(args, before) {
			t.Fatal("managed launch mutated original arguments")
		}
	}
	if !(&Config{}).usesLegacyQEMUCgroup() || (&Config{skipCgroupManagement: true}).usesLegacyQEMUCgroup() {
		t.Fatal("ordinary setup/skip branch changed")
	}
}

func TestQEMUProcessCommandDSGAStillRefusesUnresolvedExecutable(t *testing.T) {
	for _, skip := range []bool{false, true} {
		cfg := &Config{architecture: architectureAmd64, skipCgroupManagement: skip, managedMemory: new(vmguard.Runner)}
		t.Setenv("PATH", t.TempDir())
		bin, argv, err := qemuProcessCommand(cfg, "", nil)
		if err == nil || bin != "" || argv != nil {
			t.Fatal("managed missing QEMU must fail before Start")
		}
		dir, err := os.MkdirTemp(".", ".managed-qemu-command-unit-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		if err = os.WriteFile(filepath.Join(dir, qemuBinX8664), []byte("unit executable fixture; never run"), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", dir)
		_, _, err = qemuProcessCommand(cfg, "", nil)
		if !errors.Is(err, exec.ErrDot) {
			t.Fatalf("managed relative PATH must retain ErrDot: %v", err)
		}
	}
}
