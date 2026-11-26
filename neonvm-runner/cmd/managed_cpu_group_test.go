package main

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestCPUHostPathDiscoveryUsesOnlyOriginalContainerAndQoS(t *testing.T) {
	id := cpuFixtureIdentity()
	id.RunnerPodUID = "12345678-1234-5678-9abc-123456789abc"
	cid := "containerd://" + strings.Repeat("a", 64)
	for _, qos := range []string{"Guaranteed", "Burstable", "BestEffort"} {
		paths, err := cpuGroupCandidates("/", &id, cid, qos)
		if err != nil || len(paths) != 3 || paths[0] != "/" {
			t.Fatalf("qos=%s candidates=%v err=%v", qos, paths, err)
		}
		for _, path := range paths[1:] {
			if !strings.HasPrefix(path, "/kubepods") || !strings.Contains(path, strings.Repeat("a", 64)) {
				t.Fatal("foreign path candidate", path)
			}
		}
	}
	for _, container := range []string{"docker://" + strings.Repeat("a", 64), "containerd://../../other",
		"containerd://" + strings.Repeat("A", 64)} {
		if _, err := cpuGroupCandidates("/", &id, container, "Burstable"); err == nil {
			t.Fatal("untrusted runtime path accepted")
		}
	}
	bad := id
	bad.RunnerPodUID = "../../root"
	if _, err := cpuGroupCandidates("/", &bad, cid, "Burstable"); err == nil {
		t.Fatal("Pod UID path escape accepted")
	}
	paths, err := cpuGroupCandidates("/actual/original/path", nil, "", "")
	if err != nil || len(paths) != 1 || paths[0] != "/actual/original/path" {
		t.Fatal("original namespace path should not require guessed Host paths")
	}
}

func TestCPUFDMemberChecksRequireBothOriginalProcesses(t *testing.T) {
	path := t.TempDir()
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	self := strconv.Itoa(os.Getpid())
	if err = os.WriteFile(path+"/cgroup.type", []byte("domain\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path+"/cgroup.procs", []byte(self+"\n42\n"), 0600); err != nil {
		t.Fatal(err)
	}
	group := &originalCPUGroup{fd: fd, qemu: func(check func(int, uint64) error) error { return check(42, 7) }}
	if err = group.checkMembers(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path+"/cgroup.type", []byte("domain threaded\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if group.checkMembers() == nil {
		t.Fatal("threaded QEMU process membership was called whole-process CPU ownership")
	}
	if err = os.WriteFile(path+"/cgroup.type", []byte("domain\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path+"/cgroup.procs", []byte(self+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if group.checkMembers() == nil {
		t.Fatal("missing original QEMU was accepted")
	}
	if err = os.WriteFile(path+"/cgroup.procs", []byte("42\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if group.checkMembers() == nil {
		t.Fatal("missing original Runner was accepted")
	}
	if err = os.WriteFile(path+"/cgroup.procs", []byte(self+"\n42\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(path+"/cgroup.procs", path+"/old"); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(path+"/old", path+"/cgroup.procs"); err != nil {
		t.Fatal(err)
	}
	if group.checkMembers() == nil {
		t.Fatal("member reader followed replacement symlink")
	}
}
