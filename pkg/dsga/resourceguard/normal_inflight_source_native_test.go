//go:build linux && dsga_native_normal_inflight_source

package resourceguard

import (
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"os"
	"strings"
	"syscall"
	"testing"
)

// 只有显式隔离369 Guest可写负例控制；生产reader本身全程只读。
func TestNativeNormalInflightSourceComponent(t *testing.T) {
	if os.Geteuid() != 0 || os.Getenv("DSGA_NATIVE_INFLIGHT_COMPONENT") != "isolated-369-inflight-source-v1" {
		t.Fatal("explicit isolated Guest required")
	}
	read := func(p string) string {
		t.Helper()
		b, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		return strings.TrimSpace(string(b))
	}
	if !strings.Contains(" "+read("/proc/cmdline")+" ", " dsga_inflight_source=1 ") {
		t.Fatal("isolated boot marker required")
	}
	st, e := os.Stat(pgCgroup)
	if e != nil {
		t.Fatal(e)
	}
	s := st.Sys().(*syscall.Stat_t)
	id := Identity{VMUID: "test-harness-inflight-source", ComputeInstanceID: "component-only-no-origin", GuestBootID: read("/proc/sys/kernel/random/boot_id"), CgroupDevice: control.Uint64(s.Dev), CgroupInode: control.Uint64(s.Ino)}
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if e != nil {
		t.Fatal(e)
	}
	if os.Getenv("DSGA_NATIVE_INFLIGHT_ARTIFACT") != ref.KernelArtifactSHA256 {
		t.Fatal("artifact receipt differs")
	}

	if os.Getenv("DSGA_NATIVE_INFLIGHT_EXPECT_REJECTION") == "balloon" {
		if _, e := readNormalInflightSource(id); e == nil || !strings.Contains(e.Error(), "balloon") {
			t.Fatalf("actual target0 balloon must be rejected: %v", e)
		}
		fmt.Println("DSGA_NATIVE_INFLIGHT_BALLOON_REJECTED=1")
		return
	}
	var good normalInflightSourceFacts
	t.Run("actual-readonly-source", func(t *testing.T) {
		before := map[string]string{}
		for _, p := range []string{"/proc/sys/kernel/modules_disabled", "/proc/sys/kernel/numa_balancing", "/sys/kernel/mm/numa/demotion_enabled", "/proc/sys/vm/watermark_boost_factor", "/proc/sys/vm/percpu_pagelist_high_fraction", "/proc/sys/vm/compaction_proactiveness", pgCgroup + "/memory.max"} {
			before[p] = read(p)
		}
		var err error
		good, err = readNormalInflightSource(id)
		if err != nil {
			t.Fatal(err)
		}
		terms, err := normalInflightCalculate(good)
		if err != nil {
			t.Fatal(err)
		}
		if terms.CurrentPossibleCPUs != 2 || terms.CompiledCPUCeiling != 64 || terms.AllSourcesClosed || terms.NormalDecreaseAuthorized {
			t.Fatal("source semantics changed")
		}
		again, err := readNormalInflightSource(id)
		if err != nil {
			t.Fatal(err)
		}
		if err = good.Physical.Match(again.Physical); err != nil {
			t.Fatal(err)
		}
		for p, v := range before {
			if read(p) != v {
				t.Fatalf("readonly reader wrote %s", p)
			}
		}
		raw, err := json.Marshal(struct {
			ID    string                    `json:"id"`
			Facts normalInflightSourceFacts `json:"facts"`
			Terms normalInflightSourceTerms `json:"terms"`
		}{"NEON-S-INFLIGHT-SOURCE-001", good, terms})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("DSGA_NATIVE_INFLIGHT_SOURCE=%s\n", raw)
	})
	if os.Getenv("DSGA_NATIVE_INFLIGHT_CANDIDATE_MAXIMUM") == "12884901888" {
		t.Run("actual-12GiB-source-candidate", func(t *testing.T) {
			f, e := readNormalInflightSource(id)
			if e != nil {
				t.Fatal(e)
			}
			if f.NonMovableManagedPages < 100000 || uint64(f.Physical.NonMovableUpperBytes) > 512<<20 || f.Physical.RuntimeCapabilities.PerCPUPagelistHighFraction != 1024 {
				t.Fatalf("actual12GiB source differs: lowmem=%d K=%d", f.NonMovableManagedPages, f.Physical.NonMovableUpperBytes)
			}
			r, e := normalInflightPlan(f, normalInflightPlanningInput{MaximumBytes: 12 << 30, NormalLeavesBytes: 1536 << 20, SystemBytes: 512 << 20})
			if e != nil {
				t.Fatal(e)
			}
			if !r.CandidateFits || !r.CandidateOnly || r.MmaxAuthorized || r.NormalDecreaseAuthorized {
				t.Fatalf("conditional12GiB does not fit: %+v", r)
			}
			raw, e := json.Marshal(r)
			if e != nil {
				t.Fatal(e)
			}
			fmt.Printf("DSGA_NATIVE_INFLIGHT_12G_CANDIDATE=%s\n", raw)
		})
	}

	t.Run("actual-numa-drift-rejected", func(t *testing.T) {
		path := "/proc/sys/kernel/numa_balancing"
		if read(path) != "0" {
			t.Fatal("original test control differs")
		}
		if e := os.WriteFile(path, []byte("1\n"), 0600); e != nil {
			t.Fatal(e)
		}
		defer func() {
			if e := os.WriteFile(path, []byte("0\n"), 0600); e != nil {
				t.Fatal(e)
			}
		}()
		if _, e := readNormalInflightSource(id); e == nil {
			t.Fatal("runtime drift accepted")
		}
	})
	t.Run("restored-source-still-non-authorizing", func(t *testing.T) {
		f, e := readNormalInflightSource(id)
		if e != nil {
			t.Fatal(e)
		}
		terms, e := normalInflightCalculate(f)
		if e != nil || terms.AllSourcesClosed || terms.NormalDecreaseAuthorized {
			t.Fatal(e)
		}
	})
}
