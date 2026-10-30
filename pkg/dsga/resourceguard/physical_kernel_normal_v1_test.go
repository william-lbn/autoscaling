//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// 全部为 U 模型。真实 ns/LSM/sysctl/ISO/新内核启动由独立 S 收据验证。
func normalV1PhysicalFixture(t *testing.T) PhysicalKernelObservation {
	t.Helper()
	o := physicalFixture(t)
	ref, err := LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	o.KernelRelease, o.KernelNotesSHA256 = ref.KernelRelease, ref.KernelNotesSHA256
	o.RuntimeCapabilities = &PhysicalKernelRuntimeCapabilities{
		ActiveLSMs: []string{"lockdown", "capability", "landlock", "yama", "apparmor"}, LandlockABI: 6,
		InitialPIDNamespaceDevice: 4, InitialPIDNamespaceInode: 0xEFFFFFFC, MemoryHigh: "max",
		MinFreeKBytes: 4096, WatermarkScaleFactor: 10, WatermarkBoostFactor: 0,
		PerCPUPagelistHighFraction: 64, CompactionProactiveness: 0,
	}
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestPhysicalKernelNormalV1BuildAndObservation(t *testing.T) {
	ref, err := LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
	if err != nil || ref.KernelArtifactSHA256 != "552ffabd096736e38d2d4b6c119533e4a26c2b9a47f6e04a8e1795a084f8d85d" || ref.KernelConfigSHA256 != "935e43773879d6a4f5c27c2ef966314de7e0e463a4a9bebefa3dc5dcf283ce5b" || ref.KernelNotesSHA256 != "3d22f15739bcd10735099d344c1c306f6bd7eccfbed8b3e4f1295e5893036d6b" || ref.KernelRelease != "6.12.26-william-normal-v1-b20ac4f85d74" {
		t.Fatal("new reviewed build tuple differs", ref, err)
	}
	for _, o := range []PhysicalKernelObservation{physicalFixture(t), normalV1PhysicalFixture(t)} {
		b, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		var read PhysicalKernelObservation
		if err = json.Unmarshal(b, &read); err != nil {
			t.Fatal(err)
		}
		if err = o.Match(read); err != nil {
			t.Fatal(err)
		}
		if o.RuntimeCapabilities == nil && strings.Contains(string(b), "runtime_capabilities") {
			t.Fatal("old JSON changed")
		}
	}
	o := normalV1PhysicalFixture(t)
	o.RuntimeCapabilities.PerCPUPagelistHighFraction = 0
	o.RuntimeCapabilities.CompactionProactiveness = 20
	if err := o.Validate(); err != nil {
		t.Fatal("component must observe defaults without certifying full allocator", err)
	}
	if o.VMShrinkPermit {
		t.Fatal("component granted shrink")
	}
}

func TestPhysicalKernelNormalV1RejectsSourceDrift(t *testing.T) {
	for name, change := range map[string]func(*PhysicalKernelObservation){
		"notes":                func(o *PhysicalKernelObservation) { o.KernelNotesSHA256 = physicalDigest([]byte("other")) },
		"release-prefix":       func(o *PhysicalKernelObservation) { o.KernelRelease += "-copy" },
		"capabilities-missing": func(o *PhysicalKernelObservation) { o.RuntimeCapabilities = nil },
		"abi":                  func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.LandlockABI = 5 },
		"lsm-missing": func(o *PhysicalKernelObservation) {
			o.RuntimeCapabilities.ActiveLSMs = []string{"capability", "apparmor"}
		},
		"lsm-reordered": func(o *PhysicalKernelObservation) {
			o.RuntimeCapabilities.ActiveLSMs[0], o.RuntimeCapabilities.ActiveLSMs[1] = o.RuntimeCapabilities.ActiveLSMs[1], o.RuntimeCapabilities.ActiveLSMs[0]
		},
		"pid-namespace":            func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.InitialPIDNamespaceInode++ },
		"namespace-device":         func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.InitialPIDNamespaceDevice = 0 },
		"memory-high":              func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.MemoryHigh = "1048576" },
		"scale-invalid":            func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.WatermarkScaleFactor = 0 },
		"fraction-invalid":         func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.PerCPUPagelistHighFraction = 7 },
		"proactive-invalid":        func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.CompactionProactiveness = 101 },
		"min-free-drift":           func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.MinFreeKBytes++ },
		"scale-drift":              func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.WatermarkScaleFactor++ },
		"boost-drift":              func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.WatermarkBoostFactor = 15000 },
		"fraction-drift":           func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.PerCPUPagelistHighFraction = 128 },
		"proactive-drift":          func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.CompactionProactiveness = 20 },
		"old-build-new-capability": func(o *PhysicalKernelObservation) { o.KernelRelease = physicalKernelRelease },
	} {
		t.Run(name, func(t *testing.T) {
			original, current := normalV1PhysicalFixture(t), normalV1PhysicalFixture(t)
			change(&current)
			if err := original.Match(current); err == nil {
				t.Fatal("normal-v1 source drift accepted")
			}
		})
	}
}

func TestPhysicalKernelNormalV1StrictJSONAndBootPolicy(t *testing.T) {
	c := normalV1PhysicalFixture(t).RuntimeCapabilities
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"watermark_boost_factor", "compaction_proactiveness", "percpu_pagelist_high_fraction"} {
		t.Run("missing-"+field, func(t *testing.T) {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(b, &fields); err != nil {
				t.Fatal(err)
			}
			delete(fields, field)
			raw, _ := json.Marshal(fields)
			var read PhysicalKernelRuntimeCapabilities
			if err := json.Unmarshal(raw, &read); err == nil {
				t.Fatal("missing zero-sensitive source accepted")
			}
		})
	}
	for name, raw := range map[string]string{
		"null-zero":      strings.Replace(string(b), `"watermark_boost_factor":0`, `"watermark_boost_factor":null`, 1),
		"duplicate-zero": strings.Replace(string(b), `"watermark_boost_factor":0`, `"watermark_boost_factor":0,"watermark_boost_factor":0`, 1),
		"unknown":        strings.Replace(string(b), `"watermark_boost_factor":0`, `"extra":0,"watermark_boost_factor":0`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			var read PhysicalKernelRuntimeCapabilities
			if err := json.Unmarshal([]byte(raw), &read); err == nil {
				t.Fatal("invalid capability JSON accepted")
			}
		})
	}
	good := "console=ttyS0 cgroup.memory=nokmem,nosocket,nobpf memhp_default_state=online_movable"
	if err := physicalNormalV1BootPolicy(good); err != nil {
		t.Fatal(err)
	}
	for _, extra := range []string{"lsm=landlock,yama", "lsm=", "security=apparmor", "lsm=landlock lsm=landlock", "lsm"} {
		t.Run(extra, func(t *testing.T) {
			if err := physicalNormalV1BootPolicy(good + " " + extra); err == nil {
				t.Fatal("boot override accepted")
			}
		})
	}
}

func TestPhysicalKernelNormalV1OriginBindsBuildAndKeepsHistory(t *testing.T) {
	makeNew := func(t *testing.T) (*Guard, *originFixtureKernel, physicalOriginInputs) {
		g, k, in := originFixture(t)
		next := normalV1PhysicalFixture(t)
		next.Identity = in.bootstrap.Identity
		k.first = next
		ref, err := LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
		if err != nil {
			t.Fatal(err)
		}
		in.policy.ProfileID, in.policy.KernelRelease = ref.ProfileID, ref.KernelRelease
		in.policy.KernelArtifactSHA256, in.policy.KernelConfigSHA256, in.policy.KernelNotesSHA256 = ref.KernelArtifactSHA256, ref.KernelConfigSHA256, ref.KernelNotesSHA256
		in.policySHA = physicalDigest([]byte("U new exact physical policy"))
		return g, k, in
	}
	t.Run("new-capture-reference-replay", func(t *testing.T) {
		g, _, in := makeNew(t)
		o, err := g.physicalOrigin(context.Background(), in, physicalOriginCapture)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := o.ReferenceSHA256()
		if err != nil || ref != physicalDigest(originData(t, g)) {
			t.Fatal(ref, err)
		}
		got, err := g.physicalOrigin(context.Background(), in, physicalOriginReplay)
		if err != nil || got.Nonce != o.Nonce || got.VMShrinkPermit {
			t.Fatal(got, err)
		}
	})
	t.Run("old-source-cannot-upgrade", func(t *testing.T) {
		g, _, in := originFixture(t)
		if _, err := g.physicalOrigin(context.Background(), in, physicalOriginCapture); err != nil {
			t.Fatal(err)
		}
		before := string(originData(t, g))
		ref, _ := LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
		in.policy.ProfileID, in.policy.KernelRelease = ref.ProfileID, ref.KernelRelease
		in.policy.KernelArtifactSHA256, in.policy.KernelConfigSHA256, in.policy.KernelNotesSHA256 = ref.KernelArtifactSHA256, ref.KernelConfigSHA256, ref.KernelNotesSHA256
		if _, err := g.physicalOrigin(context.Background(), in, physicalOriginCapture); err == nil {
			t.Fatal("historical origin upgraded")
		}
		if string(originData(t, g)) != before {
			t.Fatal("historical source rewritten")
		}
	})
	t.Run("cross-build-first-fact", func(t *testing.T) {
		g, _, in := makeNew(t)
		o, err := g.physicalOrigin(context.Background(), in, physicalOriginCapture)
		if err != nil {
			t.Fatal(err)
		}
		o.First = physicalFixture(t)
		o.First.Identity = in.bootstrap.Identity
		o.First.KernelNotesSHA256 = in.policy.KernelNotesSHA256
		if err := o.Validate(); err == nil {
			t.Fatal("new policy combined with old release")
		}
	})
	for _, field := range []string{"artifact", "config", "notes"} {
		t.Run("wrong-"+field, func(t *testing.T) {
			_, _, in := makeNew(t)
			switch field {
			case "artifact":
				in.policy.KernelArtifactSHA256 = physicalKernelArtifactSHA
			case "config":
				in.policy.KernelConfigSHA256 = physicalKernelConfigSHA
			case "notes":
				in.policy.KernelNotesSHA256 = physicalDigest([]byte("foreign"))
			}
			if err := in.policy.Validate(); err == nil {
				t.Fatal("new profile accepted another build field")
			}
		})
	}
}
