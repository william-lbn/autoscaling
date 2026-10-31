//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// 下列全部为U模型，不借模型sysctl/Backend冒充Guest原出生或运行证明。
func reviewedHardPIDFixture(t *testing.T, profile string) PhysicalKernelObservation {
	t.Helper()
	o := normalV1PhysicalFixture(t)
	ref, err := LookupPhysicalKernelBuild(profile)
	if err != nil {
		t.Fatal(err)
	}
	o.KernelRelease, o.KernelNotesSHA256 = ref.KernelRelease, ref.KernelNotesSHA256
	o.PIDMax = 512
	hard := uint32(512)
	o.RuntimeCapabilities.HardPIDMaxLimit = &hard
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestPhysicalKernelReviewedHardPIDBuilds(t *testing.T) {
	expected := []PhysicalKernelBuildReference{
		{NormalV1PIDCappedPhysicalKernelProfileID, "6.12.26-william-normal-v1-pidcap-946be0d25506", "3b5853b552a8adbe5de7772410c793928f0d4a86a2410d24aec806fecfea2608", "7733d4e430a2102052dd97c8b3fa64ac2b5904fa6a4f394b5653b9fa5cccbf25", "cbf448079077b712e5812d4c9c54a9ea3b4e3aacc76892699881c8aad57619dc"},
		{NormalSplitV1PhysicalKernelProfileID, "6.12.26-william-normal-split-v1-e210f5e3ee94", "3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0", "11344757b0f979c7fee35e354bd706616c4283deaa67cc30c50747e4bca855be", "7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828"},
	}
	for _, want := range expected {
		t.Run(want.ProfileID, func(t *testing.T) {
			got, err := LookupPhysicalKernelBuild(want.ProfileID)
			if err != nil || got != want {
				t.Fatal(got, err)
			}
			if got.StrictPageCounterIRQBoundary() != (want.ProfileID == NormalSplitV1PhysicalKernelProfileID) {
				t.Fatal("compiled IRQ property differs")
			}
			got.KernelConfigSHA256 = physicalKernelConfigSHA
			if got.StrictPageCounterIRQBoundary() {
				t.Fatal("cross-build config acquired strict IRQ property")
			}
			again, _ := LookupPhysicalKernelBuild(want.ProfileID)
			if again != want {
				t.Fatal("caller changed compiled lookup")
			}
		})
	}
	strict, _ := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	for name, mutate := range map[string]func(*PhysicalKernelBuildReference){
		"profile":  func(r *PhysicalKernelBuildReference) { r.ProfileID = NormalV1PIDCappedPhysicalKernelProfileID },
		"release":  func(r *PhysicalKernelBuildReference) { r.KernelRelease += "-alias" },
		"artifact": func(r *PhysicalKernelBuildReference) { r.KernelArtifactSHA256 = physicalKernelArtifactSHA },
		"config":   func(r *PhysicalKernelBuildReference) { r.KernelConfigSHA256 = physicalKernelConfigSHA },
		"notes":    func(r *PhysicalKernelBuildReference) { r.KernelNotesSHA256 = physicalDigest([]byte("foreign")) },
	} {
		t.Run("strict-reject-"+name, func(t *testing.T) {
			r := strict
			mutate(&r)
			if r.StrictPageCounterIRQBoundary() {
				t.Fatal("self-reported build field acquired property")
			}
		})
	}
	for _, profile := range []string{physicalOriginProfileID, NormalV1PhysicalKernelProfileID} {
		t.Run("history-"+profile, func(t *testing.T) {
			r, _ := LookupPhysicalKernelBuild(profile)
			if r.StrictPageCounterIRQBoundary() {
				t.Fatal("historical kernel acquired IRQ property")
			}
		})
	}
}

func TestPhysicalKernelHardPIDObservationBoundaries(t *testing.T) {
	for _, profile := range []string{NormalV1PIDCappedPhysicalKernelProfileID, NormalSplitV1PhysicalKernelProfileID} {
		t.Run(profile, func(t *testing.T) {
			for _, hard := range []uint32{0, 300, 301, 512, 4096, 4097} {
				t.Run("hard-"+strings.TrimSpace(physicalHardPIDJSONNumber(hard)), func(t *testing.T) {
					o := reviewedHardPIDFixture(t, profile)
					o.RuntimeCapabilities.HardPIDMaxLimit = &hard
					if hard >= 301 && hard < o.PIDMax {
						o.PIDMax = hard
					}
					err := o.Validate()
					want := hard >= 301 && hard <= 4096
					if (err == nil) != want {
						t.Fatalf("hard=%d validate=%v", hard, err)
					}
				})
			}
			t.Run("missing", func(t *testing.T) {
				o := reviewedHardPIDFixture(t, profile)
				o.RuntimeCapabilities.HardPIDMaxLimit = nil
				if o.Validate() == nil {
					t.Fatal("missing original hard source accepted")
				}
			})
			t.Run("runtime-over-hard", func(t *testing.T) {
				o := reviewedHardPIDFixture(t, profile)
				o.PIDMax = 513
				if o.Validate() == nil {
					t.Fatal("contradictory runtime pid_max accepted")
				}
			})
			t.Run("notes-from-other-build", func(t *testing.T) {
				o := reviewedHardPIDFixture(t, profile)
				other, _ := LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
				o.KernelNotesSHA256 = other.KernelNotesSHA256
				if o.Validate() == nil {
					t.Fatal("mixed release/notes accepted")
				}
			})
		})
	}
	for _, o := range []PhysicalKernelObservation{physicalFixture(t), normalV1PhysicalFixture(t)} {
		t.Run("old-cannot-add-"+o.KernelRelease, func(t *testing.T) {
			if o.RuntimeCapabilities == nil {
				o.RuntimeCapabilities = normalV1PhysicalFixture(t).RuntimeCapabilities
			}
			h := uint32(512)
			o.RuntimeCapabilities.HardPIDMaxLimit = &h
			if o.Validate() == nil {
				t.Fatal("historical build acquired hard source")
			}
		})
	}
}

func physicalHardPIDJSONNumber(n uint32) string { b, _ := json.Marshal(n); return string(b) }

func TestPhysicalKernelHardPIDJSONAndHistory(t *testing.T) {
	for _, o := range []PhysicalKernelObservation{physicalFixture(t), normalV1PhysicalFixture(t)} {
		t.Run("legacy-"+o.KernelRelease, func(t *testing.T) {
			b, err := json.Marshal(o)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(b), "hard_pid_max_limit") || strings.Contains(string(b), "strict_page_counter") {
				t.Fatal("legacy JSON shape changed")
			}
			var next PhysicalKernelObservation
			if err = json.Unmarshal(b, &next); err != nil || o.Match(next) != nil {
				t.Fatal("legacy JSON recovery changed", err)
			}
		})
	}
	t.Run("legacy-552-explicit-null-still-rejected", func(t *testing.T) {
		b, err := json.Marshal(normalV1PhysicalFixture(t))
		if err != nil {
			t.Fatal(err)
		}
		raw := strings.Replace(string(b), `"compaction_proactiveness":0`, `"compaction_proactiveness":0,"hard_pid_max_limit":null`, 1)
		if raw == string(b) {
			t.Fatal("fixture did not change")
		}
		var o PhysicalKernelObservation
		if json.Unmarshal([]byte(raw), &o) == nil {
			t.Fatal("new optional null broadened historical decode")
		}
	})

	for _, profile := range []string{NormalV1PIDCappedPhysicalKernelProfileID, NormalSplitV1PhysicalKernelProfileID} {
		t.Run(profile, func(t *testing.T) {
			o := reviewedHardPIDFixture(t, profile)
			b, err := json.Marshal(o)
			if err != nil {
				t.Fatal(err)
			}
			var next PhysicalKernelObservation
			if err = json.Unmarshal(b, &next); err != nil || o.Match(next) != nil {
				t.Fatal("new JSON recovery failed", err)
			}
			for name, replacement := range map[string]string{"missing": "", "null": `"hard_pid_max_limit":null`, "zero": `"hard_pid_max_limit":0`, "negative": `"hard_pid_max_limit":-1`, "text": `"hard_pid_max_limit":"512"`, "fraction": `"hard_pid_max_limit":512.0`, "duplicate": `"hard_pid_max_limit":512,"hard_pid_max_limit":512`, "self-IRQ": `"hard_pid_max_limit":512,"strict_page_counter_irq_boundary":true`} {
				t.Run(name, func(t *testing.T) {
					old := `"hard_pid_max_limit":512`
					raw := strings.Replace(string(b), old, replacement, 1)
					if replacement == "" {
						raw = strings.Replace(string(b), `,`+old, "", 1)
					}
					if raw == string(b) {
						t.Fatal("negative fixture did not change JSON")
					}
					var bad PhysicalKernelObservation
					if json.Unmarshal([]byte(raw), &bad) == nil {
						t.Fatal("unbound or self-reported source accepted")
					}
				})
			}
		})
	}
}

func TestPhysicalKernelHardPIDBootPolicy(t *testing.T) {
	base := "console=ttyS0 cgroup.memory=nokmem,nosocket,nobpf memhp_default_state=online_movable"
	for _, hard := range []uint32{301, 512, 4096} {
		t.Run("valid-"+physicalHardPIDJSONNumber(hard), func(t *testing.T) {
			if err := physicalPIDCappedBootPolicy(base+" neonvm.pid_max_limit="+physicalHardPIDJSONNumber(hard), hard); err != nil {
				t.Fatal(err)
			}
		})
	}
	for name, extra := range map[string]string{"missing": "", "bare": "neonvm.pid_max_limit", "empty": "neonvm.pid_max_limit=", "low": "neonvm.pid_max_limit=300", "high": "neonvm.pid_max_limit=4097", "noarg": "neonvm.pid_max_limit=0", "leading": "neonvm.pid_max_limit=0512", "plus": "neonvm.pid_max_limit=+512", "hex": "neonvm.pid_max_limit=0x200", "mismatch": "neonvm.pid_max_limit=301", "duplicate": "neonvm.pid_max_limit=512 neonvm.pid_max_limit=512", "duplicate-other": "neonvm.pid_max_limit=512 neonvm.pid_max_limit=1024", "lsm": "neonvm.pid_max_limit=512 lsm=landlock"} {
		t.Run(name, func(t *testing.T) {
			if physicalPIDCappedBootPolicy(base+" "+extra, 512) == nil {
				t.Fatal("unbound boot hard source accepted")
			}
		})
	}
}

func TestPhysicalKernelHardPIDSourceDrift(t *testing.T) {
	for _, profile := range []string{NormalV1PIDCappedPhysicalKernelProfileID, NormalSplitV1PhysicalKernelProfileID} {
		t.Run(profile, func(t *testing.T) {
			for name, mutate := range map[string]func(*PhysicalKernelObservation){
				"hard":         func(o *PhysicalKernelObservation) { *o.RuntimeCapabilities.HardPIDMaxLimit = 513 },
				"missing-hard": func(o *PhysicalKernelObservation) { o.RuntimeCapabilities.HardPIDMaxLimit = nil },
				"runtime":      func(o *PhysicalKernelObservation) { o.PIDMax = 511 },
				"boot":         func(o *PhysicalKernelObservation) { o.CommandLineSHA256 = physicalDigest([]byte("other")) },
				"other-build": func(o *PhysicalKernelObservation) {
					other := NormalSplitV1PhysicalKernelProfileID
					if profile == other {
						other = NormalV1PIDCappedPhysicalKernelProfileID
					}
					ref, _ := LookupPhysicalKernelBuild(other)
					o.KernelRelease, o.KernelNotesSHA256 = ref.KernelRelease, ref.KernelNotesSHA256
				},
			} {
				t.Run(name, func(t *testing.T) {
					old, next := reviewedHardPIDFixture(t, profile), reviewedHardPIDFixture(t, profile)
					mutate(&next)
					if old.Match(next) == nil {
						t.Fatal("original source drift accepted")
					}
				})
			}
		})
	}
}

func TestPhysicalKernelReviewedOriginHardPIDBuilds(t *testing.T) {
	makeNew := func(t *testing.T, profile string) (*Guard, *originFixtureKernel, physicalOriginInputs) {
		t.Helper()
		g, k, in := originFixture(t)
		o := reviewedHardPIDFixture(t, profile)
		o.Identity = in.bootstrap.Identity
		k.first = o
		ref, _ := LookupPhysicalKernelBuild(profile)
		in.policy.ProfileID, in.policy.KernelRelease = ref.ProfileID, ref.KernelRelease
		in.policy.KernelArtifactSHA256, in.policy.KernelConfigSHA256, in.policy.KernelNotesSHA256 = ref.KernelArtifactSHA256, ref.KernelConfigSHA256, ref.KernelNotesSHA256
		in.policySHA = physicalDigest([]byte("U exact " + profile))
		return g, k, in
	}
	for _, profile := range []string{NormalV1PIDCappedPhysicalKernelProfileID, NormalSplitV1PhysicalKernelProfileID} {
		t.Run(profile, func(t *testing.T) {
			t.Run("capture-reference-replay", func(t *testing.T) {
				g, _, in := makeNew(t, profile)
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
			t.Run("missing-hard-no-file", func(t *testing.T) {
				g, k, in := makeNew(t, profile)
				k.first.RuntimeCapabilities.HardPIDMaxLimit = nil
				if _, err := g.physicalOrigin(context.Background(), in, physicalOriginCapture); err == nil {
					t.Fatal("missing hard source captured")
				}
			})
			t.Run("cross-first-build", func(t *testing.T) {
				g, k, in := makeNew(t, profile)
				other := NormalV1PIDCappedPhysicalKernelProfileID
				if profile == other {
					other = NormalSplitV1PhysicalKernelProfileID
				}
				k.first = reviewedHardPIDFixture(t, other)
				k.first.Identity = in.bootstrap.Identity
				if _, err := g.physicalOrigin(context.Background(), in, physicalOriginCapture); err == nil {
					t.Fatal("cross-build first observation captured")
				}
			})
			for _, field := range []string{"release", "artifact", "config", "notes"} {
				t.Run("cross-policy-"+field, func(t *testing.T) {
					_, _, in := makeNew(t, profile)
					old, _ := LookupPhysicalKernelBuild(NormalV1PhysicalKernelProfileID)
					switch field {
					case "release":
						in.policy.KernelRelease = old.KernelRelease
					case "artifact":
						in.policy.KernelArtifactSHA256 = old.KernelArtifactSHA256
					case "config":
						in.policy.KernelConfigSHA256 = old.KernelConfigSHA256
					case "notes":
						in.policy.KernelNotesSHA256 = old.KernelNotesSHA256
					}
					if in.policy.Validate() == nil {
						t.Fatal("cross-build policy accepted")
					}
				})
			}
			t.Run("legacy-source-stays-original", func(t *testing.T) {
				g, _, in := originFixture(t)
				if _, err := g.physicalOrigin(context.Background(), in, physicalOriginCapture); err != nil {
					t.Fatal(err)
				}
				before := string(originData(t, g))
				ref, _ := LookupPhysicalKernelBuild(profile)
				in.policy.ProfileID, in.policy.KernelRelease = ref.ProfileID, ref.KernelRelease
				in.policy.KernelArtifactSHA256, in.policy.KernelConfigSHA256, in.policy.KernelNotesSHA256 = ref.KernelArtifactSHA256, ref.KernelConfigSHA256, ref.KernelNotesSHA256
				if _, err := g.physicalOrigin(context.Background(), in, physicalOriginCapture); err == nil {
					t.Fatal("legacy source upgraded")
				}
				if string(originData(t, g)) != before {
					t.Fatal("legacy origin overwritten")
				}
			})
		})
	}
}
