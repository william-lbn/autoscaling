//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	bootprotocol "github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

func initialPrebootFixture(t *testing.T) (bootprotocol.InitialPrebootChallenge, []byte, map[string][]byte, Config) {
	t.Helper()
	raw, assets := bootRequirementsFixture(t)
	var d RuntimeDocument
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	source, err := d.Source()
	if err != nil {
		t.Fatal(err)
	}
	ref, err := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	c := bootprotocol.InitialPrebootChallenge{ProtocolVersion: 1, Scope: bootprotocol.InitialPrebootScope, HostStageSHA256: strings.Repeat("10", 32), SourceSHA256: strings.Repeat("20", 32), Nonce: strings.Repeat("30", 32), Identity: bootprotocol.RunnerIdentity{VMUID: source.Config.VMUID, Incarnation: d.Incarnation, RunnerPodUID: "initial-runner", RunnerBootID: "1234567890abcdef1234567890abcdef", NodeUID: "initial-node", MinimumBytes: 1 << 30, MaximumBytes: 2 << 30, BlockBytes: 2 << 20}, RuntimeSHA256: physicalDigest(raw), ResourceSHA256: d.SourceSHA256, ConfigSecretUID: d.ConfigSecretUID, KernelBuildSHA256: ref.KernelArtifactSHA256, OriginalBase: 256 << 20, BusinessMinimum: 1 << 30, Maximum: 2 << 30}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	return c, raw, assets, source.Config
}

// PG17-U-INITIAL-GUEST-001：原Doc2全前缀+实际有效资产与challenge绑定；输入副本不能外泄。
func TestInitialPrebootGuestSourceBinding(t *testing.T) {
	c, raw, assets, config := initialPrebootFixture(t)
	in, err := initialPrebootBindInputs(c, raw, assets, config)
	if err != nil {
		t.Fatal(err)
	}
	if in.challenge != c || in.source.Config != config || in.requirements.BootMemoryUpperBoundBytes() != uint64(c.OriginalBase) {
		t.Fatal("original static input binding lost")
	}
	saved := bytes.Clone(in.runtime)
	raw[0] = ' '
	for name := range assets {
		assets[name] = nil
	}
	if !bytes.Equal(saved, in.runtime) {
		t.Fatal("caller mutated frozen input")
	}
}

// PG17-U-INITIAL-GUEST-002：来源缺失/换UID/换kernel/旧v1/局部资产均在内核写前拒绝。
func TestInitialPrebootGuestRejectsSourceDrift(t *testing.T) {
	for _, mode := range []string{"vm", "incarnation", "runtime-sha", "resource-sha", "secret", "kernel", "base-upper", "business-under-budget", "config", "changed-asset", "missing-asset", "legacy-v1"} {
		t.Run(mode, func(t *testing.T) {
			c, raw, assets, config := initialPrebootFixture(t)
			switch mode {
			case "vm":
				c.Identity.VMUID = "wrong-vm"
			case "incarnation":
				c.Identity.Incarnation = strings.Repeat("ef", 16)
			case "runtime-sha":
				c.RuntimeSHA256 = strings.Repeat("ef", 32)
			case "resource-sha":
				c.ResourceSHA256 = strings.Repeat("ef", 32)
			case "secret":
				c.ConfigSecretUID = "wrong-secret"
			case "kernel":
				c.KernelBuildSHA256 = strings.Repeat("ef", 32)
			case "base-upper":
				c.OriginalBase = 512 << 20
			case "business-under-budget":
				c.BusinessMinimum = 512 << 20
				c.Identity.MinimumBytes = 512 << 20
			case "config":
				config.ComputeInstanceID = "wrong-compute"
			case "changed-asset":
				assets["dsga-native-birth.json"] = []byte("{}")
			case "missing-asset":
				delete(assets, "dsga-normal-split.json")
			case "legacy-v1":
				d := runtimeTemplatesDocumentFixture(t)
				raw, _ = json.Marshal(d)
				assets = map[string][]byte{RuntimeResourceAssetName: raw}
				c.RuntimeSHA256 = physicalDigest(raw)
			}
			if _, err := initialPrebootBindInputs(c, raw, assets, config); err == nil {
				t.Fatal("source drift accepted")
			}
		})
	}
}
func initialPrebootPhysicalFixture(t *testing.T, c bootprotocol.InitialPrebootChallenge) PhysicalKernelObservation {
	t.Helper()
	o := newPreSystemFixtureProfile(t, NormalSplitV1PhysicalKernelProfileID).in.origin.First
	o.Identity.VMUID = c.Identity.VMUID
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
	return o
}

// PG17-U-INITIAL-GUEST-003：grow后保留原base-only历史，不能洗新首次或替换旧阶段。
func TestInitialPrebootGuestHistoricalCompletion(t *testing.T) {
	c, _, _, _ := initialPrebootFixture(t)
	p := initialPrebootPhysicalFixture(t, c)
	original := initialPrebootGuestComplete{IssuingRoot: nativeBirthConsumer{PID: 444, StartTicks: 100, InstanceNonce: strings.Repeat("ab", 32)}, ProtocolVersion: 1, Scope: initialPrebootGuestScope, IntentSHA256: strings.Repeat("01", 32), KernelPreparationSHA256: strings.Repeat("02", 32), PhysicalOriginSHA256: strings.Repeat("03", 32), PreSystemSHA256: strings.Repeat("04", 32), PreparedPhysical: p}
	current := original
	current.PreparedPhysical.Zones = append([]PhysicalKernelZone(nil), p.Zones...)
	for i := range current.PreparedPhysical.Zones {
		if current.PreparedPhysical.Zones[i].Name == "Movable" {
			current.PreparedPhysical.Zones[i].PresentPages = 1024
		}
	}
	if err := initialPrebootMatchHistoricalComplete(current, original); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"intent", "kernel", "origin", "presys", "old-movable", "fixed-kernel", "version", "missing-issuer"} {
		t.Run(mode, func(t *testing.T) {
			old, next := original, current
			switch mode {
			case "intent":
				next.IntentSHA256 = strings.Repeat("ab", 32)
			case "kernel":
				next.KernelPreparationSHA256 = strings.Repeat("ab", 32)
			case "origin":
				next.PhysicalOriginSHA256 = strings.Repeat("ab", 32)
			case "presys":
				next.PreSystemSHA256 = strings.Repeat("ab", 32)
			case "old-movable":
				old.PreparedPhysical.Zones = append([]PhysicalKernelZone(nil), p.Zones...)
				for i := range old.PreparedPhysical.Zones {
					if old.PreparedPhysical.Zones[i].Name == "Movable" {
						old.PreparedPhysical.Zones[i].PresentPages = 1
					}
				}
			case "fixed-kernel":
				next.PreparedPhysical.KernelNotesSHA256 = strings.Repeat("ef", 32)
			case "version":
				old.ProtocolVersion = 2
			case "missing-issuer":
				old.IssuingRoot.InstanceNonce = ""
			}
			if err := initialPrebootMatchHistoricalComplete(next, old); err == nil {
				t.Fatal("changed original completion accepted")
			}
		})
	}
}

// PG17-U-INITIAL-GUEST-004：实际页几何必须吻合原计划增量；Match不等于QMP测量。
func TestInitialPrebootGuestMovableGeometry(t *testing.T) {
	c, _, _, _ := initialPrebootFixture(t)
	before := initialPrebootPhysicalFixture(t, c)
	current := before
	current.Zones = append([]PhysicalKernelZone(nil), before.Zones...)
	expected := uint64(c.BusinessMinimum-c.OriginalBase) / 4096
	found := false
	for i := range current.Zones {
		if current.Zones[i].Name == "Movable" {
			current.Zones[i].PresentPages = control.Uint64(expected)
			found = true
		}
	}
	if !found {
		t.Fatal("fixture lacks Movable zone")
	}
	if got, err := initialPrebootMovablePages(c, before, current); err != nil || got != expected {
		t.Fatal(got, err)
	}
	for _, mode := range []string{"no-grow", "short-grow", "over-grow", "old-movable", "wrong-vm", "fixed-capacity", "wrong-boot"} {
		t.Run(mode, func(t *testing.T) {
			old, next := before, current
			old.Zones = append([]PhysicalKernelZone(nil), before.Zones...)
			next.Zones = append([]PhysicalKernelZone(nil), current.Zones...)
			for i := range next.Zones {
				if next.Zones[i].Name == "Movable" {
					switch mode {
					case "no-grow":
						next.Zones[i].PresentPages = 0
					case "short-grow":
						next.Zones[i].PresentPages--
					case "over-grow":
						next.Zones[i].PresentPages++
					}
				}
			}
			switch mode {
			case "old-movable":
				for i := range old.Zones {
					if old.Zones[i].Name == "Movable" {
						old.Zones[i].PresentPages = 1
					}
				}
			case "wrong-vm":
				old.Identity.VMUID = "wrong-vm"
			case "fixed-capacity":
				next.NonMovableUpperBytes += 4096
			case "wrong-boot":
				next.Identity.GuestBootID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
			if _, err := initialPrebootMovablePages(c, old, next); err == nil {
				t.Fatal("wrong actual increment accepted")
			}
		})
	}
}

// PG17-U-INITIAL-GUEST-005：真实文件身份/nonce/八摘要隔离的私有key；此夹具无内核资格。
func TestInitialPrebootGuestPrivateWitnessKey(t *testing.T) {
	c, _, _, _ := initialPrebootFixture(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := lockState(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	for _, name := range initialPrebootGuestFiles {
		if err := s.createPhysicalFile(name, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	original, err := initialPrebootGuestWitnessLocked(s, c)
	if err != nil {
		t.Fatal(err)
	}
	initialPrebootGuestWitnesses.Store(original, struct{}{})
	next := c
	next.Nonce = strings.Repeat("ab", 32)
	changed, err := initialPrebootGuestWitnessLocked(s, next)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := initialPrebootGuestWitnesses.Load(changed); ok {
		t.Fatal("nonce borrowed original witness")
	}
	path := filepath.Join(dir, initialPrebootGuestCompleteName)
	if err := os.Rename(path, filepath.Join(dir, "preserved-complete")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	replaced, err := initialPrebootGuestWitnessLocked(s, c)
	if err != nil {
		t.Fatal(err)
	}
	if replaced == original {
		t.Fatal("same-byte replacement borrowed file durability")
	}
	if _, ok := initialPrebootGuestWitnesses.Load(replaced); ok {
		t.Fatal("replacement borrowed original witness")
	}
}
