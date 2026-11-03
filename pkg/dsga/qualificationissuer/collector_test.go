package qualificationissuer

import (
	"context"
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// 本文件只核只读装配边界/最小规格算术及本地归档，不签真实Guest资格，不创建Kube对象。
func TestCollectorFirstQualificationNeedsNoPriorCMAndNeverExtrapolates(t *testing.T) {
	const gi control.Uint64 = 1 << 30
	floor7 := 7*gi - 100*(1<<20)
	floor8 := 8*gi - 100*(1<<20)
	for _, target := range []control.Uint64{7 * gi, 8 * gi} {
		floor, prior, e := observedTargetFloor(7*gi, target, 7*gi, 8*gi, floor7)
		if e != nil || prior || floor != floor7 {
			t.Fatal("first minimum incorrectly depends on prior CM or extrapolates", floor, prior, e)
		}
	}
	floor, prior, e := observedTargetFloor(8*gi, 8*gi, 7*gi, 8*gi, floor8)
	if e != nil || prior || floor != floor8 {
		t.Fatal("actual8 capacity cannot be used by target8", floor, prior, e)
	}
	floor, prior, e = observedTargetFloor(8*gi, 7*gi, 7*gi, 8*gi, floor8)
	if e != nil || !prior || floor != 0 {
		t.Fatal("current8 extrapolated target7", floor, prior, e)
	}
	limits := resourceguard.Limits{MemoryMaxBytes: 2 * gi, OutsideUpperBytes: 6278873088, SystemMemoryMaxBytes: 512 << 20, PidsMaxTasks: 128, AddressSpaceMaxBytes: 4 << 30, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048}, {PageSizeKB: 1048576}}}
	total, e := limits.TotalUpperBytes()
	if e != nil || total > uint64(floor8) || total <= uint64(floor7) {
		t.Fatal("N2Gi regression window must fit actual8 but not actual7", total, e)
	}
	for _, v := range [][5]control.Uint64{{0, 7 * gi, 7 * gi, 8 * gi, 1}, {7 * gi, 7 * gi, 0, 8 * gi, 1}, {9 * gi, 7 * gi, 7 * gi, 8 * gi, 1}, {7 * gi, 7 * gi, 7 * gi, 8 * gi, 0}, {7 * gi, 7 * gi, 7 * gi, 8 * gi, 8 * gi}} {
		if _, _, e := observedTargetFloor(v[0], v[1], v[2], v[3], v[4]); e == nil {
			t.Fatal("unknown geometry accepted")
		}
	}
}

func collectorConfigUnit(t *testing.T) CollectorConfig {
	t.Helper()
	_, _, cfg, _, _ := issuerFixture(t)
	return CollectorConfig{ProtocolVersion: 1, Binding: cfg.Binding, NodeName: cfg.NodeName, NodeUID: cfg.NodeUID, ControlSocketPath: cfg.ControlSocketPath, TimeoutSeconds: 30, OutputDirectory: t.TempDir(), Targets: []QualifiedIntent{{VMMemoryBytes: 7 << 30}, {VMMemoryBytes: 8 << 30}}}
}
func TestCollectorConfigRejectsUnboundAndUnorderedIntent(t *testing.T) {
	c := collectorConfigUnit(t)
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(c)
	var round CollectorConfig
	if e := json.Unmarshal(raw, &round); e != nil {
		t.Fatal(e)
	}
	for _, change := range []func(*CollectorConfig){func(c *CollectorConfig) { c.Binding.VMUID = "" }, func(c *CollectorConfig) { c.Binding.Incarnation = "" }, func(c *CollectorConfig) { c.NodeUID = "" }, func(c *CollectorConfig) { c.OutputDirectory = "relative" }, func(c *CollectorConfig) { c.TimeoutSeconds = 121 }, func(c *CollectorConfig) { c.Targets[1].VMMemoryBytes = c.Targets[0].VMMemoryBytes }} {
		copy := c
		copy.Targets = append([]QualifiedIntent(nil), c.Targets...)
		change(&copy)
		if copy.Validate() == nil {
			t.Fatal("incomplete collector intent accepted")
		}
	}
	if _, e := NewCollector(nil, nil, nil, nil, c); e == nil {
		t.Fatal("missing actual production readers accepted")
	}
	var none *Collector
	if _, e := none.Collect(context.Background()); e == nil {
		t.Fatal("nil collector returned report")
	}
}
func collectionUnit(t *testing.T) (Collection, string) {
	t.Helper()
	_, _, cfg, bundle, _ := issuerFixture(t)
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	cfg.ReportPath = filepath.Join(dir, "report.json")
	cfg.LinuxArtifactPath = filepath.Join(dir, "linux-artifact.json")
	cfg.ContinuousArtifactPath = filepath.Join(dir, "continuous-artifact.json")
	return Collection{Bundle: bundle, ProposedIssuerConfig: cfg}, dir
}
func TestCollectorWritesOnlyPrivateOriginalFilesAndNeverOverwrites(t *testing.T) {
	c, dir := collectionUnit(t)
	if e := WriteCollection(c); e != nil {
		t.Fatal(e)
	}
	before := map[string][]byte{}
	for _, name := range []string{"report.json", "linux-artifact.json", "continuous-artifact.json", "issuer-config.proposed.json"} {
		p := filepath.Join(dir, name)
		st, e := os.Stat(p)
		if e != nil || st.Mode().Perm() != 0600 {
			t.Fatal("private output mode differs", name, e)
		}
		before[name], e = os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
	}
	if e := WriteCollection(c); e == nil {
		t.Fatal("same nonce collection overwrote original output")
	}
	for name, raw := range before {
		now, e := os.ReadFile(filepath.Join(dir, name))
		if e != nil || !reflect.DeepEqual(raw, now) {
			t.Fatal("original evidence changed", name)
		}
	}
}
func TestCollectorRejectsTamperedArtifactAndNonprivateDirectoryBeforeWrites(t *testing.T) {
	c, dir := collectionUnit(t)
	c.Bundle.LinuxArtifact = append(append([]byte(nil), c.Bundle.LinuxArtifact...), byte('x'))
	if e := WriteCollection(c); e == nil {
		t.Fatal("unmatched physical artifact accepted")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("tampered artifact wrote output")
	}
	c, dir = collectionUnit(t)
	if e := os.Chmod(dir, 0755); e != nil {
		t.Fatal(e)
	}
	if e := WriteCollection(c); e == nil {
		t.Fatal("nonprivate output accepted")
	}
	entries, _ = os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("nonprivate output changed")
	}
}

// 这里只验证投影相等门，不把这个最小比较夹具当有效物理来源。
func TestCollectorFullArtifactEqualityRejectsSameOldHashesChangedSafetyFields(t *testing.T) {
	a := resourceguard.GuestQualificationArtifact{RootStateSHA256: strings.Repeat("a", 64), OriginalSourceSHA256: strings.Repeat("b", 64), ArraysMemoryMaxBytes: 64 << 20, CurrentFacts: json.RawMessage(`{"stable":1,"other":2}`)}
	b := a
	if !sameGuestArtifact(a, b) {
		t.Fatal("same complete projection changed")
	}
	b.ArraysMemoryMaxBytes++
	if sameGuestArtifact(a, b) {
		t.Fatal("A-only change accepted while old Root/source hashes stayed equal")
	}
	b = a
	b.CurrentFacts = json.RawMessage(`{"stable":2,"other":2}`)
	if sameGuestArtifact(a, b) {
		t.Fatal("changed current kernel facts accepted while old hashes stayed equal")
	}
	b = a
	b.CurrentFacts = json.RawMessage(`{"other":2,"stable":1}`)
	if !sameGuestArtifact(a, b) {
		t.Fatal("pure JSON object key ordering invented source drift")
	}
}

func TestCollectorGuestArtifactReportSelectionCannotDowngrade(t *testing.T) {
	_, _, _, _, r := issuerFixture(t)
	r.GuestArtifactSHA256 = strings.Repeat("a", 64)
	if r.Validate() == nil {
		t.Fatal("legacy report acquired collector capability")
	}
	r.ProtocolVersion = 2
	r.Scope = KubernetesBudgetReportScope
	r.GuestArtifactSHA256 = "bad"
	if r.Validate() == nil {
		t.Fatal("malformed selected Guest source accepted")
	}
}
