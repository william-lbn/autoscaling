//go:build linux && amd64

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"strings"
	"testing"
)

// 纯U算术/格式夹具，不调用成功的生产来源、Kube发布或任何Guest/Host动作。
func qualificationArtifactUnit(t *testing.T) GuestQualificationArtifact {
	t.Helper()
	f := normalInflightMetadataTestFacts(t)
	f.Physical.Identity.SystemCgroupDevice = 3
	f.Physical.Identity.SystemCgroupInode = 4
	cpu := uint64(f.Physical.PossibleCPUs)
	terms, e := normalInflightCalculateCPUFixed(f, f.Build, cpu)
	if e != nil {
		t.Fatal(e)
	}
	limits := Limits{MemoryMaxBytes: 2 << 30, PidsMaxTasks: 128, AddressSpaceMaxBytes: 16 << 30, SystemMemoryMaxBytes: 1 << 30, OutsideUpperBytes: 3 << 30, HugePools: []HugePool{{PageSizeKB: 2048, Pages: 0}, {PageSizeKB: 1048576, Pages: 0}}}
	plan, e := normalInflightPlanCPUFixed(f, normalInflightPlanningInput{8 << 30, 512 << 20, 1 << 30}, f.Build, cpu)
	if e != nil {
		t.Fatal(e)
	}
	outside, e := qualificationOutside(1<<30, plan, terms)
	if e != nil {
		t.Fatal(e)
	}
	fr, _ := json.Marshal(f)
	tr, _ := json.Marshal(terms)
	h := strings.Repeat("a", 64)
	return GuestQualificationArtifact{ProtocolVersion: 1, Scope: GuestQualificationArtifactScope, Identity: f.Physical.Identity, Incarnation: strings.Repeat("1", 32), ConfigSecretUID: "unit-config", ResourceSourceSHA256: h, RuntimeDocumentSHA256: h, OriginalSourceSHA256: h, RootStateSHA256: h, RootBootstrapSHA256: h, MinimumVMBytes: 7 << 30, MaximumVMBytes: 8 << 30, BlockBytes: 8 << 20, ObservedGuestMemTotalBytes: 7<<30 - 128<<20, MaximumVCPUs: uint32(cpu), PrivateMemoryMaxBytes: 256 << 20, ArraysMemoryMaxBytes: 256 << 20, CurrentLimits: limits, OriginalPhysical: f.Physical, CurrentFacts: fr, InflightTerms: tr, MinimumNormalBytes: control.Uint64(plan.ConditionalNormalTargetBytes), Outside: outside}
}
func TestGuestQualificationArtifactStrictExactBudget(t *testing.T) {
	a := qualificationArtifactUnit(t)
	if e := a.Validate(); e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(a)
	if e != nil {
		t.Fatal(e)
	}
	var again GuestQualificationArtifact
	if e = json.Unmarshal(raw, &again); e != nil {
		t.Fatal(e)
	}
	// 配置保留不足只是真实诊断，不能因此篡改观测；签报告的后继装配器必须拒绝。
	if a.Outside.TotalBytes <= a.CurrentLimits.OutsideUpperBytes {
		t.Fatal("unit 3Gi reservation unexpectedly covers exact full outside")
	}
	var terms normalInflightSourceTerms
	if e = json.Unmarshal(a.InflightTerms, &terms); e != nil {
		t.Fatal(e)
	}
	if terms.AllSourcesClosed || terms.NormalDecreaseAuthorized || len(terms.MissingSources) != 8 {
		t.Fatal("non-authorizing component boundary changed")
	}
}
func TestGuestQualificationArtifactRejectsMissingAndAlteredSources(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*GuestQualificationArtifact)
	}{
		{"permit", func(a *GuestQualificationArtifact) { a.VMShrinkPermit = true }},
		{"empty-source", func(a *GuestQualificationArtifact) { a.OriginalSourceSHA256 = "" }},
		{"less-compaction", func(a *GuestQualificationArtifact) { a.Outside.CompactionFreeReserveBytes-- }},
		{"less-total", func(a *GuestQualificationArtifact) { a.Outside.TotalBytes-- }},
		{"future-fake-cpu", func(a *GuestQualificationArtifact) { a.MaximumVCPUs = 1 }},
		{"mixed-original", func(a *GuestQualificationArtifact) { a.OriginalPhysical.Identity.GuestBootID = "other" }},
		{"missing-facts", func(a *GuestQualificationArtifact) { a.CurrentFacts = nil }},
		{"altered-normal-floor", func(a *GuestQualificationArtifact) { a.MinimumNormalBytes-- }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := qualificationArtifactUnit(t)
			tc.change(&a)
			if a.Validate() == nil {
				t.Fatal("unconfirmed source accepted")
			}
		})
	}
}
func TestGuestQualificationArtifactUnavailableNeverPreparesSource(t *testing.T) {
	for _, g := range []*Guard{nil, {config: Config{ExecutionModel: TrustedControllerExecutionModel}}} {
		_, e := g.InspectGuestQualificationArtifact(context.Background())
		if !errors.Is(e, ErrUnknown) {
			t.Fatalf("missing actual original source not Unknown: %v", e)
		}
	}
}
