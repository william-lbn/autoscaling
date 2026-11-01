//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

// 这些facts只供算术U，不构造成功原来源；真opaque只在隔离KVM目标中验证。
func normalInflightMetadataTestFacts(t *testing.T) normalInflightSourceFacts {
	t.Helper()
	f := normalInflightTestFacts(t)
	ref, e := normalInflightMetadataBuild()
	if e != nil {
		t.Fatal(e)
	}
	f.Build = ref
	f.Physical.KernelRelease = ref.KernelRelease
	f.Physical.KernelNotesSHA256 = ref.KernelNotesSHA256
	return f
}
func TestNormalInflightMetadataFixedBuildPreservesLegacyBoundary(t *testing.T) {
	old := normalInflightTestFacts(t)
	want, e := normalInflightCalculate(old)
	if e != nil {
		t.Fatal(e)
	}
	f := normalInflightMetadataTestFacts(t)
	got, e := normalInflightCalculateFixed(f, f.Build)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("metadata changed source equations or missing qualifications")
	}
	if _, e = normalInflightCalculate(f); e == nil {
		t.Fatal("legacy369 entry silently upgraded")
	}
	if _, e = normalInflightCalculateFixed(old, f.Build); e == nil {
		t.Fatal("mixed facts accepted")
	}
	if got.AllSourcesClosed || got.NormalDecreaseAuthorized || len(got.MissingSources) != 8 {
		t.Fatal("partial source acquired authorization")
	}
}
func TestNormalInflightMetadataRejectsMixedBuildAndPrerequisite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*normalInflightSourceFacts, *PhysicalKernelBuildReference)
	}{
		{"config tuple", func(f *normalInflightSourceFacts, r *PhysicalKernelBuildReference) {
			r.KernelConfigSHA256 = physicalDigest([]byte("not exact compiled config"))
			f.Build = *r
		}},
		{"old notes", func(f *normalInflightSourceFacts, r *PhysicalKernelBuildReference) {
			f.Physical.KernelNotesSHA256 = normalInflightTestFacts(t).Build.KernelNotesSHA256
		}},
		{"modules mutable", func(f *normalInflightSourceFacts, r *PhysicalKernelBuildReference) { f.ModulesDisabled = false }},
		{"FUSE outside Q1", func(f *normalInflightSourceFacts, r *PhysicalKernelBuildReference) {
			f.MountFilesystems = []string{"fuse"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := normalInflightMetadataTestFacts(t)
			r := f.Build
			tc.change(&f, &r)
			if _, e := normalInflightCalculateFixed(f, r); e == nil {
				t.Fatal("metadata source accepted drift")
			}
		})
	}
}
func TestNormalInflightMetadataGeometryBudgetKeepsCompiledCeiling(t *testing.T) {
	f := normalInflightMetadataTestFacts(t)
	f.Physical.Zones = []PhysicalKernelZone{{Node: 0, Name: "Movable", PresentPages: 1}, {Node: 0, Name: "Normal", PresentPages: 131072}}
	f.Physical.NonMovableUpperBytes = 512 << 20
	f.NonMovableManagedPages = 100000
	f.Physical.RuntimeCapabilities.PerCPUPagelistHighFraction = 1024
	in := normalInflightPlanningInput{MaximumBytes: 4 << 30, NormalLeavesBytes: 1536 << 20, SystemBytes: 512 << 20}
	a, e := normalInflightPlanFixed(f, in, f.Build)
	if e != nil {
		t.Fatal(e)
	}
	if a.CandidateFits || a.CandidateSpareBytes != 0 || !a.CandidateOnly || a.MmaxAuthorized || a.NormalDecreaseAuthorized {
		t.Fatalf("4G geometry became capacity qualification: %+v", a)
	}
	terms, e := normalInflightCalculateFixed(f, f.Build)
	if e != nil {
		t.Fatal(e)
	}
	f.Physical.PossibleCPUs = 4
	f.Physical.CPUChargeStockUpperBytes = 4 * 64 * 4096
	more, e := normalInflightCalculateFixed(f, f.Build)
	if e != nil {
		t.Fatal(e)
	}
	terms.CurrentPossibleCPUs = 4
	if !reflect.DeepEqual(terms, more) || more.CompiledCPUCeiling != 64 {
		t.Fatal("current CPU observation reduced future budget")
	}
}
func TestNormalInflightMetadataOriginRejectsUnboundBeforeEffects(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	state, e := lockState(ctx, dir)
	if e != nil {
		t.Fatal(e)
	}
	defer state.close()
	g := &Guard{}
	other := &Guard{}
	for _, tc := range []struct {
		name string
		ctx  context.Context
		g    *Guard
		m    *normalMemoryBrokerConsumerSource
	}{
		{"nil guard", ctx, nil, nil}, {"nil context", nil, g, nil}, {"no opaque", ctx, g, nil}, {"JSON shape", ctx, g, &normalMemoryBrokerConsumerSource{g: g}}, {"other Guard", ctx, other, &normalMemoryBrokerConsumerSource{g: g}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, f, v, e := tc.g.captureNormalInflightMetadataOriginLocked(tc.ctx, state, tc.m)
			if !errors.Is(e, ErrUnknown) || o != nil || !reflect.DeepEqual(f, normalInflightSourceFacts{}) || !reflect.DeepEqual(v, normalInflightSourceTerms{}) {
				t.Fatal("unbound source returned facts", e)
			}
		})
	}
	for _, n := range normalMemoryConfirmedFiles() {
		if _, e := os.Stat(dir + "/" + n); !os.IsNotExist(e) {
			t.Fatal("rejected source wrote original WAL", n, e)
		}
	}
}
func TestNormalInflightMetadataOpaqueReferenceCannotBeRebuiltOrReused(t *testing.T) {
	g := &Guard{}
	var decoded normalInflightMetadataOriginSource
	if e := json.Unmarshal([]byte(`{}`), &decoded); e != nil {
		t.Fatal(e)
	}
	for _, o := range []*normalInflightMetadataOriginSource{nil, &decoded, {guard: g, memory: &normalMemoryBrokerConsumerSource{g: g}}} {
		ref, f, v, e := o.referenceLocked(context.Background(), nil, g)
		if !errors.Is(e, ErrUnknown) || !reflect.DeepEqual(ref, normalInflightMetadataOriginReference{}) || !reflect.DeepEqual(f, normalInflightSourceFacts{}) || !reflect.DeepEqual(v, normalInflightSourceTerms{}) {
			t.Fatal("JSON/new object gained private witness", e)
		}
		o.close()
	}
}
