package budget

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 复用仓库原 qualifiedFixture；这里只测协议/算术，绝不把其 1GiB 数值称为
// Linux 平台资格。两个候选保留同一原父 512MiB normal，新的 272MiB 不会写入。
func allocationFixture(t *testing.T) (AllocationQualification, PlatformQualification, QualificationSource) {
	t.Helper()
	base, source := qualifiedFixture()
	q := AllocationQualification{ProtocolVersion: 2, VMUID: base.Identity.VMUID, Incarnation: base.Incarnation, ReportID: "allocation-unit-fixture-not-platform-certification", Base: source, HoldUntilExplicitSettlement: true}
	for _, x := range []struct {
		id            string
		buffers       uint32
		arrays, cache control.Uint64
	}{{"sb-heavy", 4096, 48 << 20, 32 << 20}, {"lfc-heavy", 2048, 32 << 20, 48 << 20}} {
		limits := base.Targets[0].Limits
		limits.HugePools = append([]resourceguard.HugePool(nil), limits.HugePools...)
		limits.MemoryMaxBytes = 272 << 20
		q.Targets = append(q.Targets, QualifiedAllocation{ID: x.id, VMMemoryBytes: 1 << 30, Buffers: x.buffers, LFCLimitBytes: x.cache, ArrayNormalUpperBytes: x.arrays, NoncachePrivateUpperBytes: 192 << 20, CacheNormalUpperBytes: x.cache, BackingProfileSHA256: strings.Repeat("d", 64), Limits: limits})
	}
	q.Transitions = []QualifiedAllocationTransition{{From: "sb-heavy", To: "lfc-heavy", PeakLimits: base.Targets[0].Limits}}
	require.NoError(t, q.Validate())
	require.NoError(t, q.MatchBase(source, base))
	return q, base, QualificationSource{Claim: ObjectRef{Name: "allocation", UID: "allocation-original-uid"}, SHA256: strings.Repeat("e", 64)}
}

func TestAllocationSameVMRequiresExactVectorAndOriginalSource(t *testing.T) {
	q, base, source := allocationFixture(t)
	left, err := q.Targets[0].Selector()
	require.NoError(t, err)
	right, err := q.Targets[1].Selector()
	require.NoError(t, err)
	a, err := q.Observe(source, left, base)
	require.NoError(t, err)
	b, err := q.Observe(source, right, base)
	require.NoError(t, err)
	require.Equal(t, left.VMMemoryBytes, right.VMMemoryBytes)
	require.NotEqual(t, a.SourceID, b.SourceID)
	require.Equal(t, base.Targets[0], a.RetainedTarget)
	require.Equal(t, base.Targets[0], b.RetainedTarget)
	require.Equal(t, control.Uint64(512<<20), b.RetainedTarget.Limits.MemoryMaxBytes)
	require.Equal(t, control.Uint64(272<<20), b.Candidate.Limits.MemoryMaxBytes)
	borrowed := left
	borrowed.AllocationID = right.AllocationID
	_, err = q.Observe(source, borrowed, base)
	require.Error(t, err)
	for _, change := range []func(*QualificationSource){
		func(s *QualificationSource) { s.Claim.UID = "same-name-new-uid" },
		func(s *QualificationSource) { s.SHA256 = strings.Repeat("f", 64) },
	} {
		changed := source
		change(&changed)
		other, e := q.Observe(changed, left, base)
		require.NoError(t, e, "纯类型校验不能替代真实来源认证，但新来源身份必须不同")
		require.NotEqual(t, a.SourceID, other.SourceID)
	}
	_, err = q.Observe(source, AllocationSelector{VMMemoryBytes: left.VMMemoryBytes}, base)
	require.Error(t, err, "不能按相同 VM bytes 回落任选分配")
	a.Candidate.Limits.HugePools[0].Pages++
	a.RetainedTarget.Limits.HugePools[0].Pages++
	again, err := q.Observe(source, left, base)
	require.NoError(t, err)
	require.Equal(t, q.Targets[0].Limits, again.Candidate.Limits)
	require.Equal(t, base.Targets[0], again.RetainedTarget)
}

func TestAllocationChangesCannotAdoptOldSelector(t *testing.T) {
	q, base, source := allocationFixture(t)
	selected, err := q.Targets[0].Selector()
	require.NoError(t, err)
	raw, err := json.Marshal(q)
	require.NoError(t, err)
	for name, change := range map[string]func(*QualifiedAllocation){
		"buffers":  func(a *QualifiedAllocation) { a.Buffers = 2048 },
		"lfc-zero": func(a *QualifiedAllocation) { a.LFCLimitBytes = 0 },
		"backing":  func(a *QualifiedAllocation) { a.BackingProfileSHA256 = strings.Repeat("f", 64) },
		"same-total-new-partition": func(a *QualifiedAllocation) {
			a.NoncachePrivateUpperBytes += 1 << 20
			a.CacheNormalUpperBytes -= 1 << 20
		},
		"normal": func(a *QualifiedAllocation) {
			a.NoncachePrivateUpperBytes -= 1 << 20
			a.Limits.MemoryMaxBytes -= 1 << 20
		},
	} {
		t.Run(name, func(t *testing.T) {
			var changed AllocationQualification
			require.NoError(t, json.Unmarshal(raw, &changed))
			change(&changed.Targets[0])
			require.NoError(t, changed.MatchBase(changed.Base, base))
			_, e := changed.Observe(source, selected, base)
			require.Error(t, e)
		})
	}
}

func TestAllocationRejectsIncompleteOrUnretainedQualification(t *testing.T) {
	q, base, _ := allocationFixture(t)
	raw, err := json.Marshal(q)
	require.NoError(t, err)
	for name, change := range map[string]func(*AllocationQualification){
		"legacy-version":        func(q *AllocationQualification) { q.ProtocolVersion = 1 },
		"no-held-lifetime":      func(q *AllocationQualification) { q.HoldUntilExplicitSettlement = false },
		"different-vm":          func(q *AllocationQualification) { q.VMUID = "other-vm" },
		"different-incarnation": func(q *AllocationQualification) { q.Incarnation = strings.Repeat("f", 32) },
		"duplicate-id":          func(q *AllocationQualification) { q.Targets[1].ID = q.Targets[0].ID },
		"missing-target":        func(q *AllocationQualification) { q.Targets = nil },
		"overflow":              func(q *AllocationQualification) { q.Targets[0].CacheNormalUpperBytes = control.Uint64(math.MaxUint64) },
		"partition-mismatch":    func(q *AllocationQualification) { q.Targets[0].Limits.MemoryMaxBytes++ },
		"pids-drift":            func(q *AllocationQualification) { q.Targets[0].Limits.PidsMaxTasks++ },
		"pool-drift":            func(q *AllocationQualification) { q.Targets[0].Limits.HugePools[0].Pages++ },
		"outside-drift":         func(q *AllocationQualification) { q.Targets[0].Limits.OutsideUpperBytes++ },
		"underfunded-peak":      func(q *AllocationQualification) { q.Transitions[0].PeakLimits.MemoryMaxBytes = 272 << 20 },
		"missing-edge-endpoint": func(q *AllocationQualification) { q.Transitions[0].To = "missing" },
		"duplicate-edge":        func(q *AllocationQualification) { q.Transitions = append(q.Transitions, q.Transitions[0]) },
		"cross-vm-edge":         func(q *AllocationQualification) { q.Targets[1].VMMemoryBytes += 1 << 20 },
	} {
		t.Run(name, func(t *testing.T) {
			var changed AllocationQualification
			require.NoError(t, json.Unmarshal(raw, &changed))
			change(&changed)
			require.Error(t, changed.MatchBase(changed.Base, base))
		})
	}
	wrong := q.Base
	wrong.Claim.UID = "other-base"
	require.Error(t, q.MatchBase(wrong, base))
	other := base
	other.Identity.VMUID = "other-original-vm"
	require.Error(t, q.MatchBase(q.Base, other))
}

func TestQualificationSelectionStrictVersionsAndLegacyWire(t *testing.T) {
	q, base, source := allocationFixture(t)
	before, err := json.Marshal(base)
	require.NoError(t, err)
	legacy := QualificationSelection{ProtocolVersion: 1, Source: q.Base}
	raw, err := json.Marshal(legacy)
	require.NoError(t, err)
	var decoded QualificationSelection
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, legacy, decoded)
	for _, extra := range []string{`,"allocation":null}`, `,"allocation":{}}`, `,"allocation_id":""}`} {
		require.Error(t, json.Unmarshal(append(append([]byte(nil), raw[:len(raw)-1]...), []byte(extra)...), &decoded))
	}
	selector, err := q.Targets[0].Selector()
	require.NoError(t, err)
	current := QualificationSelection{ProtocolVersion: 2, Source: source, Allocation: &selector}
	raw, err = json.Marshal(current)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, current, decoded)
	current.Allocation = nil
	require.Error(t, current.Validate())
	current.ProtocolVersion = 3
	require.Error(t, current.Validate())
	qRaw, err := json.Marshal(q)
	require.NoError(t, err)
	var old PlatformQualification
	require.Error(t, json.Unmarshal(qRaw, &old), "旧读者必须拒绝 v2，不能投影为 VM-only")
	var next AllocationQualification
	require.Error(t, json.Unmarshal(append(qRaw[:len(qRaw)-1], []byte(`,"release_proven":true}`)...), &next))
	_, err = base.Target(1 << 30)
	require.NoError(t, err)
	after, err := json.Marshal(base)
	require.NoError(t, err)
	require.Equal(t, before, after, "只读新合同不能修改旧 v1 wire")
	base.Targets = append(base.Targets, base.Targets[0])
	require.Error(t, base.Validate(), "新同 VM allocation 不放宽旧 v1 的唯一 VM 行")
}

func TestAllocationQualificationHasSameTypedAndWireBudget(t *testing.T) {
	q, base, source := allocationFixture(t)
	seeds := q.Targets
	q.Targets = nil
	for i := 0; i < 32; i++ {
		candidate := seeds[i%len(seeds)]
		candidate.ID = fmt.Sprintf("allocation-%02d", i)
		q.Targets = append(q.Targets, candidate)
	}
	q.Transitions = []QualifiedAllocationTransition{}
	for i := 0; i < 32 && len(q.Transitions) < 128; i++ {
		for j := 0; j < 32 && len(q.Transitions) < 128; j++ {
			if i != j {
				q.Transitions = append(q.Transitions, QualifiedAllocationTransition{From: q.Targets[i].ID, To: q.Targets[j].ID, PeakLimits: base.Targets[0].Limits})
			}
		}
	}
	all, err := json.Marshal(q)
	require.NoError(t, err)
	t.Run("oversized-cannot-be-issued", func(t *testing.T) {
		t.Logf("32 targets and 128 edges encode to %d bytes", len(all))
		require.Greater(t, len(all), 32*1024)
		require.Error(t, q.Validate(), "不能签出正式只读入口必拒的文档")
		require.Error(t, q.MatchBase(q.Base, base))
		var decoded AllocationQualification
		require.Error(t, json.Unmarshal(all, &decoded))
	})
	t.Run("largest-fitting-complete-roundtrip", func(t *testing.T) {
		var largest AllocationQualification
		var wire []byte
		for n := 0; n <= len(q.Transitions); n++ {
			candidate := q
			candidate.Transitions = q.Transitions[:n]
			raw, e := json.Marshal(candidate)
			require.NoError(t, e)
			if len(raw) > 32*1024 {
				break
			}
			largest, wire = candidate, raw
		}
		require.Greater(t, len(wire), 32*1024-2048)
		t.Logf("complete roundtrip: %d bytes, %d targets, %d edges", len(wire), len(largest.Targets), len(largest.Transitions))
		require.NoError(t, largest.Validate())
		require.NoError(t, largest.MatchBase(largest.Base, base))
		var decoded AllocationQualification
		require.NoError(t, json.Unmarshal(wire, &decoded))
		require.Equal(t, largest, decoded)
		selector, e := decoded.Targets[0].Selector()
		require.NoError(t, e)
		observed, e := decoded.Observe(source, selector, base)
		require.NoError(t, e)
		require.Equal(t, base.Targets[0], observed.RetainedTarget)
	})
}
