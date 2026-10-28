package budget

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func qualifiedFixture() (PlatformQualification, QualificationSource) {
	q := PlatformQualification{
		ProtocolVersion: 1, Identity: resourceguard.Identity{VMUID: "vm-uid", ComputeInstanceID: "compute-1", GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3, SystemCgroupDevice: 2, SystemCgroupInode: 4},
		Incarnation: "0123456789abcdef0123456789abcdef", ReportID: "explicit-linux-qualification-fixture", RootBootstrapSHA256: strings.Repeat("a", 64), RunnerBirth: ObjectRef{Name: "original-birth", UID: "birth-uid"}, RunnerIdentitySHA256: strings.Repeat("b", 64), RunnerPodName: "runner", RunnerPodUID: "pod-uid", NodeName: "node-one", NodeUID: "node-uid", RunnerRestartCount: 0, HoldUntilExplicitSettlement: true,
		Targets: []QualifiedTarget{{VMMemoryBytes: 1 << 30, GuestUsableMemoryBytes: 768 << 20, Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, PidsMaxTasks: 128, SystemMemoryMaxBytes: 16 << 20, OutsideUpperBytes: 32 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}}}, SystemOutsidePGBytes: 24 << 20, TransitionBytes: 4 << 20, SafetyBytes: 4 << 20}},
	}
	return q, QualificationSource{Claim: ObjectRef{Name: "qualification", UID: "qualification-uid"}, SHA256: strings.Repeat("c", 64)}
}

func TestQualificationRejectsUnqualifiedOrUnderfundedTargets(t *testing.T) {
	q, source := qualifiedFixture()
	require.NoError(t, q.Validate())
	require.NoError(t, source.Validate())
	_, err := q.Target(768 << 20)
	require.Error(t, err, "不按比例外推未认证规格")
	raw, err := json.Marshal(q)
	require.NoError(t, err)
	for name, change := range map[string]func(*PlatformQualification){
		"no-held-lifetime":   func(q *PlatformQualification) { q.HoldUntilExplicitSettlement = false },
		"legacy-system":      func(q *PlatformQualification) { q.Identity.SystemCgroupInode = 0 },
		"legacy-pids":        func(q *PlatformQualification) { q.Targets[0].Limits.PidsMaxTasks = 0 },
		"system-not-covered": func(q *PlatformQualification) { q.Targets[0].SystemOutsidePGBytes = 1 },
		"sum-mismatch":       func(q *PlatformQualification) { q.Targets[0].SafetyBytes++ },
		"usable-exceeded":    func(q *PlatformQualification) { q.Targets[0].GuestUsableMemoryBytes = 128 << 20 },
		"repeated-size":      func(q *PlatformQualification) { q.Targets = append(q.Targets, q.Targets[0]) },
		"overflow":           func(q *PlatformQualification) { q.Targets[0].TransitionBytes = control.Uint64(math.MaxUint64) },
		"unknown-bootstrap":  func(q *PlatformQualification) { q.RootBootstrapSHA256 = "" },
	} {
		t.Run(name, func(t *testing.T) {
			var changed PlatformQualification
			require.NoError(t, json.Unmarshal(raw, &changed))
			change(&changed)
			require.Error(t, changed.Validate())
		})
	}
	var decoded PlatformQualification
	require.Error(t, json.Unmarshal(append(raw[:len(raw)-1], []byte(`,"rss_fallback":true}`)...), &decoded))
}

func TestOutsideV2RequiresExactRetainedQualification(t *testing.T) {
	q, source := qualifiedFixture()
	p, _ := settlementPrepared(t)
	p.Identity = q.Identity
	p.Request.Key.Instance.VMUID, p.Request.Key.Instance.ComputeInstanceID = q.Identity.VMUID, q.Identity.ComputeInstanceID
	p.Request.TargetVMMemoryBytes = q.Targets[0].VMMemoryBytes
	system, transition, safety := q.Targets[0].Bounds(source)
	out := OutsideReservation{ProtocolVersion: 2, VMShrinkPermit: false, Request: p.Request, Identity: q.Identity, Incarnation: q.Incarnation, ResidualProfileID: "qualified-" + source.SHA256, SystemOutsidePG: system, Transition: transition, Safety: safety, HoldUntilExplicitSettlement: true, Qualification: &source}
	require.NoError(t, q.MatchOutside(out))
	out.Request.TargetVMMemoryBytes += 8 << 20
	require.Error(t, q.MatchOutside(out))
	out.Request.TargetVMMemoryBytes = q.Targets[0].VMMemoryBytes
	out.Safety.UpperBytes--
	require.Error(t, q.MatchOutside(out))
	out.Safety = safety
	out.ProtocolVersion = 1
	require.Error(t, out.ValidateSource(), "v1不可夹带新资格")
	out.ProtocolVersion, out.Qualification = 2, nil
	require.Error(t, out.ValidateSource(), "v2不能删引用降级")
}
