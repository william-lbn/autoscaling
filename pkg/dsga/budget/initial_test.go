package budget

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// 原生/root/平台均为明确领域夹具。本合同只证明完整启动算术/绑定，不
// 伪造Resize、Host操作或Linux物理资格；完整首次服务生产链须另行接入。
func initialFixture(t *testing.T) (Initial, PlatformQualification) {
	t.Helper()
	q, source := qualifiedFixture()
	target := q.Targets[0]
	view := resourceguard.View{
		ProtocolVersion: 1, VMShrinkPermit: false, Identity: q.Identity, IssuerFloorEpoch: 1,
		Bootstrap:        resourceguard.BootstrapCommand{ProtocolVersion: 1, Identity: q.Identity, IssuerEpoch: 1, BudgetCommitmentID: "original-initial-bootstrap", Limits: target.Limits},
		BootstrapApplied: true, SystemPlacementObserved: true, Original: nil, Held: nil, Applied: false,
		Birth:  &resourceguard.ProcessIdentity{ProtocolVersion: 1, Identity: q.Identity, PostmasterPID: 123, PostmasterStartTicks: 456},
		Actual: resourceguard.Evidence{ProtocolVersion: 1, VMShrinkPermit: false, Identity: q.Identity, MemoryMaxBytes: target.Limits.MemoryMaxBytes, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: target.GuestUsableMemoryBytes, SwapMaxBytes: 0, SwapCurrentBytes: 0, HugePools: target.Limits.HugePools, WholePoolUpperBytes: 16 << 20, HugeSurplusPages: 0, HugeOvercommitDisabled: true, CgroupPopulated: true, TaskLimit: &resourceguard.TaskLimitEvidence{MaxTasks: 128, CurrentTasks: 5}, System: &resourceguard.SystemEvidence{Scope: resourceguard.SystemChargedScope, MemoryMaxBytes: 16 << 20, MemoryCurrentBytes: 4 << 20, SwapMaxBytes: 0, SwapCurrentBytes: 0, ProcessCount: 3, PID1Member: true, RootUserspaceCount: 0}},
	}
	root, err := RootAggregateFromView(view)
	require.NoError(t, err)
	raw, err := json.Marshal(view.Bootstrap)
	require.NoError(t, err)
	digest := sha256.Sum256(raw)
	q.RootBootstrapSHA256 = hex.EncodeToString(digest[:])
	raw, err = json.Marshal(q)
	require.NoError(t, err)
	digest = sha256.Sum256(raw)
	source.SHA256 = hex.EncodeToString(digest[:])
	p, _ := settlementPrepared(t)
	instance := control.Instance{VMUID: q.Identity.VMUID, ComputeInstanceID: q.Identity.ComputeInstanceID, PGBootID: 1}
	p.Shared.Observed.Instance = instance
	p.Shared.Observed.IssuerFloorEpoch = 1
	p.Shared.Observed.AdmissionOpen, p.Shared.Observed.AdmissionRevision = false, 0
	system, transition, safety := target.Bounds(source)
	i := Initial{ProtocolVersion: 1, VMShrinkPermit: false, Instance: instance, Incarnation: q.Incarnation, IssuerEpoch: 1, Qualification: source, PGAggregate: root, Shared: p.Shared, VMMemoryBytes: target.VMMemoryBytes, GuestUsableMemoryBytes: target.GuestUsableMemoryBytes, RunnerBirth: q.RunnerBirth, RunnerIdentitySHA256: q.RunnerIdentitySHA256, RunnerPodName: q.RunnerPodName, RunnerPodUID: q.RunnerPodUID, RunnerRestartCount: q.RunnerRestartCount, NodeName: q.NodeName, NodeUID: q.NodeUID, HostProfileID: "explicit-qualified-host-fixture", HostMemoryBytes: target.VMMemoryBytes, HostOverheadBytes: 64 << 20, SystemOutsidePG: system, Transition: transition, Safety: safety}
	require.NoError(t, i.Validate())
	require.NoError(t, i.MatchQualification(q))
	return i, q
}

func TestInitialBudgetContainsStartupAndOutsideWithoutSyntheticResize(t *testing.T) {
	i, q := initialFixture(t)
	require.EqualValues(t, (512+16)<<20, i.PGAggregate.Bound.UpperBytes, "PG normal与整池相加一次")
	require.NoError(t, i.MatchQualification(q))
	current := i.Shared
	current.Observed.IssuerFloorEpoch = 2
	current.Observed.AdmissionOpen, current.Observed.AdmissionRevision = true, 1
	require.NoError(t, i.MatchCurrent(current, i.PGAggregate, 2, i.VMMemoryBytes, i.GuestUsableMemoryBytes), "floor和门禁前进不重置预算")
	wrong := q
	wrong.RunnerPodUID = "replacement-pod"
	require.Error(t, i.MatchQualification(wrong))
	wrong = q
	wrong.Targets = append([]QualifiedTarget(nil), q.Targets...)
	wrong.Targets[0].GuestUsableMemoryBytes--
	require.Error(t, i.MatchQualification(wrong))
	current.Observed.CapacityGeneration++
	require.Error(t, i.MatchCurrent(current, i.PGAggregate, 2, i.VMMemoryBytes, i.GuestUsableMemoryBytes))
	current.Observed.CapacityGeneration--
	require.Error(t, i.MatchCurrent(current, i.PGAggregate, 2, i.VMMemoryBytes, i.GuestUsableMemoryBytes-1))
}

func TestInitialBudgetRejectsRestartGrowthUnknownAndIncompleteCommitments(t *testing.T) {
	i, _ := initialFixture(t)
	raw, err := json.Marshal(i)
	require.NoError(t, err)
	for name, change := range map[string]func(*Initial){
		"not-first-generation":   func(i *Initial) { i.Shared.Observed.CapacityGeneration++ },
		"already-open":           func(i *Initial) { i.Shared.Observed.AdmissionOpen = true },
		"external-gate-revision": func(i *Initial) { i.Shared.Observed.AdmissionRevision = 1 },
		"unstable-prefix":        func(i *Initial) { i.Shared.Observed.LowBuffers-- },
		"unknown-allocation":     func(i *Initial) { i.Shared.Observed.Arrays[0].AllocationUnknown = true },
		"stale-floor":            func(i *Initial) { i.Shared.Observed.IssuerFloorEpoch++ },
		"guest-too-small":        func(i *Initial) { i.GuestUsableMemoryBytes = 128 << 20 },
		"outside-missing":        func(i *Initial) { i.Safety.UpperBytes-- },
		"outside-duplicate":      func(i *Initial) { i.Safety.SourceID = i.Transition.SourceID },
		"outside-is-rss":         func(i *Initial) { i.SystemOutsidePG.Kind = "rss" },
		"host-too-small":         func(i *Initial) { i.HostMemoryBytes-- },
		"host-overflow":          func(i *Initial) { i.HostOverheadBytes = control.Uint64(math.MaxUint64) },
		"missing-qualification":  func(i *Initial) { i.Qualification.Claim.UID = "" },
		"vm-permit":              func(i *Initial) { i.VMShrinkPermit = true },
	} {
		t.Run(name, func(t *testing.T) {
			var next Initial
			require.NoError(t, json.Unmarshal(raw, &next))
			change(&next)
			require.Error(t, next.Validate())
		})
	}
	var decoded Initial
	require.Error(t, json.Unmarshal(append(raw[:len(raw)-1], []byte(`,"resize_succeeded":true}`)...), &decoded), "不能夹带伪造Resize许可")
}
