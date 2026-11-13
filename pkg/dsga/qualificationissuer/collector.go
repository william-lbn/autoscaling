package qualificationissuer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	vmclient "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootstrap"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"os"
	"path/filepath"
	"time"
)

// QualifiedIntent只是Root保护配置里的确切目标，不提供Guest usable/Outside证明。
// 原Root在实际normal动作时仍独立核PG完成事实、P/A/F/I及完整cap事务。
type QualifiedIntent struct {
	VMMemoryBytes        control.Uint64 `json:"vm_memory_bytes"`
	NormalMemoryMaxBytes control.Uint64 `json:"normal_memory_max_bytes,omitempty"`
}
type CollectorConfig struct {
	ProtocolVersion   uint32            `json:"protocol_version"`
	Binding           journal.Binding   `json:"binding"`
	NodeName          string            `json:"node_name"`
	NodeUID           string            `json:"node_uid"`
	ControlSocketPath string            `json:"control_socket_path"`
	TimeoutSeconds    uint32            `json:"timeout_seconds"`
	OutputDirectory   string            `json:"output_directory"`
	Targets           []QualifiedIntent `json:"targets"`
	// 在8Gi状态重新取证7Gi目标时，必须显式选择原已发布资格，不能从8Gi倒推7Gi。
	PreviousQualification *budget.QualificationSource `json:"previous_qualification,omitempty"`
}

func (c CollectorConfig) Validate() error {
	if c.ProtocolVersion != 1 || control.ValidateID(c.Binding.Namespace, "namespace") != nil || control.ValidateID(c.Binding.Name, "VM name") != nil || control.ValidateID(string(c.Binding.VMUID), "VM UID") != nil || control.ValidateIncarnation(c.Binding.Incarnation) != nil || control.ValidateID(c.NodeName, "Node name") != nil || control.ValidateID(c.NodeUID, "Node UID") != nil || c.TimeoutSeconds < 1 || c.TimeoutSeconds > 120 || len(c.Targets) < 1 || len(c.Targets) > 32 || !filepath.IsAbs(c.OutputDirectory) || filepath.Clean(c.OutputDirectory) != c.OutputDirectory || c.OutputDirectory == "/" {
		return fmt.Errorf("fixed original collector binding, exact targets, finite timeout and private output directory required")
	}
	var previous control.Uint64
	for _, target := range c.Targets {
		if target.VMMemoryBytes <= previous {
			return fmt.Errorf("collector targets must be exact increasing VM sizes")
		}
		previous = target.VMMemoryBytes
	}
	if c.PreviousQualification != nil && c.PreviousQualification.Validate() != nil {
		return fmt.Errorf("explicit original qualification reference invalid")
	}
	return nil
}
func (c *CollectorConfig) UnmarshalJSON(raw []byte) error {
	type plain CollectorConfig
	var p plain
	if e := control.UnmarshalObjectBounded(raw, &p, MaxReportBytes, 2048); e != nil {
		return e
	}
	n := CollectorConfig(p)
	if e := n.Validate(); e != nil {
		return e
	}
	*c = n
	return nil
}
func LoadCollectorConfig(path string) (CollectorConfig, error) {
	var c CollectorConfig
	raw, e := ReadTrustedFile(path, MaxReportBytes)
	if e != nil {
		return c, e
	}
	if e = json.Unmarshal(raw, &c); e != nil {
		return c, e
	}
	return c, nil
}

type Collector struct {
	core   coreclient.CoreV1Interface
	coord  coordclient.CoordinationV1Interface
	vm     vmclient.Interface
	root   RootReader
	config CollectorConfig
}

func NewCollector(core coreclient.CoreV1Interface, coord coordclient.CoordinationV1Interface, vm vmclient.Interface, root RootReader, c CollectorConfig) (*Collector, error) {
	if core == nil || coord == nil || vm == nil || root == nil || c.Validate() != nil {
		return nil, fmt.Errorf("collector requires fixed actual clients and protected configuration")
	}
	return &Collector{core, coord, vm, root, c}, nil
}

type Collection struct {
	Bundle               Bundle
	ProposedIssuerConfig Config
	SnapshotSHA256       string
}

// CollectionStage reports the failing check without exposing the underlying input or API response.
func CollectionStage(err error) string {
	var failure *collectionFailure
	if errors.As(err, &failure) {
		return failure.stage
	}
	return "unknown"
}

type collectionFailure struct {
	stage string
	cause error
}

func (e *collectionFailure) Error() string { return e.cause.Error() }
func (e *collectionFailure) Unwrap() error { return e.cause }

func qualifiedTargets(a resourceguard.GuestQualificationArtifact, floors map[control.Uint64]control.Uint64, intents []QualifiedIntent) ([]budget.QualifiedTarget, error) {
	if a.Validate() != nil {
		return nil, fmt.Errorf("complete actual minimum-Guest source required")
	}
	// Source Outside是预留合同，不是证明；先核全部真实内核/在途/allocator分项确被覆盖。
	if a.CurrentLimits.OutsideUpperBytes < a.Outside.TotalBytes {
		return nil, fmt.Errorf("configured Guest Outside is below the exact enforced physical bound")
	}
	system := a.Outside.SystemBytes + a.Outside.KernelAndInitialRootBytes
	transition := a.Outside.ConditionalOccupiedBytes + a.Outside.CPUStockPaidBytes
	minimumSafety := a.Outside.CompactionFreeReserveBytes + a.Outside.PCPFreeReserveBytes + a.Outside.WatermarkFreeReserveBytes
	if system < a.Outside.SystemBytes || transition < a.Outside.ConditionalOccupiedBytes || a.CurrentLimits.OutsideUpperBytes < system || a.CurrentLimits.OutsideUpperBytes-system < transition {
		return nil, fmt.Errorf("Guest Outside partition overflow")
	}
	safety := a.CurrentLimits.OutsideUpperBytes - system - transition
	if system < a.CurrentLimits.SystemMemoryMaxBytes || transition == 0 || safety < minimumSafety {
		return nil, fmt.Errorf("exact continuous outside commitments incomplete")
	}
	targets := make([]budget.QualifiedTarget, 0, len(intents))
	var previous control.Uint64
	for _, intent := range intents {
		n := intent.VMMemoryBytes
		if n <= previous || n < a.MinimumVMBytes || n > a.MaximumVMBytes || (n-a.MinimumVMBytes)%a.BlockBytes != 0 {
			return nil, fmt.Errorf("target differs from original exact Runner geometry")
		}
		previous = n
		usable := floors[n]
		if usable == 0 || usable > n {
			return nil, fmt.Errorf("exact target usable floor unavailable")
		}
		limits := a.CurrentLimits
		limits.HugePools = append([]resourceguard.HugePool(nil), limits.HugePools...)
		if intent.NormalMemoryMaxBytes != 0 {
			limits.MemoryMaxBytes = intent.NormalMemoryMaxBytes
		}
		total, e := limits.TotalUpperBytes()
		if e != nil || total > uint64(usable) {
			return nil, fmt.Errorf("target complete budget exceeds observed original minimum usable memory")
		}
		targets = append(targets, budget.QualifiedTarget{VMMemoryBytes: n, GuestUsableMemoryBytes: usable, Limits: limits, SystemOutsidePGBytes: system, TransitionBytes: transition, SafetyBytes: safety})
	}
	return targets, nil
}

// observedTargetFloor逐目标给floor；当前/未来规格可用本次保守观测，较小目标必须
// 选择同出生的原已发布确切floor，不按规格比例或减1Gi推算。后继仍逐次核actual。
func observedTargetFloor(current, target, minimum, maximum, observed control.Uint64) (control.Uint64, bool, error) {
	if minimum == 0 || maximum < minimum || current < minimum || current > maximum || target < minimum || target > maximum || observed == 0 || observed > current {
		return 0, false, fmt.Errorf("exact actual original target geometry unavailable")
	}
	if target >= current {
		return observed, false, nil
	}
	return 0, true, nil
}

// canonicalGuestArtifactSHA覆盖全部安全字段，不能只比较不含P/A子cap的RootStateSHA。
// Raw facts的key顺序不赋予含义；UseNumber避免通过float64改变任何原整数。
func canonicalGuestArtifactSHA(a resourceguard.GuestQualificationArtifact) (string, error) {
	raw, e := json.Marshal(a)
	if e != nil {
		return "", e
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if e = decoder.Decode(&value); e != nil {
		return "", e
	}
	canonical, e := json.Marshal(value)
	if e != nil {
		return "", e
	}
	return SHA256(canonical), nil
}
func sameGuestArtifact(a, b resourceguard.GuestQualificationArtifact) bool {
	first, e := canonicalGuestArtifactSHA(a)
	if e != nil {
		return false
	}
	last, e := canonicalGuestArtifactSHA(b)
	return e == nil && first == last
}

// Collect只读原API/UDS。复用原生产capture两次完整核验，不调用Publish/Root写/PG/QMP。
func (c *Collector) Collect(ctx context.Context) (Collection, error) {
	var zero Collection
	if c == nil || ctx == nil || ctx.Err() != nil {
		return zero, fmt.Errorf("collector unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.config.TimeoutSeconds)*time.Second)
	defer cancel()
	report, bundle, e := c.gather(ctx)
	if e != nil {
		return zero, &collectionFailure{"gather", e}
	}
	proposed := Config{ProtocolVersion: 1, Binding: c.config.Binding, NodeName: c.config.NodeName, NodeUID: c.config.NodeUID, ReportSHA256: SHA256(bundle.Report), ControlSocketPath: c.config.ControlSocketPath, TimeoutSeconds: c.config.TimeoutSeconds, ReportPath: filepath.Join(c.config.OutputDirectory, "report.json"), LinuxArtifactPath: filepath.Join(c.config.OutputDirectory, "linux-artifact.json"), ContinuousArtifactPath: filepath.Join(c.config.OutputDirectory, "continuous-artifact.json")}
	inspector, e := New(c.core, c.coord, c.vm, c.root, proposed)
	if e != nil {
		return zero, e
	}
	before, e := inspector.capture(ctx, report)
	if e != nil {
		return zero, &collectionFailure{"capture_first", e}
	}
	after, e := inspector.capture(ctx, report)
	if e != nil {
		return zero, &collectionFailure{"capture_second", e}
	}
	if before != after {
		return zero, &collectionFailure{"capture_drift", fmt.Errorf("actual original sources changed during collection")}
	}
	_, final, e := c.root.Read(ctx, c.config.Binding)
	if e != nil || final.GuestQualification == nil || final.GuestQualificationUnavailable != nil || final.GuestQualification.MatchView(final) != nil {
		return zero, &collectionFailure{"final_guest", fmt.Errorf("final original Guest source unconfirmed")}
	}
	var first struct {
		Artifact resourceguard.GuestQualificationArtifact `json:"artifact"`
	}
	if json.Unmarshal(bundle.LinuxArtifact, &first) != nil || !sameGuestArtifact(first.Artifact, *final.GuestQualification) {
		return zero, &collectionFailure{"final_guest_drift", fmt.Errorf("original Guest artifact changed after complete capture")}
	}
	return Collection{Bundle: bundle, ProposedIssuerConfig: proposed, SnapshotSHA256: after}, nil
}

func (c *Collector) gather(ctx context.Context) (Report, Bundle, error) {
	var report Report
	var bundle Bundle
	b := c.config.Binding
	vm, e := c.vm.NeonvmV1().VirtualMachines(b.Namespace).Get(ctx, b.Name, metav1.GetOptions{})
	if e != nil {
		return report, bundle, e
	}
	if vm == nil || vm.UID != b.VMUID || vm.Spec.DSGABootstrap == nil || vm.Spec.DSGABootstrap.Incarnation != b.Incarnation || vm.DeletionTimestamp != nil {
		return report, bundle, fmt.Errorf("original actual collector VM changed")
	}
	snap, e := journal.Inspect(vm, b)
	if e != nil {
		return report, bundle, e
	}
	model := snap.State()
	discovered, view, e := c.root.Read(ctx, b)
	if e != nil {
		return report, bundle, e
	}
	if view.GuestQualification == nil || view.GuestQualificationUnavailable != nil {
		return report, bundle, fmt.Errorf("original Guest qualification source unavailable; old state success is not qualification")
	}
	a := *view.GuestQualification
	if e = a.MatchView(view); e != nil {
		return report, bundle, e
	}
	if a.Identity.VMUID != string(b.VMUID) || a.Incarnation != b.Incarnation || model.ResourceIdentity == nil || *model.ResourceIdentity != a.Identity || discovered.OwnerEpoch != model.Owner.Epoch {
		return report, bundle, fmt.Errorf("original collector Root/Owner binding changed")
	}
	secret, e := c.core.Secrets(b.Namespace).Get(ctx, b.Name+"-config", metav1.GetOptions{})
	if e != nil {
		return report, bundle, e
	}
	if bootstrap.CheckReady(vm, secret, false) != nil || string(secret.UID) != a.ConfigSecretUID || SHA256(secret.Data[resourceguard.ResourceSecretKey]) != a.ResourceSourceSHA256 {
		return report, bundle, fmt.Errorf("artifact differs from original immutable Runtime Secret")
	}
	var source resourceguard.SourceConfig
	if json.Unmarshal(secret.Data[resourceguard.ResourceSecretKey], &source) != nil || source.Validate() != nil || source.Config.ExecutionModel != resourceguard.TrustedControllerExecutionModel {
		return report, bundle, fmt.Errorf("original supported source required")
	}
	// 不能自行拼固定v2：通用Guest的v3包含原生配置摘要及逻辑Compute身份。
	// 使用唯一启动投影，随后仍与实际Root报告摘要逐字节核对。
	runtime, e := bootstrap.RuntimeForObservation(vm, secret)
	if e != nil {
		return report, bundle, e
	}
	runtimeRaw, e := json.Marshal(runtime)
	if e != nil || SHA256(runtimeRaw) != a.RuntimeDocumentSHA256 {
		return report, bundle, fmt.Errorf("artifact original Runtime bytes changed")
	}
	lease, e := c.coord.Leases(b.Namespace).Get(ctx, journal.LeaseName(b), metav1.GetOptions{})
	if e != nil {
		return report, bundle, e
	}
	reader, e := vmguard.NewReader(c.core, b, string(lease.UID))
	if e != nil {
		return report, bundle, e
	}
	birth, ref, pub, e := reader.Inspect(ctx, nil, model.Owner)
	if e != nil {
		return report, bundle, e
	}
	if birth.Identity.VMUID != string(b.VMUID) || birth.Identity.Incarnation != b.Incarnation || birth.InitialMode == nil || birth.InitialMode.RuntimeSHA256 != a.RuntimeDocumentSHA256 || birth.InitialMode.ResourceSHA256 != a.ResourceSourceSHA256 || birth.InitialMode.ConfigSecretUID != a.ConfigSecretUID || birth.Identity.MinimumBytes != a.MinimumVMBytes || birth.Identity.MaximumBytes != a.MaximumVMBytes || birth.Identity.BlockBytes != a.BlockBytes || pub.Actual == nil || pub.State == nil || pub.State.Pending != nil || pub.Actual.RequestedBytes != pub.Actual.ActualBytes || pub.InitialBootstrap == nil {
		return report, bundle, fmt.Errorf("actual settled original Runner initial publication required")
	}
	if vm.Status.MemorySize == nil {
		return report, bundle, fmt.Errorf("actual VM capacity unavailable")
	}
	capacity, exact := vm.Status.MemorySize.AsInt64()
	if !exact || capacity <= 0 || control.Uint64(capacity) != pub.Actual.ActualBytes {
		return report, bundle, fmt.Errorf("VM actual capacity differs from Runner")
	}
	identityRaw, e := json.Marshal(birth.Identity)
	if e != nil {
		return report, bundle, e
	}
	runnerSHA := SHA256(identityRaw)
	floors := map[control.Uint64]control.Uint64{}
	var prior *budget.PlatformQualification
	for _, intent := range c.config.Targets {
		usable, needsPrior, floorErr := observedTargetFloor(control.Uint64(capacity), intent.VMMemoryBytes, a.MinimumVMBytes, a.MaximumVMBytes, a.ObservedGuestMemTotalBytes)
		if floorErr != nil {
			return report, bundle, floorErr
		}
		if needsPrior {
			if c.config.PreviousQualification == nil {
				return report, bundle, fmt.Errorf("smaller target requires explicit retained exact qualification; never extrapolate usable memory")
			}
			if prior == nil {
				claims, readErr := budgetclaims.New(c.core, b)
				if readErr != nil {
					return report, bundle, readErr
				}
				value, readErr := claims.Qualification(ctx, *c.config.PreviousQualification)
				if readErr != nil {
					return report, bundle, readErr
				}
				if value.Identity != a.Identity || value.Incarnation != b.Incarnation || value.RootBootstrapSHA256 != a.RootBootstrapSHA256 || value.RunnerBirth != ref || value.RunnerIdentitySHA256 != runnerSHA {
					return report, bundle, fmt.Errorf("retained qualification belongs to another original birth")
				}
				prior = &value
			}
			target, readErr := prior.Target(intent.VMMemoryBytes)
			if readErr != nil {
				return report, bundle, readErr
			}
			usable = target.GuestUsableMemoryBytes
			if usable > a.ObservedGuestMemTotalBytes {
				return report, bundle, fmt.Errorf("retained actual usable floor no longer holds")
			}
		}
		floors[intent.VMMemoryBytes] = usable
	}
	targets, e := qualifiedTargets(a, floors, c.config.Targets)
	if e != nil {
		return report, bundle, e
	}
	node, e := c.core.Nodes().Get(ctx, c.config.NodeName, metav1.GetOptions{})
	if e != nil {
		return report, bundle, e
	}
	if string(node.UID) != c.config.NodeUID || birth.Identity.NodeUID != c.config.NodeUID || vm.Status.Node != node.Name {
		return report, bundle, fmt.Errorf("actual Host identity differs")
	}
	var ledger hostguard.Model
	if json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger) != nil || ledger.Profile == nil {
		return report, bundle, fmt.Errorf("actual original Host ledger/profile unavailable")
	}
	profileSHA, e := hostguard.ProfileSHA256(*ledger.Profile)
	if e != nil {
		return report, bundle, e
	}
	nodeCapacity, ok1 := node.Status.Capacity.Memory().AsInt64()
	allocatable, ok2 := node.Status.Allocatable.Memory().AsInt64()
	if !ok1 || !ok2 || nodeCapacity <= 0 || allocatable <= 0 {
		return report, bundle, fmt.Errorf("exact Host K8s capacity unavailable")
	}
	policy := hostguard.KubernetesBudgetPolicy{ProtocolVersion: 1, Scope: hostguard.KubernetesBudgetScope, NodeName: node.Name, NodeUID: string(node.UID), CapacityBytes: control.Uint64(nodeCapacity), AllocatableBytes: control.Uint64(allocatable), SystemReservationBytes: ledger.OutsideBytes, ProfileSHA256: profileSHA}
	if e = policy.Inspect(node, *ledger.Profile, ledger.OutsideBytes); e != nil {
		return report, bundle, e
	}
	artifactSHA, e := canonicalGuestArtifactSHA(a)
	if e != nil {
		return report, bundle, e
	}
	idRaw, e := json.Marshal(struct {
		ArtifactSHA string
		Targets     []budget.QualifiedTarget
		Host        hostguard.Binding
		ProfileSHA  string
	}{artifactSHA, targets, ledger.Binding, profileSHA})
	if e != nil {
		return report, bundle, e
	}
	reportID := "k8s-original-" + SHA256(idRaw)[:40]
	qualification := budget.PlatformQualification{ProtocolVersion: 1, Identity: a.Identity, Incarnation: b.Incarnation, ReportID: reportID, RootBootstrapSHA256: a.RootBootstrapSHA256, RunnerBirth: ref, RunnerIdentitySHA256: runnerSHA, RunnerPodName: vm.Status.PodName, RunnerPodUID: birth.Identity.RunnerPodUID, RunnerRestartCount: vm.Status.RestartCount, NodeName: node.Name, NodeUID: string(node.UID), HoldUntilExplicitSettlement: true, Targets: targets}
	if e = qualification.Validate(); e != nil {
		return report, bundle, e
	}
	linux, e := json.Marshal(struct {
		ProtocolVersion   uint32                                   `json:"protocol_version"`
		Scope             string                                   `json:"scope"`
		Artifact          resourceguard.GuestQualificationArtifact `json:"artifact"`
		RunnerBirth       vmguard.Birth                            `json:"runner_birth"`
		RunnerPublication vmguard.Publication                      `json:"runner_publication"`
	}{1, "original-guest-root-runner-linux-artifact-v1", a, birth, pub})
	if e != nil {
		return report, bundle, e
	}
	continuous, e := json.Marshal(struct {
		ProtocolVersion       uint32                                  `json:"protocol_version"`
		Scope                 string                                  `json:"scope"`
		OriginalSourceSHA256  string                                  `json:"original_source_sha256"`
		RuntimeDocumentSHA256 string                                  `json:"runtime_document_sha256"`
		RootStateSHA256       string                                  `json:"root_state_sha256"`
		Outside               resourceguard.GuestQualificationOutside `json:"required_outside"`
		Limits                resourceguard.Limits                    `json:"actual_limits"`
		HostPolicy            hostguard.KubernetesBudgetPolicy        `json:"host_kubernetes_budget"`
	}{1, "original-guest-hard-domains-and-kubernetes-budget-v1", a.OriginalSourceSHA256, a.RuntimeDocumentSHA256, a.RootStateSHA256, a.Outside, a.CurrentLimits, policy})
	if e != nil || len(linux) > MaxArtifactBytes || len(continuous) > MaxArtifactBytes {
		return report, bundle, fmt.Errorf("original collected artifact exceeds existing bound")
	}

	report = Report{GuestArtifactSHA256: artifactSHA, ProtocolVersion: 2, Scope: KubernetesBudgetReportScope, Qualification: qualification, ConfigSecretUID: string(secret.UID), ResourceSourceSHA256: a.ResourceSourceSHA256, LinuxArtifactSHA256: SHA256(linux), ContinuousArtifactSHA256: SHA256(continuous), HostBinding: ledger.Binding, HostProfileSHA256: profileSHA, HostSystemBytes: ledger.OutsideBytes, HostKubernetesBudget: &policy}
	if e = report.Validate(); e != nil {
		return report, bundle, e
	}
	raw, e := json.Marshal(report)
	if e != nil || len(raw) > MaxReportBytes {
		return report, bundle, fmt.Errorf("collected report exceeds existing contract")
	}
	bundle = Bundle{Report: raw, LinuxArtifact: linux, ContinuousArtifact: continuous}
	return report, bundle, nil
}

// WriteCollection仅写部署者预建的root保护私有目录。O_EXCL保留每次原结果，不覆盖旧失败。
// 该配置仍待Root审阅后显式调用原Publish；本函数绝不创建Kube资格CM。
func WriteCollection(c Collection) error {
	cfg := c.ProposedIssuerConfig
	if cfg.Validate() != nil || cfg.ReportSHA256 != SHA256(c.Bundle.Report) {
		return fmt.Errorf("collected original config/report mismatch")
	}
	if _, e := cfg.authenticate(c.Bundle); e != nil {
		return e
	}
	dir := filepath.Dir(cfg.ReportPath)
	if protectedDirectory(dir, 0) != nil {
		return fmt.Errorf("root protected collection output directory required")
	}
	stat, e := os.Stat(dir)
	if e != nil || stat.Mode().Perm() != 0o700 {
		return fmt.Errorf("collection output directory must already be private 0700")
	}
	raw, e := json.MarshalIndent(cfg, "", "  ")
	if e != nil {
		return e
	}
	files := []struct {
		name string
		raw  []byte
	}{{cfg.ReportPath, c.Bundle.Report}, {cfg.LinuxArtifactPath, c.Bundle.LinuxArtifact}, {cfg.ContinuousArtifactPath, c.Bundle.ContinuousArtifact}, {filepath.Join(dir, "issuer-config.proposed.json"), append(raw, '\n')}}
	for _, f := range files {
		if filepath.Dir(f.name) != dir {
			return fmt.Errorf("collection output paths differ")
		}
		fd, e := os.OpenFile(f.name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if e != nil {
			return e
		}
		_, e = fd.Write(f.raw)
		if e == nil {
			e = fd.Sync()
		}
		closeErr := fd.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
	}
	d, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
