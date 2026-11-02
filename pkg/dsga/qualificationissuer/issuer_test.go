package qualificationissuer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/internal/dsgatest"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/localcontrol"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
	"github.com/stretchr/testify/require"
	coordination "k8s.io/api/coordination/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"
)

// 复用原有效初始领域合同。Kube/Root 仍为明确模型；本组不提供可用于真实签发
// 的配置/平台报告，不将模型文件 hash 或本组通过解释为 Linux 持续强制认证。
func issuerFixture(t *testing.T) (*Issuer, *kubefake.Clientset, Config, Bundle, Report) {
	t.Helper()
	raw, err := os.ReadFile("../journal/testdata/initial-domain.json")
	require.NoError(t, err)
	var domain struct {
		Initial budget.Initial `json:"initial"`
	}
	require.NoError(t, json.Unmarshal(raw, &domain))
	initial := domain.Initial
	require.NoError(t, initial.Validate())
	bootstrap, err := json.Marshal(initial.PGAggregate.Bootstrap)
	require.NoError(t, err)
	q := budget.PlatformQualification{ProtocolVersion: 1, Identity: initial.PGAggregate.Identity, Incarnation: initial.Incarnation, ReportID: "explicit-unit-domain-not-platform-certification", RootBootstrapSHA256: SHA256(bootstrap), RunnerBirth: initial.RunnerBirth, RunnerIdentitySHA256: initial.RunnerIdentitySHA256, RunnerPodName: initial.RunnerPodName, RunnerPodUID: initial.RunnerPodUID, RunnerRestartCount: initial.RunnerRestartCount, NodeName: initial.NodeName, NodeUID: initial.NodeUID, HoldUntilExplicitSettlement: true, Targets: []budget.QualifiedTarget{{VMMemoryBytes: initial.VMMemoryBytes, GuestUsableMemoryBytes: initial.GuestUsableMemoryBytes, Limits: initial.PGAggregate.Bootstrap.Limits, SystemOutsidePGBytes: initial.SystemOutsidePG.UpperBytes, TransitionBytes: initial.Transition.UpperBytes, SafetyBytes: initial.Safety.UpperBytes}}}
	require.NoError(t, q.Validate())
	bundle := Bundle{LinuxArtifact: []byte("explicit Linux artifact fixture; not a platform certificate"), ContinuousArtifact: []byte("explicit persistent constraint fixture; not a platform certificate")}
	report := Report{ProtocolVersion: 1, Scope: ReportScope, Qualification: q, ConfigSecretUID: "fixture-config-uid", ResourceSourceSHA256: strings.Repeat("a", 64), LinuxArtifactSHA256: SHA256(bundle.LinuxArtifact), ContinuousArtifactSHA256: SHA256(bundle.ContinuousArtifact), HostBinding: hostguard.Binding{NodeName: q.NodeName, NodeUID: types.UID(q.NodeUID), Namespace: "testing", LeaseName: "host-lease", LeaseUID: "host-lease-uid"}, HostProfileSHA256: strings.Repeat("b", 64), HostSystemBytes: 1 << 20}
	bundle.Report, err = json.Marshal(report)
	require.NoError(t, err)
	config := Config{ProtocolVersion: 1, Binding: journal.Binding{Namespace: "testing", Name: "issuer-vm", VMUID: types.UID(q.Identity.VMUID), Incarnation: q.Incarnation}, NodeName: q.NodeName, NodeUID: q.NodeUID, ReportSHA256: SHA256(bundle.Report), ControlSocketPath: "/run/issuer-test.sock", TimeoutSeconds: 5}
	coreClient := kubefake.NewSimpleClientset()
	coreClient.Fake.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		obj := action.(kt.CreateAction).GetObject().(*core.ConfigMap)
		obj.UID = "created-original-qualification-uid"
		obj.ResourceVersion = "1"
		return false, nil, nil
	})
	issuer, err := New(coreClient.CoreV1(), coreClient.CoordinationV1(), vmfake.NewSimpleClientset(), fixtureRoot{}, config)
	require.NoError(t, err)
	// 只替换本包私有读取边界，实际发行入口固定使用真实 capture/UDS 客户端。
	issuer.inspect = func(context.Context, Report) (string, error) { return "fixed-model-source-fingerprint", nil }
	return issuer, coreClient, config, bundle, report
}

type fixtureRoot struct{}

func (fixtureRoot) Read(context.Context, journal.Binding) (localcontrol.OwnerBinding, resourceguard.View, error) {
	return localcontrol.OwnerBinding{}, resourceguard.View{}, errors.New("fixture has no real Root")
}

func TestIssuerPublishesOnlyProtectedOriginalAndConsumerReads(t *testing.T) {
	issuer, client, config, bundle, report := issuerFixture(t)
	result, err := issuer.Publish(context.Background(), bundle)
	require.NoError(t, err)
	reader, err := budgetclaims.New(client.CoreV1(), config.Binding)
	require.NoError(t, err)
	q, err := reader.Qualification(context.Background(), result.Source)
	require.NoError(t, err)
	require.Equal(t, report.Qualification, q)
	object, err := client.CoreV1().ConfigMaps(config.Binding.Namespace).Get(context.Background(), result.Source.Claim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.True(t, *object.Immutable)
	require.Contains(t, object.Finalizers, budgetclaims.ProtectionFinalizer)
	require.Equal(t, config.ReportSHA256, object.Annotations[ReportAnnotation])
	require.Equal(t, report.LinuxArtifactSHA256, object.Annotations[LinuxAnnotation])
	require.Equal(t, report.ContinuousArtifactSHA256, object.Annotations[ContinuousAnnotation])
	require.Equal(t, result.Source.Claim.UID, string(object.UID))
	require.Equal(t, result.Source.SHA256, SHA256([]byte(object.Data[budgetclaims.ClaimKey])))
	for _, action := range client.Actions() {
		require.NotContains(t, []string{"update", "patch", "delete"}, action.GetVerb())
	}
}

func TestIssuerMissingOrChangedCertificationNeverWrites(t *testing.T) {
	for _, mode := range []string{"unselected-report", "missing-linux", "changed-linux", "missing-continuous", "changed-continuous", "model-scope", "different-vm", "different-node", "invalid-target", "missing-host-budget"} {
		t.Run(mode, func(t *testing.T) {
			issuer, client, _, bundle, report := issuerFixture(t)
			switch mode {
			case "unselected-report":
				issuer.config.ReportSHA256 = strings.Repeat("f", 64)
			case "missing-linux":
				bundle.LinuxArtifact = nil
			case "changed-linux":
				bundle.LinuxArtifact = append(bundle.LinuxArtifact, '!')
			case "missing-continuous":
				bundle.ContinuousArtifact = nil
			case "changed-continuous":
				bundle.ContinuousArtifact = append(bundle.ContinuousArtifact, '!')
			case "model-scope":
				report.Scope = "trusted-controller-host-api-model-v1"
			case "different-vm":
				report.Qualification.Identity.VMUID = "different-vm"
			case "different-node":
				issuer.config.NodeUID = "different-node"
			case "invalid-target":
				report.Qualification.Targets[0].GuestUsableMemoryBytes = 1
			case "missing-host-budget":
				report.HostSystemBytes = 0
			}
			if mode == "model-scope" || mode == "different-vm" || mode == "invalid-target" || mode == "missing-host-budget" {
				var err error
				bundle.Report, err = json.Marshal(report)
				require.NoError(t, err)
				issuer.config.ReportSHA256 = SHA256(bundle.Report)
			}
			_, err := issuer.Publish(context.Background(), bundle)
			require.Error(t, err)
			require.Empty(t, client.Actions(), "没有原真实认证来源时连 CM CREATE 都不能触发")
		})
	}
}

func TestIssuerIdentityDriftPreservesUnconfirmedObject(t *testing.T) {
	for _, stage := range []string{"before-create", "after-create"} {
		t.Run(stage, func(t *testing.T) {
			issuer, client, config, bundle, _ := issuerFixture(t)
			calls := 0
			issuer.inspect = func(context.Context, Report) (string, error) {
				calls++
				if (stage == "before-create" && calls == 2) || (stage == "after-create" && calls == 3) {
					return "drift", nil
				}
				return "stable", nil
			}
			_, err := issuer.Publish(context.Background(), bundle)
			require.Error(t, err)
			if stage == "before-create" {
				require.Empty(t, client.Actions())
				return
			}
			var unknown *Unconfirmed
			require.ErrorAs(t, err, &unknown)
			require.NotEmpty(t, unknown.Claim.UID)
			object, readErr := client.CoreV1().ConfigMaps(config.Binding.Namespace).Get(context.Background(), unknown.Claim.Name, metav1.GetOptions{})
			require.NoError(t, readErr)
			require.Contains(t, object.Finalizers, budgetclaims.ProtectionFinalizer)
			for _, action := range client.Actions() {
				require.NotEqual(t, "delete", action.GetVerb())
			}
		})
	}
}

func TestIssuerLostCreateReplyRequiresExplicitOriginalUID(t *testing.T) {
	issuer, client, config, bundle, _ := issuerFixture(t)
	lose := true
	client.Fake.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		if !lose {
			return false, nil, nil
		}
		lose = false
		object := action.(kt.CreateAction).GetObject().(*core.ConfigMap).DeepCopy()
		object.UID = "committed-before-lost-reply"
		object.ResourceVersion = "1"
		require.NoError(t, client.Tracker().Create(core.SchemeGroupVersion.WithResource("configmaps"), object, object.Namespace))
		return true, nil, errors.New("fixture reply lost after actual tracker commit")
	})
	_, err := issuer.Publish(context.Background(), bundle)
	var unknown *Unconfirmed
	require.ErrorAs(t, err, &unknown)
	require.Empty(t, unknown.Claim.UID)
	object, err := client.CoreV1().ConfigMaps(config.Binding.Namespace).Get(context.Background(), unknown.Claim.Name, metav1.GetOptions{})
	require.NoError(t, err)
	before := len(client.Actions())
	_, err = issuer.Publish(context.Background(), bundle)
	require.ErrorAs(t, err, &unknown)
	for _, action := range client.Actions()[before:] {
		require.NotEqual(t, "get", action.GetVerb(), "AlreadyExists 不得触发同名自动收养")
	}
	issuer.config.ResumeClaimUID = string(object.UID)
	result, err := issuer.Publish(context.Background(), bundle)
	require.NoError(t, err)
	require.Equal(t, string(object.UID), result.Source.Claim.UID)
	issuer.config.ResumeClaimUID = "same-name-new-uid"
	_, err = issuer.Publish(context.Background(), bundle)
	require.ErrorAs(t, err, &unknown)
}

func TestIssuerRootClientUsesOnlyExistingReadAction(t *testing.T) {
	raw, err := os.ReadFile("../journal/testdata/initial-domain.json")
	require.NoError(t, err)
	var domain struct {
		Initial budget.Initial `json:"initial"`
	}
	require.NoError(t, json.Unmarshal(raw, &domain))
	initial := domain.Initial
	view := resourceguard.View{ProtocolVersion: 1, NormalQuotaProtocolVersion: 1, ArrayAuthorityProtocolVersion: 1, Identity: initial.PGAggregate.Identity, IssuerFloorEpoch: 1, Bootstrap: initial.PGAggregate.Bootstrap, BootstrapApplied: true, SystemPlacementObserved: true, Birth: &initial.PGAggregate.Birth, Actual: resourceguard.Evidence{ProtocolVersion: 1, Identity: initial.PGAggregate.Identity}}
	require.NoError(t, view.Validate())
	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(socket, 0600))
	binding := localcontrol.OwnerBinding{ProtocolVersion: 1, Instance: controlBinding(initial), OwnerEpoch: 1}
	var calls atomic.Int32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Cache-Control", "no-store")
		if r.Method == http.MethodGet {
			require.Equal(t, localcontrol.OwnerPath, r.URL.Path)
			_ = json.NewEncoder(w).Encode(binding)
			return
		}
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, localcontrol.CommandPath, r.URL.Path)
		var command struct {
			Action string `json:"action"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&command))
		require.Equal(t, "resource_state", command.Action)
		_ = json.NewEncoder(w).Encode(owner.CoordinationResult{ProtocolVersion: 1, Instance: binding.Instance, OwnerEpoch: 1, ResourceState: &view})
	})}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	client, err := NewLocalRoot(socket, time.Second)
	require.NoError(t, err)
	defer client.Close()
	ownerBinding, root, err := client.Read(context.Background(), journal.Binding{VMUID: types.UID(initial.Instance.VMUID)})
	require.NoError(t, err)
	require.Equal(t, binding, ownerBinding)
	require.Equal(t, initial.PGAggregate.Identity, root.Identity)
	require.EqualValues(t, 2, calls.Load())
}

func controlBinding(i budget.Initial) control.CoordinationBinding {
	return control.CoordinationBinding(i.Instance)
}

func TestIssuerTrustedFilesRejectWritableAndSymlinkInputs(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-owned input filesystem test requires root")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "report")
	require.NoError(t, os.WriteFile(path, []byte("original"), 0600))
	raw, err := ReadTrustedFile(path, 64)
	require.NoError(t, err)
	require.Equal(t, "original", string(raw))
	require.NoError(t, os.Chmod(path, 0666))
	_, err = ReadTrustedFile(path, 64)
	require.Error(t, err)
	require.NoError(t, os.Chmod(path, 0600))
	alias := filepath.Join(directory, "alias")
	require.NoError(t, os.Symlink(path, alias))
	_, err = ReadTrustedFile(alias, 64)
	require.Error(t, err)
	_, err = ReadTrustedFile(path, 4)
	require.Error(t, err)
	require.NoError(t, os.Chmod(directory, 0777))
	_, err = ReadTrustedFile(path, 64)
	require.Error(t, err)
}

func TestIssuerLiveCaptureRejectsMissingAndRecreatedVM(t *testing.T) {
	for _, mode := range []string{"missing-vm", "recreated-vm", "missing-bootstrap"} {
		t.Run(mode, func(t *testing.T) {
			issuer, client, config, bundle, _ := issuerFixture(t)
			if mode != "missing-vm" {
				identity := config.Binding.VMUID
				if mode == "recreated-vm" {
					identity = "recreated-vm"
				}
				_, err := issuer.vm.NeonvmV1().VirtualMachines(config.Binding.Namespace).Create(context.Background(), &vmv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Name: config.Binding.Name, Namespace: config.Binding.Namespace, UID: identity, ResourceVersion: "1"}}, metav1.CreateOptions{})
				require.NoError(t, err)
			}
			issuer.inspect = issuer.capture
			_, err := issuer.Publish(context.Background(), bundle)
			require.Error(t, err)
			require.Empty(t, client.Actions(), "真实读取拒绝不能触发任何 Core 写")
		})
	}
}

func TestIssuerRootSocketAndInputAncestorsRemainOriginal(t *testing.T) {
	directory := t.TempDir()
	socket := filepath.Join(directory, "agent.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	defer listener.Close()
	require.NoError(t, os.Chmod(socket, 0666))
	_, err = NewLocalRoot(socket, time.Second)
	require.Error(t, err)
	require.NoError(t, os.Chmod(socket, 0600))
	client, err := NewLocalRoot(socket, time.Second)
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, os.Rename(socket, socket+".old"))
	replacement, err := net.Listen("unix", socket)
	require.NoError(t, err)
	defer replacement.Close()
	require.NoError(t, os.Chmod(socket, 0600))
	_, _, err = client.Read(context.Background(), journal.Binding{VMUID: "vm-uid"})
	require.ErrorContains(t, err, "socket identity changed")
	if os.Geteuid() != 0 {
		return
	}
	ancestor := filepath.Join(directory, "replaceable")
	child := filepath.Join(ancestor, "private")
	require.NoError(t, os.MkdirAll(child, 0700))
	require.NoError(t, os.Chmod(ancestor, 0777))
	file := filepath.Join(child, "report")
	require.NoError(t, os.WriteFile(file, []byte("original"), 0600))
	_, err = ReadTrustedFile(file, 64)
	require.Error(t, err, "私有文件的可替换祖先同样不能被信任")
}

// 此夹具不替换 Issuer.inspect：VM/Lease/Secret/Runner/Host 均经真实生产读者
// 和既有 UDS 协议读取；Kubernetes 与 Root 是显式 protocol peer，非真实平台认证。
func issuerCaptureFixture(t *testing.T, replaceNode bool) (*Issuer, *kubefake.Clientset, Config, Bundle, *atomic.Int32) {
	t.Helper()
	_, _, config, bundle, report := issuerFixture(t)
	raw, err := os.ReadFile("../journal/testdata/initial-domain.json")
	require.NoError(t, err)
	var domain struct {
		Initial budget.Initial `json:"initial"`
	}
	require.NoError(t, json.Unmarshal(raw, &domain))
	initial := domain.Initial
	b := config.Binding
	yes := true
	vm := &vmv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{Namespace: b.Namespace, Name: b.Name, UID: b.VMUID, ResourceVersion: "1"}}
	vm.Spec.DSGABootstrap = &vmv1.DSGABootstrap{ProtocolVersion: 1, Incarnation: b.Incarnation, ComputeInstanceID: initial.Instance.ComputeInstanceID}
	vms := vmfake.NewSimpleClientset(vm)
	store, err := journal.NewStore(vms.NeonvmV1().VirtualMachines(b.Namespace), b)
	require.NoError(t, err)
	snapshot, err := store.Initialize(context.Background(), "1", "issuer-capture-owner")
	require.NoError(t, err)
	snapshot, err = store.ReleaseBootstrap(context.Background(), snapshot.ResourceVersion(), types.UID(report.ConfigSecretUID))
	require.NoError(t, err)
	vm = snapshot.VirtualMachine()
	vm.Status.Phase = vmv1.VmRunning
	vm.Status.PodName = initial.RunnerPodName
	vm.Status.Node = initial.NodeName
	vm.Status.PodIP = "10.0.0.8"
	vm.Status.MemorySize = resource.NewQuantity(int64(initial.VMMemoryBytes), resource.BinarySI)
	_, err = vms.NeonvmV1().VirtualMachines(b.Namespace).Update(context.Background(), vm, metav1.UpdateOptions{})
	require.NoError(t, err)
	snapshot, err = store.Load(context.Background())
	require.NoError(t, err)
	snapshot, err = store.BindResource(context.Background(), snapshot, snapshot.State().Owner, initial.PGAggregate.Identity)
	require.NoError(t, err)
	snapshot, err = store.BindPG(context.Background(), snapshot, snapshot.State().Owner, initial.Instance)
	require.NoError(t, err)
	vm = snapshot.VirtualMachine()
	actor := snapshot.State().Owner
	ownerRef := []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: b.Name, UID: b.VMUID, Controller: &yes, BlockOwnerDeletion: &yes}}
	pod := &core.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: b.Namespace, Name: initial.RunnerPodName, UID: types.UID(initial.RunnerPodUID), ResourceVersion: "1", OwnerReferences: ownerRef}, Spec: core.PodSpec{NodeName: initial.NodeName}, Status: core.PodStatus{Phase: core.PodRunning, PodIP: vm.Status.PodIP}}
	node := dsgatest.InitialNode(&core.Node{ObjectMeta: metav1.ObjectMeta{Name: initial.NodeName, UID: types.UID(initial.NodeUID), ResourceVersion: "1"}}, b, pod.Name, string(pod.UID), initial.VMMemoryBytes)
	var ledger hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &ledger))
	report.HostBinding = ledger.Binding
	report.HostSystemBytes = ledger.OutsideBytes
	profile, err := json.Marshal(ledger.Profile)
	require.NoError(t, err)
	report.HostProfileSHA256 = SHA256(profile)
	duration, transitions := int32(60), int32(0)
	now := metav1.NewMicroTime(time.Now())
	vmLease := &coordination.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: b.Namespace, Name: journal.LeaseName(b), UID: "issuer-owner-lease", ResourceVersion: "1", OwnerReferences: ownerRef, Annotations: map[string]string{journal.LeaseBindingAnnotation: journal.LeaseMarker(b)}}, Spec: coordination.LeaseSpec{HolderIdentity: &actor.HolderID, LeaseDurationSeconds: &duration, LeaseTransitions: &transitions, AcquireTime: &now, RenewTime: &now}}
	hostLease := &coordination.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: ledger.Binding.Namespace, Name: ledger.Binding.LeaseName, UID: ledger.Binding.LeaseUID, ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID, Controller: &yes}}, Annotations: map[string]string{hostguard.LeaseAnnotation: hostguard.LeaseMarker(ledger.Binding)}}, Spec: coordination.LeaseSpec{HolderIdentity: &ledger.LeaseFloor.HolderID, LeaseDurationSeconds: &duration, LeaseTransitions: &transitions, AcquireTime: &now, RenewTime: &now}}
	source := resourceguard.SourceConfig{ProtocolVersion: 1, Config: resourceguard.Config{VMUID: initial.Instance.VMUID, ComputeInstanceID: initial.Instance.ComputeInstanceID, StateDir: resourceguard.FixedStateDir, SystemCgroup: true, ExecutionModel: resourceguard.TrustedControllerExecutionModel}, IssuerEpoch: initial.PGAggregate.Bootstrap.IssuerEpoch, BudgetCommitmentID: initial.PGAggregate.Bootstrap.BudgetCommitmentID, Limits: initial.PGAggregate.Bootstrap.Limits, ControlPublicKey: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))}
	require.NoError(t, source.Validate())
	sourceRaw, err := json.Marshal(source)
	require.NoError(t, err)
	report.ResourceSourceSHA256 = SHA256(sourceRaw)
	nativeConfig, err := json.Marshal(map[string]any{"spec": map[string]any{"dynamic_shared_buffers": map[string]any{"protocol_version": 2, "vm_uid": string(b.VMUID), "compute_instance_id": initial.Instance.ComputeInstanceID}}})
	require.NoError(t, err)
	secret := &core.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: b.Namespace, Name: b.Name + "-config", UID: types.UID(report.ConfigSecretUID), ResourceVersion: "1", OwnerReferences: ownerRef}, Immutable: &yes, Data: map[string][]byte{resourceguard.ResourceSecretKey: sourceRaw, "config.json": nativeConfig}}
	limits := source.Limits
	actual := resourceguard.Evidence{ProtocolVersion: 1, Identity: initial.PGAggregate.Identity, MemoryMaxBytes: limits.MemoryMaxBytes, MemoryCurrentBytes: 4 << 20, GuestMemTotalBytes: initial.GuestUsableMemoryBytes, HugePools: append([]resourceguard.HugePool(nil), limits.HugePools...), HugeOvercommitDisabled: true, CgroupPopulated: true, TaskLimit: &resourceguard.TaskLimitEvidence{MaxTasks: limits.PidsMaxTasks, CurrentTasks: 5}, System: &resourceguard.SystemEvidence{Scope: resourceguard.SystemChargedScope, MemoryMaxBytes: limits.SystemMemoryMaxBytes, MemoryCurrentBytes: 4 << 20, ProcessCount: 3, PID1Member: true}}
	for _, pool := range limits.HugePools {
		actual.WholePoolUpperBytes += pool.Pages * pool.PageSizeKB * 1024
	}
	if limits.AddressSpaceMaxBytes != 0 {
		actual.AddressSpace = &resourceguard.AddressSpaceEvidence{Processes: 5, MaxHardBytes: limits.AddressSpaceMaxBytes, MaxVirtualBytes: 32 << 20, Irreversible: true}
	}
	view := resourceguard.View{ProtocolVersion: 1, NormalQuotaProtocolVersion: 1, ArrayAuthorityProtocolVersion: 1, Identity: initial.PGAggregate.Identity, IssuerFloorEpoch: actor.Epoch, Bootstrap: initial.PGAggregate.Bootstrap, BootstrapApplied: true, SystemPlacementObserved: true, Birth: &initial.PGAggregate.Birth, Actual: actual}
	_, err = view.ConfirmedLimits()
	require.NoError(t, err)
	runnerID := vmguard.Identity{VMUID: string(b.VMUID), Incarnation: b.Incarnation, RunnerPodUID: initial.RunnerPodUID, RunnerBootID: "1234567890abcdef1234567890abcdef", NodeUID: initial.NodeUID, MinimumBytes: initial.VMMemoryBytes, MaximumBytes: initial.VMMemoryBytes * 2, BlockBytes: 8 << 20}
	birth := vmguard.Birth{ProtocolVersion: 1, Identity: runnerID, StateClaim: budget.ObjectRef{Name: "issuer-runner-state", UID: "issuer-runner-state-uid"}}
	ref := budget.ObjectRef{Name: vmguard.BindingName(b), UID: "issuer-runner-birth-uid"}
	report.Qualification.RunnerBirth = ref
	identity, err := json.Marshal(runnerID)
	require.NoError(t, err)
	report.Qualification.RunnerIdentitySHA256 = SHA256(identity)
	runnerState := vmguard.View{ProtocolVersion: 1, Identity: runnerID, BindingClaim: ref, Sequence: 1, Floor: actor, LeaseUID: string(vmLease.UID)}
	require.NoError(t, runnerState.Validate())
	publication := vmguard.Publication{ProtocolVersion: 1, Identity: runnerID, BindingClaim: &ref, State: &runnerState, Actual: &vmguard.Observation{RequestedBytes: initial.VMMemoryBytes, ActualBytes: initial.VMMemoryBytes}}
	birthRaw, err := json.Marshal(birth)
	require.NoError(t, err)
	stateRaw, err := json.Marshal(publication)
	require.NoError(t, err)
	makeClaim := func(name, uid, kind string, immutable bool, raw []byte) *core.ConfigMap {
		return &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: b.Namespace, Name: name, UID: types.UID(uid), ResourceVersion: "1", OwnerReferences: ownerRef, Finalizers: []string{vmguard.ProtectionFinalizer}, Labels: map[string]string{vmguard.ClaimLabel: kind}}, Immutable: &immutable, Data: map[string]string{vmguard.ClaimKey: string(raw)}}
	}
	coreClient := kubefake.NewSimpleClientset(pod, node, secret, vmLease, hostLease, makeClaim(ref.Name, ref.UID, "binding-v1", true, birthRaw), makeClaim(birth.StateClaim.Name, birth.StateClaim.UID, "state-v1", false, stateRaw))
	coreClient.PrependReactor("list", "pods", func(kt.Action) (bool, runtime.Object, error) {
		return true, &core.PodList{ListMeta: metav1.ListMeta{ResourceVersion: "pod-inventory-1"}, Items: []core.Pod{*pod.DeepCopy()}}, nil
	})
	coreClient.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		object := action.(kt.CreateAction).GetObject().(*core.ConfigMap)
		object.UID = "issuer-actual-capture-claim"
		object.ResourceVersion = "1"
		return false, nil, nil
	})
	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(socket, 0600))
	var requests atomic.Int32
	discovered := localcontrol.OwnerBinding{ProtocolVersion: 1, Instance: control.CoordinationBinding(initial.Instance), OwnerEpoch: actor.Epoch}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if request.Method == http.MethodGet {
			require.Equal(t, localcontrol.OwnerPath, request.URL.Path)
			_ = json.NewEncoder(w).Encode(discovered)
			return
		}
		require.Equal(t, localcontrol.CommandPath, request.URL.Path)
		var command control.CoordinationCommand
		require.NoError(t, json.NewDecoder(request.Body).Decode(&command))
		require.Equal(t, control.CoordinationResourceState, command.Action)
		count := requests.Add(1)
		if replaceNode && count == 2 {
			copy := node.DeepCopy()
			copy.UID = "replacement-node-uid"
			require.NoError(t, coreClient.Tracker().Update(core.SchemeGroupVersion.WithResource("nodes"), copy, ""))
		}
		_ = json.NewEncoder(w).Encode(owner.CoordinationResult{ProtocolVersion: 1, Instance: discovered.Instance, OwnerEpoch: actor.Epoch, ResourceState: &view})
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	root, err := NewLocalRoot(socket, time.Second)
	require.NoError(t, err)
	t.Cleanup(root.Close)
	bundle.Report, err = json.Marshal(report)
	require.NoError(t, err)
	config.ReportSHA256 = SHA256(bundle.Report)
	config.ControlSocketPath = socket
	issuer, err := New(coreClient.CoreV1(), coreClient.CoordinationV1(), vms, root, config)
	require.NoError(t, err)
	return issuer, coreClient, config, bundle, &requests
}

func TestIssuerCapturePublishesThroughActualReaders(t *testing.T) {
	issuer, client, config, bundle, requests := issuerCaptureFixture(t, false)
	result, err := issuer.Publish(context.Background(), bundle)
	require.NoError(t, err)
	require.EqualValues(t, 3, requests.Load(), "两次写前和一次写后必须实际经过完整 capture/UDS")
	reader, err := budgetclaims.New(client.CoreV1(), config.Binding)
	require.NoError(t, err)
	q, err := reader.Qualification(context.Background(), result.Source)
	require.NoError(t, err)
	require.Equal(t, string(config.Binding.VMUID), q.Identity.VMUID)
	require.Equal(t, config.NodeUID, q.NodeUID)
	creates := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			require.Equal(t, "configmaps", action.GetResource().Resource)
			creates++
		}
		require.NotContains(t, []string{"delete", "patch", "update"}, action.GetVerb())
	}
	require.Equal(t, 1, creates)
}

func TestIssuerCaptureRejectsOriginalNodeReplacementBeforeCreate(t *testing.T) {
	issuer, client, _, bundle, requests := issuerCaptureFixture(t, true)
	_, err := issuer.Publish(context.Background(), bundle)
	require.Error(t, err)
	require.EqualValues(t, 2, requests.Load(), "第一遍已执行完整正分支，第二遍再读原 Node 必须发现替换")
	for _, action := range client.Actions() {
		require.NotEqual(t, "create", action.GetVerb(), "身份漂移必须早于资格 CREATE")
	}
}
