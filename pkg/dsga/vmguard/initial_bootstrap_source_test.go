//go:build linux

package vmguard

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootstrap"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	coordination "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// 合法policy值取自已冻结跨包producer夹具，全部API、预算/文件是明确模型；
// 正例验证真实生产reader/Secret/原launch/WAL代码，绝非真实Kube/Guest/QEMU资格。
func initialBootFullRuntime(t *testing.T, f *runnerFixture) ([]byte, *corev1.Secret) {
	t.Helper()
	snap, e := f.store.Load(context.Background())
	require.NoError(t, e)
	vm := snap.VirtualMachine()
	secretUID := vm.Spec.DSGABootstrap.ConfigSecretUID
	controller, immutable := true, true
	secret := new(corev1.Secret)
	secret.Name, secret.Namespace, secret.ResourceVersion = "vm-config", "testing", "1"
	secret.UID = types.UID(secretUID)
	secret.Immutable = &immutable
	secret.OwnerReferences = []metav1.OwnerReference{{APIVersion: vmv1.SchemeGroupVersion.String(), Kind: "VirtualMachine", Name: vm.Name, UID: vm.UID, Controller: &controller}}
	key := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	source := resourceguard.SourceConfig{ProtocolVersion: 1, Config: resourceguard.Config{VMUID: string(vm.UID), ComputeInstanceID: vm.Spec.DSGABootstrap.ComputeInstanceID, StateDir: resourceguard.FixedStateDir, SystemCgroup: true}, IssuerEpoch: 1, BudgetCommitmentID: "template-bootstrap-budget",
		Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 128 << 20, SystemMemoryMaxBytes: 64 << 20, PidsMaxTasks: 128, AddressSpaceMaxBytes: 4 << 30, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 0}, {PageSizeKB: 1048576, Pages: 0}}}, ControlPublicKey: key}
	resource, _ := json.Marshal(source)
	config := map[string]any{"spec": map[string]any{"dynamic_shared_buffers": map[string]any{"protocol_version": 2, "vm_uid": string(vm.UID), "compute_instance_id": vm.Spec.DSGABootstrap.ComputeInstanceID, "minimum_guest_memory_bytes": 1 << 30, "bootstrap_buffer_bytes": 2 << 20}}, "compute_ctl_config": map[string]any{"jwks": map[string]any{"keys": []any{map[string]any{"use": "sig", "key_ops": []string{"verify"}, "alg": "EdDSA", "kty": "OKP", "crv": "Ed25519", "x": key}}}}}
	configRaw, _ := json.Marshal(config)
	ref, err := resourceguard.LookupPhysicalKernelBuild(resourceguard.NormalSplitV1PhysicalKernelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"dsga-physical-kernel.json", "dsga-kernel-bootstrap.json", "dsga-prebootstrap-system.json", "dsga-normal-split.json", "dsga-native-birth.json"}
	policies := []map[string]any{
		{"protocol_version": 1, "profile_id": ref.ProfileID, "kernel_release": ref.KernelRelease, "kernel_notes_sha256": ref.KernelNotesSHA256, "kernel_artifact_sha256": ref.KernelArtifactSHA256, "kernel_config_sha256": ref.KernelConfigSHA256},
		{"protocol_version": 1, "profile_id": ref.ProfileID, "controls": resourceguard.NormalV1KernelAllocatorControls{PIDMax: 512, MinFreeKBytes: 2048, WatermarkScaleFactor: 10, PerCPUPagelistHighFraction: 16}, "modules_disabled": 1, "numa_balancing": 0, "demotion_enabled": false},
		{"protocol_version": 2, "scope": "pre-bootstrap-system-placement-only", "physical_profile_id": ref.ProfileID, "filesystem_profile_sha256": strings.Repeat("ab", 32), "boot_memory_upper_bytes": "268435456"},
		{"protocol_version": 1, "scope": "normal-split-topology-preparation-only", "physical_profile_id": ref.ProfileID, "private_memory_max_bytes": "67108864", "initial_arrays_memory_max_bytes": "67108864", "private_pids_max_tasks": "64"},
		{"protocol_version": 1, "scope": "neon-native-birth-policy-v1", "build_manifest_sha256": strings.Repeat("cd", 32), "pg_major": 17, "max_buffers": 128, "startup_buffers": 32, "page_bytes": 4096, "element_bytes": [4]uint64{64, 8192, 16, 20}},
	}
	var templates []map[string]any
	for i, p := range policies {
		templates = append(templates, map[string]any{"asset": names[i], "policy": p})
	}
	blob, _ := json.Marshal(map[string]any{"protocol_version": 1, "scope": "neon-dsga-policy-templates-v1", "templates": templates})
	secret.Data = map[string][]byte{"config.json": configRaw, resourceguard.ResourceSecretKey: append([]byte(" \n"), resource...), resourceguard.PolicyTemplatesSecretKey: append([]byte(" \n"), blob...)}
	d, e := bootstrap.Runtime(vm, secret)
	require.NoError(t, e)
	raw, e := json.Marshal(d)
	require.NoError(t, e)
	return raw, secret
}

func initialBootSourceFixture(t *testing.T) (*runnerFixture, []byte, *exec.Cmd) {
	return initialBootFixture(t, true)
}
func initialBootFixture(t *testing.T, withLaunch bool) (*runnerFixture, []byte, *exec.Cmd) {
	t.Helper()
	f := runnerBirthFixture(t, false)
	ctx := context.Background()
	node, e := f.core.CoreV1().Nodes().Get(ctx, f.config.NodeName, metav1.GetOptions{})
	require.NoError(t, e)
	var h hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &h))
	yes := true
	holder := "model-host-owner"
	transitions, seconds := int32(0), int32(60)
	now := metav1.NewMicroTime(time.Now())
	hl := &coordination.Lease{ObjectMeta: metav1.ObjectMeta{Name: h.Binding.LeaseName, Namespace: h.Binding.Namespace, UID: h.Binding.LeaseUID, ResourceVersion: "1", Annotations: map[string]string{hostguard.LeaseAnnotation: hostguard.LeaseMarker(h.Binding)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: h.Binding.NodeName, UID: h.Binding.NodeUID, Controller: &yes}}}, Spec: coordination.LeaseSpec{HolderIdentity: &holder, LeaseTransitions: &transitions, LeaseDurationSeconds: &seconds, AcquireTime: &now, RenewTime: &now}}
	_, e = f.core.CoordinationV1().Leases(h.Binding.Namespace).Create(ctx, hl, metav1.CreateOptions{})
	require.NoError(t, e)
	raw, secret := initialBootFullRuntime(t, f)
	_, e = f.core.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, metav1.CreateOptions{})
	require.NoError(t, e)
	shortState, e := os.MkdirTemp("/tmp", "initial-source-")
	require.NoError(t, e)
	t.Cleanup(func() { _ = os.RemoveAll(shortState) })
	f.config.StateDir = shortState
	f.config.InitialBootstrap = &InitialBootstrapConfig{RuntimeJSON: append([]byte(nil), raw...), OriginalBaseBytes: 256 << 20}
	f.runner, e = Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
	require.NoError(t, e)
	path := filepath.Join(t.TempDir(), "qemu-system-x86_64")
	require.NoError(t, os.WriteFile(path, []byte("NOT_A_QEMU_TEST_ONLY_SOURCE_FIXTURE\n"), 0700))
	args := []string{path, "-m", "size=256M,slots=1,maxmem=4G", "-object", "memory-backend-ram,id=vmem0,size=3840M", "-device", "virtio-mem-pci,id=vm0,memdev=vmem0,node=0,block-size=8M,requested-size=0"}
	args = append(args, "-smp", "cpus=2,maxcpus=2,sockets=1,cores=2,threads=1", "-monitor", "none")
	args = append(args, f.runner.QMPArguments()...)
	args = append(args, f.runner.BootChannelArguments()...)
	cmd := &exec.Cmd{Path: path, Args: args}
	digest, e := f.runner.validateProcessCommand(cmd)
	require.NoError(t, e)
	launch := processLaunch{Identity: f.runner.birth.Identity, BirthClaim: f.runner.engine.binding, Nonce: strings.Repeat("12", 16), CommandSHA256: digest, ContainerID: "containerd://model-only"}
	if withLaunch {
		e = f.runner.engine.withRecord(ctx, func(_ *record, s *lockedState) error {
			return s.writeOnce("process-launch.json", initialBootCanonical(launch))
		})
		require.NoError(t, e)
	}
	return f, raw, cmd
}
func initialBootModelSource(t *testing.T) initialBootSource {
	t.Helper()
	f, raw, cmd := initialBootSourceFixture(t)
	_, e := f.runner.freezeInitialBootstrap(context.Background(), raw, cmd, 256<<20)
	require.NoError(t, e)
	s, e := lockState(context.Background(), f.config.StateDir)
	require.NoError(t, e)
	defer s.close()
	entries, e := initialBootRead(s)
	require.NoError(t, e)
	require.Len(t, entries, 1)
	return entries[0].Source
}
func TestInitialBootstrapV2ActualReadersBindOriginalSources(t *testing.T) {
	// PG17-U-INITIAL-H04：真实typed fakeAPI source路径+真实私有文件，非实际Kube。
	f, raw, cmd := initialBootSourceFixture(t)
	ref, e := f.runner.freezeInitialBootstrap(context.Background(), raw, cmd, 256<<20)
	require.NoError(t, e)
	again, e := f.runner.freezeInitialBootstrap(context.Background(), raw, cmd, 256<<20)
	require.NoError(t, e)
	require.Equal(t, ref, again)

	// 跨轨合同：Guest严格核同一原Runtime派生的artifact，而非KernelRef对象哈希。
	logical, e := resourceguard.RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	logical[resourceguard.RuntimeResourceAssetName] = raw
	plan, e := resourceguard.DeriveRuntimeBootRequirements(raw, logical)
	require.NoError(t, e)
	state, e := lockState(context.Background(), f.config.StateDir)
	require.NoError(t, e)
	entries, e := initialBootRead(state)
	require.NoError(t, e)
	require.Equal(t, plan.KernelBuildReference().KernelArtifactSHA256, entries[0].Source.KernelBuildSHA256)
	require.NotEqual(t, initialBootSHA(initialBootCanonical(plan.KernelBuildReference())), entries[0].Source.KernelBuildSHA256)
	baseRef, e := initialBootAppend(state, ref, json.RawMessage(`{"model_only":"cross-track source/challenge binding"}`))
	require.NoError(t, e)
	challenge, e := initialBootChallenge(entries[0].Source, baseRef)
	require.NoError(t, e)
	require.Equal(t, plan.KernelBuildReference().KernelArtifactSHA256, challenge.KernelBuildSHA256)
	state.close()
	require.Equal(t, "source_frozen", ref.Stage)
	require.Len(t, ref.Nonce, 64)
	for _, a := range f.core.Actions() {
		require.NotEqual(t, "patch", a.GetVerb(), "Source不修改Node/Pod/容量")
	}
	// 冻结字节不因读取改变，原v1 birth仍原协议。
	require.EqualValues(t, 2, f.runner.birth.ProtocolVersion)
	require.NotNil(t, f.runner.birth.InitialMode)
}
func TestInitialBootstrapV2RejectsSourceReplacementAndMissingClosure(t *testing.T) {
	// PG17-U-INITIAL-H05：没有合法Source不创建initial阶段，旧日志/预算不重置。
	for _, name := range []string{"missing_host_lease", "replacement_host_lease", "host_row_short", "owner_epoch", "secret_uid", "mutable_secret", "raw_template_drift", "raw_resource_drift", "runtime_partial", "runtime_noncanonical", "launch_missing", "launch_nonce", "argv_mismatch", "base_upper", "qemu_writable"} {
		t.Run(name, func(t *testing.T) {
			f, raw, cmd := initialBootSourceFixture(t)
			ctx := context.Background()
			node, e := f.core.CoreV1().Nodes().Get(ctx, f.config.NodeName, metav1.GetOptions{})
			require.NoError(t, e)
			var h hostguard.Model
			require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &h))
			base := uint64(256 << 20)
			switch name {
			case "missing_host_lease":
				require.NoError(t, f.core.CoordinationV1().Leases(h.Binding.Namespace).Delete(ctx, h.Binding.LeaseName, metav1.DeleteOptions{}))
			case "replacement_host_lease":
				l, e := f.core.CoordinationV1().Leases(h.Binding.Namespace).Get(ctx, h.Binding.LeaseName, metav1.GetOptions{})
				require.NoError(t, e)
				l.UID = "replacement"
				require.NoError(t, f.core.Tracker().Update(coordination.SchemeGroupVersion.WithResource("leases"), l, l.Namespace))
			case "host_row_short":
				(*h.Placements)[0].MemoryBytes--
				node.Annotations[hostguard.LedgerAnnotation] = string(initialBootCanonical(h))
				require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), node, ""))
			case "owner_epoch":
				l, e := f.core.CoordinationV1().Leases(f.store.Binding().Namespace).Get(ctx, journal.LeaseName(f.store.Binding()), metav1.GetOptions{})
				require.NoError(t, e)
				*l.Spec.LeaseTransitions++
				_, e = f.core.CoordinationV1().Leases(l.Namespace).Update(ctx, l, metav1.UpdateOptions{})
				require.NoError(t, e)
			case "secret_uid", "mutable_secret", "raw_template_drift", "raw_resource_drift":
				s, e := f.core.CoreV1().Secrets(f.store.Binding().Namespace).Get(ctx, "vm-config", metav1.GetOptions{})
				require.NoError(t, e)
				switch name {
				case "secret_uid":
					s.UID = "recreated"
				case "mutable_secret":
					*s.Immutable = false
				case "raw_template_drift":
					s.Data[resourceguard.PolicyTemplatesSecretKey] = append([]byte(" "), s.Data[resourceguard.PolicyTemplatesSecretKey]...)
				case "raw_resource_drift":
					s.Data[resourceguard.ResourceSecretKey] = append([]byte(" "), s.Data[resourceguard.ResourceSecretKey]...)
				}
				require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("secrets"), s, s.Namespace))
			case "runtime_partial":
				raw = raw[:len(raw)/2]
			case "runtime_noncanonical":
				raw = append([]byte(" "), raw...)
			case "launch_missing":
				require.NoError(t, os.Remove(filepath.Join(f.config.StateDir, "process-launch.json")))
			case "launch_nonce":
				path := filepath.Join(f.config.StateDir, "process-launch.json")
				b, e := os.ReadFile(path)
				require.NoError(t, e)
				var l processLaunch
				require.NoError(t, json.Unmarshal(b, &l))
				l.Nonce = "not-original-nonce"
				require.NoError(t, os.WriteFile(path, initialBootCanonical(l), 0600))
			case "argv_mismatch":
				cmd.Args = append(cmd.Args, "-no-reboot")
			case "base_upper":
				base = 512 << 20
			case "qemu_writable":
				require.NoError(t, os.Chmod(cmd.Path, 0777))
			}
			_, e = f.runner.freezeInitialBootstrap(ctx, raw, cmd, control.Uint64(base))
			require.Error(t, e)
			_, e = os.Stat(filepath.Join(f.config.StateDir, initialBootFile(0)))
			require.True(t, os.IsNotExist(e), "Source拒绝不能创建初始日志")
		})
	}
}
