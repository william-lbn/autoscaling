//go:build dsga_native_trusted_control && dsga_native_stock_preparation && linux && dsga_native_initial_lifecycle && dsga_native_memory_origin_wire && dsga_native_memory_commit2 && dsga_native_role_transfer2 && dsga_native_ordinary2

package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	vmfake "github.com/neondatabase/autoscaling/neonvm/client/clientset/versioned/fake"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	coordination "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 仅在原DEV失败后记录固定类型和既有客户端代码，不输出Error正文/URL/JWT。
// 原错误仍交原require；不重试、不改写事务或消除Unknown。
// 固定硬件只读响应只在原失败后私有归档，不输出原帧，也不重发已may-have-sent的动作。
func trustedControlHostGeometryFailure(err error) {
	var failure *originalMemoryGeometryFailure
	if !errors.As(err, &failure) {
		return
	}
	raw, e := json.Marshal(failure)
	if e == nil && len(raw) <= 6*qmpFrameLimit+8192 {
		_ = os.WriteFile("/proof/original-geometry-failure.private.json", raw, 0600)
		fmt.Printf("TRUSTED_PG17_GEOMETRY_FAILURE stage=%s frame=%d\n", failure.Stage, failure.FrameIndex)
	}
}

func trustedControlHostFailureDiagnostic(err error) {
	if err == nil {
		return
	}
	trustedControlHostGeometryFailure(err)
	out := struct {
		Types           []string `json:"types"`
		ResourceCode    string   `json:"resource_code"`
		MayHaveExecuted bool     `json:"may_have_executed"`
		Canceled        bool     `json:"canceled"`
		Deadline        bool     `json:"deadline"`
		Truncated       bool     `json:"truncated"`
	}{ResourceCode: "NONE", Canceled: errors.Is(err, context.Canceled), Deadline: errors.Is(err, context.DeadlineExceeded)}
	var ce *resourceguard.ClientError
	if errors.As(err, &ce) {
		out.ResourceCode = "OTHER"
		out.MayHaveExecuted = ce.MayHaveExecuted
		switch ce.Code {
		case "response_unconfirmed", "resource_state_unconfirmed", "transport_unconfirmed", "credential_unavailable", "array_original_profile_downgraded", "normal_original_profile_downgraded":
			out.ResourceCode = ce.Code
		}
	}
	pending := []error{err}
	for len(pending) > 0 && len(out.Types) < 16 {
		current := pending[0]
		pending = pending[1:]
		if current == nil {
			continue
		}
		out.Types = append(out.Types, fmt.Sprintf("%T", current))
		switch value := current.(type) {
		case interface{ Unwrap() []error }:
			children := value.Unwrap()
			if len(children) > 16 {
				children = children[:16]
				out.Truncated = true
			}
			pending = append(pending, children...)
		case interface{ Unwrap() error }:
			pending = append(pending, value.Unwrap())
		}
	}
	out.Truncated = out.Truncated || len(pending) > 0
	encoded, encodeErr := json.Marshal(out)
	if encodeErr == nil {
		_ = os.WriteFile("/proof/owner-failure-diagnostic.json", encoded, 0600)
		fmt.Printf("TRUSTED_PG17_HOST_FAILURE_DIAG %s\n", encoded)
	}
}

// PG17真实内存控制单机轨：Host API/节点分配仍是显式模型；Guest、QEMU、PG和Root走生产入口。
// min7GiB/max8GiB的VM/配置与8GiB Host预留在首次Initialize前一致绑定；不代表K8s端到端通过。
func TestNativePG17TrustedControlHost(t *testing.T) {
	runNativePG17TrustedControlHost(t, nil, nil)
}

// NativePG17TrustedHostFixture只在DEV test构建存在，公开原生产客户端/Store/Runner，
// 便于external test包组合真实owner/prepare/resource/vm执行器；它不提供预算或权限回调。
// Core/VMs是显式Host API模型，不能称为Kubernetes真实端到端。
type NativePG17TrustedHostFixture struct {
	Runner          *Runner
	Core            *kubefake.Clientset
	VMs             *vmfake.Clientset
	Store           *journal.Store
	Config          Config
	Actor           journal.Owner
	RuntimeJSON     []byte
	OriginalCommand *exec.Cmd
	original        *runnerFixture
}

func NewNativePG17TrustedHostFixture(t *testing.T, runtimePath string) *NativePG17TrustedHostFixture {
	t.Helper()
	f, raw, command := normalPG17TrustedControlHostFixture(t, runtimePath)
	return &NativePG17TrustedHostFixture{Runner: f.runner, Core: f.core, VMs: f.vms, Store: f.store, Config: f.config, Actor: f.actor, RuntimeJSON: raw, OriginalCommand: command, original: f}
}

// Run在实际initial/memory2/WAL与7GiB发布完整通过后、锁外调用唯一DEV组合器。
// 组合器必须调用原生产执行器取得权限；回调返回不被当成GuestReady/VM完成证据。
func (f *NativePG17TrustedHostFixture) Run(t *testing.T, afterInitial func(context.Context, *NativePG17TrustedHostFixture) error) {
	t.Helper()
	require.NotNil(t, f)
	require.NotNil(t, f.original)
	require.Same(t, f.original.runner, f.Runner)
	require.Same(t, f.original.core, f.Core)
	require.Same(t, f.original.vms, f.VMs)
	require.Same(t, f.original.store, f.Store)
	require.Equal(t, f.original.actor, f.Actor)
	runNativePG17TrustedControlHost(t, f, afterInitial)
}

func runNativePG17TrustedControlHost(t *testing.T, fixture *NativePG17TrustedHostFixture, afterInitial func(context.Context, *NativePG17TrustedHostFixture) error) {
	mode := os.Getenv("DSGA_INITIAL_HOST_NATIVE")
	require.Contains(t, []string{"export-trusted-pg17-7g8g", "qemu18c-trusted-pg17-7g8g"}, mode, "explicit isolated component mode required")
	require.Equal(t, "/proof", os.Getenv("DSGA_INITIAL_HOST_OUTPUT"))
	runtimePath := ""
	if mode == "qemu18c-trusted-pg17-7g8g" {
		runtimePath = "/proof/fixture/runtime.json"
	}
	if fixture == nil {
		fixture = NewNativePG17TrustedHostFixture(t, runtimePath)
	}
	f, raw, original := fixture.original, fixture.RuntimeJSON, fixture.OriginalCommand
	require.NotNil(t, f)
	require.Equal(t, f.config, fixture.Config)
	require.Same(t, f.runner, fixture.Runner)
	if mode == "export-trusted-pg17-7g8g" {
		require.NoError(t, os.WriteFile("/proof/host-seed-runtime.json", raw, 0444))
		return
	}
	assets, e := resourceguard.RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[resourceguard.RuntimeResourceAssetName] = raw
	dir := "/proof/fixture/isofiles"

	for name, want := range assets {
		disk, e := resourceguard.RuntimeISOAssetName(name)
		require.NoError(t, e)
		got, e := os.ReadFile(filepath.Join(dir, disk))
		require.NoError(t, e)
		require.Equal(t, want, got, "actual ISO source and current Host raw differ")
	}
	buildRef, be := resourceguard.LookupPhysicalKernelBuild(resourceguard.NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, be)
	for name, want := range map[string]string{"vmlinuz": buildRef.KernelArtifactSHA256, "kernel.config": buildRef.KernelConfigSHA256, "kernel-notes.bin": buildRef.KernelNotesSHA256} {
		b, e := os.ReadFile(filepath.Join("/kernel", name))
		require.NoError(t, e)
		require.Equal(t, want, initialBootSHA(b))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 480*time.Second)
	defer cancel()
	pod, e := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(ctx, f.config.PodName, metav1.GetOptions{})
	require.NoError(t, e)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "neonvm-runner", ContainerID: "containerd://explicit-component", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	args := append([]string(nil), original.Args[1:]...)
	line, e := os.ReadFile("/proof/fixture/required-kernel-args.txt")
	require.NoError(t, e)
	args = append(args, "-machine", "q35", "-accel", "kvm", "-cpu", "host", "-display", "none", "-serial", "stdio", "-no-reboot", "-no-user-config", "-netdev", "user,id=trustedpg17,hostfwd=tcp:127.0.0.1:33080-:3080", "-device", "virtio-net-pci,netdev=trustedpg17", "-runas", "qemu", "-drive", "file=/proof/root.ext4,if=virtio,format=raw,cache=none", "-drive", "file=/proof/runtime.iso,if=virtio,format=raw,readonly=on", "-kernel", "/kernel/vmlinuz", "-initrd", "/proof/initramfs.cpio.gz", "-append", "console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=512M "+strings.TrimSpace(string(line)))
	cmd := exec.CommandContext(ctx, "/usr/bin/qemu-system-x86_64", args...)
	log, e := os.OpenFile("/proof/console.log", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, e)
	defer log.Close()
	cmd.Stdout, cmd.Stderr = log, log
	done := make(chan error, 1)
	go func() { done <- f.runner.RunQEMU(ctx, cmd); close(done) }()
	joined := false
	t.Cleanup(func() {
		cancel()
		if !joined {
			select {
			case <-done:
			case <-time.After(12 * time.Second):
				t.Error("original RunQEMU worker/Wait not joined")
			}
		}
		copyInitialNativeWAL(t, f.config.StateDir)
	})
	l := f.runner.initial.(*linuxInitialLifecycle)
	var source initialBootSource
	var kvm json.RawMessage
	for {
		l.processGuard.RLock()
		s, p, err := l.sourceAndProcessLocked()
		if err == nil {
			c, ce := l.q.connect(ctx)
			if ce == nil {
				ce = p.checkPeer(c, s)
				if ce == nil {
					kvm, ce = c.execute("query-kvm", nil)
					if ce == nil {
						var value struct {
							Enabled bool `json:"enabled"`
							Present bool `json:"present"`
						}
						require.NoError(t, json.Unmarshal(kvm, &value))
						require.True(t, value.Enabled)
						require.True(t, value.Present)
						require.NotEqualValues(t, 0, p.uid, "real -runas qemu must differ from Root")
						require.NoError(t, p.currentCredentials())
						source = s
					}
				}
				c.close()
			}
			err = ce
		}
		l.processGuard.RUnlock()
		if err == nil {
			break
		}
		select {
		case err := <-done:
			joined = true
			t.Fatalf("original child exited before runas/peer proof: %v", err)
		default:
		}
		require.NoError(t, initialBootPause(ctx))
	}
	saveInitialNative(t, "actual-runas-peer", map[string]any{"process_uid": source.ProcessUID, "process_gid": source.ProcessGID, "QEMU_executable_sha256": source.QEMUSHA256, "SMP_hard_maximum": source.SMPMaximum, "actual_query_kvm": kvm, "Host_API_is_model": true})
	// 原完整initial完成之后、首个普通Tick之前捕获；仍持原进程/engine锁。
	for {
		l.mu.Lock()
		ready := l.ready != nil
		l.mu.Unlock()
		if ready {
			break
		}
		select {
		case err := <-done:
			joined = true
			trustedControlHostGeometryFailure(err)
			t.Fatalf("original QEMU exited before memory origin capture: %v", err)
		default:
		}
		require.NoError(t, initialBootPause(ctx))
	}
	l.processGuard.RLock()
	e = f.runner.engine.withRecord(ctx, func(_ *record, s *lockedState) error {
		proof, err := l.publication(s)
		if err != nil {
			return err
		}
		l.mu.Lock()
		origin := l.memoryOrigin
		l.mu.Unlock()
		require.NotNil(t, origin, "actual6/7/8 must retain original opaque source")
		require.NotNil(t, origin.commit2)
		if err = origin.recheckLocked(ctx, s, l, proof); err != nil {
			return err
		}
		geometry, err := bootprotocol.DecodeNormalMemoryGeometry(origin.reference.Prepared.Challenge.Identity, origin.reference.Prepared.Challenge.OriginalBase, origin.reference.QMP)
		if err != nil {
			return err
		}
		require.Equal(t, source.Identity.MaximumBytes, geometry.MaximumBytes)
		require.Equal(t, source.OriginalBase+geometry.BackendBytes, geometry.MaximumBytes)
		require.Equal(t, source.QEMUSHA256, origin.reference.Process.ExecutableSHA256)
		require.Equal(t, source.SMPMaximum, origin.reference.MaximumVCPUs)
		require.NoError(t, origin.recheckLocked(ctx, s, l, proof), "actual Guest ACK and source must stay original")
		saveInitialNative(t, "normal-memory-host-reference", origin.reference)
		// 旧initial归档helper只认识8阶段；新source两WAL须在原flock下实际复制，
		// 同时保留原FD/inode证明，不能用重新编码JSON冒充原持久字节。
		memoryDir := "/proof/Host-Memory-WAL"
		require.NoError(t, os.MkdirAll(memoryDir, 0700))
		identities := make(map[string][3]uint64)
		for i, name := range []string{normalMemoryHostReferenceFile, normalMemoryHostAckFile, normalMemoryHostCommitFile} {
			raw, err := os.ReadFile(filepath.Join(f.config.StateDir, name))
			require.NoError(t, err)
			identity, err := normalMemoryHostFileIdentity(s, name, raw)
			require.NoError(t, err)
			if i < 2 {
				require.Equal(t, origin.files[i], identity)
			} else {
				require.NotNil(t, origin.commit2)
				require.Equal(t, origin.commit2.identity, identity)
			}
			require.NoError(t, os.WriteFile(filepath.Join(memoryDir, name), raw, 0600))
			identities[name] = identity
		}
		saveInitialNative(t, "normal-memory-original-file-identities", identities)
		saveInitialNative(t, "normal-memory-geometry", geometry)
		saveInitialNative(t, "normal-memory-scope", map[string]any{"stable_id": "PG17-S-TRUSTED-CONTROL-HOST-001", "actual_QMP": true, "Guest_new_memory_producer_executed": true, "Host_API_is_model": true, "normal_authorized": false, "E": 0})
		return nil
	})
	l.processGuard.RUnlock()
	require.NoError(t, e)
	for {
		e = f.runner.Tick(ctx)
		if e == nil {
			break
		}
		select {
		case err := <-done:
			joined = true
			t.Fatalf("original child exited before complete publication: %v; Tick=%v", err, e)
		default:
		}
		require.NoError(t, initialBootPause(ctx))
	}
	birth, _, publication, e := ReadPublished(ctx, f.core.CoreV1(), f.store.Binding(), &f.runner.engine.binding)
	require.NoError(t, e)
	require.EqualValues(t, 2, birth.ProtocolVersion)
	require.EqualValues(t, 2, publication.ProtocolVersion)
	require.NotNil(t, publication.InitialBootstrap)
	require.NotNil(t, publication.Actual)
	require.Equal(t, control.Uint64(7<<30), publication.Actual.ActualBytes)
	require.NoError(t, validateInitialLifecyclePublication(birth, f.runner.engine.binding, *publication.InitialBootstrap))
	require.Error(t, f.runner.engine.withRecord(ctx, func(_ *record, _ *lockedState) error { return l.Set(ctx, 6<<30) }), "explicit 7GiB first-run minimum must remain enforced")
	saveInitialNative(t, "completed-publication", publication)
	if afterInitial != nil {
		// 此处已释放processGuard及原engine flock；不在资源事务内等待外部网络。
		compositionErr := afterInitial(ctx, fixture)
		if compositionErr != nil {
			trustedControlHostFailureDiagnostic(compositionErr)
		}
		require.NoError(t, compositionErr, "actual production owner/control composition failed")
	}
	select {
	case err := <-done:
		joined = true
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("original QEMU/Guest did not reach actual Wait", ctx.Err())
	}
	require.NotNil(t, cmd.ProcessState)
	require.True(t, cmd.ProcessState.Success())
	require.NoError(t, log.Sync())
	text, e := os.ReadFile("/proof/console.log")
	require.NoError(t, e)
	require.Contains(t, string(text), "TRUSTED_PG17_GUEST_EXIT=0")
	require.Contains(t, string(text), "KERNEL_RELEASE="+buildRef.KernelRelease)

	require.NotContains(t, string(text), "--- FAIL:")

	require.Equal(t, 1, bytes.Count(text, []byte("TRUSTED_PG17_GUEST_EXIT=0")))
	_, _, _, e = ReadPublished(context.Background(), f.core.CoreV1(), f.store.Binding(), &f.runner.engine.binding)
	require.ErrorIs(t, e, ErrStopped)
	saveInitialNative(t, "terminal", map[string]any{"id": "PG17-S-TRUSTED-CONTROL-HOST-001", "Host_Source_API_is_model": true, "real_QEMU_KVM": true, "real_runas": true, "actual_unique_Wait_exit": cmd.ProcessState.ExitCode(), "Guest_trusted_control_terminal": true, "PG_started": true, "formal_Bootstrap_started": true, "normal_authorized": false, "E": 0})
}

// 独立DEV工厂先固定min7/max8GiB模型，再调用原生产Initialize；数据不是实际Kubernetes容量凭据。
func normalPG17TrustedControlHostFixture(t *testing.T, runtimePath string) (*runnerFixture, []byte, *exec.Cmd) {
	t.Helper()
	f := runnerBirthFixture(t, false)
	ctx := context.Background()
	// 只能修改尚未Initialize的全新夹具，原4GiB轨及其持久历史不受影响。
	snap, e := f.store.Load(ctx)
	require.NoError(t, e)
	vm := snap.VirtualMachine()
	vm.Spec.Guest.MemorySlots.Min, vm.Spec.Guest.MemorySlots.Max, vm.Spec.Guest.MemorySlots.Use = 7, 8, 7
	vm.Status.MemorySize = resource.NewQuantity(7<<30, resource.BinarySI)
	_, e = f.vms.NeonvmV1().VirtualMachines(f.store.Binding().Namespace).Update(ctx, vm, metav1.UpdateOptions{})
	require.NoError(t, e)
	f.config.MinimumBytes, f.config.MaximumBytes = 7<<30, 8<<30
	node, e := f.core.CoreV1().Nodes().Get(ctx, f.config.NodeName, metav1.GetOptions{})
	require.NoError(t, e)
	var h hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &h))
	require.NotNil(t, h.Placements)
	require.Len(t, *h.Placements, 1)
	(*h.Placements)[0].MemoryBytes = 8 << 30
	require.NoError(t, h.Validate())
	modelRaw, e := json.Marshal(h)
	require.NoError(t, e)
	node.Annotations[hostguard.LedgerAnnotation] = string(modelRaw)
	_, e = f.core.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, e)
	yes := true
	holder := h.LeaseFloor.HolderID
	transitions, seconds := int32(0), int32(60)
	now := metav1.NewMicroTime(time.Now())
	hl := &coordination.Lease{ObjectMeta: metav1.ObjectMeta{Name: h.Binding.LeaseName, Namespace: h.Binding.Namespace, UID: h.Binding.LeaseUID, ResourceVersion: "1", Annotations: map[string]string{hostguard.LeaseAnnotation: hostguard.LeaseMarker(h.Binding)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: h.Binding.NodeName, UID: h.Binding.NodeUID, Controller: &yes}}}, Spec: coordination.LeaseSpec{HolderIdentity: &holder, LeaseTransitions: &transitions, LeaseDurationSeconds: &seconds, AcquireTime: &now, RenewTime: &now}}
	_, e = f.core.CoordinationV1().Leases(h.Binding.Namespace).Create(ctx, hl, metav1.CreateOptions{})
	require.NoError(t, e)
	raw, secret := initialBootFullRuntime(t, f)
	raw = normalMemoryIntegrationRuntime(t, raw)
	if runtimePath != "" {
		raw, e = os.ReadFile(runtimePath)
		require.NoError(t, e)
	}
	var doc resourceguard.RuntimeDocument
	require.NoError(t, json.Unmarshal(raw, &doc))
	// 导出seed自身也须与显式512M原base一致；真实run从最终库存读取，不改其原字节。
	if runtimePath == "" {
		var envelope struct {
			ProtocolVersion uint32 `json:"protocol_version"`
			Scope           string `json:"scope"`
			Templates       []struct {
				Asset  string          `json:"asset"`
				Policy json.RawMessage `json:"policy"`
			} `json:"templates"`
		}
		require.NoError(t, json.Unmarshal(doc.PolicyTemplatesJSON, &envelope))
		for i := range envelope.Templates {
			if envelope.Templates[i].Asset == "dsga-prebootstrap-system.json" {
				var v map[string]any
				require.NoError(t, json.Unmarshal(envelope.Templates[i].Policy, &v))
				v["boot_memory_upper_bytes"] = "536870912"
				envelope.Templates[i].Policy, e = json.Marshal(v)
				require.NoError(t, e)
			}
		}
		doc.PolicyTemplatesJSON, e = json.Marshal(envelope)
		require.NoError(t, e)
		doc.PolicyTemplatesSHA256 = initialBootSHA(doc.PolicyTemplatesJSON)
		raw, e = json.Marshal(doc)
		require.NoError(t, e)
	}
	secret.Data[resourceguard.ResourceSecretKey] = append([]byte(nil), doc.SourceJSON...)
	secret.Data[resourceguard.PolicyTemplatesSecretKey] = append([]byte(nil), doc.PolicyTemplatesJSON...)
	_, e = f.core.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, metav1.CreateOptions{})
	require.NoError(t, e)
	shortState, e := os.MkdirTemp("/tmp", "initial-source-")
	require.NoError(t, e)
	t.Cleanup(func() { _ = os.RemoveAll(shortState) })
	f.config.StateDir = shortState
	f.config.InitialBootstrap = &InitialBootstrapConfig{RuntimeJSON: append([]byte(nil), raw...), OriginalBaseBytes: 512 << 20}
	f.runner, e = Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
	require.NoError(t, e)
	path := filepath.Join(t.TempDir(), "qemu-system-x86_64")
	require.NoError(t, os.WriteFile(path, []byte("NOT_A_QEMU_TEST_ONLY_SOURCE_FIXTURE\n"), 0700))
	args := []string{path, "-m", "size=512M,slots=1,maxmem=8G", "-object", "memory-backend-ram,id=vmem0,size=7680M", "-device", "virtio-mem-pci,id=vm0,memdev=vmem0,node=0,block-size=8M,requested-size=0"}
	args = append(args, "-smp", "cpus=2,maxcpus=2,sockets=1,cores=2,threads=1", "-monitor", "none")
	args = append(args, f.runner.QMPArguments()...)
	args = append(args, f.runner.BootChannelArguments()...)
	cmd := &exec.Cmd{Path: path, Args: args}
	_, e = f.runner.validateProcessCommand(cmd)
	require.NoError(t, e)
	return f, raw, cmd
}

// 直接核首次生产Initialize实际消费min7/max8GiB VM/Node/config，不以改写旧Runner状态伪造几何。
func TestPG17TrustedControlHostFixtureBindsSevenToEightGiBBeforeInitialize(t *testing.T) {
	f, raw, cmd := normalPG17TrustedControlHostFixture(t, "")
	require.Equal(t, control.Uint64(8<<30), f.runner.birth.Identity.MaximumBytes)
	require.Equal(t, control.Uint64(7<<30), f.runner.birth.Identity.MinimumBytes)
	require.Equal(t, control.Uint64(7<<30), f.config.MinimumBytes)
	require.Equal(t, control.Uint64(8<<30), f.config.MaximumBytes)
	require.Contains(t, cmd.Args, "size=512M,slots=1,maxmem=8G")
	require.Contains(t, cmd.Args, "memory-backend-ram,id=vmem0,size=7680M")
	require.Contains(t, cmd.Args, "cpus=2,maxcpus=2,sockets=1,cores=2,threads=1")
	require.NotEmpty(t, raw)
}
