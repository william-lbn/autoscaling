//go:build dsga_native_stock_preparation && linux && dsga_native_initial_lifecycle && dsga_native_memory_origin_wire && dsga_native_memory_commit2 && dsga_native_role_transfer2 && dsga_native_ordinary2

package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 独立ordinary2多停点组件的有界210s，不改旧transfer2 105s和原S判定。
func TestNativeNormalStockUdevPreparationHost(t *testing.T) {
	mode := os.Getenv("DSGA_INITIAL_HOST_NATIVE")
	require.Contains(t, []string{"export-transfer2", "qemu18c-transfer2"}, mode, "explicit isolated component mode required")
	require.Equal(t, "/proof", os.Getenv("DSGA_INITIAL_HOST_OUTPUT"))
	runtimePath := ""
	if mode == "qemu18c-transfer2" {
		runtimePath = "/proof/fixture/runtime.json"
	}
	f, raw, original := normalRoleTransferV2NativeSourceFixture(t, false, runtimePath)
	if mode == "export-transfer2" {
		require.NoError(t, os.WriteFile("/proof/host-seed-runtime.json", raw, 0444))
		return
	}
	assets, e := resourceguard.RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[resourceguard.RuntimeResourceAssetName] = raw
	dir := "/proof/fixture/isofiles"
	if mode == "unreachable-legacy-export" {
		require.NoError(t, os.MkdirAll(dir, 0755))
		for name, data := range assets {
			disk, e := resourceguard.RuntimeISOAssetName(name)
			require.NoError(t, e)
			require.NoError(t, os.WriteFile(filepath.Join(dir, disk), data, 0444))
		}
		req, e := resourceguard.DeriveRuntimeBootRequirements(raw, assets)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile("/proof/fixture/required-kernel-args.txt", []byte(strings.Join(req.RequiredKernelArgs(), " ")+"\n"), 0444))
		saveInitialNative(t, "export", map[string]any{"mode": mode, "resource_sha256": initialBootSHA(raw), "scope": "ENV fixture only; not S"})
		return
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 210*time.Second)
	defer cancel()
	pod, e := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(ctx, f.config.PodName, metav1.GetOptions{})
	require.NoError(t, e)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "neonvm-runner", ContainerID: "containerd://explicit-component", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	args := append([]string(nil), original.Args[1:]...)
	line, e := os.ReadFile("/proof/fixture/required-kernel-args.txt")
	require.NoError(t, e)
	args = append(args, "-machine", "q35", "-accel", "kvm", "-cpu", "host", "-display", "none", "-serial", "stdio", "-no-reboot", "-no-user-config", "-nic", "none", "-runas", "qemu", "-drive", "file=/proof/root.ext4,if=virtio,format=raw,cache=none", "-drive", "file=/proof/runtime.iso,if=virtio,format=raw,readonly=on", "-kernel", "/kernel/vmlinuz", "-initrd", "/proof/initramfs.cpio.gz", "-append", "console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=512M "+strings.TrimSpace(string(line)))
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
		saveInitialNative(t, "normal-memory-scope", map[string]any{"stable_id": "NEON-S-STOCK-UDEV-PREPARATION-001", "actual_QMP": true, "Guest_new_memory_producer_executed": true, "Host_API_is_model": true, "normal_authorized": false, "E": 0})
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
	require.Equal(t, control.Uint64(1<<30), publication.Actual.ActualBytes)
	require.NoError(t, validateInitialLifecyclePublication(birth, f.runner.engine.binding, *publication.InitialBootstrap))
	require.Error(t, f.runner.engine.withRecord(ctx, func(_ *record, _ *lockedState) error { return l.Set(ctx, 256<<20) }), "ordinary business minimum is unchanged")
	saveInitialNative(t, "completed-publication", publication)
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
	require.Contains(t, string(text), "STOCK_PREPARATION_GUEST_EXIT=0")
	require.Contains(t, string(text), "KERNEL_RELEASE="+buildRef.KernelRelease)

	require.NotContains(t, string(text), "--- FAIL:")
	require.Contains(t, string(text), "STOCK_PREPARATION_R1_ALIVE_REFERENCE_SHA256")

	require.Equal(t, 1, bytes.Count(text, []byte("STOCK_PREPARATION_GUEST_EXIT=0")))
	require.Contains(t, string(text), "STOCK_PREPARATION_R1_EXITED_REFERENCE_SHA256")
	require.Contains(t, string(text), "STOCK_PREPARATION_INITIAL_EXIT=0")
	_, _, _, e = ReadPublished(context.Background(), f.core.CoreV1(), f.store.Binding(), &f.runner.engine.binding)
	require.ErrorIs(t, e, ErrStopped)
	saveInitialNative(t, "terminal", map[string]any{"id": "NEON-S-STOCK-UDEV-PREPARATION-001", "Host_Source_API_is_model": true, "real_QEMU_KVM": true, "real_runas": true, "actual_unique_Wait_exit": cmd.ProcessState.ExitCode(), "Guest_fixed_exchange": true, "PG_started": false, "formal_Bootstrap_started": true, "normal_authorized": false, "E": 0})
}

// 独立DEV工厂在首次Initialize前绑定最终原Runtime，不能重建有历史的Runner。
