//go:build linux && dsga_native_initial_lifecycle && dsga_native_normal_memory_origin_host

package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PG17-S-MEMORY-ORIGIN-HOST-001：新Host代码真实KVM/QMP/原8WAL捕获。
// 旧DEV Guest仅生产已有初始阶段；新Guest6/7未执行，不是normal/Source最终资格。
func TestNativeNormalMemoryOriginHostComponent(t *testing.T) {
	mode := os.Getenv("DSGA_INITIAL_HOST_NATIVE")
	require.Contains(t, []string{"export-v1", "qemu369-v1"}, mode, "explicit isolated component mode required")
	require.Equal(t, "/proof", os.Getenv("DSGA_INITIAL_HOST_OUTPUT"))
	f, raw, original := initialBootFixture(t, false)
	assets, e := resourceguard.RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[resourceguard.RuntimeResourceAssetName] = raw
	dir := "/proof/isofiles"
	if mode == "export-v1" {
		require.NoError(t, os.MkdirAll(dir, 0755))
		for name, data := range assets {
			disk, e := resourceguard.RuntimeISOAssetName(name)
			require.NoError(t, e)
			require.NoError(t, os.WriteFile(filepath.Join(dir, disk), data, 0444))
		}
		req, e := resourceguard.DeriveRuntimeBootRequirements(raw, assets)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile("/proof/required-kernel-args.txt", []byte(strings.Join(req.RequiredKernelArgs(), " ")+"\n"), 0444))
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
	for name, want := range map[string]string{"vmlinuz": "3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0", "kernel.config": "11344757b0f979c7fee35e354bd706616c4283deaa67cc30c50747e4bca855be", "kernel-notes.bin": "7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828"} {
		b, e := os.ReadFile(filepath.Join("/kernel", name))
		require.NoError(t, e)
		require.Equal(t, want, initialBootSHA(b))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 105*time.Second)
	defer cancel()
	pod, e := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(ctx, f.config.PodName, metav1.GetOptions{})
	require.NoError(t, e)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "neonvm-runner", ContainerID: "containerd://explicit-component", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	args := append([]string(nil), original.Args[1:]...)
	line, e := os.ReadFile("/proof/required-kernel-args.txt")
	require.NoError(t, e)
	args = append(args, "-machine", "q35", "-accel", "kvm", "-cpu", "host", "-display", "none", "-serial", "stdio", "-no-reboot", "-no-user-config", "-nic", "none", "-runas", "qemu", "-drive", "file=/proof/state.ext4,if=virtio,format=raw,cache=none", "-drive", "file=/proof/runtime.iso,if=virtio,format=raw,readonly=on", "-kernel", "/kernel/vmlinuz", "-initrd", "/proof/initramfs.cpio.gz", "-append", "console=ttyS0,115200 rdinit=/init panic=-1 kernelcore=256M "+strings.TrimSpace(string(line)))
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
		origin, err := captureNormalMemoryHostOriginLocked(ctx, s, l, proof)
		if err != nil {
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
		require.Error(t, origin.recheckLocked(ctx, s, l, proof), "no new Guest ACK means no released opaque source")
		saveInitialNative(t, "normal-memory-host-reference", origin.reference)
		saveInitialNative(t, "normal-memory-geometry", geometry)
		saveInitialNative(t, "normal-memory-scope", map[string]any{"stable_id": "PG17-S-MEMORY-ORIGIN-HOST-001", "actual_QMP": true, "Guest_new_memory_producer_executed": false, "Host_API_is_model": true, "normal_authorized": false, "E": 0})
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
	require.Contains(t, string(text), "INITIAL_FIXED_GUEST_EXIT=0")
	require.Contains(t, string(text), "INITIAL_FIXED_GUEST_RELEASE=6.12.26-william-normal-split-v1-e210f5e3ee94")
	require.Contains(t, string(text), "INITIAL_FIXED_GUEST_NOTES=7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828")
	require.NotContains(t, string(text), "--- FAIL:")
	require.Equal(t, 1, bytes.Count(text, []byte("INITIAL_FIXED_GUEST_EXIT=0")))
	_, _, _, e = ReadPublished(context.Background(), f.core.CoreV1(), f.store.Binding(), &f.runner.engine.binding)
	require.ErrorIs(t, e, ErrStopped)
	saveInitialNative(t, "terminal", map[string]any{"id": "PG17-S-MEMORY-ORIGIN-HOST-001", "Host_Source_API_is_model": true, "real_QEMU_KVM": true, "real_runas": true, "actual_unique_Wait_exit": cmd.ProcessState.ExitCode(), "Guest_fixed_exchange": true, "PG_started": false, "formal_Bootstrap_started": false, "normal_authorized": false, "E": 0})
}
