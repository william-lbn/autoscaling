//go:build dsga_native_qmp_geometry

package vmguard

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// 必须在显式隔离KVM镜像运行；缺二进制/KVM直接FAIL，不Skip。QEMU暂停CPU，
// 本组件只认证真实QMP读取，未启动Guest/PG，不声称物理页已回收或normal可降。
func TestOriginalMemoryGeometryActualKVM(t *testing.T) {
	cases := []struct {
		name, base, backend, requested string
		extra                          bool
		expectedBase                   control.Uint64
		change                         string
		success                        bool
	}{
		{"early-base", "256M", "768M", "0", false, 256 << 20, "", true},
		{"requested-is-not-actual-or-maximum", "256M", "768M", "16M", false, 256 << 20, "", true},
		{"legacy-base", "512M", "512M", "0", false, 512 << 20, "", true},
		{"wrong-original-base", "256M", "768M", "0", false, 248 << 20, "", false},
		{"wrong-original-maximum", "256M", "768M", "0", false, 256 << 20, "maximum", false},
		{"underprovisioned-backend", "256M", "512M", "0", false, 256 << 20, "", false},
		{"extra-backend", "256M", "768M", "0", true, 256 << 20, "", false},
		{"wrong-original-uuid", "256M", "768M", "0", false, 256 << 20, "uuid", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			original := geometryIdentityFixture()
			uuid, _ := original.QEMUUUID()
			path := filepath.Join(t.TempDir(), "qmp.sock")
			args := []string{"-machine", "q35", "-accel", "kvm", "-cpu", "host", "-smp", "1", "-S", "-m", "size=" + tc.base + ",slots=1,maxmem=1G", "-object", "memory-backend-ram,id=vmem0,size=" + tc.backend, "-device", "virtio-mem-pci,id=vm0,memdev=vmem0,block-size=8M,requested-size=" + tc.requested, "-uuid", uuid, "-display", "none", "-serial", "none", "-monitor", "none", "-nic", "none", "-qmp", "unix:" + path + ",server=on,wait=off", "-no-user-config"}
			if tc.extra {
				args = append(args, "-object", "memory-backend-ram,id=extra,size=8M")
			}
			cmd := exec.Command("/usr/bin/qemu-system-x86_64", args...)
			logPath := filepath.Join(t.TempDir(), "qemu.log")
			output, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, output.Close()) })
			readOutput := func() string { raw, _ := os.ReadFile(logPath); return string(raw) }
			cmd.Stdout = output
			cmd.Stderr = output
			require.NoError(t, cmd.Start())
			t.Cleanup(func() {
				_ = cmd.Process.Signal(syscall.SIGTERM)
				_ = cmd.Wait()
				t.Logf("QEMU termination=%v output=%s", cmd.ProcessState, readOutput())
			})
			q := nativeQMP{id: original, path: path, timeout: time.Second}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var connected *qmpConnection
			for ctx.Err() == nil {
				connected, err = q.connect(ctx)
				if err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			require.NoError(t, err, readOutput())
			raw, err := connected.execute("query-kvm", nil)
			require.NoError(t, err)
			connected.close()
			var kvm struct {
				Enabled bool `json:"enabled"`
				Present bool `json:"present"`
			}
			require.NoError(t, json.Unmarshal(raw, &kvm))
			require.True(t, kvm.Enabled)
			require.True(t, kvm.Present)
			if tc.change == "maximum" {
				q.id.MaximumBytes += VirtioBlockBytes
			}
			if tc.change == "uuid" {
				q.id.RunnerBootID = "00000000000000000000000000000001"
			}
			got, err := q.readOriginalMemoryGeometry(ctx, tc.expectedBase)
			if tc.success {
				require.NoError(t, err)
				require.Equal(t, control.Uint64(1<<30), got.MaximumBytes)
				if tc.requested == "16M" {
					require.Equal(t, control.Uint64(272<<20), got.RequestedBytes)
					require.Equal(t, control.Uint64(256<<20), got.ActualBytes)
				}
				t.Logf("QMP geometry=%+v", got)
			} else {
				require.Error(t, err)
				t.Logf("expected rejection=%v", err)
			}
			if tc.name == "early-base" {
				_, err = q.Read(ctx)
				require.Error(t, err, "old production reader must still reject smaller earlyboot geometry")
			}
			t.Logf("actual_kvm=%s argv=%q", raw, args)
		})
	}
}
