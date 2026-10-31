//go:build linux && dsga_native_initial_preboot_guest

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bootprotocol "github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

// TEST_ONLY：固定实际Guest来源，Host Node/Source摘要在本夹具中仍是模型，非全链许可。
func TestExportInitialPrebootGuestFixture(t *testing.T) {
	if os.Getenv("DSGA_INITIAL_GUEST_EXPORT") != "/out/isofiles" {
		t.Fatal("explicit fixture export required")
	}
	c, raw, assets, _ := initialPrebootFixture(t)
	for logical, data := range assets {
		physical, err := RuntimeISOAssetName(logical)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("/out/isofiles", 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join("/out/isofiles", physical), data, 0444); err != nil {
			t.Fatal(err)
		}
	}
	frame, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("/out/challenge.json", frame, 0444); err != nil {
		t.Fatal(err)
	}
	req, err := DeriveRuntimeBootRequirements(raw, assets)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("/out/required-kernel-args.txt", []byte(strings.Join(req.RequiredKernelArgs(), " ")+"\n"), 0444); err != nil {
		t.Fatal(err)
	}
}
func initialGuestNativeSave(t *testing.T, name string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("/evidence", name+".json"), append(raw, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
func initialGuestNativeState(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	names, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range names {
		if e.Name() == "lock" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = physicalDigest(raw)
	}
	return out
}
func initialGuestNativeRecover(t *testing.T, g *Guard, c bootprotocol.InitialPrebootChallenge, mode string) {
	t.Helper()
	before := initialGuestNativeState(t, g.config.StateDir)
	_, err := g.ObserveInitialPreboot(context.Background(), c)
	if !errors.Is(err, ErrUnknown) {
		t.Fatal("new Root inherited old process durability", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	oldFrame, err := os.ReadFile("/evidence/host-original-preboot.frame")
	if err != nil {
		t.Fatal(err)
	}
	var hostOriginal bootprotocol.InitialPrebootReceipt
	if err := json.Unmarshal(oldFrame, &hostOriginal); err != nil {
		t.Fatal(err)
	}
	if _, err := g.InspectInitialPrebootMovable(ctx, hostOriginal); !errors.Is(err, ErrUnknown) {
		t.Fatal("new Root inspected old receipt before explicit Recover", err)
	}
	receipt, err := g.RecoverInitialPreboot(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if receipt != hostOriginal {
		t.Fatal("explicit recovery replaced original Host receipt")
	}
	if _, err := g.ObserveInitialPreboot(ctx, c); err != nil {
		t.Fatal(err)
	}
	after := initialGuestNativeState(t, g.config.StateDir)
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if !bytes.Equal(a, b) {
		t.Fatal("new Root recovery changed original JSON")
	}
	var movable *InitialPrebootMovableObservation
	if mode == "after-grow" {
		m, err := g.InspectInitialPrebootMovable(ctx, hostOriginal)
		if err != nil {
			t.Fatal(err)
		}
		movable = &m
		for _, mode := range []string{"issuer", "boot", "physical"} {
			wrong := hostOriginal
			switch mode {
			case "issuer":
				wrong.RootInstanceNonce = strings.Repeat("ef", 32)
			case "boot":
				wrong.GuestBootID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			case "physical":
				wrong.CurrentPhysicalSHA256 = strings.Repeat("ef", 32)
			}
			if _, err := g.InspectInitialPrebootMovable(ctx, wrong); err == nil {
				t.Fatal("changed historical receipt accepted", mode)
			}
		}
	}
	initialGuestNativeSave(t, "initial-guest-recovery-"+mode, map[string]any{"ordinary_observe_unknown": true, "original_host_receipt_preserved": true, "old_receipt_before_recover_rejected": true, "explicit_recovery": true, "original_wal_bytes_unchanged": true, "receipt": receipt, "movable": movable, "HostSourceIsFixture": true, "normalAuthorized": false})
}
func initialGuestNativeSpawnRecovery(t *testing.T, mode string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestNativeInitialPrebootGuestComponent$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "DSGA_INITIAL_GUEST_RECOVER="+mode)
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("new Root recovery %s failed: %v %s", mode, err, raw)
	}
}

func TestNativeInitialPrebootGuestComponent(t *testing.T) {
	if os.Getenv("DSGA_INITIAL_GUEST_NATIVE") != "isolated369-initial-preboot-v1" {
		t.Fatal("isolated explicit Guest required")
	}
	release, err := kernelText("/proc/sys/kernel/osrelease")
	if err != nil || release != "6.12.26-william-normal-split-v1-e210f5e3ee94" {
		t.Fatal("accurate369 required", err)
	}
	raw, err := os.ReadFile("/fixture/challenge.json")
	if err != nil {
		t.Fatal(err)
	}
	var c bootprotocol.InitialPrebootChallenge
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	source, _, err := runtimeSource()
	if err != nil {
		t.Fatal(err)
	}
	g, err := Open(source.Config)
	if err != nil {
		t.Fatal(err)
	}
	if mode := os.Getenv("DSGA_INITIAL_GUEST_RECOVER"); mode != "" {
		initialGuestNativeRecover(t, g, c, mode)
		return
	}
	t.Run("changed-source-before-writes", func(t *testing.T) {
		wrong := c
		wrong.ConfigSecretUID = "wrong-initial-secret"
		if _, err := g.PrepareInitialPreboot(context.Background(), wrong); err == nil {
			t.Fatal("wrong source accepted")
		}
		names, err := os.ReadDir(source.Config.StateDir)
		if err != nil || len(names) != 0 {
			t.Fatal("rejected source changed original state", err, names)
		}
		disabled, err := readNumber("/proc/sys/kernel/modules_disabled", false)
		if err != nil || disabled != 0 {
			t.Fatal("rejected source changed kernel", err, disabled)
		}
		initialGuestNativeSave(t, "initial-guest-rejected-source", map[string]any{"wrong_uid_rejected_before_wal_and_kernel_write": true, "HostSourceIsFixture": true, "normalAuthorized": false})
	})
	t.Run("real-prepare-grow-and-recovery", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		prepared, err := g.PrepareInitialPreboot(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		before := initialGuestNativeState(t, source.Config.StateDir)
		if len(before) != 8 {
			t.Fatal("original full stage file count", len(before))
		}
		observed, err := g.ObserveInitialPreboot(ctx, c)
		if err != nil || observed != prepared {
			t.Fatal("ordinary observe drifted", err)
		}
		frame, err := json.Marshal(prepared)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile("/evidence/host-original-preboot.frame", frame, 0600); err != nil {
			t.Fatal(err)
		}
		initialGuestNativeSpawnRecovery(t, "before-grow")
		initialGuestNativeSave(t, "initial-guest-prepared", map[string]any{"receipt": prepared, "stage_files": before, "HostSourceIsFixture": true, "normalAuthorized": false})
		if _, err := g.InspectInitialPrebootMovable(ctx, prepared); err == nil {
			t.Fatal("planned grow was treated as observed")
		}
		fmt.Println("INITIAL_PREBOOT_READY_FOR_HOST_QMP_GROW=1")
		deadline := time.Now().Add(20 * time.Second)
		var movable InitialPrebootMovableObservation
		for {
			movable, err = g.InspectInitialPrebootMovable(ctx, prepared)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("actual Movable never reached original increment: %v", err)
			}
			time.Sleep(25 * time.Millisecond)
		}
		initialGuestNativeSpawnRecovery(t, "after-grow")
		after := initialGuestNativeState(t, source.Config.StateDir)
		a, _ := json.Marshal(before)
		b, _ := json.Marshal(after)
		if !bytes.Equal(a, b) {
			t.Fatal("grow/observe/recovery rewrote original stage files")
		}
		if _, err := os.Stat(filepath.Join(source.Config.StateDir, "state.json")); !os.IsNotExist(err) {
			t.Fatal("early producer created formal resource journal", err)
		}
		initialGuestNativeSave(t, "initial-guest-movable", map[string]any{"observation": movable, "original_wal_bytes_unchanged": true, "formal_resource_journal_created": false, "PG_started": false, "HostSourceIsFixture": true, "normalAuthorized": false})
	})
}
