//go:build linux && dsga_native_memory_commit2 && dsga_native_filesystem_origin && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func init() {
	if len(os.Args) == 2 && os.Args[0] == "/neonvm/bin/dsga-resource-guard" && os.Args[1] == "initial-boot" && os.Getenv("DSGA_MEMORY_COMMIT2_COMPONENT") == "DEV369_COMMIT8_NOT_NORMAL" {
		normalFilesystemNativeMode = true
		os.Args = []string{os.Args[0], "-test.run=^TestNativeNormalMemoryCommit2Guest$", "-test.v", "-test.timeout=110s"}
	}
}

// 仅DEV库存构造器。保留Host原VM/Secret/预算和真实文件摘要，不能从该工厂取得资格。
func TestExportNormalMemoryCommit2FilesystemFixture(t *testing.T) {
	TestExportNormalFilesystemFixture(t)
	out := os.Getenv("DSGA_SYSTEM_LIFECYCLE_EXPORT")
	read := func(p string) []byte { b, e := os.ReadFile(p); require.NoError(t, e); return b }
	var seed, fs RuntimeDocument
	require.NoError(t, json.Unmarshal(read(os.Getenv("DSGA_MEMORY_COMMIT2_HOST_RUNTIME")), &seed))
	root := filepath.Join(out, "root")
	require.NoError(t, json.Unmarshal(read(filepath.Join(root, FixedSealedRuntimePath)), &fs))
	var source SourceConfig
	require.NoError(t, json.Unmarshal(seed.SourceJSON, &source))
	source.Limits.MemoryMaxBytes = 512 << 20
	source.Limits.SystemMemoryMaxBytes = 192 << 20
	source.Limits.OutsideUpperBytes = 256 << 20
	sourceRaw, e := json.Marshal(source)
	require.NoError(t, e)
	seed.SourceJSON = sourceRaw
	seed.SourceSHA256 = physicalDigest(sourceRaw)
	env, e := parseRuntimePolicyTemplates(fs.PolicyTemplatesJSON)
	require.NoError(t, e)
	for i := range env.Templates {
		var fields map[string]any
		require.NoError(t, json.Unmarshal(env.Templates[i].Policy, &fields))
		switch env.Templates[i].Asset {
		case "dsga-normal-split.json":
			fields["protocol_version"] = 3
			fields["memory_origin_protocol_version"] = 2
		case "dsga-prebootstrap-system.json":
			fields["boot_memory_upper_bytes"] = strconv.FormatUint(256<<20, 10)
		}
		env.Templates[i].Policy, e = json.Marshal(fields)
		require.NoError(t, e)
	}
	seed = runtimeTemplatesSetBlob(seed, runtimeTemplatesEnvelope(env.Templates))
	raw, e := json.Marshal(seed)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = raw
	plan, e := DeriveRuntimeBootRequirements(raw, assets)
	require.NoError(t, e)
	require.Equal(t, uint32(2), plan.MemoryOriginProtocolVersion())
	for logical, b := range assets {
		disk, e := RuntimeISOAssetName(logical)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(out, "isofiles", disk), b, 0444))
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, FixedSealedRuntimePath), raw, 0444))
	cfg, e := json.Marshal(source.Config)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, FixedConfigPath), cfg, 0444))
	require.NoError(t, os.WriteFile(filepath.Join(out, "runtime.json"), raw, 0444))
	require.NoError(t, os.WriteFile(filepath.Join(out, "required-kernel-args.txt"), []byte(strings.Join(plan.RequiredKernelArgs(), " ")+"\n"), 0444))
}

// PG17-S-MEMORY-COMMIT2-001的一侧；原8+6/7由真实Kernel/QMP/virtio构造，
// Commit8完成后才清退port、建立拓扑/Bootstrap及FS2。服务库存为stand-in，无PG/normal资格。
func TestNativeNormalMemoryCommit2Guest(t *testing.T) {
	require.True(t, normalFilesystemNativeMode)
	require.Equal(t, "DEV369_COMMIT8_NOT_NORMAL", os.Getenv("DSGA_MEMORY_COMMIT2_COMPONENT"))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	defer func() {
		details := map[string]any{}
		for _, name := range []string{"/proc/self/mountinfo", "/proc/1/mountinfo"} {
			b, e := os.ReadFile(name)
			if e != nil {
				details[name] = e.Error()
			} else {
				details[name] = string(b)
			}
		}
		wal := map[string]string{}
		entries, _ := os.ReadDir(FixedStateDir)
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".json") {
				b, e := os.ReadFile(filepath.Join(FixedStateDir, entry.Name()))
				if e == nil {
					wal[entry.Name()] = string(b)
				}
			}
		}
		details["actual_WAL"] = wal
		normalFilesystemNativeSave("filesystem-diagnostics.json", details)
	}()
	source, raw, assets, e := initialGuestChannelInputs()
	require.NoError(t, e)
	plan, e := DeriveRuntimeBootRequirements(raw, assets)
	require.NoError(t, e)
	require.Equal(t, uint32(2), plan.MemoryOriginProtocolVersion())
	var port *initialGuestStream
	for {
		port, e = openInitialGuestPort(ctx)
		if e == nil {
			break
		}
		require.True(t, errors.Is(e, errInitialGuestPortPending), "%v", e)
		require.NoError(t, initialGuestWait(ctx))
	}
	defer port.close()
	challenge, e := bootprotocol.ReadInitialPrebootChallenge(port)
	require.NoError(t, e)
	_, e = initialPrebootBindInputs(challenge, raw, assets, source.Config)
	require.NoError(t, e)
	g, e := initialGuestProvisionInputs(source, raw)
	require.NoError(t, e)
	defer g.normalMemoryDiscardOriginalConfirmedSource()
	require.NoError(t, initialGuestExchangeWithRequirements(ctx, port, g, challenge, false, plan))
	s, e := lockState(ctx, g.config.StateDir)
	require.NoError(t, e)
	original, e := g.normalMemoryOriginalConfirmedSourceLocked(ctx, s)
	require.NoError(t, e)
	ref, e := original.confirmedReferenceLocked(ctx, s, g)
	require.NoError(t, e)
	require.Equal(t, challenge.Nonce, original.commit2.record.Commit.OriginalNonce)
	require.ErrorIs(t, g.normalMemoryInstallOriginalConfirmedSourceLocked(ctx, s, original), ErrUnknown, "same Guard must not re-admit even same pointer")
	again, e := g.normalMemoryOriginalConfirmedSourceLocked(ctx, s)
	require.NoError(t, e)
	require.Same(t, original, again)
	another, e := Open(source.Config)
	require.NoError(t, e)
	missing, e := another.normalMemoryOriginalConfirmedSourceLocked(ctx, s)
	require.Nil(t, missing)
	require.ErrorIs(t, e, ErrUnknown, "new Guard must not reconstruct R1 from visible JSON")
	before := map[string]string{}
	for _, name := range normalMemoryConfirmedFiles() {
		b, e := os.ReadFile(filepath.Join(FixedStateDir, name))
		require.NoError(t, e)
		before[name] = physicalDigest(b)
	}
	s.close()
	oldFD := port.fd
	port.close()
	require.Equal(t, -1, port.fd)
	_, e = unix.FcntlInt(uintptr(oldFD), unix.F_GETFD, 0)
	require.ErrorIs(t, e, unix.EBADF, "original port must be closed before topology/FS")
	_, e = g.PrepareNormalSplitTopology(ctx)
	require.NoError(t, e)
	_, e = ProvisionGuest(ctx)
	require.NoError(t, e)
	require.NoError(t, CheckGuest(ctx))
	system, e := inspectNormalSystemLifecycleOriginal(ctx)
	require.NoError(t, e)
	defer system.close()
	fs, e := g.captureNormalFilesystemOrigin(ctx, system)
	require.NoError(t, e)
	defer fs.close()
	observed, e := fs.observe(ctx)
	require.NoError(t, e)
	require.True(t, observed.DurabilityConfirmed)
	require.Equal(t, uint32(2), fs.intent.ProtocolVersion)
	require.NotNil(t, fs.intent.Memory)
	require.Equal(t, ref, *fs.intent.Memory)
	require.ErrorIs(t, fs.requireContinuousQualification(), ErrUnknown)
	for name, h := range before {
		b, e := os.ReadFile(filepath.Join(FixedStateDir, name))
		require.NoError(t, e)
		require.Equal(t, h, physicalDigest(b))
	}
	normalFilesystemNativeSave("filesystem-origin.json", map[string]any{"stable_id": "PG17-S-MEMORY-COMMIT2-001", "single_commit8": true, "actual_selector": plan.MemoryOriginProtocolVersion(), "commit8": original.commit2.record.Commit, "original_confirmed_reference": ref, "same_Guard_pointer_retained": true, "duplicate_admission_rejected": true, "new_Guard_JSON_adoption_rejected": true, "port_closed_before_split_and_FS": true, "actual_split_then_Bootstrap_then_CheckGuest": true, "filesystem2": observed, "memory_prefix_unchanged": true, "normal_authorized": false, "PG_executed": false, "Host_API_is_model": true, "System_services_are_standins": true, "E": 0})
	fmt.Println("NORMAL_MEMORY_COMMIT2_GUEST_SOURCE=PASS_SINGLE8_PORT_CLOSED_FS2_CAPTURED")
	// 为Host原QEMU/pidfd来源复核留下有界窗口；不是额外ACK/无限ready假设。
	time.Sleep(3 * time.Second)
}
