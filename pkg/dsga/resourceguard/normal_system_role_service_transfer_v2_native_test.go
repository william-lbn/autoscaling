//go:build linux && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func init() {
	if len(os.Args) == 2 && os.Args[0] == normalSystemRoleBrokerExecutable && os.Args[1] == "initial-boot" {
		os.Args = []string{os.Args[0], "-test.run=^TestNativeNormalSystemRoleTransferV2Initial$", "-test.v", "-test.timeout=150s"}
	}
	normalRoleServiceNativeBrokerContext = func(ctx context.Context) context.Context {
		return context.WithValue(ctx, normalSystemRoleTransferPreparedCheckpointKey{}, func(g *Guard, fs *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof) error {
			if fs == nil || proof == nil || !proof.declaration.declaration.kernelPreparationV2() || proof.transfer.ProtocolVersion != 2 || proof.kernelPreparation == nil || fs.kernelConsumer == nil || fs.memoryConsumer == nil {
				return ErrUnknown
			}
			check := func() error {
				return g.withRecord(ctx, func(_ *record, s *lockedState) error {
					ref, e := fs.referenceLocked(ctx, s)
					if e != nil {
						return e
					}
					if ref.ProtocolVersion != 2 {
						return ErrUnknown
					}
					k, e := fs.kernelConsumer.referenceLocked(ctx, s)
					if e != nil {
						return e
					}
					if k != *proof.transfer.KernelPreparation {
						return ErrUnknown
					}
					if e = fs.memoryConsumer.recheckLocked(ctx, s, g); e != nil {
						return e
					}
					return nil
				})
			}
			if e := arrayRegistryPIDFDAlive(proof.parent.pidfd); e != nil {
				return e
			}
			if e := check(); e != nil {
				return e
			}
			fmt.Println("ROLE_TRANSFER2_R1_ALIVE_KERNEL_MEMORY_READ=PASS")
			go func() {
				until := time.Now().Add(45 * time.Second)
				for {
					fd := []unix.PollFd{{Fd: int32(proof.parent.pidfd), Events: unix.POLLIN}}
					n, e := unix.Poll(fd, 50)
					if e == unix.EINTR {
						continue
					}
					if e != nil {
						fmt.Println("ROLE_TRANSFER2_AFTER_PARENT_ERROR", e)
						return
					}
					if n > 0 && fd[0].Revents&unix.POLLIN != 0 {
						break
					}
					if time.Now().After(until) {
						fmt.Println("ROLE_TRANSFER2_PARENT_EXIT_TIMEOUT")
						return
					}
				}
				if e := check(); e != nil {
					fmt.Println("ROLE_TRANSFER2_AFTER_PARENT_ERROR", e)
					return
				}
				fmt.Println("ROLE_TRANSFER2_R1_EXITED_KERNEL_MEMORY_READ=PASS")
			}()
			return nil
		})
	}
}

// 从原Host模型身份和实际最终ELF构造库存，仅真实运行后才获得各opaque。
func TestExportNormalSystemRoleTransferV2Fixture(t *testing.T) {
	TestExportNormalSystemRoleServiceFixture(t)
	out := os.Getenv("DSGA_SYSTEM_LIFECYCLE_EXPORT")
	root := filepath.Join(out, "root")
	read := func(n string) []byte { b, e := os.ReadFile(n); require.NoError(t, e); return b }
	put := func(n string, b []byte) { require.NoError(t, os.WriteFile(filepath.Join(root, n), b, 0444)) }
	var d, seed RuntimeDocument
	require.NoError(t, json.Unmarshal(read(filepath.Join(root, FixedSealedRuntimePath)), &d))
	require.NoError(t, json.Unmarshal(read(os.Getenv("DSGA_TRANSFER2_HOST_RUNTIME")), &seed))
	buildRef, e := LookupPhysicalKernelBuild(NormalSplitMetadataV1PhysicalKernelProfileID)
	require.NoError(t, e)
	m, e := DecodeNormalSystemSourceManifest(read(filepath.Join(root, NormalSystemSourceManifestPath)))
	require.NoError(t, e)
	m.Kernel = buildRef
	decl, e := NormalSystemRoleBrokerDeclarationV3JSON(physicalDigest(read(filepath.Join(root, normalSystemRoleBrokerExecutable))), physicalDigest(read(filepath.Join(root, normalSystemRoleBuildPlanPath))))
	require.NoError(t, e)
	put(normalSystemRoleBrokerDeclarationPath, decl)
	for i := range m.Files {
		if m.Files[i].Path == normalSystemRoleBrokerDeclarationPath {
			m.Files[i].SHA256 = physicalDigest(decl)
			m.Files[i].Bytes = uint64(len(decl))
		}
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	require.NoError(t, m.Validate())
	mraw, e := json.Marshal(m)
	require.NoError(t, e)
	put(NormalSystemSourceManifestPath, mraw)
	var build nativeBirthBuildManifest
	require.NoError(t, json.Unmarshal(read(filepath.Join(root, fixedNativeBirthBuildPath)), &build))
	build.RuntimeClosure.SystemManifestSHA256 = physicalDigest(mraw)
	braw, e := json.Marshal(build)
	require.NoError(t, e)
	put(fixedNativeBirthBuildPath, braw)
	var source SourceConfig
	require.NoError(t, json.Unmarshal(seed.SourceJSON, &source))
	source.ControlPublicKey = base64.RawURLEncoding.EncodeToString(normalAuthorityDEVKey().Public().(ed25519.PublicKey))
	source.Limits.MemoryMaxBytes = 512 << 20
	source.Limits.SystemMemoryMaxBytes = 384 << 20
	source.Limits.OutsideUpperBytes = 384 << 20
	seed.SourceJSON, e = json.Marshal(source)
	require.NoError(t, e)
	seed.SourceSHA256 = physicalDigest(seed.SourceJSON)
	ts, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	require.NoError(t, e)
	for i := range ts.Templates {
		var v map[string]any
		require.NoError(t, json.Unmarshal(ts.Templates[i].Policy, &v))
		switch ts.Templates[i].Asset {
		case "dsga-physical-kernel.json":
			v["profile_id"] = buildRef.ProfileID
			v["kernel_release"] = buildRef.KernelRelease
			v["kernel_artifact_sha256"] = buildRef.KernelArtifactSHA256
			v["kernel_config_sha256"] = buildRef.KernelConfigSHA256
			v["kernel_notes_sha256"] = buildRef.KernelNotesSHA256
		case "dsga-kernel-bootstrap.json":
			v["protocol_version"] = 2
			v["profile_id"] = buildRef.ProfileID
			v["max_user_namespaces"] = "0"
			// 原512M基座给nr_free_buffer_pages硬上界；sqrt((512MiB/4KiB)*64)<2897KiB。
			// 原ISO显式4096先落实再封存，禁止依当前free/minfree采样或热增后补写。
			controls, ok := v["controls"].(map[string]any)
			require.True(t, ok)
			stable, err := NormalMetadataStableMinFreeKBytes(512 << 20)
			require.NoError(t, err)
			require.EqualValues(t, 4096, stable)
			controls["min_free_kbytes"] = strconv.FormatUint(stable, 10)
		case "dsga-prebootstrap-system.json":
			v["protocol_version"] = 3
			v["physical_profile_id"] = buildRef.ProfileID
			v["boot_memory_upper_bytes"] = strconv.FormatUint(512<<20, 10)
		case "dsga-normal-split.json":
			v["protocol_version"] = 4
			v["physical_profile_id"] = buildRef.ProfileID
			v["memory_origin_protocol_version"] = 2
		case "dsga-native-birth.json":
			v["build_manifest_sha256"] = physicalDigest(braw)
		}
		ts.Templates[i].Policy, e = json.Marshal(v)
		require.NoError(t, e)
	}
	seed = runtimeTemplatesSetBlob(seed, runtimeTemplatesEnvelope(ts.Templates))
	raw, e := json.Marshal(seed)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = raw
	plan, e := DeriveRuntimeBootRequirements(raw, assets)
	require.NoError(t, e)
	require.EqualValues(t, 2, plan.ProtocolVersion())
	require.Equal(t, normalInitialWALFamilyKernelPrepV2, plan.InitialWALFamily())
	for logical, b := range assets {
		name, e := RuntimeISOAssetName(logical)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(out, "isofiles", name), b, 0444))
	}
	put(FixedSealedRuntimePath, raw)
	cfg, e := json.Marshal(source.Config)
	require.NoError(t, e)
	put(FixedConfigPath, cfg)
	require.NoError(t, os.WriteFile(filepath.Join(out, "runtime.json"), raw, 0444))
	require.NoError(t, os.WriteFile(filepath.Join(out, "required-kernel-args.txt"), []byte(strings.Join(plan.RequiredKernelArgs(), " ")+"\n"), 0444))
}
func TestNativeNormalSystemRoleTransferV2Initial(t *testing.T) {
	defer func() {
		rc := 0
		if t.Failed() {
			rc = 1
			for _, name := range []string{"/proc/sys/user/max_user_namespaces", "/proc/sys/vm/min_free_kbytes", "/proc/sys/vm/watermark_scale_factor", "/proc/sys/vm/watermark_boost_factor", "/proc/sys/vm/percpu_pagelist_high_fraction", "/proc/sys/vm/compaction_proactiveness", "/proc/sys/kernel/pid_max", "/proc/sys/kernel/modules_disabled", "/proc/sys/kernel/numa_balancing", "/sys/kernel/mm/numa/demotion_enabled", "/proc/modules"} {
				b, e := os.ReadFile(name)
				if e != nil {
					fmt.Println("ROLE_TRANSFER2_FIXED_KNOB_ERROR", name, e)
				} else if len(b) <= 4096 {
					fmt.Printf("ROLE_TRANSFER2_FIXED_KNOB %s=%q\n", name, string(b))
				}
			}
			raw, e := os.ReadFile(filepath.Join(FixedStateDir, normalKernelPrepV2CompleteName))
			if e != nil {
				fmt.Println("ROLE_TRANSFER2_ORIGINAL_CONTROLS_ERROR", e)
			} else {
				var evidence NormalV2KernelBootstrapEvidence
				if e = json.Unmarshal(raw, &evidence); e != nil {
					fmt.Println("ROLE_TRANSFER2_ORIGINAL_CONTROLS_ERROR", e)
				} else {
					b, _ := json.Marshal(evidence.After.controls())
					fmt.Println("ROLE_TRANSFER2_ORIGINAL_CONTROLS", string(b))
				}
			}
		}
		fmt.Printf("ROLE_SERVICE_INITIAL_EXIT=%d\n", rc)
		if rc != 0 {
			fmt.Printf("ROLE_SERVICE_GUEST_EXIT=%d\n", rc)
			unix.Sync()
			_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
		}
	}()
	require.Equal(t, 1, os.Getppid())
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	source, raw, assets, e := initialGuestChannelInputs()
	require.NoError(t, e)
	plan, e := DeriveRuntimeBootRequirements(raw, assets)
	require.NoError(t, e)
	require.Equal(t, normalInitialWALFamilyKernelPrepV2, plan.InitialWALFamily())
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
	port.close()
	// 正式state.json在ProvisionGuest才创建；原私有来源只要求同flock，不提前造record。
	initialState, e := lockState(ctx, g.config.StateDir)
	require.NoError(t, e)
	_, e = g.normalKernelPrepV2OriginalLocked(ctx, initialState)
	require.NoError(t, e)
	_, e = g.normalMemoryOriginalConfirmedSourceLocked(ctx, initialState)
	require.NoError(t, e)
	initialState.close()
	_, e = g.PrepareNormalSplitTopology(ctx)
	require.NoError(t, e)
	_, e = ProvisionGuest(ctx)
	require.NoError(t, e)
	require.NoError(t, CheckGuest(ctx))
	require.NoError(t, startInitialNormalSystemV3Broker(ctx, g))
	m, e := readNormalSystemRoleServiceMetadata()
	require.NoError(t, e)
	require.EqualValues(t, 2, m.Origin.ProtocolVersion)
	require.EqualValues(t, 2, m.Origin.Filesystem.ProtocolVersion)
	require.NotEmpty(t, m.Origin.KernelPreparationSHA256)
	require.NotEmpty(t, m.ReadySHA256)
	fmt.Println("NEON-S-SYSTEM-ROLE-SERVICE-001 PASS actual KernelPrep2 original same-Guard, Host6/7/single8 then FS2 and strict original5FD typed rebind")
	fmt.Println("NEON-S-SYSTEM-ROLE-SERVICE-002 PASS actual listener/protected ready COMMIT; R1 remains alive at KernelR2+MemoryR2 bind then exits normally")
}
