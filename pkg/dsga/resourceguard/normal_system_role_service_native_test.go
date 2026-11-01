//go:build linux && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	bootprotocol "github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func normalRoleServiceExportFilesystemBase(t *testing.T) {
	// 复用已冻结System2 TEST_ONLY工厂，所有未执行服务仍为声明清楚的静态stand-in。
	// 实际BusyBox init/vminit、本Go Guard和musl改为真实被执行字节后重建全部摘要。
	TestExportNormalSystemLifecycleFixture(t)
	out := os.Getenv("DSGA_SYSTEM_LIFECYCLE_EXPORT")
	root := filepath.Join(out, "root")
	read := func(p string) []byte { b, e := os.ReadFile(p); require.NoError(t, e); return b }
	put := func(p string, b []byte, mode os.FileMode) {
		q := filepath.Join(root, p)
		require.NoError(t, os.MkdirAll(filepath.Dir(q), 0755))
		require.NoError(t, os.WriteFile(q, b, mode))
	}
	guard := read(os.Getenv("DSGA_SERVICE_GUARD_ELF"))
	busybox := read("fixture/bin/busybox")
	musl := read("fixture/lib/ld-musl-x86_64.so.1")
	vminit := read("fixture/vminit-service")
	m, e := DecodeNormalSystemSourceManifest(read(filepath.Join(root, NormalSystemSourceManifestPath)))
	require.NoError(t, e)
	for i := range m.Files {
		f := &m.Files[i]
		var b []byte
		switch f.CanonicalPath {
		case "/neonvm/bin/busybox":
			b = busybox
		case "/neonvm/bin/dsga-resource-guard":
			b = guard
		case "/neonvm/bin/vminit":
			b = vminit
		}
		if b != nil {
			f.Bytes = uint64(len(b))
			f.SHA256 = physicalDigest(b)
			if f.Path == f.CanonicalPath {
				put(f.Path, b, 0755)
			}
		}
	}
	put("/lib/ld-musl-x86_64.so.1", musl, 0755)
	require.NoError(t, os.Symlink("/lib/ld-musl-x86_64.so.1", filepath.Join(root, "lib/libc.musl-x86_64.so.1")))
	for _, name := range []string{"/lib/ld-musl-x86_64.so.1", "/lib/libc.musl-x86_64.so.1"} {
		m.Files = append(m.Files, NormalSystemSourceFile{Path: name, CanonicalPath: "/lib/ld-musl-x86_64.so.1", Kind: "loader", Bytes: uint64(len(musl)), SHA256: physicalDigest(musl)})
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	require.NoError(t, m.Validate())
	systemRaw, e := json.Marshal(m)
	require.NoError(t, e)
	put(NormalSystemSourceManifestPath, systemRaw, 0444)
	var build nativeBirthBuildManifest
	require.NoError(t, json.Unmarshal(read(filepath.Join(root, fixedNativeBirthBuildPath)), &build))
	build.RuntimeClosure = &nativeRuntimeClosureBuild{ProtocolVersion: 1, SystemManifestSHA256: physicalDigest(systemRaw)}
	helper := read("fixture/neon-dsga-array-populate")
	put(fixedArrayHelperPath, helper, 0755)
	build.ArrayHelper = &nativeArrayHelperBuild{ProtocolVersion: 1, Path: fixedArrayHelperPath, SHA256: physicalDigest(helper)}
	buildRaw, e := json.Marshal(build)
	require.NoError(t, e)
	put(fixedNativeBirthBuildPath, buildRaw, 0444)
	var d RuntimeDocument
	require.NoError(t, json.Unmarshal(read(filepath.Join(root, FixedSealedRuntimePath)), &d))
	var source SourceConfig
	require.NoError(t, json.Unmarshal(d.SourceJSON, &source))
	source.Limits.MemoryMaxBytes = 512 << 20
	source.Limits.SystemMemoryMaxBytes = 192 << 20
	source.Limits.OutsideUpperBytes = 256 << 20
	d.SourceJSON, e = json.Marshal(source)
	require.NoError(t, e)
	d.SourceSHA256 = physicalDigest(d.SourceJSON)
	envelope, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	require.NoError(t, e)
	for i := range envelope.Templates {
		var fields map[string]any
		require.NoError(t, json.Unmarshal(envelope.Templates[i].Policy, &fields))
		if envelope.Templates[i].Asset == "dsga-normal-split.json" {
			fields["private_memory_max_bytes"] = "134217728"
			fields["initial_arrays_memory_max_bytes"] = "268435456"
		}
		if envelope.Templates[i].Asset == "dsga-native-birth.json" {
			fields["build_manifest_sha256"] = physicalDigest(buildRaw)
			fields["array_helper_protocol_version"] = 1
			fields["array_helper_path"] = fixedArrayHelperPath
			fields["array_helper_elf_sha256"] = physicalDigest(helper)
			fields["array_helper_address_space_max_bytes"] = "67108864"
			fields["array_helper_memlock_upper_bytes"] = "1048576"
			fields["array_helper_reserve_bytes"] = "68157440"
		}
		if envelope.Templates[i].Asset == "dsga-prebootstrap-system.json" {
			fields["boot_memory_upper_bytes"] = strconv.FormatUint(1<<30, 10)
		}
		envelope.Templates[i].Policy, e = json.Marshal(fields)
		require.NoError(t, e)
	}
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(envelope.Templates))
	raw, e := json.Marshal(d)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = raw
	requirements, e := DeriveRuntimeBootRequirements(raw, assets)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(out, "required-kernel-args.txt"), []byte(strings.Join(requirements.RequiredKernelArgs(), " ")+"\n"), 0444))
	for logical, b := range assets {
		name, e := RuntimeISOAssetName(logical)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(out, "isofiles", name), b, 0444))
	}
	put(FixedSealedRuntimePath, raw, 0444)
	cfg, e := json.Marshal(source.Config)
	require.NoError(t, e)
	put(FixedConfigPath, cfg, 0444)
	// 固定持久孔为真实ext4；该文件仅用于共享write/private COW的机制负例。
	put("/neonvm/fs-map.bin", make([]byte, 4096), 0644)
	for _, dir := range []string{"/proc", "/sys", "/dev/pts", "/dev/shm", "/run", "/tmp", "/neonvm/runtime", "/var/db/postgres", "/var/log/chrony", "/var/lib/chrony", "/var/lib/neon-dsga/state"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, dir), 0755))
	}
	require.NoError(t, os.Chmod(filepath.Join(root, FixedStateDir), 0700))
	put("/etc/resolv.conf", []byte("# TEST_ONLY network disabled\n"), 0644)
	require.NoError(t, os.Chown(filepath.Join(root, "var/db/postgres"), 1000, 1000))
	// 测试Host challenge只绑定实际ISO/内核和固定基座，不用于Host真实预留授权。
	challenge := bootprotocol.InitialPrebootChallenge{ProtocolVersion: 1, Scope: bootprotocol.InitialPrebootScope, SourceSHA256: strings.Repeat("ab", 32), Nonce: strings.Repeat("cd", 32), HostStageSHA256: strings.Repeat("ef", 32)}
	// 沿既有标准夹具的Runner身份，不能添加不存在的Compute字段。
	originalChallenge, _, _, _ := initialPrebootFixture(t)
	challenge.Identity = originalChallenge.Identity
	challenge.Identity.VMUID = source.Config.VMUID
	challenge.Identity.Incarnation = d.Incarnation
	challenge.Identity.MinimumBytes = 1536 << 20
	challenge.Identity.MaximumBytes = 2 << 30
	challenge.Identity.BlockBytes = 8 << 20
	challenge.RuntimeSHA256 = physicalDigest(raw)
	challenge.ResourceSHA256 = d.SourceSHA256
	challenge.ConfigSecretUID = d.ConfigSecretUID
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	require.NoError(t, e)
	challenge.KernelBuildSHA256 = ref.KernelArtifactSHA256
	challenge.OriginalBase = 1 << 30
	challenge.BusinessMinimum = 1536 << 20
	challenge.Maximum = 2 << 30
	require.NoError(t, challenge.Validate())
	b, e := json.Marshal(challenge)
	require.NoError(t, e)
	put("/neonvm/fs-challenge.json", b, 0444)
}

func exportNormalRoleServiceV3(t *testing.T) {
	out := os.Getenv("DSGA_SYSTEM_LIFECYCLE_EXPORT")
	root := filepath.Join(out, "root")
	read := func(p string) []byte { b, e := os.ReadFile(p); require.NoError(t, e); return b }
	put := func(p string, b []byte, mode os.FileMode) {
		q := filepath.Join(root, p)
		require.NoError(t, os.MkdirAll(filepath.Dir(q), 0755))
		require.NoError(t, os.WriteFile(q, b, mode))
	}
	m, e := DecodeNormalSystemSourceManifest(read(filepath.Join(root, NormalSystemSourceManifestPath)))
	require.NoError(t, e)
	m.ProtocolVersion = 3
	m.ProfileID = NormalSystemSourceV3ProfileID
	plan := normalSystemRoleBuildPlan{ProtocolVersion: 1, ComputeUID: 1000, ComputeGID: 1000, ComputeArgv: []string{"/usr/local/bin/compute_ctl", "--pgdata", "/var/db/postgres/compute"}, ComputeEnv: []string{"PATH=/neonvm/bin"}, LoaderPaths: []string{}}
	planRaw, e := json.Marshal(plan)
	require.NoError(t, e)
	guard := read(filepath.Join(root, normalSystemRoleBrokerExecutable))
	broker, e := NormalSystemRoleBrokerDeclarationJSON(physicalDigest(guard), physicalDigest(planRaw))
	require.NoError(t, e)
	controls, e := NormalKernelControlSourceManifestJSON()
	require.NoError(t, e)
	rows, e := NormalSystemSourceInitEntriesForVersion(3)
	require.NoError(t, e)
	replacements := map[string][]byte{"/etc/inittab": []byte(strings.Join(rows, "\n") + "\n"), "/neonvm/bin/vmstart": read("fixture/vmstart-v3"), normalSystemRoleBuildPlanPath: planRaw, normalSystemRoleBrokerDeclarationPath: broker, NormalKernelControlSourceManifestPath: controls}
	for path, data := range replacements {
		kind := "config"
		mode := os.FileMode(0444)
		if path == "/neonvm/bin/vmstart" {
			kind = "script"
			mode = 0755
		}
		put(path, data, mode)
		entry := NormalSystemSourceFile{Path: path, CanonicalPath: path, Kind: kind, Bytes: uint64(len(data)), SHA256: physicalDigest(data)}
		found := false
		for i := range m.Files {
			if m.Files[i].Path == path {
				m.Files[i] = entry
				found = true
			}
		}
		if !found {
			m.Files = append(m.Files, entry)
		}
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	require.NoError(t, m.Validate())
	raw, e := json.Marshal(m)
	require.NoError(t, e)
	put(NormalSystemSourceManifestPath, raw, 0444)
	var build nativeBirthBuildManifest
	require.NoError(t, json.Unmarshal(read(filepath.Join(root, fixedNativeBirthBuildPath)), &build))
	build.RuntimeClosure.SystemManifestSHA256 = physicalDigest(raw)
	buildRaw, e := json.Marshal(build)
	require.NoError(t, e)
	put(fixedNativeBirthBuildPath, buildRaw, 0444)
	var d RuntimeDocument
	require.NoError(t, json.Unmarshal(read(filepath.Join(root, FixedSealedRuntimePath)), &d))
	var source SourceConfig
	require.NoError(t, json.Unmarshal(d.SourceJSON, &source))
	source.Limits.SystemMemoryMaxBytes = 512 << 20
	source.Limits.OutsideUpperBytes = 768 << 20
	d.SourceJSON, e = json.Marshal(source)
	require.NoError(t, e)
	d.SourceSHA256 = physicalDigest(d.SourceJSON)
	templates, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	require.NoError(t, e)
	for i := range templates.Templates {
		var f map[string]any
		require.NoError(t, json.Unmarshal(templates.Templates[i].Policy, &f))
		if templates.Templates[i].Asset == "dsga-native-birth.json" {
			f["build_manifest_sha256"] = physicalDigest(buildRaw)
		}
		if templates.Templates[i].Asset == "dsga-prebootstrap-system.json" {
			f["boot_memory_upper_bytes"] = strconv.FormatUint(2<<30, 10)
		}
		templates.Templates[i].Policy, e = json.Marshal(f)
		require.NoError(t, e)
	}
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(templates.Templates))
	raw, e = json.Marshal(d)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = raw
	for logical, data := range assets {
		name, e := RuntimeISOAssetName(logical)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(out, "isofiles", name), data, 0444))
	}
	put(FixedSealedRuntimePath, raw, 0444)
	cfg, e := json.Marshal(source.Config)
	require.NoError(t, e)
	put(FixedConfigPath, cfg, 0444)
	// 在开VM前精确核全固定payload的DEV分派和字节，漏一个角色就导出失败。
	for _, role := range normalSystemRoleServiceRoles {
		if role == "compute" || role == "resource" {
			continue
		}
		argv, e := normalSystemFixedRoleArgvForVersion(3, role)
		require.NoError(t, e)
		canonical, e := filepath.EvalSymlinks(filepath.Join(root, argv[0]))
		if argv[0] == "/neonvm/bin/busybox" || argv[0] == "/neonvm/bin/udhcpc" || argv[0] == "/neonvm/bin/sh" {
			continue
		}
		require.NoError(t, e)
		require.True(t, normalRoleServiceDEVLongStandin(argv[0]), role)
		require.Equal(t, guard, read(canonical))
	}
	requirements, e := DeriveRuntimeBootRequirements(raw, assets)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(out, "required-kernel-args.txt"), []byte(strings.Join(requirements.RequiredKernelArgs(), " ")+"\n"), 0444))
	var challenge bootprotocol.InitialPrebootChallenge
	require.NoError(t, json.Unmarshal(read(filepath.Join(root, "/neonvm/fs-challenge.json")), &challenge))
	challenge.RuntimeSHA256 = physicalDigest(raw)
	challenge.ResourceSHA256 = d.SourceSHA256
	challenge.OriginalBase = 2 << 30
	challenge.BusinessMinimum = 3 << 30
	challenge.Maximum = 4 << 30
	challenge.Identity.MinimumBytes = 3 << 30
	challenge.Identity.MaximumBytes = 4 << 30
	require.NoError(t, challenge.Validate())
	b, e := json.Marshal(challenge)
	require.NoError(t, e)
	put("/neonvm/fs-challenge.json", b, 0444)
}

func TestExportNormalSystemRoleServiceFixture(t *testing.T) {
	normalRoleServiceExportFilesystemBase(t)
	exportNormalRoleServiceV3(t)
}

// 仅DEV工厂的角色替身表；生产argv仍来自原RO版本表，不能把它当服务兼容性。
func normalRoleServiceDEVLongStandin(path string) bool {
	switch path {
	case "/neonvm/bin/udevd", "/neonvm/bin/acpid", "/neonvm/bin/vector", "/neonvm/bin/chronyd", "/neonvm/bin/neonvmd", "/neonvm/bin/su-exec":
		return true
	}
	return false
}
func TestMain(m *testing.M) {
	if len(os.Args) >= 2 && os.Args[0] == normalSystemRoleBrokerExecutable {
		switch os.Args[1] {
		case normalSystemRoleGateAction:
			if e := RunNormalSystemRoleExecGate(context.Background()); e != nil {
				fmt.Println("ROLE_SERVICE_GATE_REFUSED", e)
				os.Exit(42)
			}
			os.Exit(0)
		case "system-role-broker":
			if e := RunNormalSystemRoleBroker(context.Background()); e != nil {
				fmt.Println("ROLE_SERVICE_BROKER_REFUSED", e)
				os.Exit(43)
			}
			os.Exit(0)
		case "initial-boot":
			_ = flag.Set("test.run", "^TestNativeNormalSystemRoleServiceInitial$")
			_ = flag.Set("test.v", "true")
			rc := m.Run()
			fmt.Printf("ROLE_SERVICE_INITIAL_EXIT=%d\n", rc)
			if rc != 0 {
				fmt.Printf("ROLE_SERVICE_GUEST_EXIT=%d\n", rc)
				unix.Sync()
				_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
			}
			os.Exit(rc)
		case "check-guest":
			if e := CheckGuest(context.Background()); e != nil {
				fmt.Println("ROLE_SERVICE_CHECK_REFUSED", e)
				os.Exit(44)
			}
			os.Exit(0)
		case "exec-system-role":
			if len(os.Args) != 3 {
				os.Exit(45)
			}
			code, e := RunNormalSystemRoleClient(context.Background(), os.Args[2])
			if e != nil {
				fmt.Println("ROLE_SERVICE_CLIENT_REFUSED", os.Args[2], e)
				os.Exit(46)
			}

			probes := map[string]struct {
				code int
				id   string
			}{"logs": {2, "005"}, "clock": {7, "006"}, "acpi": {143, "007"}}
			if probe, ok := probes[os.Args[2]]; ok {
				if code != probe.code {
					fmt.Println("ROLE_SERVICE_EXACT_EXIT_REFUSED", os.Args[2], code)
					os.Exit(51)
				}
				marker := "/run/neon-dsga/dev-exit-" + probe.id
				f, e := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				if e == nil {
					_, e = f.Write([]byte("confirmed\n"))
					if e == nil {
						e = f.Sync()
					}
					f.Close()
					if e != nil {
						os.Exit(52)
					}
					fmt.Printf("NEON-S-SYSTEM-ROLE-SERVICE-%s PASS actual target WaitStatus through durable exited, broker reply and fixed client returned exact exit %d\n", probe.id, code)
				} else if !os.IsExist(e) {
					os.Exit(53)
				}
			}
			if os.Args[2] == "allow-start" && code == 0 {
				fmt.Println("NEON-S-SYSTEM-ROLE-SERVICE-003 PASS actual PID1 CLI and live fixed enum listener; target touch true Wait/durable exited")
			}
			if os.Args[2] == "compute" && code == 0 {
				pin, e := normalSystemRoleServiceCaptureComputeLockSelf()
				if e != nil {
					fmt.Println("ROLE_SERVICE_LOCK_POSTWAIT_REFUSED", e)
					os.Exit(47)
				}
				pin.Close()
				fmt.Println("NEON-S-SYSTEM-ROLE-SERVICE-004 PASS real PID1 vmstart FD9 same OFD FLOCK retained through target true Wait; no target FD9 forwarding")
				fmt.Println("ROLE_SERVICE_GUEST_EXIT=0")
				unix.Sync()
				_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
			}
			os.Exit(code)
		case "serve":
			for {
				time.Sleep(time.Second)
			} // DEV resource stand-in; no PG/TLS authority.
		}
	}

	if os.Args[0] == "/neonvm/bin/vector" {
		os.Exit(2)
	}
	if os.Args[0] == "/neonvm/bin/chronyd" {
		os.Exit(7)
	}
	if os.Args[0] == "/neonvm/bin/acpid" {
		if unix.Kill(os.Getpid(), unix.SIGTERM) != nil {
			os.Exit(54)
		}
		for {
			time.Sleep(time.Millisecond)
		}
	}
	if os.Args[0] == "/usr/local/bin/compute_ctl" {
		if os.Getuid() != 1000 || os.Geteuid() != 1000 {
			os.Exit(48)
		}
		if p, e := unix.PrctlRetInt(unix.PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0); e != nil || p != 0 {
			os.Exit(49)
		}
		if link, _ := os.Readlink("/proc/self/fd/9"); link == normalSystemRoleComputeClientLockPath {
			os.Exit(50)
		}

		// 真目标保持12秒，跨过旧10秒窗口；还须等三类真实client退出码检查。
		time.Sleep(12 * time.Second)
		until := time.Now().Add(30 * time.Second)
		for {
			all := true
			for _, id := range []string{"005", "006", "007"} {
				if _, e := os.Stat("/run/neon-dsga/dev-exit-" + id); e != nil {
					all = false
				}
			}
			if all {
				break
			}
			if time.Now().After(until) {
				os.Exit(55)
			}
			time.Sleep(20 * time.Millisecond)
		}
		fmt.Println("ROLE_SERVICE_DEV_COMPUTE_UID1000_EXIT=0")
		os.Exit(0)
	}
	if normalRoleServiceDEVLongStandin(os.Args[0]) {
		for {
			time.Sleep(time.Second)
		}
	}

	os.Exit(m.Run())
}
func TestNativeNormalSystemRoleServiceInitial(t *testing.T) {
	require.Equal(t, 1, os.Getppid())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	source, raw, e := runtimeSource()
	require.NoError(t, e)
	g, e := initialGuestProvisionInputs(source, raw)
	require.NoError(t, e)
	challengeRaw, e := os.ReadFile("/neonvm/fs-challenge.json")
	require.NoError(t, e)
	var challenge bootprotocol.InitialPrebootChallenge
	require.NoError(t, json.Unmarshal(challengeRaw, &challenge))
	_, e = g.PrepareInitialPreboot(ctx, challenge)
	require.NoError(t, e)
	_, e = g.PrepareNormalSplitTopology(ctx)
	require.NoError(t, e)
	_, e = ProvisionGuest(ctx)
	require.NoError(t, e)
	require.NoError(t, CheckGuest(ctx))
	require.NoError(t, startInitialNormalSystemV3Broker(ctx, g), "actual production thin Source capture + Start, original4FD + typedFS recover + listener probe/ready/COMMIT")
	m, e := readNormalSystemRoleServiceMetadata()
	require.NoError(t, e)
	require.NotEmpty(t, m.ReadySHA256)
	require.NotEqual(t, m.Origin.Parent.RootConsumer, m.Broker.RootConsumer)
	fmt.Println("NEON-S-SYSTEM-ROLE-SERVICE-001 PASS actual KernelPrep/Origin/PreSys/Bootstrap/Check then production original FS capture and 4FD typed broker rebind")
	fmt.Println("NEON-S-SYSTEM-ROLE-SERVICE-002 PASS actual listener probe and protected ready COMMIT ACK; original initialRoot can exit normally without broker kill")
}
