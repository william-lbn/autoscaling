//go:build linux && dsga_native_filesystem_origin && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	bootprotocol "github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

var normalFilesystemNativeMode bool

func init() {
	if len(os.Args) == 2 && os.Args[0] == "/neonvm/bin/dsga-resource-guard" && os.Args[1] == "initial-boot" && os.Getenv("DSGA_FS_COMPONENT") == "DEV369_FILESYSTEM_ORIGIN" {
		normalFilesystemNativeMode = true
		// 只改Go testing选项；/proc原kernel argv仍为实际initial-boot，生产校验照常实读。
		os.Args = []string{os.Args[0], "-test.run=^TestNativeNormalFilesystemOriginalSource$", "-test.v", "-test.timeout=100s"}
	}
}
func TestMain(m *testing.M) {
	rc := m.Run()
	if normalFilesystemNativeMode {
		for _, name := range []string{"filesystem-origin.json", "filesystem-diagnostics.json"} {
			b, e := os.ReadFile("/run/neon-dsga/" + name)
			if e == nil {
				fmt.Printf("FS_REPORT_BEGIN=%s:%d:%s\n", name, len(b), physicalDigest(b))
				for index, offset := 0, 0; offset < len(b); index, offset = index+1, offset+1024 {
					end := offset + 1024
					if end > len(b) {
						end = len(b)
					}
					fmt.Printf("FS_REPORT_CHUNK=%s:%d:%s\n", name, index, base64.StdEncoding.EncodeToString(b[offset:end]))
				}
				fmt.Printf("FS_REPORT_END=%s\n", name)
			}
		}
		fmt.Printf("FS_GUEST_EXIT=%d\n", rc)
		time.Sleep(250 * time.Millisecond)
		unix.Sync()
		if e := unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF); e != nil {
			fmt.Println("FS_REBOOT_ERROR", e)
			os.Exit(92)
		}
	}
	os.Exit(rc)
}

func TestExportNormalFilesystemFixture(t *testing.T) {
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
	guard := read(os.Getenv("DSGA_FS_GUARD_ELF"))
	busybox := read("fixture/bin/busybox")
	musl := read("fixture/lib/ld-musl-x86_64.so.1")
	vminit := read("vm-builder/files/normal-split/vminit")
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

func normalFilesystemNativeSave(name string, value any) {
	b, _ := json.MarshalIndent(value, "", "  ")
	_ = os.WriteFile("/run/neon-dsga/"+name, append(b, '\n'), 0600)
}
func TestNativeNormalFilesystemOriginalSource(t *testing.T) {
	require.True(t, normalFilesystemNativeMode, "only exact initial-boot entry inside isolated Guest")
	defer func() {
		details := map[string]any{}
		for _, p := range []string{"/proc/self/mountinfo", "/proc/1/mountinfo"} {
			b, e := os.ReadFile(p)
			if e != nil {
				details[p] = e.Error()
			} else {
				details[p] = string(b)
			}
		}
		files := map[string]string{}
		for _, name := range append(append([]string(nil), initialPrebootGuestFiles[:]...), normalFilesystemOriginIntentName, normalFilesystemOriginCompleteName) {
			b, e := os.ReadFile(filepath.Join(FixedStateDir, name))
			if e == nil {
				files[name] = string(b)
			}
		}
		details["actual_WAL"] = files
		normalFilesystemNativeSave("filesystem-diagnostics.json", details)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	source, runtimeRaw, e := runtimeSource()
	require.NoError(t, e)
	g, e := initialGuestProvisionInputs(source, runtimeRaw)
	require.NoError(t, e)
	raw, e := os.ReadFile("/neonvm/fs-challenge.json")
	require.NoError(t, e)
	var challenge bootprotocol.InitialPrebootChallenge
	require.NoError(t, json.Unmarshal(raw, &challenge))
	_, e = g.PrepareInitialPreboot(ctx, challenge)
	require.NoError(t, e, "actual KernelPrep/Origin/PreSys/8WAL")
	_, e = g.PrepareNormalSplitTopology(ctx)
	require.NoError(t, e)
	_, e = ProvisionGuest(ctx)
	require.NoError(t, e, "actual formal pre-Birth Bootstrap")
	require.NoError(t, CheckGuest(ctx))
	system, e := inspectNormalSystemLifecycleOriginal(ctx)
	require.NoError(t, e)
	defer system.close()
	pid1, e := normalFilesystemPinInitialTask(true)
	require.NoError(t, e)
	defer pid1.close()
	self, e := normalFilesystemPinInitialTask(false)
	require.NoError(t, e)
	defer self.close()
	require.Equal(t, pid1.ns.Device, self.ns.Device)
	require.Equal(t, pid1.ns.Inode, self.ns.Inode)
	require.Equal(t, pid1.rootObject, self.rootObject)
	_, allowed, e := normalFilesystemCurrentGraph(ctx, pid1.ns, pid1.rootObject)
	require.NoError(t, e)
	check := func() error { _, e := self.inspect(ctx, pid1.ns, pid1.rootObject, allowed); return e }
	rejectCapture := func() error {
		object, e := g.captureNormalFilesystemOrigin(ctx, system)
		if object != nil {
			object.close()
		}
		_, statErr := os.Stat(filepath.Join(FixedStateDir, normalFilesystemOriginIntentName))
		require.True(t, os.IsNotExist(statErr), "FD rejection must create zero FS WAL")
		return e
	}
	// 真实已经卸载的全RW ext4对象。可见17mount图恢复正常仍必须拒FD/cwd。
	view := "/run/neon-dsga/fs-detached-view"
	require.NoError(t, os.Mkdir(view, 0700))
	mountRW := func() {
		require.NoError(t, unix.Mount("/", view, "", unix.MS_BIND, ""))
		require.NoError(t, unix.Mount("", view, "", unix.MS_BIND|unix.MS_REMOUNT, ""))
	}
	mountRW()
	held, e := unix.Open(view, unix.O_PATH|unix.O_CLOEXEC, 0)
	require.NoError(t, e)
	require.NoError(t, unix.Unmount(view, unix.MNT_DETACH))
	_, _, e = normalFilesystemCurrentGraph(ctx, pid1.ns, pid1.rootObject)
	require.NoError(t, e)
	require.ErrorContains(t, rejectCapture(), "outside original RW holes")
	goodCWD, e := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	require.NoError(t, e)
	require.NoError(t, unix.Fchdir(held))
	require.NoError(t, unix.Close(held))
	require.ErrorContains(t, rejectCapture(), "outside original RW holes")
	require.NoError(t, unix.Fchdir(goodCWD))
	require.NoError(t, unix.Close(goodCWD))
	mountRW()
	fd, e := unix.Open(view+"/neonvm/fs-map.bin", unix.O_RDWR|unix.O_CLOEXEC, 0)
	require.NoError(t, e)
	shared, e := unix.Mmap(fd, 0, 4096, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	require.NoError(t, e)
	require.NoError(t, unix.Close(fd))
	require.NoError(t, unix.Unmount(view, unix.MNT_DETACH))
	require.ErrorContains(t, rejectCapture(), "outside original RW holes")
	require.NoError(t, unix.Munmap(shared))
	require.NoError(t, os.Remove(view))
	fd, e = unix.Open("/neonvm/fs-map.bin", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	require.NoError(t, e)
	cow, e := unix.Mmap(fd, 0, 4096, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE)
	require.NoError(t, e)
	require.NoError(t, unix.Close(fd))
	cow[0] = 42
	require.NoError(t, check(), "private COW on readonly ELF/data must remain allowed")
	origin, e := g.captureNormalFilesystemOrigin(ctx, system)
	require.NoError(t, e)
	defer origin.close()
	require.NoError(t, unix.Munmap(cow))
	observed, e := origin.observe(ctx)
	require.NoError(t, e)
	require.True(t, observed.DurabilityConfirmed)
	require.False(t, observed.NormalDecreaseAuthorized)
	require.False(t, observed.ContinuousWritersClosed)
	require.ErrorIs(t, origin.requireContinuousQualification(), ErrUnknown)
	// 真正PG身份在原ext4孔remove/create PGDATA和warmup兄弟；没有实际PG/SQL。
	data := exec.Command("/neonvm/bin/busybox", "sh", "-c", "set -eu; /neonvm/bin/busybox mkdir /var/db/postgres/compute; printf first > /var/db/postgres/compute/old; /neonvm/bin/busybox rm /var/db/postgres/compute/old; /neonvm/bin/busybox rmdir /var/db/postgres/compute; /neonvm/bin/busybox mkdir /var/db/postgres/compute /var/db/postgres/compute.warmup; printf PGDATA_EXT4 > /var/db/postgres/compute/fs.bin; printf WARMUP_EXT4 > /var/db/postgres/compute.warmup/fs.bin; /neonvm/bin/busybox sync")
	data.Env = []string{"PATH=/neonvm/bin"}
	data.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000}}
	output, err := data.CombinedOutput()
	require.NoError(t, err, string(output))
	// 捕获后有其它真实task仍只复核原引用，不把runtime observe偷换成新的两task census。
	childPIDFD := -1
	child := exec.Command("/neonvm/bin/busybox", "sleep", "60")
	child.Env = []string{"PATH=/neonvm/bin"}
	child.SysProcAttr = &syscall.SysProcAttr{PidFD: &childPIDFD}
	require.NoError(t, child.Start())
	require.GreaterOrEqual(t, childPIDFD, 0)
	_, err = origin.observe(ctx)
	require.NoError(t, unix.PidfdSendSignal(childPIDFD, unix.SIGKILL, nil, 0))
	waitErr := child.Wait()
	require.Error(t, waitErr)
	require.True(t, child.ProcessState.Exited() || child.ProcessState.Sys().(syscall.WaitStatus).Signaled())
	require.NoError(t, unix.Close(childPIDFD))
	require.NoError(t, err, "runtime Source must not require original two-task census")
	// 新Guard可见完整JSON也不自动得到原见证；显式Recover仅同步原完整10WAL。
	newGuard, e := Open(source.Config)
	require.NoError(t, e)
	rawBefore := map[string]string{}
	for _, name := range []string{normalFilesystemOriginIntentName, normalFilesystemOriginCompleteName} {
		b, e := os.ReadFile(filepath.Join(FixedStateDir, name))
		require.NoError(t, e)
		rawBefore[name] = physicalDigest(b)
	}
	// 原始只读引用和真正移交的namespace/root FD必须是相同Layout2合同。
	// 故意给零版本只应在输入核验阶段拒绝；不能调用后面的显式Recover来补造见证。
	s, e := lockState(ctx, source.Config.StateDir)
	require.NoError(t, e)
	defer s.close()
	reference, e := origin.referenceLocked(ctx, s)
	require.NoError(t, e)
	pinned, ns, root, e := origin.pinOriginalNamespaceLocked(ctx, s)
	require.NoError(t, e)
	defer ns.Close()
	defer root.Close()
	require.Equal(t, uint32(2), reference.LayoutVersion)
	require.Equal(t, reference, pinned)
	_, _, prefix, e := normalFilesystemInitialPrefix(s, system.binding, false)
	require.NoError(t, e)
	witness, e := normalFilesystemWitnessLocked(newGuard, s, system.binding.RootConsumer, prefix)
	require.NoError(t, e)
	_, beforeWitness := normalFilesystemOriginWitnesses.Load(witness)
	require.False(t, beforeWitness, "new Guard must not inherit a visible complete prefix")
	wrong := pinned
	wrong.LayoutVersion = 0
	rejected, e := newGuard.rebindNormalFilesystemOriginLocked(ctx, s, system, nil, wrong, ns, root)
	require.Nil(t, rejected)
	require.ErrorContains(t, e, "transferred original reference differs")
	_, afterRejectWitness := normalFilesystemOriginWitnesses.Load(witness)
	require.False(t, afterRejectWitness, "invalid reference must return before recovery/sync enrollment")
	for name, h := range rawBefore {
		b, readErr := os.ReadFile(filepath.Join(FixedStateDir, name))
		require.NoError(t, readErr)
		require.Equal(t, h, physicalDigest(b))
	}
	// 只有准确原引用和原FD才进入显式恢复；同步现有10WAL而不重新捕获两task。
	recovered, e := newGuard.rebindNormalFilesystemOriginLocked(ctx, s, system, nil, pinned, ns, root)
	require.NoError(t, e)
	defer recovered.close()
	recovered.mu.Lock()
	final, e := recovered.observeLocked(ctx, s)
	recovered.mu.Unlock()
	require.NoError(t, e)
	require.Equal(t, observed.IntentSHA256, final.IntentSHA256)
	for name, h := range rawBefore {
		b, e := os.ReadFile(filepath.Join(FixedStateDir, name))
		require.NoError(t, e)
		require.Equal(t, h, physicalDigest(b))
	}
	normalFilesystemNativeSave("filesystem-origin.json", map[string]any{"id": "PG17-S-FILESYSTEM-ORIGIN-001", "scope": "REAL_369_SWITCH_ROOT_EXT4_DEV_SOURCE_NOT_PG_NORMAL", "observation": final, "same_namespace_and_root_as_PID1": true, "hidden_detached_RW_O_PATH_rejected": true, "hidden_RW_cwd_rejected": true, "hidden_shared_write_mapping_rejected": true, "private_COW_allowed": true, "new_Guard_explicit_sync_no_bytes_changed": true, "reference_and_pinned_layout_version": reference.LayoutVersion, "reference_equals_namespace_root_FD_handoff": reference == pinned, "zero_layout_rejected_before_recovery": true, "rebind_exact_original_FDs_passed": true, "actual_original_8_preparation_WAL": true, "actual_formal_Bootstrap": true, "PGDATA_recreate_warmup_actual_UID1000": true, "runtime_observe_with_other_live_task": true, "actual_System_programs_are_standins": true, "Host_challenge_is_fixture": true, "PG_executed": false, "normal_authorized": false, "E": 0})
}
