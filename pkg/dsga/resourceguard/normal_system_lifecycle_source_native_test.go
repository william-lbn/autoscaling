//go:build linux && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// DEV：本夹具所有System程序是同一个实际静态C ELF占位，PG文件也只是独立
// 声明对象而不执行。真实reader/ELF/kernel/task事实不等于真实System服务/PG资格。
func TestExportNormalSystemLifecycleFixture(t *testing.T) {
	out := os.Getenv("DSGA_SYSTEM_LIFECYCLE_EXPORT")
	elfPath := os.Getenv("DSGA_SYSTEM_ROLE_ELF")
	require.NotEmpty(t, out)
	require.NotEmpty(t, elfPath)
	elf, e := os.ReadFile(elfPath)
	require.NoError(t, e)
	root := filepath.Join(out, "root")
	require.NoError(t, os.MkdirAll(root, 0755))
	put := func(p string, b []byte, mode os.FileMode) {
		dst := filepath.Join(root, p)
		require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0755))
		require.NoError(t, os.WriteFile(dst, b, mode))
	}
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	require.NoError(t, e)
	argv := []string{"/usr/local/bin/compute_ctl", "--pgdata", "/var/db/postgres/compute"}
	env := []string{"PATH=/neonvm/bin"}
	ah, e := normalSystemVectorSHA(argv)
	require.NoError(t, e)
	eh, e := normalSystemVectorSHA(env)
	require.NoError(t, e)
	m := NormalSystemSourceManifest{ProtocolVersion: 2, ProfileID: NormalSystemSourceV2ProfileID, Kernel: ref, DeclaredImageSHA256: strings.Repeat("ab", 32), ComputePath: argv[0], ComputeArgvSHA256: ah, ComputeEnvironmentSHA256: eh}
	required, e := NormalSystemSourceRequiredFilesForVersion(2)
	require.NoError(t, e)
	for p, kind := range required {
		data := []byte("DEV static configuration\n")
		mode := os.FileMode(0644)
		canonical := p
		if kind == "exec" {
			data = elf
			mode = 0755
		}
		if kind == "script" {
			data = []byte("#!/neonvm/bin/sh\nexit 1\n")
			mode = 0755
		}
		if p == "/etc/inittab" {
			rows, e := NormalSystemSourceInitEntriesForVersion(2)
			require.NoError(t, e)
			data = []byte(strings.Join(rows, "\n") + "\n")
		}
		if p == "/neonvm/bin/sh" || p == "/neonvm/bin/cat" || p == "/neonvm/bin/touch" || p == "/neonvm/bin/udhcpc" {
			canonical = "/neonvm/bin/busybox"
			dst := filepath.Join(root, p)
			require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0755))
			require.NoError(t, os.Symlink(canonical, dst))
		} else {
			put(p, data, mode)
		}
		m.Files = append(m.Files, NormalSystemSourceFile{Path: p, CanonicalPath: canonical, Kind: kind, Bytes: uint64(len(data)), SHA256: physicalDigest(data)})
	}
	for _, p := range NormalSystemSourceConfigDirectories() {
		require.NoError(t, os.MkdirAll(filepath.Join(root, p), 0755))
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	require.NoError(t, m.Validate())
	systemRaw, e := json.Marshal(m)
	require.NoError(t, e)
	put(NormalSystemSourceManifestPath, systemRaw, 0444)
	put(FixedPostgresPath, elf, 0755)
	build := nativeBirthBuildManifest{ProtocolVersion: 1, Scope: nativeBirthBuildScope, PGmajor: 17, LaunchPath: FixedPostgresPath, PostgresPath: FixedPostgresPath, Files: []nativeBirthBuildFile{{Path: FixedPostgresPath, SHA256: physicalDigest(elf)}}, RuntimeClosure: &nativeRuntimeClosureBuild{ProtocolVersion: 1, SystemManifestSHA256: physicalDigest(systemRaw)}}
	buildRaw, e := json.Marshal(build)
	require.NoError(t, e)
	require.NoError(t, build.validate())
	put(fixedNativeBirthBuildPath, buildRaw, 0444)
	d := runtimeTemplatesFullFixture(t)
	var source SourceConfig
	require.NoError(t, json.Unmarshal(d.SourceJSON, &source))
	source.Config.SystemCgroup = true
	source.Limits.SystemMemoryMaxBytes = 64 << 20
	source.Limits.OutsideUpperBytes = 128 << 20
	source.Limits.MemoryMaxBytes = 768 << 20
	source.Limits.PidsMaxTasks = 128
	source.Limits.AddressSpaceMaxBytes = 4 << 30
	d.SourceJSON, e = json.Marshal(source)
	require.NoError(t, e)
	d.SourceSHA256 = physicalDigest(d.SourceJSON)
	templates, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	require.NoError(t, e)
	for i := range templates.Templates {
		if templates.Templates[i].Asset == "dsga-native-birth.json" {
			var fields map[string]any
			require.NoError(t, json.Unmarshal(templates.Templates[i].Policy, &fields))
			fields["build_manifest_sha256"] = physicalDigest(buildRaw)
			templates.Templates[i].Policy, e = json.Marshal(fields)
			require.NoError(t, e)
		}
	}
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(templates.Templates))
	raw, e := json.Marshal(d)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = raw
	iso := filepath.Join(out, "isofiles")
	require.NoError(t, os.MkdirAll(iso, 0755))
	for name, data := range assets {
		physical, e := RuntimeISOAssetName(name)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(iso, physical), data, 0444))
	}
	put(FixedSealedRuntimePath, raw, 0444)
	cfg, e := json.Marshal(source.Config)
	require.NoError(t, e)
	put(FixedConfigPath, cfg, 0444)
	require.NoError(t, os.WriteFile(filepath.Join(out, "export-scope.txt"), []byte("DEV real static ELF; all services are stand-ins, no PG or Source qualification\n"), 0644))
}

func TestNativeNormalSystemLifecycleOriginalSourceAndTask(t *testing.T) {
	require.Equal(t, "DEV-369-SYSTEM-LIFECYCLE-NO-QUALIFICATION", os.Getenv("NEON_NATIVE_SYSTEM_LIFECYCLE"))
	s, e := inspectNormalSystemLifecycleOriginal(context.Background())
	require.NoError(t, e)
	defer s.close()
	o, e := s.snapshot()
	require.NoError(t, e)
	require.True(t, o.OriginalMetadataBytesBound)
	require.True(t, o.ConditionalELFClosureVerified)
	require.False(t, o.ContinuousWritersClosed)
	require.ErrorIs(t, s.requireContinuousQualification(), ErrUnknown)
	fmt.Println("NEON-S-SYSTEM-LIFECYCLE-001 PASS original ISO/build/System SHA and actual static ELF closure; Source fixture")
	group, e := os.Open(systemCgroup)
	require.NoError(t, e)
	defer group.Close()
	pidfd := -1
	cmd := exec.Command("/neonvm/bin/dsga-resource-guard", "serve")
	cmd.Env = []string{"PATH=/neonvm/bin"}
	cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(group.Fd()), PidFD: &pidfd}
	require.NoError(t, cmd.Start())
	require.GreaterOrEqual(t, pidfd, 0)
	originalFD := os.NewFile(uintptr(pidfd), "DEV original atomic clone pidfd")
	defer func() {
		require.NoError(t, unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0))
		_ = cmd.Wait()
		originalFD.Close()
	}()
	proc, e := os.Open(fmt.Sprintf("/proc/%d", cmd.Process.Pid))
	require.NoError(t, e)
	defer proc.Close()
	raw, e := nativeTraceProcText(int(proc.Fd()), "stat")
	require.NoError(t, e)
	ticks, e := postmasterTicks(raw, cmd.Process.Pid)
	require.NoError(t, e)
	in := normalSystemOriginalTaskSource{PID: cmd.Process.Pid, StartTicks: ticks, UID: 0, GID: 0, GuestBootID: o.Binding.GuestBootID, PIDFD: originalFD, ProcDirectory: proc}
	task, e := newNormalSystemOriginalTask(in)
	require.NoError(t, e)
	defer task.close()
	observed, e := s.inspectOriginalTask(context.Background(), task, "resource", []string{"/neonvm/bin/dsga-resource-guard", "serve"}, []string{"PATH=/neonvm/bin"})
	require.NoError(t, e)
	require.True(t, observed.OriginalTaskObserved)
	require.True(t, observed.FixedArgvEnvironmentObserved)
	require.False(t, observed.ContinuousWritersClosed)
	fmt.Println("NEON-S-SYSTEM-LIFECYCLE-002 PASS original clone pidfd/proc/exe/Root uid/System cgroup/argv-env hashes")
	_, e = s.inspectOriginalTask(context.Background(), task, "resource", []string{"/bin/sh"}, []string{"PATH=/neonvm/bin"})
	require.ErrorIs(t, e, ErrUnknown)
	_, e = s.inspectOriginalTask(context.Background(), task, "resource", []string{"/neonvm/bin/dsga-resource-guard", "serve"}, []string{"LD_PRELOAD=DEV_FORBIDDEN"})
	require.ErrorIs(t, e, ErrUnknown)
	bad := in
	bad.StartTicks++
	_, e = newNormalSystemOriginalTask(bad)
	require.Error(t, e)
	require.ErrorIs(t, s.requireContinuousQualification(), ErrUnknown)
	fmt.Println("NEON-S-SYSTEM-LIFECYCLE-003 PASS wrong vectors/start refuse; continuous permission remains Unknown")
	summary, e := json.Marshal(map[string]any{"scope": "DEV static C stand-ins, actual reader/kernel/task facts, not actual services or PG", "source": o, "task": observed, "normal_permit": false, "E": 0})
	require.NoError(t, e)
	fmt.Printf("SYSTEM_LIFECYCLE_RESULT=%s\n", summary)
}
