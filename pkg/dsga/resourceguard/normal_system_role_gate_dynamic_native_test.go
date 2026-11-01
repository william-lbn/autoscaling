//go:build linux && dsga_native_role_gate && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// 仅导出准确formal compute_ctl及其实际default glibc依赖；其余System角色
// 仍是同static Go站位。固定--help是DEV向量，不能冒充完整compute/PG服务。
func exportNormalSystemRoleDynamicFixture(t *testing.T) {
	material := os.Getenv("DSGA_ROLE_REAL_COMPUTE")
	if material == "" {
		return
	}
	out := os.Getenv("DSGA_SYSTEM_LIFECYCLE_EXPORT")
	root := filepath.Join(out, "root")
	put := func(p string, b []byte, mode os.FileMode) {
		dst := filepath.Join(root, p)
		require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0755))
		require.NoError(t, os.WriteFile(dst, b, mode))
	}
	raw, e := os.ReadFile(filepath.Join(root, NormalSystemSourceManifestPath))
	require.NoError(t, e)
	m, e := DecodeNormalSystemSourceManifest(raw)
	require.NoError(t, e)
	paths := []string{"/usr/local/bin/compute_ctl", "/usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2", "/usr/lib/x86_64-linux-gnu/libc.so.6", "/usr/lib/x86_64-linux-gnu/libm.so.6", "/usr/lib/x86_64-linux-gnu/libgcc_s.so.1"}
	for _, p := range paths {
		data, e := os.ReadFile(filepath.Join(material, p))
		require.NoError(t, e)
		put(p, data, 0755)
		entry := NormalSystemSourceFile{Path: p, CanonicalPath: p, Kind: "loader", Bytes: uint64(len(data)), SHA256: physicalDigest(data)}
		if p == "/usr/local/bin/compute_ctl" {
			entry.Kind = "exec"
		}
		found := false
		for i := range m.Files {
			if m.Files[i].Path == p {
				m.Files[i] = entry
				found = true
			}
		}
		if !found {
			m.Files = append(m.Files, entry)
		}
	}
	require.NoError(t, os.MkdirAll(filepath.Join(root, "lib/x86_64-linux-gnu"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "lib64"), 0755))
	for _, p := range paths[1:] {
		alias := filepath.Join(root, "lib/x86_64-linux-gnu", filepath.Base(p))
		require.NoError(t, os.Symlink(p, alias))
	}
	require.NoError(t, os.Symlink(paths[1], filepath.Join(root, "lib64/ld-linux-x86-64.so.2")))
	argv := []string{"/usr/local/bin/compute_ctl", "--help"}
	env := []string{"PATH=/neonvm/bin"}
	m.ComputeArgvSHA256, e = normalSystemVectorSHA(argv)
	require.NoError(t, e)
	m.ComputeEnvironmentSHA256, e = normalSystemVectorSHA(env)
	require.NoError(t, e)
	plan := normalSystemRoleBuildPlan{ProtocolVersion: 1, ComputeUID: 1000, ComputeGID: 1000, ComputeArgv: argv, ComputeEnv: env, LoaderPaths: []string{}}
	planRaw, e := json.Marshal(plan)
	require.NoError(t, e)
	put(normalSystemRoleBuildPlanPath, planRaw, 0444)
	m.Files = append(m.Files, NormalSystemSourceFile{Path: normalSystemRoleBuildPlanPath, CanonicalPath: normalSystemRoleBuildPlanPath, Kind: "config", Bytes: uint64(len(planRaw)), SHA256: physicalDigest(planRaw)})
	put("/etc/passwd", []byte("root:x:0:0:root:/root:/neonvm/bin/sh\npostgres:x:1000:1000:postgres:/var/db/postgres:/neonvm/bin/sh\n"), 0444)
	put("/etc/group", []byte("root:x:0:\npostgres:x:1000:\n"), 0444)
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	require.NoError(t, m.Validate())
	raw, e = json.Marshal(m)
	require.NoError(t, e)
	put(NormalSystemSourceManifestPath, raw, 0444)
	buildRaw, e := os.ReadFile(filepath.Join(root, fixedNativeBirthBuildPath))
	require.NoError(t, e)
	var build nativeBirthBuildManifest
	require.NoError(t, json.Unmarshal(buildRaw, &build))
	build.RuntimeClosure.SystemManifestSHA256 = physicalDigest(raw)
	buildRaw, e = json.Marshal(build)
	require.NoError(t, e)
	put(fixedNativeBirthBuildPath, buildRaw, 0444)
	raw, e = os.ReadFile(filepath.Join(root, FixedSealedRuntimePath))
	require.NoError(t, e)
	var d RuntimeDocument
	require.NoError(t, json.Unmarshal(raw, &d))
	templates, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	require.NoError(t, e)
	for i := range templates.Templates {
		if templates.Templates[i].Asset == "dsga-native-birth.json" {
			var f map[string]any
			require.NoError(t, json.Unmarshal(templates.Templates[i].Policy, &f))
			f["build_manifest_sha256"] = physicalDigest(buildRaw)
			templates.Templates[i].Policy, e = json.Marshal(f)
			require.NoError(t, e)
		}
	}
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(templates.Templates))
	raw, e = json.Marshal(d)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = raw
	for n, b := range assets {
		physical, e := RuntimeISOAssetName(n)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(out, "isofiles", physical), b, 0444))
	}
	put(FixedSealedRuntimePath, raw, 0444)
}

type devRoleDynamicWriter struct {
	*devNormalSystemRoleGateWriter
	source      *normalSystemLifecycleSource
	entry, main uint64
}

func (w *devRoleDynamicWriter) persistOriginalSystemRoleStopped(ctx context.Context, o normalSystemRoleObservation, ns []normalSystemRoleNamespaceIdentity, plan string) error {
	spec, e := newNormalSystemRoleLaunchSpec(ctx, w.source, "compute")
	if e != nil {
		return e
	}
	task, e := nativeRoleDynamicTask(o, w.source)
	if e != nil {
		return e
	}
	defer task.close()
	w.entry, e = verifyNormalSystemRoleExecEntry(w.source, task, spec)
	if e != nil {
		return e
	}
	g, e := normalSystemRoleReadEntry(w.source.files[w.source.logical[spec.argv[0]]])
	if e != nil {
		return e
	}
	maps, e := nativeTraceProcText(task.procfd, "maps")
	if e != nil {
		return e
	}
	w.main, _, e = normalSystemRoleMappedEntry(maps, g)
	if e != nil {
		return e
	}
	return w.devNormalSystemRoleGateWriter.persistOriginalSystemRoleStopped(ctx, o, ns, plan)
}

// 仅DEV writer在同exec-stop复用原pidfd；生产writer仍收到原已核观察。
func nativeRoleDynamicTask(o normalSystemRoleObservation, s *normalSystemLifecycleSource) (*normalSystemOriginalTask, error) {
	fd, e := unix.PidfdOpen(o.PID, 0)
	if e != nil {
		return nil, e
	}
	p := os.NewFile(uintptr(fd), "DEV second stop original target")
	defer p.Close()
	proc, e := os.Open(fmt.Sprintf("/proc/%d", o.PID))
	if e != nil {
		return nil, e
	}
	defer proc.Close()
	return newNormalSystemOriginalTask(normalSystemOriginalTaskSource{PID: o.PID, StartTicks: o.StartTicks, UID: 1000, GID: 1000, GuestBootID: s.binding.GuestBootID, PIDFD: p, ProcDirectory: proc})
}
func TestNativeNormalSystemRoleGateDynamic(t *testing.T) {
	source, e := inspectNormalSystemLifecycleOriginal(context.Background())
	require.NoError(t, e)
	defer source.close()
	require.NotEmpty(t, source.metadata[source.logical["/usr/local/bin/compute_ctl"]].Interpreter)
	ns, e := captureNormalSystemRoleNamespaces()
	require.NoError(t, e)
	defer ns.close()
	runtime, e := sealedRuntimeSource()
	require.NoError(t, e)
	state, e := lockState(context.Background(), runtime.Config.StateDir)
	require.NoError(t, e)
	defer state.close()
	ids, e := ns.snapshot()
	require.NoError(t, e)
	makeWriter := func() *devRoleDynamicWriter {
		nonce, e := newNativeBirthNonce()
		require.NoError(t, e)
		plan, e := newNormalSystemRoleLaunchSpec(context.Background(), source, "compute")
		require.NoError(t, e)
		sha, e := plan.referenceSHA256()
		require.NoError(t, e)
		pending := normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: nonce, Stage: "pending", Role: "compute", PlanSHA256: sha, Binding: source.binding, Namespace: ids}
		j := &normalSystemRoleJournal{state: state, pending: pending, seen: map[string]normalSystemRoleFileWitness{}}
		require.NoError(t, j.append(context.Background(), "pending", nil, false))
		return &devRoleDynamicWriter{devNormalSystemRoleGateWriter: &devNormalSystemRoleGateWriter{j: j}, source: source}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	writer := makeWriter()
	child, e := spawnNormalSystemRoleOriginal(ctx, source, ns, "compute", writer)
	require.NoError(t, e)
	require.NotZero(t, writer.entry)
	require.NotEqual(t, writer.entry, writer.main)
	require.Equal(t, 1, writer.calls)
	require.NoError(t, child.wait())
	require.True(t, child.waited)
	fmt.Println("NEON-S-ROLE-GATE-006 PASS actual formal PIE compute_ctl and glibc PT_INTERP entry with real ASLR before loader user instruction; uid1000 --help exit0")
	// 只改变本DEV原观察中的期望loader，不碰readonly ELF；真实loader仍原字节。
	// 第二stop必须拒绝，writer零调用，不能以主ELF entry替代动态loader。
	key := source.logical["/usr/local/bin/compute_ctl"]
	metadata := source.metadata[key]
	bad := metadata
	bad.Interpreter = "/lib/x86_64-linux-gnu/libc.so.6"
	source.metadata[key] = bad
	writer = makeWriter()
	_, e = spawnNormalSystemRoleOriginal(ctx, source, ns, "compute", writer)
	source.metadata[key] = metadata
	require.ErrorIs(t, e, ErrUnknown)
	require.Zero(t, writer.calls)
	fmt.Println("NEON-S-ROLE-GATE-007 PASS wrong expected interpreter at exact second exec-stop fails before durable release/loader user entry")
	fmt.Println("ROLE_GATE_RESULT={\"scope\":\"actual formal compute_ctl dynamic entry component; DEV --help vectors and standin other System roles\",\"normal_permit\":false,\"E\":0}")
}
