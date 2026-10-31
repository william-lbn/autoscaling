//go:build linux && dsga_native_prebootstrap_system

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// 生成明确隔离harness原ISO；小预算仅便于在base-only Guest实际执行旧正式Bootstrap。
// 不认证原Host预留、完整F/I/K或生产profile业务容量。
func TestExportPreSystemNativeFixture(t *testing.T) {
	require.Equal(t, "/out/isofiles", os.Getenv("DSGA_PRESYS_EXPORT"))
	f := newPreSystemFixtureProfile(t, NormalSplitV1PhysicalKernelProfileID)
	source := f.in.source
	source.Limits.MemoryMaxBytes = 64 << 20
	source.Limits.SystemMemoryMaxBytes = 64 << 20
	source.Limits.OutsideUpperBytes = 128 << 20
	source.Limits.PidsMaxTasks = 0
	source.Limits.AddressSpaceMaxBytes = 0
	require.NoError(t, source.Validate())
	raw, e := json.Marshal(source)
	require.NoError(t, e)
	document := RuntimeDocument{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ConfigSecretUID: "isolated-presys-v2-config", SourceSHA256: physicalDigest(raw), SourceJSON: raw}
	runtimeRaw, e := json.Marshal(document)
	require.NoError(t, e)
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	require.NoError(t, e)
	physical := PhysicalKernelOriginPolicy{ProtocolVersion: 1, ProfileID: ref.ProfileID, KernelRelease: ref.KernelRelease, KernelArtifactSHA256: ref.KernelArtifactSHA256, KernelConfigSHA256: ref.KernelConfigSHA256, KernelNotesSHA256: ref.KernelNotesSHA256, RuntimeDocumentSHA256: physicalDigest(runtimeRaw)}
	physicalRaw, e := json.Marshal(physical)
	require.NoError(t, e)
	kernel := NormalV1KernelBootstrapPolicy{ProtocolVersion: 1, ProfileID: ref.ProfileID, RuntimeDocumentSHA256: physicalDigest(runtimeRaw), PhysicalOriginPolicySHA256: physicalDigest(physicalRaw), Controls: NormalV1KernelAllocatorControls{PIDMax: 512, MinFreeKBytes: 4096, WatermarkScaleFactor: 10, PerCPUPagelistHighFraction: 128}, ModulesDisabled: 1}
	kernelRaw, e := json.Marshal(kernel)
	require.NoError(t, e)
	policy := prebootstrapSystemPolicy{ProtocolVersion: 2, Scope: preSystemScope, PhysicalProfileID: ref.ProfileID, RuntimeDocumentSHA256: physicalDigest(runtimeRaw), FilesystemProfileSHA256: physicalDigest([]byte("isolated ext4/read-only view reference, not full F qualification")), BootMemoryUpperBytes: 512 << 20}
	policyRaw, e := json.Marshal(policy)
	require.NoError(t, e)
	dir := os.Getenv("DSGA_PRESYS_EXPORT")
	require.NoError(t, os.MkdirAll(dir, 0755))
	for n, b := range map[string][]byte{"dsga-resource.json": runtimeRaw, "dsga-physical-kernel.json": physicalRaw, "dsga-kernel-bootstrap.json": kernelRaw, "dsga-prebootstrap-system.json": policyRaw} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, n), b, 0444))
	}
}

func preSystemNativeRole(t *testing.T, path string) func() {
	t.Helper()
	command := exec.Command("/bin/busybox", "sleep", "120")
	require.NoError(t, command.Start())
	pid := command.Process.Pid
	require.NoError(t, writeNumber(path+"/cgroup.procs", uint64(pid)))
	return func() { _ = command.Process.Kill(); _ = command.Wait() }
}
func preSystemNativeLeaves(t *testing.T) {
	t.Helper()
	f, e := rootFile(pgCgroup+"/cgroup.subtree_control", true)
	require.NoError(t, e)
	n, e := f.Write([]byte("+memory +pids"))
	require.NoError(t, e)
	require.Equal(t, len("+memory +pids"), n)
	require.NoError(t, f.Close())
	for _, name := range []string{"private", "arrays"} {
		require.NoError(t, os.Mkdir(pgCgroup+"/"+name, 0755))
	}
}

// 实际369/ISO/readonly RootFS/ext4、真实KernelPrep与Origin发行、真实System迁组。
// 角色是sleep测试进程而非PG；不授RootBirth、正常PG容量、safe-normal或K8s资格。
func TestNativePreSystemSplitComponent(t *testing.T) {
	require.Equal(t, "isolated-kvm-presys-v2", os.Getenv("DSGA_PRESYS_NATIVE"))
	source, _, e := runtimeSource()
	require.NoError(t, e)
	g, e := Open(source.Config)
	require.NoError(t, e)
	kernel, e := g.PrepareKernelBeforeOrigin(context.Background())
	require.NoError(t, e)
	require.True(t, kernel.DurabilityConfirmed())
	origin, e := g.CapturePhysicalKernelOrigin(context.Background())
	require.NoError(t, e)
	require.Equal(t, NormalSplitV1PhysicalKernelProfileID, origin.Policy.ProfileID)
	preSystemNativeLeaves(t)
	mode := os.Getenv("DSGA_PRESYS_MODE")
	if mode == "initial-private-role" {
		stop := preSystemNativeRole(t, pgCgroup+"/private")
		defer stop()
		top, e := kernelText(pgCgroup + "/cgroup.procs")
		require.NoError(t, e)
		require.Empty(t, top)
		empty, e := preSystemRecursivePGEmpty()
		require.NoError(t, e)
		require.False(t, empty)
		_, e = g.PrepareSystemBeforeBootstrap(context.Background())
		require.Error(t, e)
		_, e = os.Stat(filepath.Join(source.Config.StateDir, preSystemIntentName))
		require.True(t, os.IsNotExist(e))
		max, e := readNumber(systemCgroup+"/memory.max", true)
		require.NoError(t, e)
		require.Equal(t, ^uint64(0), max)
		fmt.Println(`PRESYS_NATIVE_NEGATIVE={"case":"initial-private-role","parent_direct_empty":true,"recursive_pg_empty":false,"pass":true}`)
		return
	}
	require.Contains(t, []string{"completed-roles", "overcommit-corrective"}, mode)
	placed, e := g.PrepareSystemBeforeBootstrap(context.Background())
	require.NoError(t, e)
	require.True(t, placed.DurabilityConfirmed)
	require.True(t, placed.Current.System.PID1Member)
	require.Zero(t, placed.Current.System.RootUserspaceCount)
	require.Equal(t, uint32(2), placed.Original.Policy.ProtocolVersion)
	stop := preSystemNativeRole(t, pgCgroup+"/private")
	_, e = g.ObserveSystemBeforeBootstrap(context.Background())
	require.Error(t, e, "PG role before formal Bootstrap must fail")
	stop()
	if mode == "overcommit-corrective" {
		require.NoError(t, writeNumber("/sys/kernel/mm/hugepages/hugepages-2048kB/nr_overcommit_hugepages", 1))
		actual, err := readNumber("/sys/kernel/mm/hugepages/hugepages-2048kB/nr_overcommit_hugepages", false)
		require.NoError(t, err)
		require.Equal(t, uint64(1), actual)
	}
	_, e = g.Bootstrap(context.Background(), origin.ExpectedBootstrap)
	require.NoError(t, e, "small explicit harness budget must pass real applied Bootstrap")
	if mode == "overcommit-corrective" {
		actual, err := readNumber("/sys/kernel/mm/hugepages/hugepages-2048kB/nr_overcommit_hugepages", false)
		require.NoError(t, err)
		require.Zero(t, actual)
		fmt.Println(`PRESYS_OVERCOMMIT_CORRECTION={"page_size_kb":2048,"before":1,"after":0,"same_original_zero_plan":true}`)
	}

	stateRaw, err := os.ReadFile(filepath.Join(source.Config.StateDir, "state.json"))
	require.NoError(t, err)
	var formal record
	require.NoError(t, json.Unmarshal(stateRaw, &formal))
	require.NotNil(t, formal.Bootstrap)
	require.True(t, formal.Bootstrap.Applied)
	require.True(t, formal.Bootstrap.SystemPlacementObserved)
	require.Equal(t, uint32(2), formal.Bootstrap.PreSystemStageVersion)
	require.NotNil(t, formal.Bootstrap.PreSystem)
	original := formal.Bootstrap.PreSystem
	require.NoError(t, original.validate())
	for path, want := range map[string]string{
		filepath.Join(source.Config.StateDir, preSystemIntentName):    original.IntentSHA256,
		filepath.Join(source.Config.StateDir, preSystemCompleteName):  original.CompletionSHA256,
		filepath.Join(source.Config.StateDir, physicalOriginDataName): original.OriginSHA256,
		FixedPrebootstrapSystemPolicyPath:                             original.PolicySHA256,
	} {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, physicalDigest(raw), want)
	}
	_, err = g.placeSystem(context.Background(), func(context.Context) error {
		t.Fatal("confirmed original placement must never move userspace again")
		return nil
	})
	require.NoError(t, err)
	stateAfter, err := os.ReadFile(filepath.Join(source.Config.StateDir, "state.json"))
	require.NoError(t, err)
	require.Equal(t, stateRaw, stateAfter, "read-only placement must not rewrite original formal state")

	for _, role := range []string{"private", "arrays"} {
		stop := preSystemNativeRole(t, pgCgroup+"/"+role)
		observation, e := g.ObserveSystemBeforeBootstrap(context.Background())
		stop()
		require.NoError(t, e, "completed legal role must reach readonly observation")
		require.False(t, observation.DurabilityConfirmed)
		require.False(t, observation.VMShrinkPermit)
		require.Equal(t, placed.Original.Nonce, observation.Original.Nonce)
	}
	// 未知域同样由真实/proc cgroup角色census拒绝；不把路径前缀当授权。
	require.NoError(t, os.Mkdir(pgCgroup+"/unexpected", 0755))
	stop = preSystemNativeRole(t, pgCgroup+"/unexpected")
	_, e = g.ObserveSystemBeforeBootstrap(context.Background())
	stop()
	require.Error(t, e)
	// 在已启用memory子树的父域不能放task；内核自身必须拒绝这条绕行。
	command := exec.Command("/bin/busybox", "sleep", "120")
	require.NoError(t, command.Start())
	e = writeNumber(pgCgroup+"/cgroup.procs", uint64(command.Process.Pid))
	_ = command.Process.Kill()
	_ = command.Wait()
	require.Error(t, e, "split parent direct task must be rejected by hierarchy")
	before, e := os.ReadFile(filepath.Join(source.Config.StateDir, preSystemIntentName))
	require.NoError(t, e)
	again, e := g.ObserveSystemBeforeBootstrap(context.Background())
	require.NoError(t, e)
	after, e := os.ReadFile(filepath.Join(source.Config.StateDir, preSystemIntentName))
	require.NoError(t, e)
	require.Equal(t, before, after)
	record := struct {
		Case          string         `json:"case"`
		Nonce         string         `json:"nonce"`
		Identity      Identity       `json:"identity"`
		System        SystemEvidence `json:"system"`
		PolicyVersion uint32         `json:"policy_version"`
		RoleProcesses string         `json:"role_processes"`
		RootBirth     bool           `json:"root_birth"`
		NormalPermit  bool           `json:"normal_permit"`
	}{mode, again.Original.Nonce, again.Current.Physical.Identity, again.Current.System, 2, "sleep fixtures PID " + strconv.Itoa(os.Getpid()), false, false}
	raw, e := json.Marshal(record)
	require.NoError(t, e)
	fmt.Printf("PRESYS_NATIVE_COMPONENT=%s\n", raw)
}
