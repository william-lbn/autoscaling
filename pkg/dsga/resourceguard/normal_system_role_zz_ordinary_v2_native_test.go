//go:build linux && amd64 && dsga_native_ordinary2 && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func init() {
	if len(os.Args) > 1 && os.Args[0] == normalSystemRoleBrokerExecutable && strings.Contains(os.Args[1], "TestNativeNormalSystemRoleTransferV2Initial") {
		os.Args = []string{os.Args[0], "-test.run=^TestNativeNormalSystemRoleOrdinaryV2Initial$", "-test.v", "-test.timeout=150s"}
	}
	normalRoleServiceAdditionalDEVDispatch = ordinary2DEVDispatch
}
func TestExportNormalSystemRoleOrdinaryV2Fixture(t *testing.T) {
	t.Setenv("DSGA_SYSTEM_ROLE_ELF", "fixture/system-role")
	ordinary2DEVExportTransfer2(t)
	out := os.Getenv("DSGA_SYSTEM_LIFECYCLE_EXPORT")
	root := filepath.Join(out, "root")
	read := func(name string) []byte { b, e := os.ReadFile(name); require.NoError(t, e); return b }
	put := func(name string, b []byte, mode os.FileMode) {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(root, name), b, mode))
	}
	m, e := DecodeNormalSystemSourceManifest(read(filepath.Join(root, NormalSystemSourceManifestPath)))
	require.NoError(t, e)
	raw, e := NormalSystemOrdinaryV2ManifestJSON()
	require.NoError(t, e)
	put(NormalSystemOrdinaryV2ManifestPath, raw, 0444)
	guard := read(filepath.Join(root, normalSystemRoleBrokerExecutable))
	vmstart := read(filepath.Join(root, "/neonvm/bin/vmstart"))
	vmstart = []byte(strings.ReplaceAll(string(vmstart), "/run/neon-dsga/vmstart.allowed", "/run/neon-ordinary/allow-start/vmstart.allowed"))
	put("/neonvm/bin/vmstart", vmstart, 0755)
	put("/usr/local/bin/compute_ctl", guard, 0755)
	// network为明确DEV长目标，不冒实际udhcpc/DHCP兼容；真实BusyBox的touch/uname不换。
	require.NoError(t, os.Remove(filepath.Join(root, "/neonvm/bin/udhcpc")))
	put("/neonvm/bin/udhcpc", guard, 0755)
	for n := range m.Files {
		if m.Files[n].Path == "/neonvm/bin/vmstart" {
			m.Files[n].SHA256 = physicalDigest(vmstart)
			m.Files[n].Bytes = uint64(len(vmstart))
		}
		if m.Files[n].Path == "/usr/local/bin/compute_ctl" {
			m.Files[n].SHA256 = physicalDigest(guard)
			m.Files[n].Bytes = uint64(len(guard))
		}
		if m.Files[n].Path == NormalSystemOrdinaryV2ManifestPath {
			m.Files[n].SHA256 = physicalDigest(raw)
			m.Files[n].Bytes = uint64(len(raw))
		}
		if m.Files[n].Path == "/neonvm/bin/udhcpc" {
			m.Files[n].CanonicalPath = m.Files[n].Path
			m.Files[n].SHA256 = physicalDigest(guard)
			m.Files[n].Bytes = uint64(len(guard))
		}
	}
	// 原DEV runner过去在mkfs前补这两账号；本新fixture必须连普通2账号一起
	// 在构建期生成并进入最终RO root。新runner只保留，不于Guest出生后补签。
	passwd := []byte("root:x:0:0:root:/:/neonvm/bin/sh\npostgres:x:1000:1000:PG:/var/db/postgres:/neonvm/bin/sh\n")
	groups := []byte("root:x:0:\npostgres:x:1000:\n")
	for _, role := range normalSystemOrdinaryV2Expected().Roles {
		passwd = append(passwd, []byte(fmt.Sprintf("%s:x:%d:%d::/nonexistent:/neonvm/bin/false\n", role.User, role.UID, role.GID))...)
		groups = append(groups, []byte(fmt.Sprintf("%s:x:%d:\n", role.Group, role.GID))...)
	}
	require.NoError(t, ValidateNormalSystemOrdinaryV2InstalledAccounts(passwd, groups, 1000, 1000))
	put("/etc/passwd", passwd, 0644)
	put("/etc/group", groups, 0644)
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	require.NoError(t, m.Validate())
	mraw, e := json.Marshal(m)
	require.NoError(t, e)
	put(NormalSystemSourceManifestPath, mraw, 0444)
	var build nativeBirthBuildManifest
	require.NoError(t, json.Unmarshal(read(filepath.Join(root, fixedNativeBirthBuildPath)), &build))
	build.RuntimeClosure.SystemManifestSHA256 = physicalDigest(mraw)
	braw, e := json.Marshal(build)
	require.NoError(t, e)
	put(fixedNativeBirthBuildPath, braw, 0444)
	var d RuntimeDocument
	require.NoError(t, json.Unmarshal(read(filepath.Join(root, FixedSealedRuntimePath)), &d))
	templates, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	require.NoError(t, e)
	for n := range templates.Templates {
		if templates.Templates[n].Asset == "dsga-native-birth.json" {
			var v map[string]any
			require.NoError(t, json.Unmarshal(templates.Templates[n].Policy, &v))
			v["build_manifest_sha256"] = physicalDigest(braw)
			templates.Templates[n].Policy, e = json.Marshal(v)
			require.NoError(t, e)
		}
	}
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(templates.Templates))
	draw, e := json.Marshal(d)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(draw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = draw
	plan, e := DeriveRuntimeBootRequirements(draw, assets)
	require.NoError(t, e)
	for name, b := range assets {
		physical, e := RuntimeISOAssetName(name)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(out, "isofiles", physical), b, 0444))
	}
	put(FixedSealedRuntimePath, draw, 0444)
	require.NoError(t, os.WriteFile(filepath.Join(out, "runtime.json"), draw, 0444))
	require.NoError(t, os.WriteFile(filepath.Join(out, "required-kernel-args.txt"), []byte(strings.Join(plan.RequiredKernelArgs(), " ")+"\n"), 0444))
}
func ordinary2DEVPrepareFixedDirectories(t *testing.T) {
	require.NoError(t, os.MkdirAll("/run/neon-ordinary", 0755))
	require.NoError(t, os.Chown("/run/neon-ordinary", 0, 0))
	require.NoError(t, os.Chmod("/run/neon-ordinary", 0755))
	// 仅在真实FS拍原始图前创建被测两个角色的固定tmpfs私有目录；不改生产stock回调。
	for _, name := range []string{"allow-start", "network"} {
		role, e := NormalSystemOrdinaryV2RolePlan(name)
		require.NoError(t, e)
		for _, dir := range role.PrivatePaths {
			if dir == "/etc/resolv.conf" {
				require.NoError(t, os.Chown(dir, int(role.UID), int(role.GID)))
				require.NoError(t, os.Chmod(dir, 0600)) // DEV独立私有文件，不声称真实DNS/PG兼容。
				continue
			}
			require.NoError(t, os.MkdirAll(dir, 0700))
			require.NoError(t, os.Chown(dir, int(role.UID), int(role.GID)))
			require.NoError(t, os.Chmod(dir, 0700))
		}
	}
}
func ordinary2DEVRecord(stage string, p *normalSystemRoleV2Publication) error {
	if p == nil || p.installed == nil {
		return ErrUnknown
	}
	i := p.installed
	result := map[string]any{"stage": stage, "role": i.role, "nonce": i.nonce, "pid": i.task.source.PID, "uid": i.task.source.UID, "gid": i.task.source.GID, "original_three_prefixes": i.prefixes, "stdio_same_OFD_at_original_exec_stop": true, "full_source": false, "normal": false}
	if stage == "stopped" {
		if _, e := i.g.normalSystemOriginalRoleMainLeaseLocked(context.Background(), p.journal.state, p.record, i.role); e == nil {
			return fmt.Errorf("candidate authorized")
		}
		if i.role == "compute" {
			lease, e := i.g.normalSystemOriginalComputeLeaseLocked(context.Background(), p.journal.state, p.record)
			if e == nil {
				lease.close()
				return fmt.Errorf("original Compute candidate authorized before Detach")
			}
			result["compute_candidate_getter_rejected_before_Detach"] = true
		}
		status, e := nativeTraceProcText(i.task.procfd, "status")
		if e != nil {
			return e
		}
		// 原完整status已由停点reader核过，串口只传实际解析的必要字段，避免DEV
		// 大status在resource锁内阻塞长时间；摘要供准确关联，不输出env/日志正文。
		values := map[string]string{}
		for _, line := range strings.Split(status, "\n") {
			for _, key := range []string{"Uid", "Gid", "CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb", "NoNewPrivs", "Seccomp", "Seccomp_filters", "Threads"} {
				if strings.HasPrefix(line, key+":") {
					values[key] = strings.TrimSpace(strings.TrimPrefix(line, key+":"))
				}
			}
		}
		result["actual_status_fields"] = values
		result["actual_status_sha256"] = physicalDigest([]byte(status))
		if controls, ok := i.controls.(*normalSystemOrdinaryV2InstalledControlSource); ok {
			result["actual_BPF_program_sha256"] = controls.filterSHA
		}
	}
	if stage == "wait-confirmed" {
		wait, e := i.child.originalRoleWaitConfirmed(i.creator)
		if e != nil {
			return e
		}
		result["actual_unique_Wait_status"] = uint32(wait.Status)
		result["actual_creator_thread_poll_exit"] = true
	}
	if stage == "active" {
		if i.role == "compute" {
			lease, e := i.g.normalSystemOriginalComputeLeaseLocked(context.Background(), p.journal.state, p.record)
			if e != nil {
				return e
			}
			e = arrayMatchPIDFD(lease.task.pidfd, i.task.pidfd)
			lease.close()
			if e != nil {
				return e
			}
			result["compute_original_lease_same_clone_before_resource_unlock"] = true
		}
		lease, e := i.g.normalSystemOriginalRoleMainLeaseLocked(context.Background(), p.journal.state, p.record, i.role)
		if e != nil {
			exited, x := normalSystemRoleGateExitObserved(i.task.pidfd)
			if x != nil || !exited {
				return errors.Join(e, x)
			}
			result["exited_after_active_before_probe_pending_Wait"] = true
		} else {
			lease.close()
			result["actual_original_active_lease_getter"] = true
		}
	}
	if stage == "wait-confirmed" {
		go func() {
			totals := [2]uint64{}
			hashes := [2]string{}
			for n, d := range p.endpoints.objects.drains {
				<-d.done
				d.mu.Lock()
				err := d.err
				d.mu.Unlock()
				if !errors.Is(err, io.EOF) {
					fmt.Println("ORDINARY2_EOF_ERROR", i.role, err)
					return
				}
				total, _, hash := d.summary()
				totals[n] = total
				hashes[n] = fmt.Sprintf("%x", hash)
			}
			proof := map[string]any{"role": i.role, "nonce": i.nonce, "actual_pipe_EOF": [2]bool{true, true}, "observed_bytes": totals, "bounded_tail_hashes": hashes, "main_exit_not_physical_free": true}
			raw, e := json.Marshal(proof)
			if e != nil {
				return
			}
			name := filepath.Join("/run/neon-dsga", "dev-ordinary2-"+i.role+"-"+i.nonce+"-EOF.json")
			if e = os.WriteFile(name, raw, 0600); e != nil {
				fmt.Println("ORDINARY2_EOF_RECORD_ERROR", e)
				return
			}
			fmt.Printf("ORDINARY2_EOF_JSON %s\n", raw)
		}()
	}
	raw, e := json.Marshal(result)
	if e != nil {
		return e
	}
	name := filepath.Join("/run/neon-dsga", "dev-ordinary2-"+i.role+"-"+i.nonce+"-"+stage+".json")
	f, e := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(raw)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil && ce == nil {
		fmt.Printf("ORDINARY2_AUDIT_JSON %s\n", raw)
	}
	return errors.Join(e, ce)
}
func ordinary2DEVDispatch(m *testing.M) bool {
	if len(os.Args) > 1 && os.Args[0] == normalSystemRoleBrokerExecutable {
		if os.Args[1] == "system-role-broker" {
			ctx := normalRoleServiceNativeBrokerContext(context.Background())
			ctx = context.WithValue(ctx, normalSystemRoleV2CheckpointKey{}, ordinary2DEVRecord)
			if e := RunNormalSystemRoleBroker(ctx); e != nil {
				fmt.Println("ORDINARY2_BROKER_ERROR", e)
				os.Exit(43)
			}
			os.Exit(0)
		}
		if os.Args[1] == "exec-system-role" && len(os.Args) == 3 {
			role := os.Args[2]
			if role != "allow-start" && role != "kernel-version" && role != "network" && role != "compute" {
				fmt.Println("ORDINARY2_EXPLICIT_UNTESTED_ROLE", role)
				if role == "udev-init" {
					os.Exit(0)
				}
				// 不创建ordinary目标；仅保DEV客户端静候，避免BusyBox的未测角色respawn污染计数。
				for {
					time.Sleep(time.Second)
				}
			}
			count := 1
			if role == "allow-start" {
				count = 2
			}
			for n := 0; n < count; n++ {
				code, e := RunNormalSystemRoleClient(context.Background(), role)
				if e != nil || code != 0 {
					fmt.Println("ORDINARY2_CLIENT_ERROR", role, n, code, e)
					os.Exit(46)
				}
				fmt.Println("ORDINARY2_CLIENT_COMPLETED", role, n)
			}
			if role == "network" {
				for {
					time.Sleep(time.Second)
				}
			} // 已真Wait一次；固定DEV client不再重复此目标。
			if role == "compute" {
				requireCount := map[string]int{"allow-start": 2, "kernel-version": 1, "network": 1, "compute": 1}
				until := time.Now().Add(30 * time.Second)
				for {
					good := true
					for name, n := range requireCount {
						files, _ := filepath.Glob("/run/neon-dsga/dev-ordinary2-" + name + "-*-wait-confirmed.json")
						eof, _ := filepath.Glob("/run/neon-dsga/dev-ordinary2-" + name + "-*-EOF.json")
						if len(files) != n || len(eof) != n {
							good = false
						}
					}
					if good {
						break
					}
					if time.Now().After(until) {
						fmt.Println("ORDINARY2_WAIT_CHECKPOINT_TIMEOUT")
						os.Exit(61)
					}
					time.Sleep(20 * time.Millisecond)
				}
				fmt.Println("ROLE_SERVICE_GUEST_EXIT=0")
				unix.Sync()
				_ = unix.Reboot(unix.LINUX_REBOOT_CMD_POWER_OFF)
			}
			os.Exit(0)
		}
	}
	if os.Args[0] == "/neonvm/bin/udhcpc" {
		if os.Getuid() != 60003 || os.Geteuid() != 60003 || os.Getgid() != 60003 {
			os.Exit(71)
		}
		if len(os.Args) > 1 && os.Args[1] == "--ordinary2-dev-worker" {
			fmt.Println("ORDINARY2_SAME_SCOPE_WORKER_UID60003")
			os.Exit(0)
		}
		c := exec.Command(os.Args[0], "--ordinary2-dev-worker")
		c.Env = []string{"PATH=/neonvm/bin"}
		c.Stdin = os.Stdin
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		if e := c.Run(); e != nil {
			fmt.Println("ORDINARY2_WORKER_ERROR", e)
			os.Exit(72)
		}
		fmt.Println("ORDINARY2_LONG_TARGET_UID60003_RUNNING")
		time.Sleep(12 * time.Second)
		os.Exit(0)
	}
	if os.Args[0] == "/usr/local/bin/compute_ctl" {
		if os.Getuid() != 1000 {
			os.Exit(73)
		}
		time.Sleep(14 * time.Second)
		os.Exit(0)
	}
	return false
}
func TestNativeNormalSystemRoleOrdinaryV2Initial(t *testing.T) {
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
	ordinary2DEVPrepareFixedDirectories(t)
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

func ordinary2DEVExportServiceV3(t *testing.T) {
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
	broker, e := NormalSystemRoleBrokerDeclarationV2JSON(physicalDigest(guard), physicalDigest(planRaw))
	if os.Getenv("DSGA_TEST_BROKER_DECLARATION_VERSION") == "1" {
		broker, e = NormalSystemRoleBrokerDeclarationJSON(physicalDigest(guard), physicalDigest(planRaw))
	}
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
	source.ControlPublicKey = base64.RawURLEncoding.EncodeToString(normalAuthorityDEVKey().Public().(ed25519.PublicKey))
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
	// 本独立subset夹具的未执行服务保留9KiB原C占位，不把Go大ELF复制到每个角色。
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

func ordinary2DEVExportTransfer2(t *testing.T) {
	normalRoleServiceExportFilesystemBase(t)
	ordinary2DEVExportServiceV3(t)
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
