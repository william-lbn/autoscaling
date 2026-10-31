//go:build linux && dsga_native_array_service

package resourceguard

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	bootprotocol "github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"golang.org/x/sys/unix"
)

// TEST_ONLY：此key只属于隔离S夹具，不写生产ISO/不从用户凭据生成。
func arrayServiceFixtureKey() ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x51}, 32))
}
func TestExportArrayServiceFixture(t *testing.T) {
	if os.Getenv("DSGA_ARRAY_SERVICE_EXPORT") != "/out/fixture" {
		t.Fatal("explicit export required")
	}
	manifest, e := os.ReadFile("fixture/native-manifest.json")
	if e != nil {
		t.Fatal(e)
	}
	var build nativeBirthBuildManifest
	if json.Unmarshal(manifest, &build) != nil || build.ArrayHelper == nil || build.ArrayHelper.validate() != nil {
		t.Fatal("exact independent helper manifest required")
	}
	d := runtimeTemplatesFullFixture(t)
	var source SourceConfig
	if e = json.Unmarshal(d.SourceJSON, &source); e != nil {
		t.Fatal(e)
	}
	source.Limits.MemoryMaxBytes = 768 << 20
	source.Limits.SystemMemoryMaxBytes = 256 << 20
	source.Limits.OutsideUpperBytes = 512 << 20
	source.Limits.PidsMaxTasks = 128
	source.Limits.AddressSpaceMaxBytes = 4 << 30
	source.ControlPublicKey = base64.RawURLEncoding.EncodeToString(arrayServiceFixtureKey().Public().(ed25519.PublicKey))
	d.SourceJSON, _ = json.Marshal(source)
	d.SourceSHA256 = physicalDigest(d.SourceJSON)
	templates, e := parseRuntimePolicyTemplates(d.PolicyTemplatesJSON)
	if e != nil {
		t.Fatal(e)
	}
	for i := range templates.Templates {
		var fields map[string]any
		if e = json.Unmarshal(templates.Templates[i].Policy, &fields); e != nil {
			t.Fatal(e)
		}
		switch i {
		case 3:
			fields["private_memory_max_bytes"] = "134217728"
			fields["initial_arrays_memory_max_bytes"] = "268435456"
		case 4:
			fields["build_manifest_sha256"] = physicalDigest(manifest)
			fields["max_buffers"] = 8192
			fields["startup_buffers"] = 256
			fields["array_helper_protocol_version"] = 1
			fields["array_helper_path"] = fixedArrayHelperPath
			fields["array_helper_elf_sha256"] = build.ArrayHelper.SHA256
			fields["array_helper_address_space_max_bytes"] = "67108864"
			fields["array_helper_memlock_upper_bytes"] = "1048576"
			fields["array_helper_reserve_bytes"] = "68157440"
		}
		templates.Templates[i].Policy, _ = json.Marshal(fields)
	}
	d = runtimeTemplatesSetBlob(d, runtimeTemplatesEnvelope(templates.Templates))
	raw, _ := json.Marshal(d)
	assets, e := RenderRuntimePolicyAssets(raw)
	if e != nil {
		t.Fatal(e)
	}
	assets[RuntimeResourceAssetName] = raw
	if e = os.MkdirAll("/out/fixture/isofiles", 0755); e != nil {
		t.Fatal(e)
	}
	for logical, data := range assets {
		name, e := RuntimeISOAssetName(logical)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile("/out/fixture/isofiles/"+name, data, 0444); e != nil {
			t.Fatal(e)
		}
	}
	if e = os.WriteFile("/out/fixture/native-manifest.json", manifest, 0444); e != nil {
		t.Fatal(e)
	}
	requirements, e := DeriveRuntimeBootRequirements(raw, assets)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile("/out/fixture/required-kernel-args.txt", []byte(strings.Join(requirements.RequiredKernelArgs(), " ")+"\n"), 0444); e != nil {
		t.Fatal(e)
	}
	ref, _ := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	challenge := bootprotocol.InitialPrebootChallenge{ProtocolVersion: 1, Scope: bootprotocol.InitialPrebootScope, SourceSHA256: strings.Repeat("20", 32), Nonce: strings.Repeat("30", 32), HostStageSHA256: strings.Repeat("10", 32), Identity: bootprotocol.RunnerIdentity{VMUID: source.Config.VMUID, Incarnation: d.Incarnation, RunnerPodUID: "array-service-runner", RunnerBootID: "1234567890abcdef1234567890abcdef", NodeUID: "array-service-node", MinimumBytes: 1536 << 20, MaximumBytes: 2 << 30, BlockBytes: 8 << 20}, RuntimeSHA256: physicalDigest(raw), ResourceSHA256: d.SourceSHA256, ConfigSecretUID: d.ConfigSecretUID, KernelBuildSHA256: ref.KernelArtifactSHA256, OriginalBase: 256 << 20, BusinessMinimum: 1536 << 20, Maximum: 2 << 30}
	if e = challenge.Validate(); e != nil {
		t.Fatal(e)
	}
	challengeRaw, _ := json.Marshal(challenge)
	if e = os.WriteFile("/out/fixture/challenge.json", challengeRaw, 0444); e != nil {
		t.Fatal(e)
	}
}
func arrayServiceNativeSave(t *testing.T, name string, value any) {
	t.Helper()
	raw, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile("/evidence/"+name+".json", append(raw, '\n'), 0644); e != nil {
		t.Fatal(e)
	}
}
func arrayServiceNativeWait(t *testing.T, ctx context.Context, what string, predicate func() bool) {
	t.Helper()
	for !predicate() {
		if ctx.Err() != nil {
			t.Fatal(what, ctx.Err())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func arrayServiceNativeCommand(t *testing.T, ctx context.Context, name string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000, Groups: []uint32{1000}}}
	if output, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("%s failed %v: %s", name, e, output)
	}
}

// PG17-S-ARRAY-SERVICE-001：准确369内真实SourceISO/Root生产函数、SUID同ELF
// preexec→原pidfd→实际PG NativeBase/Final→private→4map_files→原A helper。
// Host Node/issuer仍TEST_ONLY来源，尚不运行普通normal下降或K8s，不算E。
func TestNativeArrayServiceBirthAndPopulate(t *testing.T) {
	if os.Getenv("DSGA_ARRAY_SERVICE_LAUNCHER") == "1" {
		if os.Getuid() != 1000 || os.Geteuid() != 0 {
			t.Fatal("actual SUID entry credentials differ")
		}
		if e := ExecPostgres([]string{"-D", "/data/pgdata"}); e != nil {
			t.Fatal(e)
		}
		t.Fatal("exec unexpectedly returned")
	}
	if os.Getenv("DSGA_ARRAY_SERVICE_NATIVE") != "isolated369-real-array-service-v1" {
		t.Fatal("explicit isolated369 service opt-in required")
	}
	release, e := kernelText("/proc/sys/kernel/osrelease")
	if e != nil || release != "6.12.26-william-normal-split-v1-e210f5e3ee94" {
		t.Fatal("accurate369 required", e)
	}
	notes, e := physicalKernelNotes()
	if e != nil || physicalDigest(notes) != "7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828" {
		t.Fatal("accurate369 notes required", e)
	}
	defer func() {
		details := map[string]any{}
		for _, path := range []string{FixedRuntimePath, FixedSealedRuntimePath, FixedConfigPath, fixedNativeBirthPolicyPath, fixedNativeBirthBuildPath, FixedPostgresPath, fixedArrayHelperPath, FixedStateDir} {
			st, e := os.Stat(path)
			if e != nil {
				details[path] = e.Error()
			} else {
				details[path] = map[string]any{"mode": st.Mode().String(), "bytes": st.Size()}
			}
		}
		source, pg, e := readNativeBirthOriginalSource()
		if pg != nil {
			pg.Close()
		}
		if e != nil {
			details["native_source_error"] = e.Error()
		} else {
			details["native_source_sha256"] = source.binding.BinarySHA256
		}
		if resource, _, e := runtimeSource(); e == nil {
			_, e := arraySplitKernelLayout(resource.Config)
			if e != nil {
				details["split_layout_error"] = e.Error()
			}
			actual, e := (&linuxKernel{config: resource.Config}).ReadBounds(func() Identity { id, _ := (&linuxKernel{config: resource.Config}).Identity(); return id }(), resource.Limits)
			details["bounds"] = actual
			if e != nil {
				details["bounds_error"] = e.Error()
			}
		}
		entries, _ := os.ReadDir(FixedStateDir)
		records := map[string]string{}
		for _, entry := range entries {
			if entry.Name() == "lock" || entry.Name() == "server.lock" {
				continue
			}
			raw, e := os.ReadFile(FixedStateDir + "/" + entry.Name())
			if e == nil && len(raw) <= 64*1024 {
				records[entry.Name()] = string(raw)
			}
		}
		details["actual_original_wal"] = records
		arrayServiceNativeSave(t, "array-service-diagnostics", details)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	raw, e := os.ReadFile("/fixture/challenge.json")
	if e != nil {
		t.Fatal(e)
	}
	var challenge bootprotocol.InitialPrebootChallenge
	if e = json.Unmarshal(raw, &challenge); e != nil {
		t.Fatal(e)
	}
	source, _, e := runtimeSource()
	if e != nil {
		t.Fatal(e)
	}
	g, e := Open(source.Config)
	if e != nil {
		t.Fatal(e)
	}
	prepared, e := g.PrepareInitialPreboot(ctx, challenge)
	if e != nil {
		t.Fatal("actual early prepare", e)
	}
	fmt.Println("ARRAY_SERVICE_READY_FOR_HOST_QMP_GROW=1")
	arrayServiceNativeWait(t, ctx, "real Movable grow", func() bool { _, err := g.InspectInitialPrebootMovable(ctx, prepared); return err == nil })
	if _, e = g.PrepareNormalSplitTopology(ctx); e != nil {
		t.Fatal("actual split prepare", e)
	}
	if _, e = ProvisionGuest(ctx); e != nil {
		t.Fatal("actual Bootstrap/PreSys bridge", e)
	}
	// initdb为真实非特权System进程；不伪造PGDATA，也不提前发原生Birth。
	arrayServiceNativeCommand(t, ctx, "/usr/local/bin/initdb", "-D", "/data/pgdata", "-A", "trust", "--no-locale", "--no-sync")
	config := []byte("listen_addresses=''\nunix_socket_directories='/data'\nport=55432\nenable_dynamic_shared_buffers=on\nshared_buffers='2MB'\nmax_shared_buffers='64MB'\nhuge_pages=off\nmax_connections=20\nlogging_collector=on\nlog_directory='log'\nlog_filename='postgres.log'\ntimezone='GMT'\nlog_timezone='GMT'\n")
	if e = os.WriteFile("/data/pgdata/postgresql.conf", config, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Chown("/data/pgdata/postgresql.conf", 1000, 1000); e != nil {
		t.Fatal(e)
	}
	serviceCtx, stopService := context.WithCancel(ctx)
	defer stopService()
	serviceDone := make(chan error, 1)
	go func() { serviceDone <- ServeGuest(serviceCtx) }()
	arrayServiceNativeWait(t, ctx, "production fixed listener", func() bool {
		select {
		case e := <-serviceDone:
			t.Fatal("actual daemon failed", e)
		default:
		}
		_, e := os.Stat(fixedArrayBirthConsumerPath)
		return e == nil
	})
	for _, path := range []string{fixedArrayBirthSocketPath, fixedArrayPopulateSocketPath} {
		var st unix.Stat_t
		if unix.Stat(path, &st) != nil || st.Uid != 0 || st.Gid != 1000 || st.Mode&0777 != 0660 {
			t.Fatal("narrow actual socket permissions differ", path, st)
		}
	}
	log, e := os.Create("/evidence/postgres.log")
	if e != nil {
		t.Fatal(e)
	}
	defer log.Close()
	pmfd := -1
	cmd := exec.Command("/neonvm/bin/resource-guard", "-test.run=^TestNativeArrayServiceBirthAndPopulate$", "-test.timeout=90s")
	cmd.Env = append(os.Environ(), "DSGA_ARRAY_SERVICE_LAUNCHER=1")
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000, Groups: []uint32{1000}}, PidFD: &pmfd, Pdeathsig: syscall.SIGKILL}
	if e = cmd.Start(); e != nil {
		t.Fatal("SUID launcher", e)
	}
	if pmfd < 0 {
		t.Fatal("atomic original launcher pidfd missing")
	}
	defer unix.Close(pmfd)
	waited := false
	defer func() {
		if !waited {
			_ = unix.PidfdSendSignal(pmfd, unix.SIGKILL, nil, 0)
			_ = cmd.Wait()
		}
	}()
	var binding *arrayGuardBinding
	var serverGuard *Guard
	arrayServiceNativeWait(t, ctx, "original four array population", func() bool {
		if arrayRegistryPIDFDAlive(pmfd) != nil {
			raw, _ := os.ReadFile("/evidence/postgres.log")
			t.Fatalf("actual PM exited: %s", raw)
		}
		arrayGuardBindings.Range(func(key, value any) bool {
			candidate := value.(*arrayGuardBinding)
			candidate.original.mu.Lock()
			defer candidate.original.mu.Unlock()
			if candidate.original.pending.Original.PostmasterPID == cmd.Process.Pid {
				complete := candidate.original.current != nil
				if complete {
					for _, ready := range candidate.original.current.startup {
						complete = complete && ready
					}
				}
				if complete {
					binding = candidate
					serverGuard = key.(*Guard)
					return false
				}
			}
			return true
		})
		return binding != nil
	})
	arrayServiceNativeWait(t, ctx, "actual PG recovery/startup", func() bool {
		raw, _ := os.ReadFile("/data/pgdata/postmaster.pid")
		lines := strings.Split(string(raw), "\n")
		return len(lines) >= 8 && strings.TrimSpace(lines[7]) == "ready"
	})
	o := binding.original
	o.mu.Lock()
	if e = arrayMatchPIDFD(pmfd, o.pidfd); e != nil {
		o.mu.Unlock()
		t.Fatal(e)
	}
	registration := o.current.request
	files := o.current.identities
	nonce := hex.EncodeToString(o.current.nonce[:])
	o.mu.Unlock()
	if e = serverGuard.kernel.VerifyProcess(o.pending.Original); e != nil {
		t.Fatal("actual split PG identity", e)
	}
	owners := make([]map[string]uint64, 0, 8)
	pagemap, e := os.Open("/proc/self/pagemap")
	if e != nil {
		t.Fatal(e)
	}
	defer pagemap.Close()
	accounting, e := os.Open("/proc/kpagecgroup")
	if e != nil {
		t.Fatal(e)
	}
	defer accounting.Close()
	arrays, e := splitReadNode(arrayPopulateCgroup)
	if e != nil {
		t.Fatal(e)
	}
	var resident [4]uint64
	for i, r := range registration.Ranges {
		length := (registration.StartupBuffers*r.Element + 4095) &^ uint64(4095)
		// 仅测试探针：helper预触同shmem backing，不承诺PM尚未访问区域已有PTE。
		// 先无fault的MINCORE核完整prefix已resident，再只读fault原FD。若有洞或
		// 中途回收，读者Root/System新分配的页不会属于A，后续owner断言仍须FAIL。
		data, e := unix.Mmap(int(o.current.files[i].Fd()), 0, int(length), unix.PROT_READ, unix.MAP_SHARED)
		if e != nil {
			t.Fatal(e)
		}
		vec := make([]byte, length/4096)
		_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(&vec[0])))
		runtime.KeepAlive(data)
		runtime.KeepAlive(vec)
		if errno != 0 {
			unix.Munmap(data)
			t.Fatal(errno)
		}
		for _, present := range vec {
			if present&1 == 0 {
				unix.Munmap(data)
				t.Fatal("original shmem prefix not resident before observer read", i)
			}
			resident[i]++
		}
		if e = unix.Madvise(data, unix.MADV_POPULATE_READ); e != nil {
			unix.Munmap(data)
			t.Fatal(e)
		}
		base := uint64(uintptr(unsafe.Pointer(&data[0])))
		for _, offset := range []uint64{0, length - 4096} {
			address := base + offset
			var entry [8]byte
			if _, e = pagemap.ReadAt(entry[:], int64(address/4096*8)); e != nil {
				unix.Munmap(data)
				t.Fatal(e)
			}
			pte := binary.LittleEndian.Uint64(entry[:])
			pfn := pte & ((1 << 55) - 1)
			if pte>>63 != 1 || pfn == 0 {
				unix.Munmap(data)
				t.Fatal("original shmem observer PTE missing", i, pte)
			}
			if _, e = accounting.ReadAt(entry[:], int64(pfn*8)); e != nil {
				unix.Munmap(data)
				t.Fatal(e)
			}
			owner := binary.LittleEndian.Uint64(entry[:])
			if owner != uint64(arrays.Inode) {
				unix.Munmap(data)
				t.Fatal("original shmem first-touch charged wrong domain", i, owner, arrays.Inode)
			}
			owners = append(owners, map[string]uint64{"array": uint64(i), "original_pg_virtual": r.Base + offset, "observer_virtual": address, "pfn": pfn, "charged_inode": owner})
		}
		runtime.KeepAlive(data)
		if e = unix.Munmap(data); e != nil {
			t.Fatal(e)
		}
	}

	// HTTP签名只用固定测试issuer；请求经真实生产handler/真实原Birth+budget/WAL。
	h, e := newResourceHandler(source, serverGuard)
	if e != nil {
		t.Fatal(e)
	}
	transport := &http.Transport{DialTLSContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", FixedSocketPath)
	}}
	token := func(context.Context) (string, error) { return resourceToken(t, h, arrayServiceFixtureKey(), nil), nil }
	client, e := NewClient(ClientConfig{Context: ctx, BaseURL: "https://isolated-root.invalid", Identity: o.pending.Original.Identity, IssuerEpoch: 1, Timeout: 10 * time.Second, Transport: transport, Token: token, ArrayAuthorityToken: func(_ context.Context, method, path, sha string) (string, error) {
		return resourceToken(t, h, arrayServiceFixtureKey(), func(c *resourceClaims) {
			c.Scope = arrayAuthorityScope
			c.Request = &signedArrayRequest{1, method, path, sha}
		}), nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	defer client.CloseIdleConnections()
	view, e := client.InspectState(ctx)
	if e != nil || !client.ArrayAuthorityEnabled() {
		t.Fatal("actual source capability", e)
	}
	request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: control.Instance{VMUID: view.Identity.VMUID, ComputeInstanceID: view.Identity.ComputeInstanceID, PGBootID: control.Uint64(registration.PGBoot)}, IssuerEpoch: 1, Revision: 1, OperationID: control.OperationID{Low: 1}}, Kind: control.Grow, ExpectedCapacityGeneration: 1, TargetBuffers: 1024, TargetVMMemoryBytes: 1536 << 20, BudgetCommitmentID: source.BudgetCommitmentID, DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Minute).UnixMilli())}
	authority, e := client.AuthorizeArrayRequest(ctx, request)
	if e != nil {
		t.Fatal("actual body-bound Root original authority", e)
	}
	if authority.VMShrinkPermit {
		t.Fatal("component issued normal/VM permit")
	}
	arrayServiceNativeSave(t, "array-service-real", map[string]any{"scope": "REAL369_PRODUCTION_FUNCTIONS_FIXED_SOURCE_SUID_NATIVE_REGISTRY_HELPER_TEST_HOST_ISSUER_NO_NORMAL", "pg_pid": cmd.Process.Pid, "actual_pg_source_sha256": o.pending.Source.BinarySHA256, "birth": o.birthRef, "released_sha256": o.releasedSHA, "registration_nonce": nonce, "registration": registration, "actual_four_files": files, "actual_original_shmem_pfns": owners, "resident_prefix_pages_before_observer_read": resident, "array_authority": authority, "normal_authorized": false, "HostSource_is_fixture": true, "journal_StartCAS_in_this_S": false, "E": 0})
	if e = unix.PidfdSendSignal(pmfd, unix.SIGTERM, nil, 0); e != nil {
		t.Fatal(e)
	}
	e = cmd.Wait()
	waited = true
	if e != nil {
		raw, _ := os.ReadFile("/evidence/postgres.log")
		t.Fatalf("actual PG shutdown %v %s", e, raw)
	}
	stopService()
	select {
	case e := <-serviceDone:
		if e != nil && !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("production service did not stop")
	}
}
