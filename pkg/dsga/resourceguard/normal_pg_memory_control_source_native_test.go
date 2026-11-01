//go:build linux && dsga_native_array_service && dsga_native_pg17_array_resize && dsga_native_pg_memory_control

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	bootprotocol "github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"golang.org/x/sys/unix"
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
)

// 新独立准确cohort，不重写旧Grow/Shrink冻结测试/收据，不把TEST_ONLY C机制探针签normal。
func TestNativeNormalPGMemoryControlSource(t *testing.T) {
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
	config := []byte(fmt.Sprintf("listen_addresses=''\nunix_socket_directories='/data'\nport=55432\nshared_preload_libraries='neon'\nwal_consistency_checking='neon'\nneon.last_written_lsn_cache_size=1024\nneon.dsg_vm_uid='%s'\nneon.dsg_compute_instance_id='%s'\nenable_dynamic_shared_buffers=on\nshared_buffers='2MB'\nmax_shared_buffers='64MB'\nhuge_pages=off\nmax_connections=20\nlogging_collector=on\nlog_directory='log'\nlog_filename='postgres.log'\ntimezone='GMT'\nlog_timezone='GMT'\n", source.Config.VMUID, source.Config.ComputeInstanceID))
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
	cmd := exec.Command("/neonvm/bin/resource-guard", "-test.run=^TestNativeNormalPGMemoryControlSource$", "-test.timeout=90s")
	cmd.Env = append(os.Environ(), "DSGA_ARRAY_SERVICE_LAUNCHER=1")
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 1000, Gid: 1000, Groups: []uint32{1000}}, PidFD: &pmfd, Pdeathsig: syscall.SIGKILL}
	var inherited []*os.File
	var inheritedObjects []normalPGMemoryControlStdio
	for _, path := range []string{"/sys/fs/cgroup/neon-postgres/memory.max", "/dev/vdc", "/fixture/challenge.json"} {
		flags := os.O_RDONLY
		if strings.Contains(path, "memory.max") {
			flags = os.O_RDWR
		}
		f, err := os.OpenFile(path, flags, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var st unix.Stat_t
		if err = unix.Fstat(int(f.Fd()), &st); err != nil {
			t.Fatal(err)
		}
		inherited = append(inherited, f)
		inheritedObjects = append(inheritedObjects, normalPGMemoryControlStdio{Device: st.Dev, Inode: st.Ino, Rdev: st.Rdev, Mode: st.Mode})
	}
	// os/exec在真正SUID子进程的3/4/5上清CLOEXEC，确保反例原本确会跨exec泄漏。
	cmd.ExtraFiles = inherited
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
	// 原内核对象在PG最终exec后已消失，即使fd3/4/5被正常PG复用也不能混认。
	fdDir, err := unix.Openat(o.procfd, "fd", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	listing := os.NewFile(uintptr(fdDir), "original-PM-fd-list")
	fdNames, err := listing.Readdirnames(-1)
	listing.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range fdNames {
		fd, err := unix.Openat(o.procfd, "fd/"+name, unix.O_PATH|unix.O_CLOEXEC, 0)
		if err == unix.ENOENT {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		var st unix.Stat_t
		err = unix.Fstat(fd, &st)
		unix.Close(fd)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range inheritedObjects {
			if st.Dev == forbidden.Device && st.Ino == forbidden.Inode && st.Rdev == forbidden.Rdev && st.Mode&unix.S_IFMT == forbidden.Mode&unix.S_IFMT {
				t.Fatal("caller non-CLOEXEC authority leaked to original PG", name, forbidden)
			}
		}
	}
	// 使用同正式镜像的Neon管理入口；绝不装test_dsg_region或替换Provider。
	normalPG17ArraySQL(t, ctx, "CREATE EXTENSION neon WITH SCHEMA public")
	capabilities := normalPG17ArraySQL(t, ctx, "SELECT public.neon_dsg_capabilities()::text")
	arrayServiceNativeSave(t, "pg17-capabilities", json.RawMessage(capabilities))
	var baseline control.MemoryResponse
	if e = json.Unmarshal(normalPG17ArraySQL(t, ctx, "SELECT public.neon_dsg_memory_status()::text"), &baseline); e != nil {
		t.Fatal(e)
	}
	instance := baseline.Observed.Instance
	if instance.PGBootID != control.Uint64(registration.PGBoot) || instance.VMUID != view.Identity.VMUID || instance.ComputeInstanceID != view.Identity.ComputeInstanceID || baseline.Observed.HighBuffers != 256 || baseline.Observed.CapacityGeneration != 1 {
		t.Fatal("formal API did not bind original native boot", baseline)
	}
	fenceRequest := control.FenceRequest{ProtocolVersion: 1, Instance: instance, IssuerEpoch: 1}
	var fence control.FenceResponse
	if e = json.Unmarshal(normalPG17ArrayManagement(t, ctx, "neon_dsg_fence_issuer", fenceRequest), &fence); e != nil || fence.ValidateFor(fenceRequest, instance) != nil {
		t.Fatal("formal issuer fence", e, fence)
	}
	normalPG17ArraySQL(t, ctx, "CREATE TABLE public.pg17_array_data(i integer,pad text); ALTER TABLE public.pg17_array_data ALTER pad SET STORAGE PLAIN; INSERT INTO public.pg17_array_data SELECT i,repeat(md5(i::text),100) FROM generate_series(1,2000) i")
	checksum := string(normalPG17ArraySQL(t, ctx, "SELECT count(*),sum(i),md5(string_agg(pad,',' ORDER BY i)) FROM public.pg17_array_data"))
	steps := make([]map[string]any, 0, 4)
	generation := baseline.Observed.CapacityGeneration
	previous := uint32(256)
	for step, target := range []uint32{1024, 256, 2048, 256} {
		kind := control.Grow
		if target < previous {
			kind = control.Shrink
		}
		request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: instance, IssuerEpoch: 1, Revision: control.Uint64(step + 1), OperationID: control.OperationID{Low: control.Uint64(step + 1)}}, Kind: kind, ExpectedCapacityGeneration: generation, TargetBuffers: target, TargetVMMemoryBytes: 1536 << 20, BudgetCommitmentID: source.BudgetCommitmentID, DeadlineUnixMillis: control.Uint64(time.Now().Add(30 * time.Second).UnixMilli())}
		var authority *ArrayAuthorityEvidence
		if kind == control.Grow {
			actual, err := client.AuthorizeArrayRequest(ctx, request)
			if err != nil {
				t.Fatal("real signed Root array authority", err)
			}
			authority = &actual
		}
		rawResponse := normalPG17ArrayManagement(t, ctx, "neon_dsg_request_resize", request)
		var response control.ControlResponse
		if e = json.Unmarshal(rawResponse, &response); e != nil {
			t.Fatal(e)
		}
		arrayServiceNativeSave(t, fmt.Sprintf("pg17-step-%d-response", step+1), response)
		if e = response.ValidateResize(request, instance); e != nil {
			t.Fatal("formal resize validation", e, string(rawResponse))
		}
		if response.Receipt.Result == nil || response.Receipt.Result.Operation == nil || response.Receipt.Result.Operation.Phase != control.Committed || response.Observed.HighBuffers != target || response.Observed.LowBuffers != target || response.Observed.CapacityGeneration != generation+1 || response.VMShrinkPermit {
			t.Fatal("native operation did not actually commit", string(rawResponse))
		}
		probe := normalPG17ArrayPages(t, o, registration, uint64(target), uint64(previous), uint64(arrays.Inode), kind == control.Shrink)
		if kind == control.Shrink {
			for i, a := range response.Observed.Arrays {
				upper := (uint64(target)*registration.Ranges[i].Element + 4095) &^ uint64(4095)
				if a.AllocationUnknown || uint64(a.CommittedUpperBytes) != upper || a.ReleaseState != control.ReleaseVerified || !a.Provider.PhysicalReleaseVerified || a.Provider.ReleaseMayBePartial {
					t.Fatal("one array lacks actual tail release/cost", i, a)
				}
			}
		}
		got := string(normalPG17ArraySQL(t, ctx, "SELECT count(*),sum(i),md5(string_agg(pad,',' ORDER BY i)) FROM public.pg17_array_data"))
		if got != checksum {
			t.Fatal("data changed after native resize", got, checksum)
		}
		query := control.Query{ProtocolVersion: 1, Key: request.Key}
		var status control.ControlResponse
		if e = json.Unmarshal(normalPG17ArrayManagement(t, ctx, "neon_dsg_resize_status", query), &status); e != nil || status.ValidateResize(request, instance) != nil || status.Receipt.Result == nil || status.Receipt.Result.CapacityGeneration != response.Receipt.Result.CapacityGeneration {
			t.Fatal("actual post-transaction observation/history", e, status)
		}
		sharedCost := json.RawMessage(normalPG17ArraySQL(t, ctx, "SELECT public.neon_dsg_shared_cost()::text"))
		steps = append(steps, map[string]any{"request": request, "root_authority": authority, "response": response, "status_after_transaction": status, "four_array_pages": probe, "shared_cost": sharedCost, "checksum": got})
		generation, previous = response.Observed.CapacityGeneration, target
	}
	// 真正SQL backend加载一个只含syscall的TEST_ONLY C模块，模拟不可信C代码。
	// 该模块不是生产插件来源/不改filter，只在已继承的Child链内执行机制反例。
	normalPG17ArraySQL(t, ctx, "CREATE FUNCTION public.normal_pg_memory_control_probe(text) RETURNS text AS '/usr/local/lib/normal_pg_memory_control_probe.so', 'normal_pg_memory_control_probe' LANGUAGE C STRICT; REVOKE ALL ON FUNCTION public.normal_pg_memory_control_probe(text) FROM PUBLIC")
	mechanisms := make([]json.RawMessage, 0, 33)
	for _, name := range []string{"move_pages", "migrate_pages", "mbind", "set_mempolicy", "home_node", "io_uring", "userfaultfd", "process_madvise", "process_mrelease", "mlock", "madv_collapse", "madv_hugepage", "madv_cold", "madv_pageout", "madv_mergeable", "dumpable_enable", "vmsplice", "splice", "tee", "sendfile", "unshare", "setns", "ptrace", "pidfd_getfd", "memfd_create", "execve", "fork", "cgroup_write_open", "raw_device_open", "mount", "swapon", "thp_disable_clear", "huge_reservation"} {
		raw := normalPG17ArraySQL(t, ctx, "SELECT public.normal_pg_memory_control_probe('"+name+"')")
		var outcome struct {
			Result  int64 `json:"result"`
			Errno   int   `json:"errno"`
			UID     int   `json:"uid"`
			EUID    int   `json:"euid"`
			NNP     int   `json:"nnp"`
			Seccomp int   `json:"seccomp"`
		}
		if err := json.Unmarshal(raw, &outcome); err != nil {
			t.Fatal(err)
		}
		if outcome.UID != 1000 || outcome.EUID != 1000 || outcome.NNP != 1 || outcome.Seccomp != 2 {
			t.Fatal("probe is not real filtered nonprivileged backend", string(raw))
		}
		switch name {
		case "thp_disable_clear", "huge_reservation": // 不能伪称cBPF拒绝；依赖原KernelControls，不触huge页。
		case "cgroup_write_open", "raw_device_open":
			if outcome.Result != -1 || outcome.Errno != int(unix.EACCES) {
				t.Fatal("original DAC did not reject write authority", string(raw))
			}
		default:
			if outcome.Result != -1 || outcome.Errno != int(unix.EPERM) {
				t.Fatal("native syscall control not enforced", string(raw))
			}
		}
		mechanisms = append(mechanisms, json.RawMessage(raw))
	}
	pgControls, err := serverGuard.captureNormalPGMemoryControlSource(ctx)
	if err != nil {
		t.Fatal("real original PG syscall source", err)
	}
	defer pgControls.close()
	var pgReference normalPGMemoryControlReference
	err = serverGuard.withRecord(ctx, func(record *record, state *lockedState) error {
		var err error
		pgReference, err = pgControls.referenceLocked(ctx, state, record)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = pgControls.requireContinuousQualification(); !errors.Is(err, ErrUnknown) {
		t.Fatal("unbound stdio/external sources became normal", err)
	}
	arrayServiceNativeSave(t, "normal-pg-memory-control-source", map[string]any{"id": "PG17-S-PG-MEMORY-CONTROL-001", "scope": "ORIGINAL_NATIVEFINAL_FULL_BPF_PIN_AND_DESCRIPTOR_CLOSURE_COMPONENT_ONLY", "reference": pgReference, "inherited_nocloexec_original_objects": inheritedObjects, "original_three_extra_FD_objects_absent": true, "native198_created_after_exec_and_full_handshake_passed": true, "mechanisms": mechanisms, "actual_Grow_Shrink_steps": steps, "typed_stdio_original_source_closed": false, "FS_Kernel_external_sources_composed": false, "normal_authorized": false, "E": 0})
	arrayServiceNativeSave(t, "normal-pg17-array-resize", map[string]any{"id": "PG17-S-ARRAY-RESIZE-001", "scope": "REAL369_FORMAL_NEON_SQL_ROOT_BIRTH_REGISTRY_HELPER_HOST_ISSUER_FIXTURE_NOT_NORMAL", "steps": steps, "original_pg_sha256": o.pending.Source.BinarySHA256, "birth": o.birthRef, "released_sha256": o.releasedSHA, "registration": registration, "original_four_inode_files": files, "registration_nonce": nonce, "original_startup_resident": resident, "original_startup_owner_samples": owners, "Host_Source_is_fixture": true, "issuer_is_fixture": true, "journal_StartCAS_in_this_S": false, "formal_SQL_resize_executed": true, "normal_authorized": false, "E": 0})
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
