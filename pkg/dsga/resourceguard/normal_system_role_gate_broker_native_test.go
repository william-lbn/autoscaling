//go:build linux && dsga_native_role_gate && dsga_native_system_lifecycle

package resourceguard

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"syscall"
	"testing"
	"time"
	"unsafe"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func exportNormalSystemRoleBrokerFixture(t *testing.T) {
	if os.Getenv("DSGA_ROLE_BROKER_LIFETIME") != "DEV" {
		return
	}
	out := os.Getenv("DSGA_SYSTEM_LIFECYCLE_EXPORT")
	root := filepath.Join(out, "root")
	raw, e := os.ReadFile(filepath.Join(root, NormalSystemSourceManifestPath))
	require.NoError(t, e)
	m, e := DecodeNormalSystemSourceManifest(raw)
	require.NoError(t, e)
	guard, e := os.ReadFile(filepath.Join(root, normalSystemRoleBrokerExecutable))
	require.NoError(t, e)
	plan, e := os.ReadFile(filepath.Join(root, normalSystemRoleBuildPlanPath))
	require.NoError(t, e)
	ah, _ := normalSystemVectorSHA(normalSystemRoleBrokerArgv())
	eh, _ := normalSystemVectorSHA(normalSystemRoleBrokerEnvironment())
	d := normalSystemRoleBrokerDeclaration{ProtocolVersion: 1, Scope: normalSystemRoleBrokerDeclarationScope, ExecutablePath: normalSystemRoleBrokerExecutable, ExecutableSHA256: physicalDigest(guard), ArgvSHA256: ah, EnvironmentSHA256: eh, NamespaceProtocolVersion: 1, GateProtocolVersion: 1, Lifetime: normalSystemRoleBrokerLifetime, ComputeBuildPlanSHA256: physicalDigest(plan)}
	if os.Getenv("DSGA_ROLE_BROKER_DEATH") == "DEV" {
		marker := []byte("DEV broker death mechanism\n")
		path := "/neonvm/config/dev-broker-death"
		require.NoError(t, os.WriteFile(filepath.Join(root, path), marker, 0444))
		m.Files = append(m.Files, NormalSystemSourceFile{Path: path, CanonicalPath: path, Kind: "config", Bytes: uint64(len(marker)), SHA256: physicalDigest(marker)})
	}
	decl, e := json.Marshal(d)
	require.NoError(t, e)
	_, e = decodeNormalSystemRoleBrokerDeclaration(decl)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, normalSystemRoleBrokerDeclarationPath), decl, 0444))
	m.Files = append(m.Files, NormalSystemSourceFile{Path: normalSystemRoleBrokerDeclarationPath, CanonicalPath: normalSystemRoleBrokerDeclarationPath, Kind: "config", Bytes: uint64(len(decl)), SHA256: physicalDigest(decl)})
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	require.NoError(t, m.Validate())
	raw, e = json.Marshal(m)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, NormalSystemSourceManifestPath), raw, 0444))
	buildRaw, e := os.ReadFile(filepath.Join(root, fixedNativeBirthBuildPath))
	require.NoError(t, e)
	var b nativeBirthBuildManifest
	require.NoError(t, json.Unmarshal(buildRaw, &b))
	b.RuntimeClosure.SystemManifestSHA256 = physicalDigest(raw)
	buildRaw, e = json.Marshal(b)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(root, fixedNativeBirthBuildPath), buildRaw, 0444))
	raw, e = os.ReadFile(filepath.Join(root, FixedSealedRuntimePath))
	require.NoError(t, e)
	var doc RuntimeDocument
	require.NoError(t, json.Unmarshal(raw, &doc))
	templates, e := parseRuntimePolicyTemplates(doc.PolicyTemplatesJSON)
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
	doc = runtimeTemplatesSetBlob(doc, runtimeTemplatesEnvelope(templates.Templates))
	raw, e = json.Marshal(doc)
	require.NoError(t, e)
	assets, e := RenderRuntimePolicyAssets(raw)
	require.NoError(t, e)
	assets[RuntimeResourceAssetName] = raw
	for n, b := range assets {
		physical, e := RuntimeISOAssetName(n)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(filepath.Join(out, "isofiles", physical), b, 0444))
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, FixedSealedRuntimePath), raw, 0444))
}

type devBrokerLifetimePair struct {
	ProtocolVersion uint32                       `json:"protocol_version"`
	Nonce           string                       `json:"nonce"`
	Parent          normalSystemLifecycleBinding `json:"parent"`
	Broker          normalSystemLifecycleBinding `json:"broker"`
	Task            normalSystemRoleObservation  `json:"task"`
	ReleasedSHA256  string                       `json:"released_sha256"`
}

func devBrokerPacketWrite(conn *net.UnixConn, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(b) > 4084 {
		return fmt.Errorf("DEV pair exceeds bound")
	}
	raw := make([]byte, 4096)
	copy(raw, "DEVBPAIR")
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(b)))
	copy(raw[12:], b)
	_, e = conn.Write(raw)
	return e
}
func devBrokerPacketRead(conn *net.UnixConn) (devBrokerLifetimePair, error) {
	var out devBrokerLifetimePair
	raw, e := arrayServicePacket(conn, time.Now().Add(10*time.Second), 4096, false)
	if e != nil {
		return out, e
	}
	if string(raw[:8]) != "DEVBPAIR" {
		return out, fmt.Errorf("DEV pair action differs")
	}
	n := int(binary.BigEndian.Uint32(raw[8:12]))
	if n <= 0 || n > 4084 {
		return out, fmt.Errorf("DEV pair length differs")
	}
	for _, v := range raw[12+n:] {
		if v != 0 {
			return out, fmt.Errorf("DEV pair padding differs")
		}
	}
	e = control.UnmarshalObject(raw[12:12+n], &out)
	return out, e
}

type devBrokerLifetimeWriter struct {
	*devNormalSystemRoleGateWriter
	channel *os.File
}

func (w *devBrokerLifetimeWriter) originalSystemRoleBrokerGateChannel() (*os.File, error) {
	return w.channel, nil
}

// DEV目标只实现ready/commit/lifetime机制；生产listener/FS opaque proof另批接线，
// 本目标不发normal，不运行resource/ordinary角色，更不能冒称完整StartBroker。
func runDEVNormalSystemRoleBrokerLifetime() error {
	f, e := os.OpenFile("/tmp/normal-broker-target-entry", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_ = f.Close()
	conn, e := normalSystemRoleGateConnection(5)
	if e != nil {
		return e
	}
	defer conn.Close()
	var death int32
	_, _, errno := syscall.RawSyscall6(syscall.SYS_PRCTL, unix.PR_GET_PDEATHSIG, uintptr(unsafe.Pointer(&death)), 0, 0, 0, 0)
	if errno != 0 || death != 0 {
		return fmt.Errorf("DEV approved broker still has creator pdeath")
	}
	source, e := inspectNormalSystemLifecycleOriginal(context.Background())
	if e != nil {
		return e
	}
	defer source.close()
	pair, e := devBrokerPacketRead(conn)
	if e != nil {
		return e
	}
	parent := source.binding
	parent.RootConsumer = pair.Parent.RootConsumer
	if parent != pair.Parent || pair.ProtocolVersion != 1 || pair.Task.PID != os.Getpid() || pair.Task.StartTicks != uint64(source.binding.RootConsumer.StartTicks) || pair.Parent.RootConsumer == source.binding.RootConsumer {
		return fmt.Errorf("DEV original pair mismatch")
	}
	peer, pfd, e := arraySocketPeer(conn)
	if e != nil {
		return e
	}
	defer unix.Close(pfd)
	if int(peer.Pid) != pair.Parent.RootConsumer.PID || peer.Uid != 0 || peer.Gid != 0 {
		return fmt.Errorf("DEV original parent peer mismatch")
	}
	if e = arrayRegistryPIDFDAlive(pfd); e != nil {
		return e
	}
	config, e := sealedRuntimeSource()
	if e != nil {
		return e
	}
	raw, e := os.ReadFile(filepath.Join(config.Config.StateDir, normalSystemRoleStageName(pair.Nonce, "released")))
	if e != nil || physicalDigest(raw) != pair.ReleasedSHA256 {
		return fmt.Errorf("DEV original released stage mismatch")
	}
	var stage normalSystemRoleJournalEntry
	if e = control.UnmarshalObject(raw, &stage); e != nil {
		return e
	}
	if stage.Task == nil || !reflect.DeepEqual(*stage.Task, pair.Task) || stage.Binding != pair.Parent || stage.Role != normalSystemRoleBrokerKind || stage.Stage != "released" {
		return fmt.Errorf("DEV released task differs")
	}
	pair.Broker = source.binding
	if e = devBrokerPacketWrite(conn, pair); e != nil {
		return e
	}
	raw, e = arrayServicePacket(conn, time.Now().Add(10*time.Second), 8, false)
	if e != nil || string(raw) != "DEVBCOM1" {
		return fmt.Errorf("DEV broker missing original commit")
	}
	ready, e := os.ReadFile(filepath.Join(config.Config.StateDir, "role-broker-start-"+pair.Nonce+"-ready.json"))
	if e != nil {
		return e
	}
	var readyPair devBrokerLifetimePair
	if e = control.UnmarshalObject(ready, &readyPair); e != nil || !reflect.DeepEqual(readyPair, pair) {
		return fmt.Errorf("DEV broker ready not durable pair")
	}
	_ = conn.Close()
	for until := time.Now().Add(10 * time.Second); time.Now().Before(until); time.Sleep(10 * time.Millisecond) {
		dead, e := normalSystemRoleGateExitObserved(pfd)
		if e != nil {
			return e
		}
		if dead {
			if os.Getppid() != 1 {
				return fmt.Errorf("DEV broker not reparented to PID1")
			}
			proof, _ := json.Marshal(pair)
			f, e := os.OpenFile("/tmp/normal-broker-pid1-proof.json", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if e != nil {
				return e
			}
			if _, e = f.Write(proof); e != nil {
				return e
			}
			if e = f.Sync(); e != nil {
				return e
			}
			_ = f.Close()
			fmt.Println("NEON-S-ROLE-GATE-009 PASS original initialRoot exited normally; released broker is alive under actual PID1 with explicit old/new consumer pair")
			for {
				time.Sleep(time.Second)
			}
		}
	}
	return fmt.Errorf("DEV original parent did not exit")
}
func TestNativeNormalSystemRoleGateBrokerLifetime(t *testing.T) {
	require.Equal(t, 1, os.Getppid())
	source, e := inspectNormalSystemLifecycleOriginal(context.Background())
	require.NoError(t, e)
	defer source.close()
	ns, e := captureNormalSystemRoleNamespaces()
	require.NoError(t, e)
	defer ns.close()
	config, e := sealedRuntimeSource()
	require.NoError(t, e)
	state, e := lockState(context.Background(), config.Config.StateDir)
	require.NoError(t, e)
	defer state.close()
	ids, e := ns.snapshot()
	require.NoError(t, e)
	nonce, e := newNativeBirthNonce()
	require.NoError(t, e)
	plan, e := normalSystemRoleGateLaunchPlan(context.Background(), source, normalSystemRoleBrokerKind)
	require.NoError(t, e)
	ph, e := plan.referenceSHA256()
	require.NoError(t, e)
	j := &normalSystemRoleJournal{state: state, pending: normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: nonce, Stage: "pending", Role: normalSystemRoleBrokerKind, PlanSHA256: ph, Binding: source.binding, Namespace: ids}, seen: map[string]normalSystemRoleFileWitness{}}
	require.NoError(t, j.append(context.Background(), "pending", nil, false))
	fds, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, e)
	pf := os.NewFile(uintptr(fds[0]), "DEV original initialRoot broker control")
	target := os.NewFile(uintptr(fds[1]), "DEV original broker startup")
	defer target.Close()
	transport, e := net.FileConn(pf)
	pf.Close()
	require.NoError(t, e)
	conn := transport.(*net.UnixConn)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	writer := &devBrokerLifetimeWriter{devNormalSystemRoleGateWriter: &devNormalSystemRoleGateWriter{j: j}, channel: target}
	child, e := spawnNormalSystemRoleOriginal(ctx, source, ns, normalSystemRoleBrokerKind, writer)
	require.NoError(t, e)
	require.Equal(t, 1, writer.calls)
	select {
	case <-child.creatorLeaseDone:
	case <-time.After(3 * time.Second):
		t.Fatal("DEV fixed broker creator lease did not release")
	}
	require.NoError(t, arrayRegistryPIDFDAlive(int(child.pidfd.Fd())))
	pair := devBrokerLifetimePair{ProtocolVersion: 1, Nonce: nonce, Parent: source.binding, Task: child.observed.Task, ReleasedSHA256: j.previous}
	require.NoError(t, devBrokerPacketWrite(conn, pair))
	ready, e := devBrokerPacketRead(conn)
	require.NoError(t, e)
	expected := ready.Broker
	require.Equal(t, pair.Task.PID, expected.RootConsumer.PID)
	require.Equal(t, pair.Task.StartTicks, uint64(expected.RootConsumer.StartTicks))
	require.NotEqual(t, pair.Parent.RootConsumer, expected.RootConsumer)
	copy := expected
	copy.RootConsumer = pair.Parent.RootConsumer
	require.Equal(t, pair.Parent, copy)
	pair.Broker = ready.Broker
	require.Equal(t, pair, ready)
	raw, e := json.Marshal(ready)
	require.NoError(t, e)
	require.NoError(t, state.createPhysicalFile("role-broker-start-"+nonce+"-ready.json", raw))
	_, e = conn.Write([]byte("DEVBCOM1"))
	require.NoError(t, e)
	fmt.Println("NEON-S-ROLE-GATE-008 PASS fixed RO lifetime: original exec/released/ready durable and original pair commit; initialRoot may now exit normally")
	fmt.Println("ROLE_GATE_RESULT={\"scope\":\"fixed broker lifetime actual Gate component with DEV ready prelude; no production listener/FS handoff\",\"normal_permit\":false,\"E\":0}")
}

func TestNativeNormalSystemRoleGateBrokerDeathBeforeRelease(t *testing.T) {
	require.Equal(t, 1, os.Getppid())
	source, e := inspectNormalSystemLifecycleOriginal(context.Background())
	require.NoError(t, e)
	defer source.close()
	ns, e := captureNormalSystemRoleNamespaces()
	require.NoError(t, e)
	defer ns.close()
	config, e := sealedRuntimeSource()
	require.NoError(t, e)
	state, e := lockState(context.Background(), config.Config.StateDir)
	require.NoError(t, e)
	defer state.close()
	ids, e := ns.snapshot()
	require.NoError(t, e)
	nonce, e := newNativeBirthNonce()
	require.NoError(t, e)
	plan, e := normalSystemRoleGateLaunchPlan(context.Background(), source, normalSystemRoleBrokerKind)
	require.NoError(t, e)
	ph, e := plan.referenceSHA256()
	require.NoError(t, e)
	j := &normalSystemRoleJournal{state: state, pending: normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: nonce, Stage: "pending", Role: normalSystemRoleBrokerKind, PlanSHA256: ph, Binding: source.binding, Namespace: ids}, seen: map[string]normalSystemRoleFileWitness{}}
	require.NoError(t, j.append(context.Background(), "pending", nil, false))
	fds, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, e)
	defer unix.Close(fds[0])
	target := os.NewFile(uintptr(fds[1]), "DEV broker death startup")
	defer target.Close()
	writer := &devBrokerLifetimeWriter{devNormalSystemRoleGateWriter: &devNormalSystemRoleGateWriter{j: j}, channel: target}
	type proof struct{ thread, tracee int }
	proofs := make(chan proof, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, normalSystemRoleGateCheckpointKey{}, normalSystemRoleGateCheckpoint(func(stage string, pidfd, tid int) {
		if stage != "target-exec-stop" {
			return
		}
		thread, e := unix.PidfdOpen(tid, normalSystemRoleCreatorThreadFDFlag)
		if e != nil {
			panic("DEV broker thread pin missing")
		}
		tracee, e := unix.FcntlInt(uintptr(pidfd), unix.F_DUPFD_CLOEXEC, 0)
		if e != nil {
			panic("DEV broker target pin missing")
		}
		proofs <- proof{thread, tracee}
		_, _, _ = unix.Syscall(unix.SYS_EXIT, 0, 0, 0)
		panic("DEV broker SYS_exit returned")
	}))
	go func() { _, _ = spawnNormalSystemRoleOriginal(ctx, source, ns, normalSystemRoleBrokerKind, writer) }()
	var old proof
	select {
	case old = <-proofs:
	case <-time.After(20 * time.Second):
		t.Fatal("DEV broker death checkpoint missing")
	}
	defer unix.Close(old.thread)
	defer unix.Close(old.tracee)
	require.Eventually(t, func() bool { v, e := normalSystemRoleGateExitObserved(old.thread); return e == nil && v }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { v, e := normalSystemRoleGateExitObserved(old.tracee); return e == nil && v }, 8*time.Second, 10*time.Millisecond)
	self, e := unix.PidfdOpen(os.Getpid(), 0)
	require.NoError(t, e)
	defer unix.Close(self)
	require.NoError(t, arrayRegistryPIDFDAlive(self))
	require.Zero(t, writer.calls)
	require.NoFileExists(t, "/tmp/normal-broker-target-entry")
	require.FileExists(t, filepath.Join(config.Config.StateDir, normalSystemRoleStageName(nonce, "pending")))
	require.NoFileExists(t, filepath.Join(config.Config.StateDir, normalSystemRoleStageName(nonce, "released")))
	fmt.Println("NEON-S-ROLE-GATE-010 PASS broker trusted Gate already cleared Pdeath but original creator M SYS_exit at second exec-stop still kills original target before ready/user entry; TGID alive/pending retained")
	fmt.Println("ROLE_GATE_RESULT={\"scope\":\"broker-specific cleared-Pdeath original M death mechanism; pending remains Unknown\",\"normal_permit\":false,\"E\":0}")
}
