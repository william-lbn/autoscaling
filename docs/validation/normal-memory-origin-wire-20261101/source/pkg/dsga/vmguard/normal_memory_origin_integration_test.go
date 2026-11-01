//go:build linux

package vmguard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/stretchr/testify/require"
	coordination "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	if os.Getenv("NEON_MEMORY_ORIGIN_MODEL") != "explicit-memory-origin-v1" {
		return
	}
	if e := runNormalMemoryIntegrationModel(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(93)
	}
	os.Exit(0)
}
func normalMemoryIntegrationRuntime(t *testing.T, raw []byte) []byte {
	t.Helper()
	var d resourceguard.RuntimeDocument
	require.NoError(t, json.Unmarshal(raw, &d))
	var env struct {
		ProtocolVersion uint32 `json:"protocol_version"`
		Scope           string `json:"scope"`
		Templates       []struct {
			Asset  string          `json:"asset"`
			Policy json.RawMessage `json:"policy"`
		} `json:"templates"`
	}
	require.NoError(t, json.Unmarshal(d.PolicyTemplatesJSON, &env))
	found := false
	for i := range env.Templates {
		if env.Templates[i].Asset != "dsga-normal-split.json" {
			continue
		}
		found = true
		var fields map[string]any
		require.NoError(t, json.Unmarshal(env.Templates[i].Policy, &fields))
		fields["protocol_version"] = 2
		fields["memory_origin_protocol_version"] = 1
		var e error
		env.Templates[i].Policy, e = json.Marshal(fields)
		require.NoError(t, e)
	}
	require.True(t, found)
	blob, e := json.Marshal(env)
	require.NoError(t, e)
	d.PolicyTemplatesJSON = blob
	d.PolicyTemplatesSHA256 = initialBootSHA(blob)
	out, e := json.Marshal(d)
	require.NoError(t, e)
	_, e = resourceguard.RenderRuntimePolicyAssets(out)
	require.NoError(t, e)
	return out
}
func runNormalMemoryIntegrationModel() error {
	m := &initialLifecycleModel{dir: os.Getenv("NEON_INITIAL_MODEL_DIR"), scenario: os.Getenv("NEON_INITIAL_MODEL_SCENARIO")}
	var qmp string
	for i := 1; i < len(os.Args); i++ {
		if i+1 >= len(os.Args) {
			continue
		}
		switch os.Args[i] {
		case "-uuid":
			i++
			m.uuid = os.Args[i]
		case "-qmp":
			i++
			v := os.Args[i]
			if strings.Contains(v, "/qmp.sock,") {
				qmp = strings.TrimSuffix(strings.TrimPrefix(v, "unix:"), ",server,wait=off")
			}
		case "-m":
			i++
			f, e := initialBootFields(os.Args[i])
			if e != nil {
				return e
			}
			b, e := initialBootMemoryBytes(f["size"])
			if e != nil {
				return e
			}
			x, e := initialBootMemoryBytes(f["maxmem"])
			if e != nil {
				return e
			}
			m.base, m.maximum = uint64(b), uint64(x)
		}
	}
	if qmp == "" || m.uuid == "" || m.dir == "" || m.base == 0 || m.maximum <= m.base {
		return fmt.Errorf("explicit model identity/geometry missing")
	}
	q, e := net.Listen("unix", qmp)
	if e != nil {
		return e
	}
	defer q.Close()
	b, e := net.Listen("unix", filepath.Join(m.dir, initialBootSocketName))
	if e != nil {
		return e
	}
	defer b.Close()
	go func() {
		for {
			c, e := q.Accept()
			if e != nil {
				return
			}
			// 原QEMU单monitor顺序服务；不能用并发accept掩盖嵌套连接。
			m.qmp(c)
		}
	}()
	c, e := b.Accept()
	if e != nil {
		return e
	}
	defer c.Close()
	ch, e := bootprotocol.ReadInitialPrebootChallenge(c)
	if e != nil {
		return e
	}
	m.trace("prepared_received")
	if m.scenario == "cancel-prepared" {
		select {}
	}
	h := strings.Repeat("ab", 32)
	prepared := bootprotocol.InitialPrebootReceipt{Challenge: ch, GuestBootID: "12345678-abcd-1234-abcd-123456789abc", RootInstanceNonce: strings.Repeat("cd", 32), KernelPreparationSHA256: h, PhysicalOriginSHA256: h, PreSystemSHA256: h, CurrentPhysicalSHA256: h}
	if m.scenario == "bad-prepared" {
		prepared.Challenge.KernelBuildSHA256 = strings.Repeat("de", 32)
	}
	if e = bootprotocol.WriteInitialPrebootReceipt(c, prepared); e != nil {
		return e
	}
	movable, e := bootprotocol.ReadInitialMovableChallenge(c)
	if e != nil {
		return e
	}
	preparedSHA, e := prepared.SHA256()
	if e != nil {
		return e
	}
	delta := movable.TargetBytes - ch.OriginalBase
	moved := bootprotocol.InitialMovableReceipt{Challenge: movable, GuestBootID: prepared.GuestBootID, RootInstanceNonce: strings.Repeat("ef", 32), PreparedReceiptSHA256: preparedSHA, PhysicalOriginSHA256: prepared.PhysicalOriginSHA256, PreSystemSHA256: prepared.PreSystemSHA256, CurrentPhysicalSHA256: h, ExpectedIncrementBytes: delta, MovablePresentPages: delta / 4096}
	if e = bootprotocol.WriteInitialMovableReceipt(c, moved); e != nil {
		return e
	}
	completion, e := bootprotocol.ReadInitialCompletion(c)
	if e != nil {
		return e
	}
	if e = completion.MatchReceipt(moved); e != nil {
		return e
	}
	m.trace("completion_received")
	origin, e := bootprotocol.ReadNormalMemoryOriginHostReference(c)
	if e != nil {
		return e
	}
	if origin.Prepared != prepared || origin.Movable != moved || origin.Completion != completion {
		return fmt.Errorf("model new origin changed original phases")
	}
	m.trace("origin_received")
	if m.scenario == "missing-ack" {
		select {}
	}
	sha, e := origin.SHA256()
	if e != nil {
		return e
	}
	ack := bootprotocol.NormalMemoryOriginGuestAck{ProtocolVersion: 1, Scope: bootprotocol.NormalMemoryOriginAckScope, HostReferenceSHA256: sha, GuestSourceSHA256: strings.Repeat("77", 32), GuestBootID: prepared.GuestBootID, RootInstanceNonce: moved.RootInstanceNonce}
	if m.scenario == "bad-ack-source" {
		ack.HostReferenceSHA256 = strings.Repeat("66", 32)
	}
	if m.scenario == "partial-ack" {
		_, e = c.Write([]byte("DSGB"))
		m.trace("partial_ack_sent")
		return e
	}
	if e = bootprotocol.WriteNormalMemoryOriginGuestAck(c, ack); e != nil {
		return e
	}
	m.trace("origin_ack_sent")
	if m.scenario == "bad-ack-source" {
		m.trace("bad_ack_sent")
	}
	select {}
}
func normalMemoryIntegrationSourceFixture(t *testing.T, withLaunch bool) (*runnerFixture, []byte, *exec.Cmd) {
	t.Helper()
	f := runnerBirthFixture(t, false)
	ctx := context.Background()
	node, e := f.core.CoreV1().Nodes().Get(ctx, f.config.NodeName, metav1.GetOptions{})
	require.NoError(t, e)
	var h hostguard.Model
	require.NoError(t, json.Unmarshal([]byte(node.Annotations[hostguard.LedgerAnnotation]), &h))
	yes := true
	holder := "model-host-owner"
	transitions, seconds := int32(0), int32(60)
	now := metav1.NewMicroTime(time.Now())
	hl := &coordination.Lease{ObjectMeta: metav1.ObjectMeta{Name: h.Binding.LeaseName, Namespace: h.Binding.Namespace, UID: h.Binding.LeaseUID, ResourceVersion: "1", Annotations: map[string]string{hostguard.LeaseAnnotation: hostguard.LeaseMarker(h.Binding)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: h.Binding.NodeName, UID: h.Binding.NodeUID, Controller: &yes}}}, Spec: coordination.LeaseSpec{HolderIdentity: &holder, LeaseTransitions: &transitions, LeaseDurationSeconds: &seconds, AcquireTime: &now, RenewTime: &now}}
	_, e = f.core.CoordinationV1().Leases(h.Binding.Namespace).Create(ctx, hl, metav1.CreateOptions{})
	require.NoError(t, e)
	raw, secret := initialBootFullRuntime(t, f)
	raw = normalMemoryIntegrationRuntime(t, raw)
	var doc resourceguard.RuntimeDocument
	require.NoError(t, json.Unmarshal(raw, &doc))
	secret.Data[resourceguard.PolicyTemplatesSecretKey] = append([]byte(nil), doc.PolicyTemplatesJSON...)
	_, e = f.core.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, metav1.CreateOptions{})
	require.NoError(t, e)
	shortState, e := os.MkdirTemp("/tmp", "initial-source-")
	require.NoError(t, e)
	t.Cleanup(func() { _ = os.RemoveAll(shortState) })
	f.config.StateDir = shortState
	f.config.InitialBootstrap = &InitialBootstrapConfig{RuntimeJSON: append([]byte(nil), raw...), OriginalBaseBytes: 256 << 20}
	f.runner, e = Initialize(ctx, f.store, f.core.CoreV1(), f.core.CoordinationV1(), f.config)
	require.NoError(t, e)
	path := filepath.Join(t.TempDir(), "qemu-system-x86_64")
	require.NoError(t, os.WriteFile(path, []byte("NOT_A_QEMU_TEST_ONLY_SOURCE_FIXTURE\n"), 0700))
	args := []string{path, "-m", "size=256M,slots=1,maxmem=4G", "-object", "memory-backend-ram,id=vmem0,size=3840M", "-device", "virtio-mem-pci,id=vm0,memdev=vmem0,node=0,block-size=8M,requested-size=0"}
	args = append(args, "-smp", "cpus=2,maxcpus=2,sockets=1,cores=2,threads=1", "-monitor", "none")
	args = append(args, f.runner.QMPArguments()...)
	args = append(args, f.runner.BootChannelArguments()...)
	cmd := &exec.Cmd{Path: path, Args: args}
	digest, e := f.runner.validateProcessCommand(cmd)
	require.NoError(t, e)
	launch := processLaunch{Identity: f.runner.birth.Identity, BirthClaim: f.runner.engine.binding, Nonce: strings.Repeat("12", 16), CommandSHA256: digest, ContainerID: "containerd://model-only"}
	if withLaunch {
		e = f.runner.engine.withRecord(ctx, func(_ *record, s *lockedState) error {
			return s.writeOnce("process-launch.json", initialBootCanonical(launch))
		})
		require.NoError(t, e)
	}
	return f, raw, cmd
}

func normalMemoryIntegrationFixture(t *testing.T, scenario string) (*runnerFixture, *exec.Cmd, string) {
	t.Helper()
	f, _, original := normalMemoryIntegrationSourceFixture(t, false)
	pod, e := f.core.CoreV1().Pods(f.store.Binding().Namespace).Get(context.Background(), f.config.PodName, metav1.GetOptions{})
	require.NoError(t, e)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "neonvm-runner", ContainerID: "containerd://model-current", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	require.NoError(t, f.core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, pod.Namespace))
	executable, e := os.Executable()
	require.NoError(t, e)
	in, e := os.Open(executable)
	require.NoError(t, e)
	defer in.Close()
	out, e := os.OpenFile(original.Path, os.O_WRONLY|os.O_TRUNC, 0700)
	require.NoError(t, e)
	_, e = io.Copy(out, in)
	require.NoError(t, e)
	require.NoError(t, out.Close())
	cmd := &exec.Cmd{Path: original.Path, Args: append([]string(nil), original.Args...), Env: append(os.Environ(), "NEON_MEMORY_ORIGIN_MODEL=explicit-memory-origin-v1", "NEON_INITIAL_MODEL_DIR="+f.config.StateDir, "NEON_INITIAL_MODEL_SCENARIO="+scenario)}
	return f, cmd, filepath.Join(f.config.StateDir, "model-trace.txt")
}
func TestNormalMemoryIntegrationHoldsOpaqueOnlyAfterDurableAck(t *testing.T) {
	// 所有QMP/Guest/API是假；真实Start/pidfd/peer/原锁/WAL和新1–7分支。
	f, original, trace := normalMemoryIntegrationFixture(t, "success")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, original.Path, original.Args[1:]...)
	cmd.Env = original.Env
	done := make(chan error, 1)
	go func() { done <- f.runner.RunQEMU(ctx, cmd) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("original worker not joined")
		}
	}()
	require.NoError(t, waitInitialModelTrace(ctx, trace, "origin_ack_sent"))
	l := f.runner.initial.(*linuxInitialLifecycle)
	for {
		l.mu.Lock()
		ready := l.ready != nil
		origin := l.memoryOrigin
		l.mu.Unlock()
		if ready {
			require.NotNil(t, origin)
			l.processGuard.RLock()
			e := f.runner.engine.withRecord(ctx, func(_ *record, s *lockedState) error { return origin.recheckLocked(ctx, s, l, l.ready) })
			l.processGuard.RUnlock()
			require.NoError(t, e)
			break
		}
		require.NoError(t, initialBootPause(ctx))
	}
	ack, e := os.ReadFile(filepath.Join(f.config.StateDir, normalMemoryHostAckFile))
	require.NoError(t, e)
	require.NotEmpty(t, ack)
	b, e := os.ReadFile(trace)
	require.NoError(t, e)
	require.Equal(t, 1, bytes.Count(b, []byte("qom_set:")))
	require.Equal(t, 1, bytes.Count(b, []byte("origin_received")))
}
func TestNormalMemoryIntegrationRejectsForeignPartialOrMissingAck(t *testing.T) {
	for _, mode := range []string{"bad-ack-source", "partial-ack", "missing-ack"} {
		t.Run(mode, func(t *testing.T) {
			f, original, trace := normalMemoryIntegrationFixture(t, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, original.Path, original.Args[1:]...)
			cmd.Env = original.Env
			done := make(chan error, 1)
			go func() { done <- f.runner.RunQEMU(ctx, cmd) }()
			// 所有反例先证明真正到达新kind6，不能借更早超时取得假绿。
			require.NoError(t, waitInitialModelTrace(ctx, trace, "origin_received"))
			if mode == "bad-ack-source" {
				require.NoError(t, waitInitialModelTrace(ctx, trace, "bad_ack_sent"))
			}
			if mode == "partial-ack" {
				require.NoError(t, waitInitialModelTrace(ctx, trace, "partial_ack_sent"))
			}
			if mode == "missing-ack" {
				l := f.runner.initial.(*linuxInitialLifecycle)
				l.mu.Lock()
				require.Nil(t, l.ready)
				require.Nil(t, l.memoryOrigin)
				l.mu.Unlock()
				cancel()
			}
			select {
			case e := <-done:
				require.Error(t, e)
			case <-time.After(100 * time.Second):
				t.Fatal("original worker/Wait did not join")
			}
			l := f.runner.initial.(*linuxInitialLifecycle)
			l.mu.Lock()
			require.Nil(t, l.ready)
			require.Nil(t, l.memoryOrigin)
			l.mu.Unlock()
			_, e := os.Stat(filepath.Join(f.config.StateDir, normalMemoryHostAckFile))
			require.True(t, os.IsNotExist(e))
			b, e := os.ReadFile(trace)
			require.NoError(t, e)
			require.Equal(t, 1, bytes.Count(b, []byte("qom_set:")))
		})
	}
}

// 真实Unix协议模型明确证明第二connect不能在第一monitor未释放时取得greeting。
// 新完整1–7正控也采用相同串行monitor，防再次被并发model掩盖。
func TestNormalMemorySingleMonitorRequiresFirstConnectionClosed(t *testing.T) {
	e, _, _ := setupEngine(t)
	uuid, err := e.id.QEMUUUID()
	require.NoError(t, err)
	m := &initialLifecycleModel{uuid: uuid, dir: t.TempDir(), base: 256 << 20, maximum: 4 << 30}
	path := filepath.Join(t.TempDir(), "single.sock")
	listener, err := net.Listen("unix", path)
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			m.qmp(c)
		}
	}()
	defer func() { listener.Close(); <-done }()
	q := &nativeQMP{id: e.id, path: path, timeout: 150 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, err := q.connect(ctx)
	require.NoError(t, err)
	defer first.close()
	_, err = q.connect(ctx)
	require.Error(t, err, "nested monitor must not get a second greeting")
	first.close()
	third, err := q.connect(ctx)
	require.NoError(t, err, "closed monitor must allow next real handshake")
	third.close()
}
