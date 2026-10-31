//go:build linux

package vmguard

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 仅测试binary的显式模型分支，不会编入Runner。子进程确实持有两个Unix端点，
// QEMU/Guest/API/物理预算是模型；不得把此U说成KVM或真实Guest初始化。
func init() {
	if os.Getenv("NEON_INITIAL_LIFECYCLE_MODEL") != "explicit-model-v1" {
		return
	}
	if e := runInitialLifecycleModel(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(91)
	}
	os.Exit(0)
}

type initialLifecycleModel struct {
	mu                               sync.Mutex
	base, maximum, requested, actual uint64
	uuid, dir, scenario              string
	setCount                         int
}

func (m *initialLifecycleModel) trace(line string) {
	f, e := os.OpenFile(filepath.Join(m.dir, "model-trace.txt"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if e == nil {
		_, _ = fmt.Fprintln(f, line)
		_ = f.Close()
	}
}
func runInitialLifecycleModel() error {
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
			go m.qmp(c)
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
	select {}
}
func (m *initialLifecycleModel) qmp(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	enc := json.NewEncoder(c)
	_ = enc.Encode(map[string]any{"QMP": map[string]any{}})
	dec := json.NewDecoder(bufio.NewReader(c))
	for {
		var request struct {
			Execute   string          `json:"execute"`
			Arguments json.RawMessage `json:"arguments"`
			ID        string          `json:"id"`
		}
		if e := dec.Decode(&request); e != nil {
			return
		}
		var result any = map[string]any{}
		m.mu.Lock()
		switch request.Execute {
		case "qmp_capabilities":
		case "query-uuid":
			result = map[string]any{"UUID": m.uuid}
		case "query-version":
			result = map[string]any{"qemu": map[string]any{"major": 8, "minor": 1, "micro": 5}, "package": ""}
		case "qom-get":
			var a struct{ Path, Property string }
			_ = json.Unmarshal(request.Arguments, &a)
			if a.Path == "/machine" {
				result = map[string]any{"size": m.base, "slots": 1, "max-size": m.maximum}
			} else {
				result = m.maximum - m.base
			}
		case "query-memory-devices":
			result = []any{map[string]any{"type": "virtio-mem", "data": map[string]any{"id": "vm0", "memdev": "/objects/vmem0", "requested-size": m.requested, "size": m.actual, "max-size": m.maximum - m.base, "block-size": 8388608, "node": 0, "memaddr": 4294967296}}}
		case "query-memory-size-summary":
			result = map[string]any{"base-memory": m.base, "plugged-memory": m.actual}
		case "query-memdev":
			result = []any{map[string]any{"id": "vmem0", "size": m.maximum - m.base, "host-nodes": []any{}, "policy": "default"}, map[string]any{"id": "pc.ram", "size": m.base, "host-nodes": []any{}, "policy": "default"}}
		case "query-hotpluggable-cpus":
			result = []any{map[string]any{"type": "host-x86_64-cpu", "vcpus-count": 1, "props": map[string]any{"socket-id": 0, "core-id": 0, "thread-id": 0}}, map[string]any{"type": "host-x86_64-cpu", "vcpus-count": 1, "props": map[string]any{"socket-id": 0, "core-id": 1, "thread-id": 0}}}
			if m.scenario == "cpu-maximum-drift" {
				result = []any{map[string]any{"type": "host-x86_64-cpu", "vcpus-count": 1, "props": map[string]any{"socket-id": 0, "core-id": 0, "thread-id": 0}}}
			}

		case "qom-list":
			result = []any{map[string]any{"name": "vm0", "type": "child<virtio-mem-pci>"}}
			if m.scenario == "balloon-inventory" {
				result = []any{map[string]any{"name": "forbidden", "type": "child<virtio-balloon-pci>"}}
			}

		case "qom-set":
			if _, e := os.Stat(filepath.Join(m.dir, initialBootFile(4))); e != nil {
				m.trace("BAD_NO_MAY_HAVE_SENT")
				m.mu.Unlock()
				return
			}
			var a struct {
				Value uint64 `json:"value"`
			}
			_ = json.Unmarshal(request.Arguments, &a)
			m.requested, m.actual = a.Value, a.Value
			m.setCount++
			m.trace("qom_set:" + strconv.Itoa(m.setCount) + ":" + strconv.FormatUint(a.Value, 10))
			if m.scenario == "set-drop" {
				m.mu.Unlock()
				return
			}
		default:
			m.trace("BAD_COMMAND:" + request.Execute)
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		if e := enc.Encode(map[string]any{"return": result, "id": request.ID}); e != nil {
			return
		}
	}
}
func initialLifecycleFixture(t *testing.T, scenario string) (*runnerFixture, *exec.Cmd, string) {
	t.Helper()
	f, _, original := initialBootFixture(t, false)
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
	cmd := &exec.Cmd{Path: original.Path, Args: append([]string(nil), original.Args...), Env: append(os.Environ(), "NEON_INITIAL_LIFECYCLE_MODEL=explicit-model-v1", "NEON_INITIAL_MODEL_DIR="+f.config.StateDir, "NEON_INITIAL_MODEL_SCENARIO="+scenario)}
	return f, cmd, filepath.Join(f.config.StateDir, "model-trace.txt")
}
func waitInitialModelTrace(ctx context.Context, path, needle string) error {
	for {
		b, e := os.ReadFile(path)
		if e == nil && strings.Contains(string(b), needle) {
			return nil
		}
		if e = initialBootPause(ctx); e != nil {
			return e
		}
	}
}
func TestInitialLifecycleHostConsumerWholeModelChain(t *testing.T) {
	// PG17-U-INITIAL-L01：真实Start/唯一Wait、原pidfd/socket/WAL，QEMU/Guest/API明确模型。
	f, cmd, trace := initialLifecycleFixture(t, "success")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	original := cmd
	cmd = exec.CommandContext(ctx, original.Path, original.Args[1:]...)
	cmd.Env = original.Env
	result := make(chan error, 1)
	go func() { result <- f.runner.RunQEMU(ctx, cmd); close(result) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-result:
		case <-time.After(10 * time.Second):
			t.Error("original RunQEMU did not terminate")
		}
	})
	require.NoError(t, waitInitialModelTrace(ctx, trace, "completion_received"))
	for {
		e := f.runner.Tick(ctx)
		if e == nil {
			break
		}
		require.NoError(t, initialBootPause(ctx))
	}
	reader, e := NewReader(f.core.CoreV1(), f.store.Binding(), "lease-uid")
	require.NoError(t, e)
	birth, _, publication, e := reader.Inspect(ctx, &f.runner.engine.binding, f.actor)
	require.NoError(t, e)
	require.EqualValues(t, 2, birth.ProtocolVersion)
	require.EqualValues(t, 2, publication.ProtocolVersion)
	require.NotNil(t, publication.InitialBootstrap)
	require.Equal(t, control.Uint64(1<<30), publication.Actual.ActualBytes)
	b, e := os.ReadFile(trace)
	require.NoError(t, e)
	require.Equal(t, 1, bytes.Count(b, []byte("qom_set:")))
	require.NotContains(t, string(b), "BAD_")
	require.Error(t, f.runner.engine.native.Set(ctx, 256<<20), "普通业务Min边界不下调")
	cancel()
	require.Error(t, <-result)
	require.NotNil(t, cmd.ProcessState, "唯一RunQEMU实际Wait必须收原child")
	_, _, _, e = ReadPublished(context.Background(), f.core.CoreV1(), f.store.Binding(), &f.runner.engine.binding)
	require.ErrorIs(t, e, ErrStopped)
}
func TestInitialLifecycleUncertainGrowNeverRepeats(t *testing.T) {
	// PG17-U-INITIAL-L02：Set已应用但回复丢失，保may-have-sent/原预算，不改NoEffect。
	f, cmd, trace := initialLifecycleFixture(t, "set-drop")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.Error(t, f.runner.RunQEMU(ctx, cmd))
	b, e := os.ReadFile(trace)
	require.NoError(t, e)
	require.Equal(t, 1, bytes.Count(b, []byte("qom_set:")))
	state, e := lockState(context.Background(), f.config.StateDir)
	require.NoError(t, e)
	defer state.close()
	entries, e := initialBootRead(state)
	require.NoError(t, e)
	require.Len(t, entries, 5)
	require.Equal(t, "grow_may_have_sent", entries[4].Stage)
	require.Nil(t, f.runner.initial.(*linuxInitialLifecycle).ready)
	require.NotNil(t, cmd.ProcessState)
}
func TestInitialLifecycleForeignReceiptAndCancellationCloseOriginal(t *testing.T) {
	// PG17-U-INITIAL-L03：错Guest字段/取消先关流并join，不能留写者或启动普通接口。
	for _, scenario := range []string{"bad-prepared", "cancel-prepared"} {
		t.Run(scenario, func(t *testing.T) {
			f, cmd, trace := initialLifecycleFixture(t, scenario)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			original := cmd
			cmd = exec.CommandContext(ctx, original.Path, original.Args[1:]...)
			cmd.Env = original.Env
			result := make(chan error, 1)
			go func() { result <- f.runner.RunQEMU(ctx, cmd); close(result) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-result:
				case <-time.After(10 * time.Second):
					t.Error("original RunQEMU did not terminate")
				}
			})
			require.NoError(t, waitInitialModelTrace(ctx, trace, "prepared_received"))
			if scenario == "cancel-prepared" {
				cancel()
			}
			require.Error(t, <-result)
			b, e := os.ReadFile(trace)
			require.NoError(t, e)
			require.NotContains(t, string(b), "qom_set:")
			require.NotNil(t, cmd.ProcessState)
			require.Error(t, f.runner.Tick(context.Background()))
		})
	}
}
func TestInitialLifecycleWireVersionsAndLegacyBytes(t *testing.T) {
	// PG17-U-INITIAL-L04：v1无新字段/即便null存在也拒；v2不凭generic模型链当完成。
	id := fixtureIdentity()
	birth := Birth{ProtocolVersion: 1, Identity: id, StateClaim: budget.ObjectRef{Name: "state", UID: "state-uid"}}
	raw, e := json.Marshal(birth)
	require.NoError(t, e)
	require.NotContains(t, string(raw), "initial_mode")
	var decoded Birth
	corrupt := bytes.Replace(raw, []byte(`"protocol_version":1`), []byte(`"initial_mode":null,"protocol_version":1`), 1)
	require.Error(t, json.Unmarshal(corrupt, &decoded))
	publication := Publication{ProtocolVersion: 1, Identity: id}
	raw, e = json.Marshal(publication)
	require.NoError(t, e)
	require.NotContains(t, string(raw), "initial_bootstrap")
	corrupt = bytes.Replace(raw, []byte(`"protocol_version":1`), []byte(`"initial_bootstrap":null,"protocol_version":1`), 1)
	var parsed Publication
	require.Error(t, json.Unmarshal(corrupt, &parsed))
	f, _, _ := initialBootFixture(t, false)
	require.Error(t, validateInitialLifecyclePublication(f.runner.birth, f.runner.engine.binding, InitialBootstrapPublication{}))
	require.Error(t, f.runner.Tick(context.Background()), "没有真实初始完成不得发布")
}

func TestInitialLifecycleRejectsBalloonAndExternalCommandSurfaces(t *testing.T) {
	// PG17-U-INITIAL-L05：长期禁device_add的入口约束，不能只凭一帧库存采样。
	for _, name := range []string{"virtio-balloon", "xen-balloon", "dimm", "alternate-driver", "hmp", "implicit-hmp", "duplicate-port", "extra-monitor"} {
		t.Run(name, func(t *testing.T) {
			f, _, cmd := initialBootFixture(t, false)
			switch name {
			case "virtio-balloon":
				cmd.Args = append(cmd.Args, "-device", "virtio-balloon-pci,id=bad")
			case "xen-balloon":
				cmd.Args = append(cmd.Args, "-device", "xen-balloon,id=bad")
			case "dimm":
				cmd.Args = append(cmd.Args, "-device", "pc-dimm,id=bad")
			case "alternate-driver":
				cmd.Args = append(cmd.Args, "-device", "driver=virtio-balloon-pci,id=bad")
			case "hmp":
				cmd.Args = append(cmd.Args, "-serial", "mon:stdio")
			case "implicit-hmp":
				for i := 1; i < len(cmd.Args); i++ {
					if cmd.Args[i] == "-monitor" {
						cmd.Args = append(cmd.Args[:i], cmd.Args[i+2:]...)
						break
					}
				}
			case "duplicate-port":
				cmd.Args = append(cmd.Args, f.runner.BootChannelArguments()...)
			case "extra-monitor":
				cmd.Args = append(cmd.Args, "-mon", "chardev=bad")
			}
			require.Error(t, validateInitialBootLaunch(cmd, f.runner, f.runner.BootChannelArguments()))
			require.Nil(t, cmd.Process)
		})
	}
}
func TestInitialLifecycleRejectsActualBalloonAndCPUInventory(t *testing.T) {
	// PG17-U-INITIAL-L06：真实子进程QMP模型库存与原argv互证，不从current CPU取上限。
	for _, scenario := range []string{"balloon-inventory", "cpu-maximum-drift"} {
		t.Run(scenario, func(t *testing.T) {
			f, cmd, trace := initialLifecycleFixture(t, scenario)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.Error(t, f.runner.RunQEMU(ctx, cmd))
			b, e := os.ReadFile(trace)
			if e == nil {
				require.NotContains(t, string(b), "qom_set:")
				require.NotContains(t, string(b), "prepared_received")
			} else {
				require.True(t, os.IsNotExist(e))
			}
			require.NotNil(t, cmd.ProcessState, "错误库存必须关闭并实际Wait原child")
			require.Error(t, f.runner.Tick(context.Background()))
		})
	}
}
