package vmguard

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

// 真实Unix socket协议夹具；没有启动QEMU/KVM。验证精确整数、逐帧有界IO、
// UUID/设备/几何交叉校验、qom-set的单次发送和WAL恢复，不外推Guest物理回收。
type qmpFixture struct {
	mu       sync.Mutex
	id       Identity
	actual   Observation
	mode     string
	writes   int
	requests []string
	walDir   string
}

func startQMPFixture(t *testing.T, f *qmpFixture) string {
	t.Helper()
	// Darwin sockaddr_un长度较短，系统t.TempDir的长路径不适合作socket地址。
	dir, err := os.MkdirTemp("/private/tmp", "dsga-qmp-")
	require.NoError(t, err)
	require.NoError(t, os.Chmod(dir, 0o700))
	path := filepath.Join(dir, "qmp.sock")
	l, err := net.Listen("unix", path)
	require.NoError(t, err)
	var wg sync.WaitGroup
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() { defer wg.Done(); defer c.Close(); f.serve(c) }()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		<-finished
		wg.Wait()
		os.RemoveAll(dir)
	})
	return path
}

func (f *qmpFixture) serve(c net.Conn) {
	if err := c.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return
	}
	enc := json.NewEncoder(c)
	if err := enc.Encode(map[string]any{"QMP": map[string]any{"version": map[string]any{}, "capabilities": []string{}}}); err != nil {
		return
	}
	scan := bufio.NewScanner(c)
	for scan.Scan() {
		var command struct {
			Execute   string `json:"execute"`
			ID        string `json:"id"`
			Arguments struct {
				Path     string `json:"path"`
				Property string `json:"property"`
				Value    uint64 `json:"value"`
			} `json:"arguments"`
		}
		if json.Unmarshal(scan.Bytes(), &command) != nil {
			return
		}
		f.mu.Lock()
		f.requests = append(f.requests, command.Execute)
		mode := f.mode
		var value any = map[string]any{}
		switch command.Execute {
		case "qmp_capabilities":
			if mode == "event" {
				if err := enc.Encode(map[string]any{"event": "TEST"}); err != nil {
					f.mu.Unlock()
					return
				}
			}
		case "query-uuid":
			uuid, _ := f.id.QEMUUUID()
			if mode == "wrong-qemu" {
				uuid = "00000000-0000-0000-0000-000000000000"
			}
			value = map[string]any{"UUID": uuid}
		case "query-memory-devices":
			dev := map[string]any{"id": "vm0", "memdev": "/objects/vmem0", "requested-size": uint64(f.actual.RequestedBytes - f.id.MinimumBytes), "size": uint64(f.actual.ActualBytes - f.id.MinimumBytes), "max-size": uint64(f.id.MaximumBytes - f.id.MinimumBytes), "block-size": uint64(f.id.BlockBytes)}
			switch mode {
			case "wrong-device":
				dev["id"] = "different-device"
			case "wrong-block":
				dev["block-size"] = 4096
			case "wrong-maximum":
				dev["max-size"] = uint64(f.id.MaximumBytes)
			case "missing-size":
				delete(dev, "size")
			case "numeric-string":
				dev["size"] = "123"
			}
			value = []any{map[string]any{"type": "virtio-mem", "data": dev}}
			if f.id.MinimumBytes == f.id.MaximumBytes {
				value = []any{}
			}
			if mode == "extra-device" {
				value = []any{map[string]any{"type": "virtio-mem", "data": dev}, map[string]any{"type": "dimm"}}
			}
		case "query-memory-size-summary":
			base := uint64(f.id.MinimumBytes)
			if mode == "wrong-base" {
				base++
			}
			value = map[string]any{"base-memory": base, "plugged-memory": uint64(f.actual.ActualBytes - f.id.MinimumBytes)}
		case "qom-set":
			if command.Arguments.Path != "/machine/peripheral/vm0" || command.Arguments.Property != "requested-size" {
				f.mu.Unlock()
				return
			}
			if f.walDir != "" {
				raw, err := os.ReadFile(filepath.Join(f.walDir, "state.json"))
				var r record
				if err != nil || json.Unmarshal(raw, &r) != nil || r.Pending == nil || r.Pending.Stage != mayHaveSent || uint64(r.Pending.Command.Request.TargetVMMemoryBytes-f.id.MinimumBytes) != command.Arguments.Value {
					f.mu.Unlock()
					return
				}
			}
			f.writes++
			target := f.id.MinimumBytes + control.Uint64(command.Arguments.Value)
			f.actual = Observation{RequestedBytes: target, ActualBytes: target}
			if mode == "lost-set-reply" {
				f.mu.Unlock()
				return
			}
		default:
			f.mu.Unlock()
			return
		}
		f.mu.Unlock()
		if mode == "oversized" {
			fmt.Fprintln(c, strings.Repeat("x", qmpFrameLimit+1))
			return
		}
		id := command.ID
		if mode == "wrong-response-id" {
			id = "other-response"
		}
		if mode == "duplicate-field" {
			fmt.Fprintf(c, "{\"return\":{},\"return\":{},\"id\":%q}\n", id)
			return
		}
		if mode == "return-and-error" {
			if err := enc.Encode(map[string]any{"id": id, "return": value, "error": map[string]any{}}); err != nil {
				return
			}
			return
		}
		if mode == "endless-events" {
			for range 66 {
				if err := enc.Encode(map[string]any{"event": "TEST"}); err != nil {
					return
				}
			}
			return
		}
		if mode == "silent" {
			time.Sleep(250 * time.Millisecond)
			return
		}
		if err := enc.Encode(map[string]any{"return": value, "id": id}); err != nil {
			return
		}
	}
}

func TestPrivateQMPEngineGrowShrinkLostReplyAndExactReadback(t *testing.T) {
	e, _, a := setupEngine(t)
	a.Command.Request.Kind = control.Grow
	a.Command.BeforeBytes, a.Command.Request.TargetVMMemoryBytes, a.Command.GuestSHA256 = 2<<30, 3<<30, ""
	f := &qmpFixture{id: e.id, actual: Observation{RequestedBytes: 2 << 30, ActualBytes: 2 << 30}, mode: "lost-set-reply", walDir: e.dir, mu: sync.Mutex{}, writes: 0, requests: nil}
	q := &nativeQMP{id: e.id, path: startQMPFixture(t, f), timeout: time.Second}
	e.native = q
	require.ErrorIs(t, e.advance(context.Background()), ErrUnknown)
	require.Equal(t, mayHaveSent, readFixtureRecord(t, e).Pending.Stage)
	// 新连接只能读回同一原目标。服务已执行但丢回复时，不重发即可结清。
	restarted := *e
	require.NoError(t, restarted.advance(context.Background()))
	r := readFixtureRecord(t, e)
	require.Equal(t, completed, r.Last.Kind)
	require.Equal(t, control.Uint64(3<<30), r.Last.Actual.ActualBytes)
	f.mu.Lock()
	f.mode = "event"
	f.mu.Unlock()
	// 第二个真实原意图走同一执行者反向缩回；不是重复测试同一个目标。
	a.Command.Request.Kind = control.Shrink
	a.Command.Request.Key.Revision++
	a.Command.Request.Key.OperationID.Low++
	a.Command.BeforeBytes, a.Command.Request.TargetVMMemoryBytes, a.Command.GuestSHA256 = 3<<30, 2<<30, strings.Repeat("c", 64)
	require.ErrorContains(t, e.advance(context.Background()), "durably published")
	// 本用例只验证原生适配器；以明确来源夹具模拟上层确认不可变CM已持有。
	// 实际Kube发布/UID/RV丢回复在Runner组合用例单独验证，不由此夹具冒充。
	r.LastPublished = &budget.ObjectRef{Name: "fixture-published", UID: "fixture-uid"}
	writeFixtureRecord(t, e, r)
	require.NoError(t, e.advance(context.Background()))
	r = readFixtureRecord(t, e)
	require.Equal(t, control.Uint64(2<<30), r.Last.Actual.ActualBytes)
	f.mu.Lock()
	writes := f.writes
	f.mu.Unlock()
	require.Equal(t, 2, writes, "one mutation per original intent even after lost reply")
}

func TestPrivateQMPRejectsAmbiguousIdentityGeometryAndFrames(t *testing.T) {
	for _, mode := range []string{"wrong-qemu", "wrong-device", "wrong-block", "wrong-maximum", "wrong-base", "extra-device", "missing-size", "numeric-string", "wrong-response-id", "return-and-error", "duplicate-field", "oversized", "endless-events"} {
		t.Run(mode, func(t *testing.T) {
			id := fixtureIdentity()
			f := &qmpFixture{id: id, actual: Observation{RequestedBytes: 2 << 30, ActualBytes: 2 << 30}, mode: mode, mu: sync.Mutex{}, writes: 0, requests: nil, walDir: ""}
			q := &nativeQMP{id: id, path: startQMPFixture(t, f), timeout: time.Second}
			_, err := q.Read(context.Background())
			require.Error(t, err)
			err = q.Set(context.Background(), 3<<30)
			var absent *notSentError
			require.ErrorAs(t, err, &absent, "preflight rejection proves this call never began a mutation")
			f.mu.Lock()
			writes := f.writes
			f.mu.Unlock()
			require.Zero(t, writes)
		})
	}
}

func TestPrivateQMPExactLargeIntegersFixedSizeAndDeadlines(t *testing.T) {
	t.Run("beyond-float64", func(t *testing.T) {
		id := fixtureIdentity()
		id.MinimumBytes, id.MaximumBytes = 1<<54, 1<<55
		id.BlockBytes = 1 // 仅数字协议夹具；实际Runner固定8MiB出生几何。
		n := id.MinimumBytes + 17
		f := &qmpFixture{id: id, actual: Observation{RequestedBytes: n, ActualBytes: n}, mu: sync.Mutex{}, mode: "", writes: 0, requests: nil, walDir: ""}
		q := &nativeQMP{id: id, path: startQMPFixture(t, f), timeout: time.Second}
		o, err := q.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, n, o.ActualBytes)
		require.NoError(t, q.Set(context.Background(), n+2))
		o, err = q.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, n+2, o.ActualBytes)
	})
	t.Run("no-virtio-device", func(t *testing.T) {
		id := fixtureIdentity()
		id.MaximumBytes = id.MinimumBytes
		f := &qmpFixture{id: id, actual: Observation{RequestedBytes: id.MinimumBytes, ActualBytes: id.MinimumBytes}, mu: sync.Mutex{}, mode: "", writes: 0, requests: nil, walDir: ""}
		q := &nativeQMP{id: id, path: startQMPFixture(t, f), timeout: time.Second}
		_, err := q.Read(context.Background())
		require.NoError(t, err)
		var absent *notSentError
		require.ErrorAs(t, q.Set(context.Background(), id.MinimumBytes), &absent)
	})
	t.Run("deadline-no-background-write", func(t *testing.T) {
		id := fixtureIdentity()
		f := &qmpFixture{id: id, actual: Observation{RequestedBytes: id.MinimumBytes, ActualBytes: id.MinimumBytes}, mode: "silent", mu: sync.Mutex{}, writes: 0, requests: nil, walDir: ""}
		q := &nativeQMP{id: id, path: startQMPFixture(t, f), timeout: 100 * time.Millisecond}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := q.Read(ctx)
		require.ErrorIs(t, err, context.Canceled)
		begin := time.Now()
		err = q.Set(context.Background(), 2<<30)
		var absent *notSentError
		require.True(t, errors.As(err, &absent))
		require.Less(t, time.Since(begin), time.Second)
		f.mu.Lock()
		writes := f.writes
		f.mu.Unlock()
		require.Zero(t, writes)
	})
}
