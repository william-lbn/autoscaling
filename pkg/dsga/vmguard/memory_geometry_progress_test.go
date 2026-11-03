package vmguard

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

// 六份原 return 来自 29fe9c 实际失败帧（SHA256 2b7fba19...e29ecf），
// 不包含 private Runtime、凭据或原 VM identity。只以本包 fixture identity 绑定。
func actual29feGeometryFixture() (Identity, control.Uint64, memoryGeometryInputs) {
	id := geometryIdentityFixture()
	id.MinimumBytes = 7 << 30
	id.MaximumBytes = 8 << 30
	return id, 512 << 20, memoryGeometryInputs{
		Version:     json.RawMessage("{\"qemu\":{\"micro\":5,\"minor\":1,\"major\":8},\"package\":\"\"}"),
		Machine:     json.RawMessage("{\"size\":536870912,\"slots\":1,\"max-size\":8589934592}"),
		Devices:     json.RawMessage("[{\"type\":\"virtio-mem\",\"data\":{\"memdev\":\"/objects/vmem0\",\"memaddr\":4294967296,\"block-size\":8388608,\"size\":4697620480,\"node\":0,\"max-size\":8053063680,\"requested-size\":6979321856,\"id\":\"vm0\"}}]"),
		Summary:     json.RawMessage("{\"base-memory\":536870912,\"plugged-memory\":4831838208}"),
		Backends:    json.RawMessage("[{\"share\":false,\"reserve\":true,\"prealloc\":false,\"host-nodes\":[],\"size\":8053063680,\"merge\":true,\"dump\":true,\"policy\":\"default\",\"id\":\"vmem0\"},{\"share\":false,\"reserve\":true,\"prealloc\":false,\"host-nodes\":[],\"size\":536870912,\"merge\":true,\"dump\":true,\"policy\":\"default\",\"id\":\"pc.ram\"}]"),
		BackendSize: json.RawMessage("8053063680"),
	}
}

func progressGeometryValues(t *testing.T, in memoryGeometryInputs, requested, device, summary uint64) memoryGeometryInputs {
	t.Helper()
	var devices []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(in.Devices, &devices))
	var data map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(devices[0]["data"], &data))
	data["requested-size"] = json.RawMessage(strconv.FormatUint(requested, 10))
	data["size"] = json.RawMessage(strconv.FormatUint(device, 10))
	var err error
	devices[0]["data"], err = json.Marshal(data)
	require.NoError(t, err)
	in.Devices, err = json.Marshal(devices)
	require.NoError(t, err)
	var s map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(in.Summary, &s))
	s["plugged-memory"] = json.RawMessage(strconv.FormatUint(summary, 10))
	in.Summary, err = json.Marshal(s)
	require.NoError(t, err)
	return in
}

func TestOriginalGeometryActual29feRaceWaitsForFreshCompleteFrame(t *testing.T) {
	id, base, in := actual29feGeometryFixture()
	target := control.Uint64(7 << 30)
	got, err := decodeOriginalMemoryGeometry(id, base, in)
	require.Error(t, err)
	require.Equal(t, originalMemoryGeometry{}, got, "在途帧不能产生成功几何")
	require.True(t, awaitsOriginalMemoryGeometryTarget(err, id, base, target, true))
	var diagnostic *originalMemoryGeometryFailure
	require.ErrorAs(t, err, &diagnostic)
	require.Equal(t, "summary", diagnostic.Stage)
	final := progressGeometryValues(t, in, 6979321856, 6979321856, 6979321856)
	settled, err := decodeOriginalMemoryGeometry(id, base, final)
	require.NoError(t, err)
	require.Equal(t, target, settled.RequestedBytes)
	require.Equal(t, target, settled.ActualBytes)
}

func TestOriginalGeometryProgressDirectionAndTargetBounds(t *testing.T) {
	id, base, in := actual29feGeometryFixture()
	const targetOffset uint64 = 6979321856
	cases := []struct {
		name            string
		device, summary uint64
		await           bool
	}{
		{"grow", 4697620480, 4831838208, true}, {"grow-final-query-reaches-target", 4697620480, targetOffset, true},
		{"shrink", 7516192768, 7113539584, true}, {"shrink-final-query-reaches-target", 7516192768, targetOffset, true},
		{"reverse-grow", 4697620480, 4689231872, false}, {"reverse-shrink", 7516192768, 7524581376, false},
		{"grow-past-target", 4697620480, targetOffset + 8388608, false}, {"shrink-past-target", 7516192768, targetOffset - 8388608, false},
		{"settled-device-disagrees", targetOffset, targetOffset - 8388608, false}, {"unaligned-summary", 4697620480, 4831838209, false},
		{"summary-over-backend", 4697620480, 8061452288, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			frame := progressGeometryValues(t, in, targetOffset, c.device, c.summary)
			got, err := decodeOriginalMemoryGeometry(id, base, frame)
			require.Error(t, err)
			require.Equal(t, originalMemoryGeometry{}, got)
			require.Equal(t, c.await, awaitsOriginalMemoryGeometryTarget(err, id, base, base+control.Uint64(targetOffset), c.device < targetOffset))
			if c.device > targetOffset {
				require.False(t, awaitsOriginalMemoryGeometryTarget(err, id, base, base+control.Uint64(targetOffset), true), "原 Grow 循环不得消费 Shrink 在途分类")
			}
		})
	}
}

func TestOriginalGeometryProgressCannotHideInvalidStaticFrame(t *testing.T) {
	id, base, in := actual29feGeometryFixture()
	fields := []struct {
		name string
		set  func(*memoryGeometryInputs)
	}{
		{"version", func(x *memoryGeometryInputs) { x.Version = json.RawMessage(`{"qemu":{"major":8,"minor":1,"micro":6}}`) }},
		{"machine", func(x *memoryGeometryInputs) {
			x.Machine = json.RawMessage(`{"size":536870912,"slots":2,"max-size":8589934592}`)
		}},
		{"backend-size", func(x *memoryGeometryInputs) { x.BackendSize = json.RawMessage(`8061452288`) }},
		{"backend-properties", func(x *memoryGeometryInputs) {
			x.Backends = json.RawMessage(`[{"id":"vmem0","size":8053063680,"host-nodes":[0],"policy":"default"},{"id":"pc.ram","size":536870912,"host-nodes":[],"policy":"default"}]`)
		}},
		{"summary-base", func(x *memoryGeometryInputs) {
			x.Summary = json.RawMessage(`{"base-memory":268435456,"plugged-memory":4831838208}`)
		}},
		{"devices-json", func(x *memoryGeometryInputs) { x.Devices = json.RawMessage(`null`) }},
	}
	for _, f := range fields {
		t.Run(f.name, func(t *testing.T) {
			frame := in
			f.set(&frame)
			_, err := decodeOriginalMemoryGeometry(id, base, frame)
			require.Error(t, err)
			require.False(t, awaitsOriginalMemoryGeometryTarget(err, id, base, 7<<30, true))
		})
	}
}

func TestOriginalGeometryAwaitRequiresOriginalTargetAndLiveContext(t *testing.T) {
	id, base, in := actual29feGeometryFixture()
	_, err := decodeOriginalMemoryGeometry(id, base, in)
	require.True(t, awaitsOriginalMemoryGeometryTarget(err, id, base, 7<<30, true))
	require.False(t, awaitsOriginalMemoryGeometryTarget(err, id, base, (7<<30)-VirtioBlockBytes, true))
	changed := id
	changed.VMUID = "different-vm"
	require.False(t, awaitsOriginalMemoryGeometryTarget(err, changed, base, 7<<30, true))
	require.False(t, awaitsOriginalMemoryGeometryTarget(err, id, base+VirtioBlockBytes, 7<<30, true))
	require.False(t, awaitsOriginalMemoryGeometryTarget(errors.New("unrelated geometry error"), id, base, 7<<30, true))
	var original *originalMemoryGeometryFailure
	require.ErrorAs(t, err, &original)
	raw, e := json.Marshal(original)
	require.NoError(t, e)
	var archive originalMemoryGeometryFailure
	require.NoError(t, json.Unmarshal(raw, &archive))
	require.False(t, awaitsOriginalMemoryGeometryTarget(&archive, id, base, 7<<30, true), "归档不能伪造本次decoder分类")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, initialBootPause(ctx), context.Canceled, "等待必须消费原ctx，不能换新deadline")
}
