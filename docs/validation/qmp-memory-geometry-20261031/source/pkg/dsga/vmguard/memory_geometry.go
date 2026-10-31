package vmguard

import (
	"context"
	"encoding/json"
	"fmt"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// originalMemoryGeometry只记同一原UUID/QMP连接的真实容量。它没有公开票据接口，
// 不能独立认证Host保留、Guest物理区、原进程或normal降限。BaseBytes允许比业务
// MinimumBytes小，以便将来显式的earlyboot合同先安置System，再按原意图加入Movable；
// 这里不调用Set、不放宽旧Observation/Command的最小值，也不产生Bootstrap资格。
type originalMemoryGeometry struct {
	Identity       Identity
	BaseBytes      control.Uint64
	BackendBytes   control.Uint64
	MaximumBytes   control.Uint64
	RequestedBytes control.Uint64
	ActualBytes    control.Uint64
	DeviceAddress  control.Uint64
	QEMUVersion    [3]uint32
}

type memoryGeometryInputs struct {
	Version     json.RawMessage
	Machine     json.RawMessage
	Devices     json.RawMessage
	Summary     json.RawMessage
	Backends    json.RawMessage
	BackendSize json.RawMessage
}

// decodeOriginalMemoryGeometry不接受“最大值”自报参数：expectedBase来自原启动合同，
// 最大值来自原Runner Identity，并与机器maxmem、单个virtio-mem最大值、实际后端
// 容量及base逐个互证。QEMU8.1.5真实query-memdev包括自动pc.ram，不能误认成额外设备。
// 整数不经float64；缺字段/null/第二个设备/未知backend都拒绝。原始QMP帧已由
// qmpConnection验证重复键和大小，这里仍对独立调用逐份验证，避免测试/未来调用绕过。
func decodeOriginalMemoryGeometry(id Identity, expectedBase control.Uint64, in memoryGeometryInputs) (originalMemoryGeometry, error) {
	fail := func() (originalMemoryGeometry, error) {
		return originalMemoryGeometry{}, fmt.Errorf("original QEMU maximum memory geometry differs")
	}
	if id.Validate() != nil || id.BlockBytes != VirtioBlockBytes || expectedBase == 0 || expectedBase > id.MinimumBytes || expectedBase%id.BlockBytes != 0 || (id.MinimumBytes-expectedBase)%id.BlockBytes != 0 || (id.MaximumBytes-expectedBase)%id.BlockBytes != 0 || id.MaximumBytes == expectedBase {
		return fail()
	}
	for _, raw := range []json.RawMessage{in.Version, in.Machine, in.Devices, in.Summary, in.Backends, in.BackendSize} {
		if control.ValidateJSON(raw, qmpFrameLimit, 4096) != nil {
			return fail()
		}
	}
	var version struct {
		QEMU *struct {
			Major *uint32 `json:"major"`
			Minor *uint32 `json:"minor"`
			Micro *uint32 `json:"micro"`
		} `json:"qemu"`
	}
	if json.Unmarshal(in.Version, &version) != nil || version.QEMU == nil || version.QEMU.Major == nil || version.QEMU.Minor == nil || version.QEMU.Micro == nil || *version.QEMU.Major != 8 || *version.QEMU.Minor != 1 || *version.QEMU.Micro != 5 {
		return fail()
	}
	var machine struct {
		Size    *uint64 `json:"size"`
		Maximum *uint64 `json:"max-size"`
		Slots   *uint64 `json:"slots"`
	}
	if json.Unmarshal(in.Machine, &machine) != nil || machine.Size == nil || machine.Maximum == nil || machine.Slots == nil || *machine.Size != uint64(expectedBase) || *machine.Maximum != uint64(id.MaximumBytes) || *machine.Slots != 1 {
		return fail()
	}
	var devices []struct {
		Type string `json:"type"`
		Data *struct {
			ID        string  `json:"id"`
			Memdev    string  `json:"memdev"`
			Requested *uint64 `json:"requested-size"`
			Size      *uint64 `json:"size"`
			Maximum   *uint64 `json:"max-size"`
			Block     *uint64 `json:"block-size"`
			Node      *uint32 `json:"node"`
			Address   *uint64 `json:"memaddr"`
		} `json:"data"`
	}
	if json.Unmarshal(in.Devices, &devices) != nil || len(devices) != 1 || devices[0].Type != "virtio-mem" || devices[0].Data == nil {
		return fail()
	}
	d := devices[0].Data
	backend := uint64(id.MaximumBytes - expectedBase)
	if d.ID != "vm0" || d.Memdev != "/objects/vmem0" || d.Requested == nil || d.Size == nil || d.Maximum == nil || d.Block == nil || d.Node == nil || d.Address == nil || *d.Maximum != backend || *d.Block != uint64(id.BlockBytes) || *d.Node != 0 || *d.Address < uint64(expectedBase) || *d.Address%uint64(id.BlockBytes) != 0 || *d.Address > ^uint64(0)-backend || *d.Requested > backend || *d.Size > backend || *d.Requested%uint64(id.BlockBytes) != 0 || *d.Size%uint64(id.BlockBytes) != 0 {
		return fail()
	}
	var summary struct {
		Base    *uint64 `json:"base-memory"`
		Plugged *uint64 `json:"plugged-memory"`
	}
	if json.Unmarshal(in.Summary, &summary) != nil || summary.Base == nil || summary.Plugged == nil || *summary.Base != uint64(expectedBase) || *summary.Plugged != *d.Size {
		return fail()
	}
	var backends []struct {
		ID     string    `json:"id"`
		Size   *uint64   `json:"size"`
		Nodes  *[]uint32 `json:"host-nodes"`
		Policy *string   `json:"policy"`
	}
	if json.Unmarshal(in.Backends, &backends) != nil || len(backends) != 2 {
		return fail()
	}
	seen := map[string]bool{}
	for _, b := range backends {
		want := backend
		if b.ID == "pc.ram" {
			want = uint64(expectedBase)
		} else if b.ID != "vmem0" {
			return fail()
		}
		if seen[b.ID] || b.Size == nil || *b.Size != want || b.Nodes == nil || len(*b.Nodes) != 0 || b.Policy == nil || *b.Policy != "default" {
			return fail()
		}
		seen[b.ID] = true
	}
	var size *uint64
	if json.Unmarshal(in.BackendSize, &size) != nil || size == nil || *size != backend {
		return fail()
	}
	return originalMemoryGeometry{Identity: id, BaseBytes: expectedBase, BackendBytes: control.Uint64(backend), MaximumBytes: id.MaximumBytes, RequestedBytes: expectedBase + control.Uint64(*d.Requested), ActualBytes: expectedBase + control.Uint64(*d.Size), DeviceAddress: control.Uint64(*d.Address), QEMUVersion: [3]uint32{8, 1, 5}}, nil
}

// readOriginalMemoryGeometry在同一连接核原UUID；所有命令固定只读，绝不qom-set。
// 必须在原Runner锁/原进程生命周期合同内调用。此私有组件尚未替代生产出生/预算
// 消费者；只读成功仍不说明当前Guest已达到业务Minimum或允许降低normal。
func (q *nativeQMP) readOriginalMemoryGeometry(ctx context.Context, expectedBase control.Uint64) (originalMemoryGeometry, error) {
	c, err := q.connect(ctx)
	if err != nil {
		return originalMemoryGeometry{}, err
	}
	defer c.close()
	in := memoryGeometryInputs{}
	commands := []struct {
		name string
		args any
		dst  *json.RawMessage
	}{
		{"query-version", nil, &in.Version},
		{"qom-get", struct {
			Path     string `json:"path"`
			Property string `json:"property"`
		}{"/machine", "memory"}, &in.Machine},
		{"query-memory-devices", nil, &in.Devices},
		{"query-memory-size-summary", nil, &in.Summary},
		{"query-memdev", nil, &in.Backends},
		{"qom-get", struct {
			Path     string `json:"path"`
			Property string `json:"property"`
		}{"/objects/vmem0", "size"}, &in.BackendSize},
	}
	for _, command := range commands {
		raw, err := c.execute(command.name, command.args)
		if err != nil {
			return originalMemoryGeometry{}, err
		}
		*command.dst = raw
	}
	return decodeOriginalMemoryGeometry(q.id, expectedBase, in)
}
