package bootprotocol

import (
	"encoding/json"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 与旧Host严格decoder同一初始规则；新Host同时交叉运行两者，尚不替换旧路径。
// Mmax=真实machine max=pc.ram+vmem0全backend，不是Kpresent/online/requested。
type NormalMemoryGeometry struct {
	Identity       RunnerIdentity
	BaseBytes      control.Uint64
	BackendBytes   control.Uint64
	MaximumBytes   control.Uint64
	RequestedBytes control.Uint64
	ActualBytes    control.Uint64
	DeviceAddress  control.Uint64
	QEMUVersion    [3]uint32
}

func DecodeNormalMemoryGeometry(id RunnerIdentity, expectedBase control.Uint64, in NormalMemoryQMPInputs) (NormalMemoryGeometry, error) {
	fail := func() (NormalMemoryGeometry, error) {
		return NormalMemoryGeometry{}, fmt.Errorf("original QEMU maximum memory geometry differs")
	}
	if id.Validate() != nil || id.BlockBytes != (8<<20) || expectedBase == 0 || expectedBase > id.MinimumBytes || expectedBase%id.BlockBytes != 0 || (id.MinimumBytes-expectedBase)%id.BlockBytes != 0 || (id.MaximumBytes-expectedBase)%id.BlockBytes != 0 || id.MaximumBytes == expectedBase {
		return fail()
	}
	for _, raw := range []json.RawMessage{in.Version, in.Machine, in.Devices, in.Summary, in.Backends, in.BackendSize} {
		if control.ValidateJSON(raw, MaxFrameBytes, 4096) != nil {
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
	return NormalMemoryGeometry{Identity: id, BaseBytes: expectedBase, BackendBytes: control.Uint64(backend), MaximumBytes: id.MaximumBytes, RequestedBytes: expectedBase + control.Uint64(*d.Requested), ActualBytes: expectedBase + control.Uint64(*d.Size), DeviceAddress: control.Uint64(*d.Address), QEMUVersion: [3]uint32{8, 1, 5}}, nil
}
