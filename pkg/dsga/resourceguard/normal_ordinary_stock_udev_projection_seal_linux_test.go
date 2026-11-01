//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"strconv"
	"testing"
)

func normalStockSealModel() normalOrdinaryUdevV2DeviceProjection {
	return normalOrdinaryUdevV2DeviceProjection{Devpath: "/devices/model", Subsystem: "misc", UeventObject: normalFilesystemObject{Device: 1, Inode: 2, MountID: 3, Filesystem: unix.SYSFS_MAGIC}, Properties: map[string]string{"DEVPATH": "/devices/model", "SUBSYSTEM": "misc", "ACTION": "add", "SEQNUM": "1", "ID_MODEL": "original"}}
}
func TestNormalStockProjectionSealKernelWriteProtection(t *testing.T) {
	p, e := sealNormalOrdinaryStockUdevProjection(context.Background(), []normalOrdinaryUdevV2DeviceProjection{normalStockSealModel()})
	if e != nil {
		t.Fatal(e)
	}
	defer p.close()
	sha, e := p.digest()
	if e != nil || !physicalDigestValid(sha) {
		t.Fatal(e)
	}
	rw, e := unix.Open("/proc/self/fd/"+strconv.Itoa(int(p.file.Fd())), unix.O_RDWR|unix.O_CLOEXEC, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer unix.Close(rw)
	var st unix.Stat_t
	if e = unix.Fstat(rw, &st); e != nil {
		t.Fatal(e)
	}
	if _, e = unix.Pwrite(rw, []byte("changed"), 0); !errors.Is(e, unix.EPERM) {
		t.Fatal("sealed write not denied", e)
	}
	for _, size := range []int64{st.Size - 1, st.Size + 1} {
		if e = unix.Ftruncate(rw, size); !errors.Is(e, unix.EPERM) {
			t.Fatal("sealed truncate not denied", e)
		}
	}
	if _, e = unix.FcntlInt(uintptr(rw), unix.F_ADD_SEALS, unix.F_SEAL_WRITE); !errors.Is(e, unix.EPERM) {
		t.Fatal("seals still mutable", e)
	}
	if got, e := p.digest(); e != nil || got != sha {
		t.Fatal("sealed content identity changed", e)
	}
}
func TestNormalStockProjectionSealCopiesAndClosedLifetime(t *testing.T) {
	original := normalStockSealModel()
	p, e := sealNormalOrdinaryStockUdevProjection(context.Background(), []normalOrdinaryUdevV2DeviceProjection{original})
	if e != nil {
		t.Fatal(e)
	}
	original.Properties["ID_MODEL"] = "caller changed"
	got, e := p.projection(original.Devpath)
	if e != nil || got.Properties["ID_MODEL"] != "original" {
		t.Fatal("mutable input alias", e)
	}
	got.Properties["ID_MODEL"] = "consumer changed"
	again, e := p.projection(original.Devpath)
	if e != nil || again.Properties["ID_MODEL"] != "original" {
		t.Fatal("mutable output alias", e)
	}
	if _, e = p.projection("/devices/other"); !errors.Is(e, ErrUnknown) {
		t.Fatal("invented device accepted")
	}
	p.close()
	p.close()
	if _, e = p.digest(); !errors.Is(e, ErrUnknown) {
		t.Fatal("closed seal accepted")
	}
}
func TestNormalStockProjectionSealRejectsUnboundRecords(t *testing.T) {
	cases := map[string]func(*normalOrdinaryUdevV2DeviceProjection){
		"path":              func(v *normalOrdinaryUdevV2DeviceProjection) { v.Devpath = "../caller" },
		"identity":          func(v *normalOrdinaryUdevV2DeviceProjection) { v.UeventObject.Inode = 0 },
		"wrong-filesystem":  func(v *normalOrdinaryUdevV2DeviceProjection) { v.UeventObject.Filesystem = unix.TMPFS_MAGIC },
		"identity-property": func(v *normalOrdinaryUdevV2DeviceProjection) { v.Properties["DEVPATH"] = "/devices/other" },
		"arbitrary-dynamic": func(v *normalOrdinaryUdevV2DeviceProjection) {
			v.DynamicProperties = []normalOrdinaryUdevV2DynamicProperty{99}
		},
		"invented-time": func(v *normalOrdinaryUdevV2DeviceProjection) {
			v.DynamicProperties = []normalOrdinaryUdevV2DynamicProperty{1}
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			v := normalStockSealModel()
			change(&v)
			if p, e := sealNormalOrdinaryStockUdevProjection(context.Background(), []normalOrdinaryUdevV2DeviceProjection{v}); e == nil {
				p.close()
				t.Fatal("unbound record accepted")
			}
		})
	}
	v := normalStockSealModel()
	if p, e := sealNormalOrdinaryStockUdevProjection(context.Background(), []normalOrdinaryUdevV2DeviceProjection{v, v}); e == nil {
		p.close()
		t.Fatal("duplicate device accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := sealNormalOrdinaryStockUdevProjection(ctx, []normalOrdinaryUdevV2DeviceProjection{v}); !errors.Is(e, ErrUnknown) {
		t.Fatal("cancelled input accepted")
	}
}
