//go:build linux

package resourceguard

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func stockUdevKernelFixture() map[string]string {
	return map[string]string{"DEVPATH": "/devices/pci0000:00/0000:00:03.0/virtio0/block/vda", "SUBSYSTEM": "block", "DEVNAME": "vda", "MAJOR": "8", "MINOR": "0", "ACTION": "add", "SEQNUM": "51"}
}

// PG17-U-STOCK-UDEV-004 对照官方I/S/G构造和L/W内部语义的Golden，不读实际DB。
func TestNormalOrdinaryStockUdevProjectionOfficialGolden(t *testing.T) {
	kernel := stockUdevKernelFixture()
	raw := []byte("S:disk/by-path/pci-demo\nS:disk/by-id/virtio-demo\nL:-100\nW:0\nI:123\nE:ID_SERIAL=demo\nG:systemd\nG:seat\n")
	got, e := projectNormalOrdinaryStockUdevDB(kernel, raw, true)
	if e != nil {
		t.Fatal(e)
	}
	want := "ACTION=add\x00DEVLINKS=/dev/disk/by-id/virtio-demo /dev/disk/by-path/pci-demo\x00DEVNAME=/dev/vda\x00DEVPATH=" + kernel["DEVPATH"] + "\x00ID_SERIAL=demo\x00MAJOR=8\x00MINOR=0\x00SEQNUM=51\x00SUBSYSTEM=block\x00TAGS=:seat:systemd:\x00USEC_INITIALIZED=123\x00"
	if string(got.MonitorBytes) != want || !got.Initialized || !got.DatabasePresent || got.DevlinkPriority != -100 || got.WatchHandle != 0 {
		t.Fatalf("golden mismatch: %#v", got)
	}
	if kernel["DEVNAME"] != "vda" {
		t.Fatal("input changed")
	}
	got.Properties["DEVPATH"] = "changed"
	got.MonitorBytes[0] = 'X'
	again, e := projectNormalOrdinaryStockUdevDB(kernel, raw, true)
	if e != nil || string(again.MonitorBytes) != want {
		t.Fatalf("alias: %v", e)
	}
	t.Run("official-DMI-indexed-metadata", func(t *testing.T) {
		base := map[string]string{"DEVPATH": "/devices/virtual/dmi/id", "SUBSYSTEM": "dmi"}
		v, e := projectNormalOrdinaryStockUdevDB(base, []byte("E:MEMORY_ARRAY_MAX_CAPACITY=4294967296\nE:MEMORY_DEVICE_0_SIZE=1073741824\nE:MEMORY_DEVICE_0_LOCATOR=DIMM 0\n"), true)
		if e != nil {
			t.Fatal(e)
		}
		if v.Properties["MEMORY_DEVICE_0_SIZE"] != "1073741824" {
			t.Fatal(v)
		}
	})
	t.Run("private-not-monitored", func(t *testing.T) {
		v, e := projectNormalOrdinaryStockUdevDB(kernel, []byte("E:.INPUT_CLASS=kbd\n"), true)
		if e != nil {
			t.Fatal(e)
		}
		if bytes.Contains(v.MonitorBytes, []byte(".INPUT_CLASS")) {
			t.Fatal("private variable leaked")
		}
	})
}

// PG17-U-STOCK-UDEV-005 官方has_info空DB/无文件不同；不能凭initialized编USEC。
func TestNormalOrdinaryStockUdevProjectionNoInformationDB(t *testing.T) {
	t.Run("missing-nondevnum-node", func(t *testing.T) {
		got, e := projectNormalOrdinaryStockUdevDB(map[string]string{"DEVPATH": "/devices/system/memory/memory0", "SUBSYSTEM": "memory"}, nil, false)
		if e != nil {
			t.Fatal(e)
		}
		if got.Initialized || got.DatabasePresent || got.Properties["USEC_INITIALIZED"] != "" {
			t.Fatal(got)
		}
	})
	t.Run("empty-file-initialized-no-usec", func(t *testing.T) {
		got, e := projectNormalOrdinaryStockUdevDB(stockUdevKernelFixture(), nil, true)
		if e != nil {
			t.Fatal(e)
		}
		if !got.Initialized || got.Properties["USEC_INITIALIZED"] != "" {
			t.Fatal(got)
		}
	})
	t.Run("missing-devnum-reject", func(t *testing.T) {
		if _, e := projectNormalOrdinaryStockUdevDB(stockUdevKernelFixture(), nil, false); !errors.Is(e, ErrUnknown) {
			t.Fatal(e)
		}
	})
	t.Run("missing-ifindex-reject", func(t *testing.T) {
		if _, e := projectNormalOrdinaryStockUdevDB(map[string]string{"DEVPATH": "/devices/virtual/net/lo", "SUBSYSTEM": "net", "IFINDEX": "1"}, nil, false); !errors.Is(e, ErrUnknown) {
			t.Fatal(e)
		}
	})
}

// PG17-U-STOCK-UDEV-006 未知/冲突/路径或控制注入一律Unknown，不泛许ID_*。
func TestNormalOrdinaryStockUdevProjectionRejectsUnsupported(t *testing.T) {
	cases := map[string][]byte{"unknown-record": []byte("X:value\n"), "unknown-ID": []byte("E:ID_ARBITRARY_PRIVATE=1\n"), "hwdb-env-not-property": []byte("E:UDEV_HWDB_PATH=/tmp/override\n"), "prefix-literal-not-property": []byte("E:MEMORY_DEVICE=1\n"), "root-command": []byte("E:REMOVE_CMD=/bin/sh\n"), "core-override": []byte("E:DEVPATH=/devices/fake\n"), "generated-override": []byte("E:USEC_INITIALIZED=77\n"), "duplicate-E": []byte("E:ID_SERIAL=a\nE:ID_SERIAL=b\n"), "duplicate-I": []byte("I:12\nI:13\n"), "duplicate-G": []byte("G:seat\nG:seat\n"), "path-dotdot": []byte("S:../etc/passwd\n"), "path-absolute": []byte("S:/dev/a\n"), "path-clean": []byte("S:disk//a\n"), "tag-colon": []byte("G:a:b\n"), "tag-space": []byte("G:a b\n"), "missing-newline": []byte("I:12"), "NUL": []byte("E:ID_SERIAL=x\x00\n"), "control": []byte("E:ID_SERIAL=x\r\n"), "bad-UTF8": []byte("E:ID_SERIAL=\xff\n"), "line-over": []byte("E:ID_SERIAL=" + strings.Repeat("a", 16384) + "\n"), "L-overflow": []byte("L:2147483648\n"), "W-negative": []byte("W:-1\n"), "I-noncanonical": []byte("I:012\n"), "I-overflow": []byte("I:18446744073709551616\n")}
	for name, db := range cases {
		t.Run(name, func(t *testing.T) {
			if _, e := projectNormalOrdinaryStockUdevDB(stockUdevKernelFixture(), db, true); !errors.Is(e, ErrUnknown) {
				t.Fatalf("accepted: %v", e)
			}
		})
	}
	for _, name := range []string{"bad-path", "bad-devname", "incomplete-devnum", "bad-key-concatenation"} {
		t.Run(name, func(t *testing.T) {
			base := stockUdevKernelFixture()
			switch name {
			case "bad-path":
				base["DEVPATH"] = "/devices/../etc"
			case "bad-devname":
				base["DEVNAME"] = "../mem"
			case "incomplete-devnum":
				delete(base, "MINOR")
			case "bad-key-concatenation":
				base["ACTION|DEVPATH"] = "value"
			}
			if _, e := projectNormalOrdinaryStockUdevDB(base, nil, true); !errors.Is(e, ErrUnknown) {
				t.Fatal(e)
			}
		})
	}
	for _, key := range []string{"MEMORY_DEVICE_00_SIZE", "MEMORY_DEVICE_4294967296_SIZE", "MEMORY_DEVICE_0_ARBITRARY_ROOT_VALUE"} {
		t.Run("reject-"+key, func(t *testing.T) {
			if _, e := projectNormalOrdinaryStockUdevDB(stockUdevKernelFixture(), []byte("E:"+key+"=1\n"), true); !errors.Is(e, ErrUnknown) {
				t.Fatal(e)
			}
		})
	}
	t.Run("S-no-devnum", func(t *testing.T) {
		if _, e := projectNormalOrdinaryStockUdevDB(map[string]string{"DEVPATH": "/devices/a", "SUBSYSTEM": "memory"}, []byte("S:disk/demo\n"), true); !errors.Is(e, ErrUnknown) {
			t.Fatal(e)
		}
	})
}

// PG17-U-STOCK-UDEV-007 原monitor4096/路径1024等界拒截断，不认超界合法。
func TestNormalOrdinaryStockUdevProjectionBounds(t *testing.T) {
	t.Run("monitor-over", func(t *testing.T) {
		if _, e := projectNormalOrdinaryStockUdevDB(stockUdevKernelFixture(), []byte("E:ID_SERIAL="+strings.Repeat("a", 4095)+"\n"), true); !errors.Is(e, ErrUnknown) {
			t.Fatal(e)
		}
	})
	t.Run("links-over", func(t *testing.T) {
		db := "S:disk/" + strings.Repeat("a", 700) + "\nS:disk/" + strings.Repeat("b", 700) + "\n"
		if _, e := projectNormalOrdinaryStockUdevDB(stockUdevKernelFixture(), []byte(db), true); !errors.Is(e, ErrUnknown) {
			t.Fatal(e)
		}
	})
	t.Run("raw-over", func(t *testing.T) {
		if _, e := projectNormalOrdinaryStockUdevDB(stockUdevKernelFixture(), bytes.Repeat([]byte{'a'}, 65537), true); !errors.Is(e, ErrUnknown) {
			t.Fatal(e)
		}
	})
	t.Run("absent-with-raw", func(t *testing.T) {
		if _, e := projectNormalOrdinaryStockUdevDB(map[string]string{"DEVPATH": "/devices/a", "SUBSYSTEM": "memory"}, []byte("I:1\n"), false); !errors.Is(e, ErrUnknown) {
			t.Fatal(e)
		}
	})
}
