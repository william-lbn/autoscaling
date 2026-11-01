//go:build dsga_native_ordinary_v2_layout && linux

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

// 仅DEV二进制的诊断入口，生产CLI不链接本文件。不伪造Guard/原Host8/typedFS。
// 真实生产vminit安装17图后到initial-boot处运行低层FD机制，结束固定返回42。
func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "initial-boot":
			e := nativeOrdinaryV2LayoutMechanism()
			if e != nil {
				fmt.Printf("ORDINARY_LAYOUT_ERROR=%v\n", e)
				os.Exit(44)
			}
			fmt.Println("ORDINARY_LAYOUT_DIAGNOSTIC_EXIT=42")
			os.Exit(42)
		case "check-guest":
			fmt.Println("ORDINARY_LAYOUT_CHECK_GUEST_REFUSED=43")
			os.Exit(43)
		}
	}
	os.Exit(m.Run())
}
func nativeOrdinaryV2LayoutMechanism() error {
	ctx := context.Background()
	if os.Getuid() != 0 || os.Geteuid() != 0 || os.Getppid() != 1 {
		return fmt.Errorf("DEV original PID1 child required")
	}
	p, e := normalFilesystemPinInitialTask(true)
	if e != nil {
		return e
	}
	defer p.close()
	facts, _, e := normalFilesystemCurrentGraph(ctx, p.ns, p.rootObject)
	if e != nil || len(facts) != 17 {
		return fmt.Errorf("DEV real17 graph: %v/count%d", e, len(facts))
	}
	anchors := map[string]normalFilesystemObject{}
	for _, x := range facts {
		switch x.Mount.Point {
		case "/run", "/var/log", "/var/lib/chrony", "/etc/resolv.conf":
			anchors[x.Mount.Point] = x.Object
		}
	}
	root, e := os.Open("/")
	if e != nil {
		return e
	}
	defer root.Close()
	state := "/var/lib/neon-dsga/ordinary-layout-component"
	if e = os.Mkdir(state, 0700); e != nil {
		return e
	}
	dir, e := os.Open(state)
	if e != nil {
		return e
	}
	defer dir.Close()
	var stages []string
	pins, e := prepareNormalOrdinaryV2PathsAt(ctx, root, anchors, func(stage string, raw []byte) error {
		f, e := os.OpenFile(filepath.Join(state, stage+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return e
		}
		_, e = f.Write(raw)
		if e == nil {
			e = f.Sync()
		}
		ce := f.Close()
		if e == nil {
			e = ce
		}
		if e == nil {
			e = dir.Sync()
		}
		if e == nil {
			stages = append(stages, stage)
		}
		return e
	})
	if e != nil {
		return e
	}
	defer pins.close()
	touch := exec.Command("/neonvm/bin/busybox", "touch", NormalSystemOrdinaryV2AllowMarkerPath)
	touch.Env = []string{"PATH=/neonvm/bin"}
	touch.Dir = "/"
	touch.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 60001, Gid: 60001, NoSetGroups: false}}
	if b, e := touch.CombinedOutput(); e != nil {
		return fmt.Errorf("real UID60001 marker failed: %v/%s", e, b)
	}
	var st unix.Stat_t
	if e = unix.Lstat(NormalSystemOrdinaryV2AllowMarkerPath, &st); e != nil || st.Uid != 60001 || st.Gid != 60001 || st.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("marker real owner mismatch %v", e)
	}
	// 有新marker也不能绕原CheckGuest。DEV Guard只返回43；原脚本必须先拒且不建FD9锁。
	c := exec.Command("/neonvm/bin/sh", "/neonvm/bin/vmstart")
	c.Env = []string{"PATH=/neonvm/bin", "DSGA_ORDINARY_LAYOUT_NATIVE=DEV_COMPONENT_NOT_SOURCE"}
	if e = c.Run(); e == nil {
		return fmt.Errorf("vmstart accepted diagnostic Guard")
	}
	if _, e = os.Lstat("/run/neon-dsga/vmstart.lock"); !os.IsNotExist(e) {
		return fmt.Errorf("lock created before CheckGuest")
	}
	if e = pins.verify(); e != nil {
		return e
	}
	after := []normalOrdinaryV2PathFact{}
	for _, x := range pins.paths {
		after = append(after, x.fact)
	}
	raw, _ := json.Marshal(struct {
		Scope       string                     `json:"scope"`
		Graph       int                        `json:"graph_mounts"`
		Stages      []string                   `json:"stages"`
		Paths       []normalOrdinaryV2PathFact `json:"paths"`
		MarkerUID   uint32                     `json:"marker_uid"`
		CheckDenied bool                       `json:"check_guest_denied_before_flock"`
		Qualified   bool                       `json:"source_qualified"`
	}{"DEV_FD_MECHANISM_ONLY_NOT_ORIGINAL_SOURCE", 17, stages, after, st.Uid, true, false})
	fmt.Printf("ORDINARY_LAYOUT_ACTUAL_JSON %s\n", raw)
	return nil
}
