package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func normalOriginalDHCPFixture(t *testing.T) []byte {
	t.Helper()
	raw, e := os.ReadFile("testdata/udhcpc-alpine-default.script")
	if e != nil {
		t.Fatal(e)
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != normalAlpineDHCPHookSHA {
		t.Fatal("locked Alpine fixture differs")
	}
	return raw
}

func TestNormalProductionDHCPMatchesExistingPrepareRootAWK(t *testing.T) {
	raw := normalOriginalDHCPFixture(t)
	got, e := normalProductionDHCPHook(raw, true)
	if e != nil {
		t.Fatal(e)
	}
	// 直接执行原embedded构建脚本内的awk，不再复制一份测试转换算法。
	const begin = "$bb awk '\n"
	const end = "\n' \"$dhcp\" >\"$patched\""
	if strings.Count(scriptNormalPrepareRoot, begin) != 1 || strings.Count(scriptNormalPrepareRoot, end) != 1 {
		t.Fatal("embedded awk boundary drift")
	}
	awk := strings.SplitN(strings.SplitN(scriptNormalPrepareRoot, begin, 2)[1], end, 2)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "awk", awk)
	cmd.Stdin = bytes.NewReader(raw)
	want, e := cmd.Output()
	if e != nil || !bytes.Equal(got, want) {
		t.Fatalf("production Go and original prepare-root awk differ: %v", e)
	}
	cmd = exec.CommandContext(ctx, "/bin/sh", "-n")
	cmd.Stdin = bytes.NewReader(got)
	if b, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("prepared shell syntax: %v %s", e, b)
	}
	again, e := normalProductionDHCPHook(got, true)
	if e != nil || !bytes.Equal(again, got) {
		t.Fatal("prepared hook not idempotent", e)
	}
	if _, e = normalProductionDHCPHook(raw, false); e == nil {
		t.Fatal("Collector accepted old rename hook")
	}
	if _, e = normalProductionDHCPHook(got, false); e != nil {
		t.Fatal("Collector rejected prepared hook", e)
	}
	for _, unknown := range [][]byte{nil, []byte("#!/neonvm/bin/sh\nexit 0\n"), append(append([]byte(nil), raw...), []byte("# unknown edit\n")...), bytes.Replace(raw, []byte("resolvconf()"), []byte("otherfunc()"), 1)} {
		if _, e = normalProductionDHCPHook(unknown, true); e == nil {
			t.Fatal("unknown or illegal hook accepted")
		}
	}
}

func TestNormalProductionDHCPRejectsUnsafeBeforeConfigurationWrites(t *testing.T) {
	for _, name := range []string{"missing", "alias", "hardlink", "unknown", "bin-parent-alias"} {
		t.Run(name, func(t *testing.T) {
			root := normalProductionConfigurationFixture(t)
			path := filepath.Join(root, "/neonvm/bin/udhcpc.script")
			switch name {
			case "missing":
				os.Remove(path)
			case "alias":
				os.Remove(path)
				if e := os.Symlink("../config/vector.yaml", path); e != nil {
					t.Fatal(e)
				}
			case "hardlink":
				if e := os.Link(path, filepath.Join(root, "/neonvm/bin/second-hook")); e != nil {
					t.Fatal(e)
				}
			case "unknown":
				if e := os.WriteFile(path, []byte("#!/neonvm/bin/sh\nexit 0\n"), 0755); e != nil {
					t.Fatal(e)
				}
			case "bin-parent-alias":
				original := filepath.Join(root, "/neonvm/bin")
				if e := os.Rename(original, filepath.Join(root, "/neonvm/original-bin")); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink("original-bin", original); e != nil {
					t.Fatal(e)
				}
			}
			if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, true, ""); e == nil {
				t.Fatal("unsafe DHCP input accepted")
			}
			for _, logical := range []string{"/neonvm/config/vector.yaml", "/neonvm/config/chrony.conf"} {
				raw, e := os.ReadFile(filepath.Join(root, logical))
				if e != nil || !bytes.Equal(raw, []byte("DEV static configuration\n")) {
					t.Fatal("DHCP precheck ran after other writes", e)
				}
			}
		})
	}
}

func TestNormalProductionDHCPMaterializedBeforeInventory(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("ENV: production root-owned preparation must actually run as root")
	}
	root := normalProductionConfigurationFixture(t)
	if e := normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, true, ""); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(root, "/neonvm/bin/udhcpc.script")
	raw, e := os.ReadFile(path)
	if e != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != normalPreparedDHCPHookSHA {
		t.Fatal("prepared bytes not on actual base input", e)
	}
	st, e := os.Stat(path)
	if e != nil || st.Mode().Perm() != 0755 {
		t.Fatal("prepared hook mode differs", e)
	}
	if e = normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, false, ""); e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(path, 0644); e != nil {
		t.Fatal(e)
	}
	if e = normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, false, ""); e == nil {
		t.Fatal("Collector accepted non-executable prepared hook")
	}
	if e = os.Chmod(path, 0755); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, normalOriginalDHCPFixture(t), 0755); e != nil {
		t.Fatal(e)
	}
	if e = normalProductionConfigurationPhase(root, normalSplitV3BuildMode, 2, false, ""); e == nil {
		t.Fatal("Collector let renamed old hook through")
	}
}
