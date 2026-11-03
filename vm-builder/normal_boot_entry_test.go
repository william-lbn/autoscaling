package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 这些夹具只检查生成器的最终镜像布局，不启动 PID1/Guest，不冒充真实启动验收。
func TestNormalSplitKernelInitAlias(t *testing.T) {
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{"absolute-BusyBox-alias", true}, {"relative-BusyBox-alias", true},
		{"missing-init", false}, {"wrong-vminit-target", false},
		{"regular-copy-is-not-original-alias", false}, {"alias-cycle", false},
		{"init-directory", false}, {"nonexecutable-BusyBox", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := normalFixtureRoot(t)
			init := filepath.Join(root, "neonvm/bin/init")
			if tc.name != "absolute-BusyBox-alias" && tc.name != "nonexecutable-BusyBox" {
				if err := os.Remove(init); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			switch tc.name {
			case "relative-BusyBox-alias":
				err = os.Symlink("busybox", init)
			case "wrong-vminit-target":
				err = os.Symlink("/neonvm/bin/vminit", init)
			case "regular-copy-is-not-original-alias":
				var b []byte
				b, err = os.ReadFile(filepath.Join(root, "neonvm/bin/busybox"))
				if err == nil {
					err = os.WriteFile(init, b, 0755)
				}
			case "alias-cycle":
				err = os.Symlink("/neonvm/bin/init", init)
			case "init-directory":
				err = os.Mkdir(init, 0755)
			case "nonexecutable-BusyBox":
				err = os.Chmod(filepath.Join(root, "neonvm/bin/busybox"), 0644)
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = generateNormalSystemManifest(root, normalTestPlan(), strings.Repeat("ab", 32))
			if (err == nil) != tc.ok {
				t.Fatalf("final image init layout accepted=%t want=%t: %v", err == nil, tc.ok, err)
			}
		})
	}
}
