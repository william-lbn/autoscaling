package main

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func normalTestPlan() normalBuildPlan {
	return normalBuildPlan{ProtocolVersion: 1, ComputeUID: 1000, ComputeGID: 1000, ComputeArgv: []string{"/usr/local/bin/compute_ctl", "--pgdata", "/var/db/postgres/compute"}, ComputeEnv: []string{"PATH=/usr/local/bin:/usr/bin:/bin"}, LoaderPaths: []string{}}
}
func normalTestJSON(p normalBuildPlan) []byte { b, _ := json.Marshal(p); return b }
func TestNormalSplitBuildSelection(t *testing.T) {
	good := normalTestJSON(normalTestPlan())
	cases := []struct {
		n, mode, arch string
		spec          *imageSpec
		args          []string
		raw           []byte
		ok            bool
	}{
		{"legacy", "legacy", targetArchLinuxArm64, &imageSpec{Build: "FROM original"}, []string{"X=y"}, nil, true},
		{"fixed", normalSplitBuildMode, targetArchLinuxAmd64, nil, nil, good, true},
		{"unknown", "NORMAL", targetArchLinuxAmd64, nil, nil, good, false},
		{"arm", normalSplitBuildMode, targetArchLinuxArm64, nil, nil, good, false},
		{"spec-even-empty", normalSplitBuildMode, targetArchLinuxAmd64, &imageSpec{}, nil, good, false},
		{"build-arg", normalSplitBuildMode, targetArchLinuxAmd64, nil, []string{"VM_BUILDER_TARGET_ARCH=other"}, good, false},
		{"legacy-plan", "legacy", targetArchLinuxAmd64, nil, nil, good, false},
		{"missing-plan", normalSplitBuildMode, targetArchLinuxAmd64, nil, nil, nil, false},
	}
	for _, c := range cases {
		t.Run(c.n, func(t *testing.T) {
			e := validateNormalBuildSelection(c.mode, c.arch, c.spec, c.args, c.raw)
			if (e == nil) != c.ok {
				t.Fatalf("selection ok=%t err=%v", c.ok, e)
			}
		})
	}
}
func TestNormalSplitPlanRejectsMutableOrAmbiguousInput(t *testing.T) {
	tests := map[string]func(*normalBuildPlan){"root": func(p *normalBuildPlan) { p.ComputeUID = 0 }, "wrong-exec": func(p *normalBuildPlan) { p.ComputeArgv[0] = "/bin/sh" }, "missing-pgdata": func(p *normalBuildPlan) { p.ComputeArgv = p.ComputeArgv[:1] }, "pgdata-outside": func(p *normalBuildPlan) { p.ComputeArgv[2] = "/tmp/pg" }, "pgdata-mount": func(p *normalBuildPlan) { p.ComputeArgv[2] = "/var/db/postgres" }, "duplicate-pgdata": func(p *normalBuildPlan) { p.ComputeArgv = append(p.ComputeArgv, "-D", "/var/db/postgres/other") }, "env-preload": func(p *normalBuildPlan) { p.ComputeEnv = []string{"LD_PRELOAD=/x"} }, "env-shell": func(p *normalBuildPlan) { p.ComputeEnv = []string{"ENV=/x"} }, "env-duplicate": func(p *normalBuildPlan) { p.ComputeEnv = []string{"A=x", "A=y"} }, "env-unordered": func(p *normalBuildPlan) { p.ComputeEnv = []string{"Z=x", "A=y"} }, "loader-other": func(p *normalBuildPlan) { p.LoaderPaths = []string{"/var/lib/x.so"} }, "line": func(p *normalBuildPlan) { p.ComputeArgv = append(p.ComputeArgv, "x\ny") }}
	for n, f := range tests {
		t.Run(n, func(t *testing.T) {
			p := normalTestPlan()
			f(&p)
			if _, e := decodeNormalBuildPlan(normalTestJSON(p)); e == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
	raw := normalTestJSON(normalTestPlan())
	for _, v := range [][]byte{bytes.Replace(raw, []byte(`"protocol_version":1`), []byte(`"Protocol_Version":1`), 1), bytes.Replace(raw, []byte(`"protocol_version":1`), []byte(`"protocol_version":1,"protocol_version":1`), 1), append(raw, []byte(` {}`)...)} {
		if _, e := decodeNormalBuildPlan(v); e == nil {
			t.Fatal("ambiguous JSON accepted")
		}
	}
}
func normalFixtureRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	required, e := resourceguard.NormalSystemSourceRequiredFilesForVersion(2)
	if e != nil {
		t.Fatal(e)
	}
	for p, k := range required {
		dst := filepath.Join(root, p)
		if e := os.MkdirAll(filepath.Dir(dst), 0755); e != nil {
			t.Fatal(e)
		}
		b := []byte("fixture\n")
		if k == "exec" {
			b = []byte("\x7fELF-fixture")
		}
		if p == "/etc/inittab" {
			rows, _ := resourceguard.NormalSystemSourceInitEntriesForVersion(2)
			b = []byte(strings.Join(rows, "\n") + "\n")
		}
		if e := os.WriteFile(dst, b, 0755); e != nil {
			t.Fatal(e)
		}
	}
	for _, p := range []string{"/neonvm/bin/sh", "/neonvm/bin/cat", "/neonvm/bin/touch", "/neonvm/bin/udhcpc"} {
		os.Remove(filepath.Join(root, p))
		if e := os.Symlink("/neonvm/bin/busybox", filepath.Join(root, p)); e != nil {
			t.Fatal(e)
		}
	}
	for _, p := range resourceguard.NormalSystemSourceConfigDirectories() {
		if e := os.MkdirAll(filepath.Join(root, p), 0755); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.WriteFile(filepath.Join(root, "/etc/vector/fixture.toml"), []byte("fixed\n"), 0444); e != nil {
		t.Fatal(e)
	}
	return root
}
func TestNormalSplitFinalInventoryAndCanonicalRoot(t *testing.T) {
	root := normalFixtureRoot(t)
	raw, e := generateNormalSystemManifest(root, normalTestPlan(), strings.Repeat("ab", 32))
	if e != nil {
		t.Fatal(e)
	}
	m, e := resourceguard.DecodeNormalSystemSourceManifest(raw)
	if e != nil {
		t.Fatal(e)
	}
	if m.ProtocolVersion != 2 || m.DeclaredImageSHA256 != strings.Repeat("ab", 32) {
		t.Fatal("wrong version or original declaration")
	}
	found := false
	for _, f := range m.Files {
		if f.Path == "/neonvm/bin/sh" && f.CanonicalPath != "/neonvm/bin/busybox" {
			t.Fatal("absolute alias escaped image root")
		}
		if f.Path == "/etc/vector/fixture.toml" {
			found = true
		}
	}
	if !found {
		t.Fatal("complete config inventory omitted file")
	}
	for _, n := range []string{"missing-source", "config-symlink", "alias-cycle", "invalid-base"} {
		t.Run(n, func(t *testing.T) {
			r := normalFixtureRoot(t)
			base := strings.Repeat("ab", 32)
			switch n {
			case "missing-source":
				os.Remove(filepath.Join(r, "/usr/local/bin/compute_ctl"))
			case "config-symlink":
				os.Symlink("/etc/passwd", filepath.Join(r, "/etc/vector/leak"))
			case "alias-cycle":
				os.Remove(filepath.Join(r, "/neonvm/bin/sh"))
				os.Symlink("/neonvm/bin/sh", filepath.Join(r, "/neonvm/bin/sh"))
			case "invalid-base":
				base = "sha256:bad"
			}
			if _, e := generateNormalSystemManifest(r, normalTestPlan(), base); e == nil {
				t.Fatal("invalid inventory accepted")
			}
		})
	}
}
func TestNormalSplitFixedScriptsFailClosedPerRole(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	os.Mkdir(bin, 0755)
	trace := filepath.Join(root, "trace")
	guard := "#!/bin/sh\nprintf 'gate\\n' >>\"$TRACE\"\nexit 17\n"
	os.WriteFile(filepath.Join(bin, "dsga-resource-guard"), []byte(guard), 0755)
	script := strings.ReplaceAll(scriptNormalResourceService, "/neonvm/bin", bin)
	file := filepath.Join(root, "service")
	os.WriteFile(file, []byte(script), 0755)
	roles := []string{"allow-start", "kernel-version", "network", "udev", "udev-init", "acpi", "logs", "clock", "daemon", "resource", "compute"}
	for _, role := range roles {
		t.Run(role, func(t *testing.T) {
			os.Remove(trace)
			c := exec.Command("/bin/sh", file, role)
			c.Env = []string{"TRACE=" + trace}
			if e := c.Run(); e == nil {
				t.Fatal("failed gate allowed service")
			}
			b, _ := os.ReadFile(trace)
			if string(b) != "gate\n" {
				t.Fatal("role skipped independent gate")
			}
		})
	}
	for _, role := range []string{"/bin/sh", "sshd", "login", "anything"} {
		t.Run(role, func(t *testing.T) {
			os.Remove(trace)
			c := exec.Command("/bin/sh", file, role)
			c.Env = []string{"TRACE=" + trace}
			if e := c.Run(); e == nil {
				t.Fatal("unlisted role accepted")
			}
			if _, e := os.Stat(trace); !os.IsNotExist(e) {
				t.Fatal("unlisted role reached guard")
			}
		})
	}
	for _, s := range []string{scriptNormalVmInit, scriptNormalResourceService, scriptNormalShutdown, scriptNormalUdevInit, normalComputeScript(normalTestPlan())} {
		c := exec.Command("/bin/sh", "-n")
		c.Stdin = strings.NewReader(s)
		if b, e := c.CombinedOutput(); e != nil {
			t.Fatalf("script syntax %v %s", e, b)
		}
	}
}
func TestNormalSplitTemplateAndFixedComputeBytes(t *testing.T) {
	rows, _ := resourceguard.NormalSystemSourceInitEntriesForVersion(2)
	if len(rows) != 13 {
		t.Fatal("fixed rows")
	}
	for _, v := range rows {
		if strings.Contains(v, "sshd") || strings.Contains(v, "agetty") {
			t.Fatal("interactive service")
		}
	}
	p := normalTestPlan()
	p.ComputeArgv = append(p.ComputeArgv, "literal-{{.Env}}-$(touch unsafe)-'value'")
	script := normalComputeScript(p)
	if strings.Contains(script, "vmstarter") || strings.Contains(script, "runtime/env") || !strings.Contains(script, "env -i") || !strings.Contains(script, "check-guest") {
		t.Fatal("mutable command path")
	}
	if !strings.Contains(script, "{{.Env}}") {
		t.Fatal("fixed argv bytes changed")
	}
}

func TestNormalSplitDockerTemplateRetainsLegacyAndFinalInventoryOrder(t *testing.T) {
	for _, normal := range []bool{false, true} {
		c := TemplatesContext{RootDiskImage: "source@sha256:" + strings.Repeat("ab", 32), NeonvmDaemonImage: "daemon@sha256:" + strings.Repeat("bc", 32), BusyboxImage: "busybox", BusyboxImageTag: "1", AlpineImage: "alpine", AlpineImageTag: "3", NormalSplit: normal, NormalUID: 1000, NormalGID: 1000, NormalBaseSHA: strings.Repeat("ab", 32)}
		var b bytes.Buffer
		tw := tar.NewWriter(&b)
		if e := AddTemplatedFileToTar(tw, c, "Dockerfile", dockerfileVmBuilder); e != nil {
			t.Fatal(e)
		}
		if e := tw.Close(); e != nil {
			t.Fatal(e)
		}
		tr := tar.NewReader(&b)
		if _, e := tr.Next(); e != nil {
			t.Fatal(e)
		}
		raw, e := io.ReadAll(tr)
		if e != nil {
			t.Fatal(e)
		}
		text := string(raw)
		if normal {
			if strings.Contains(text, "openssh-server") || strings.Contains(text, "sshd >") {
				t.Fatal("normal built SSH service")
			}
			prepare := strings.Index(text, "RUN /rootdisk/neonvm/bin/busybox sh /normal-prepare-root.sh 1000 1000")
			inventory := strings.Index(text, "RUN /normal-manifest-tool")
			mkfs := strings.Index(text, "mkfs.ext4 -L vmroot")
			if prepare < 0 || inventory <= prepare || mkfs <= inventory {
				t.Fatal("inventory not after final aliases and before filesystem seal")
			}
		} else if !strings.Contains(text, "openssh-server") || strings.Contains(text, "RUN /normal-manifest-tool") {
			t.Fatal("legacy silently changed mode")
		}
	}
}

func TestNormalSplitPinsOriginalBaseAndRuntimeImages(t *testing.T) {
	id := "sha256:" + strings.Repeat("ab", 32)
	ref := "127.0.0.1:5001/neon/base@sha256:" + strings.Repeat("cd", 32)
	got, sha, e := normalPinnedBase("127.0.0.1:5001/neon/base:mutable", id, []string{ref})
	if e != nil || got != ref || sha != strings.Repeat("ab", 32) {
		t.Fatal("original repo/config identities differ", e)
	}
	if got, _, e := normalPinnedBase(ref, id, nil); e != nil || got != ref {
		t.Fatal("pinned source changed", e)
	}
	if _, _, e := normalPinnedBase("127.0.0.1:5001/neon/base:mutable", id, []string{"127.0.0.1:5001/neon/foreign@sha256:" + strings.Repeat("cd", 32)}); e == nil {
		t.Fatal("foreign RepoDigest adopted")
	}
	for _, bad := range []string{"alpine:latest", "daemon:dev", "busybox"} {
		if normalRequirePinnedImage(bad) == nil {
			t.Fatal("floating runtime image", bad)
		}
	}
	if e := normalRequirePinnedImage("alpine:3.19@sha256:" + strings.Repeat("ab", 32)); e != nil {
		t.Fatal(e)
	}
}

func TestNormalSplitInventoryRejectsFIFOWithoutBlocking(t *testing.T) {
	r := normalFixtureRoot(t)
	p := filepath.Join(r, "/usr/local/bin/compute_ctl")
	if e := os.Remove(p); e != nil {
		t.Fatal(e)
	}
	if e := syscall.Mkfifo(p, 0600); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := generateNormalSystemManifest(r, normalTestPlan(), strings.Repeat("ab", 32)); done <- e }()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("FIFO accepted as source")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("opening source blocked on a real FIFO")
	}
}

func TestNormalSplitVmInitPrevalidatesAndCleansRootView(t *testing.T) {
	for _, tc := range []struct {
		name        string
		code        int
		viewCreated bool
	}{
		{"success-diagnostic-exit", 42, true}, {"pg-parent-alias", 1, false},
		{"pg-ancestor-alias", 1, false}, {"resolv-alias", 1, false},
		{"bind-failure", 23, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := t.TempDir()
			for _, p := range []string{"/neonvm/bin", "/neonvm/runtime", "/sys/fs/cgroup", "/proc", "/dev/pts", "/dev/shm", "/run", "/tmp", "/etc/neon-dsga", "/var/lib/neon-dsga", "/var/db/postgres", "/var/log", "/var/lib/chrony"} {
				if e := os.MkdirAll(filepath.Join(r, p), 0755); e != nil {
					t.Fatal(e)
				}
			}
			for _, p := range []string{"/etc/resolv.conf", "/sys/fs/cgroup/cgroup.procs", "/sys/fs/cgroup/cgroup.threads"} {
				if e := os.WriteFile(filepath.Join(r, p), []byte("fixture\n"), 0644); e != nil {
					t.Fatal(e)
				}
			}
			switch tc.name {
			case "pg-parent-alias":
				p := filepath.Join(r, "/var/db/postgres")
				os.Remove(p)
				os.Symlink(filepath.Join(r, "/tmp"), p)
			case "pg-ancestor-alias":
				p := filepath.Join(r, "/var/db")
				os.RemoveAll(p)
				os.Mkdir(filepath.Join(r, "/tmp/postgres"), 0755)
				os.Symlink(filepath.Join(r, "/tmp"), p)
			case "resolv-alias":
				p := filepath.Join(r, "/etc/resolv.conf")
				os.Rename(p, filepath.Join(r, "/tmp/resolv"))
				os.Symlink(filepath.Join(r, "/tmp/resolv"), p)
			}
			// 仅外部脚本机制夹具：不执行Host mount，不把模拟挂载当Guest证据。
			bb := `#!/bin/sh
op=$1; shift
case "$op" in
 mountpoint)
  for last do :; done
  case "$last" in */root-writable) [ -f "$ROOT/view-mounted" ];; *) exit 0;; esac;;
 mount)
  for last do :; done
  case "$last" in */root-writable) /bin/touch "$ROOT/view-mounted"; printf 'view\n' >>"$ROOT/trace";;
  */var/log) [ "$CASE" != bind-failure ] || exit 23;; esac;;
 umount) /bin/rm -f "$ROOT/view-mounted"; printf 'cleanup\n' >>"$ROOT/trace";;
 mkdir) exec /bin/mkdir "$@";;
 rmdir) exec /bin/rmdir "$@";;
 chmod) exec /bin/chmod "$@";;
 readlink) exec /usr/bin/readlink "$@";;
 *) exit 91;;
esac
`
			os.WriteFile(filepath.Join(r, "/neonvm/bin/busybox"), []byte(bb), 0755)
			os.WriteFile(filepath.Join(r, "/neonvm/bin/dsga-resource-guard"), []byte("#!/bin/sh\nprintf 'guard\\n' >>\"$ROOT/trace\"\nexit 42\n"), 0755)
			pairs := []string{}
			for _, p := range []string{"/neonvm", "/sys", "/proc", "/dev", "/run", "/tmp", "/etc", "/var"} {
				pairs = append(pairs, p, filepath.Join(r, p))
			}
			script := strings.NewReplacer(pairs...).Replace(scriptNormalVmInit)
			file := filepath.Join(r, "init")
			os.WriteFile(file, []byte(script), 0755)
			cmd := exec.Command("/bin/sh", file)
			cmd.Env = []string{"ROOT=" + r, "CASE=" + tc.name}
			b, e := cmd.CombinedOutput()
			ee, ok := e.(*exec.ExitError)
			if !ok || ee.ExitCode() != tc.code {
				t.Fatalf("exit=%v want=%d output=%s", e, tc.code, b)
			}
			tr, _ := os.ReadFile(filepath.Join(r, "trace"))
			trace := string(tr)
			if strings.Contains(trace, "view\n") != tc.viewCreated {
				t.Fatalf("view creation mismatch: %q", trace)
			}
			if _, e := os.Lstat(filepath.Join(r, "/run/neon-dsga/root-writable")); !os.IsNotExist(e) {
				t.Fatalf("RW view directory retained: %v", e)
			}
			if _, e := os.Lstat(filepath.Join(r, "view-mounted")); !os.IsNotExist(e) {
				t.Fatal("RW bind retained")
			}
			if tc.name == "success-diagnostic-exit" {
				if !strings.Contains(trace, "guard\n") {
					t.Fatal("diagnostic not reached")
				}
			} else if strings.Contains(trace, "guard\n") {
				t.Fatal("failed setup reached guard")
			}
			if tc.viewCreated && !strings.Contains(trace, "cleanup\n") {
				t.Fatal("view cleanup skipped")
			}
		})
	}
}
