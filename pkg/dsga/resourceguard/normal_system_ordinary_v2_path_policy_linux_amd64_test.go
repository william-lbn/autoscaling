//go:build linux && amd64

package resourceguard

import (
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"testing"
)

func TestNormalSystemOrdinaryV2PrivatePathsUseActualLayout(t *testing.T) {
	cases := []struct {
		role, path     string
		uid, gid, mode uint32
		rights         uint64
	}{
		{"allow-start", "/run/neon-ordinary/allow-start", 60001, 60001, unix.S_IFDIR | 0700, nkWriteTree},
		{"network", "/run/neon-ordinary/network", 60003, 60003, unix.S_IFDIR | 0700, nkWriteTree},
		{"network", "/etc/resolv.conf", 60003, 60003, unix.S_IFREG | 0644, nkRead | nkWrite | nkTruncate},
		{"udev", "/run/udev", 0, 60004, unix.S_IFDIR | 01771, nkWriteTree},
		{"acpi", "/run/neon-ordinary/acpi", 60006, 60006, unix.S_IFDIR | 0700, nkWriteTree},
		{"logs", "/run/neon-ordinary/logs", 60007, 60007, unix.S_IFDIR | 0700, nkWriteTree},
		{"logs", "/var/log/vector", 60007, 60007, unix.S_IFDIR | 0700, nkWriteTree},
		{"clock", "/run/chrony", 60008, 60008, unix.S_IFDIR | 0700, nkWriteTree},
		{"clock", "/var/lib/chrony", 60008, 60008, unix.S_IFDIR | 0700, nkWriteTree},
		{"clock", "/var/log/chrony", 60008, 60008, unix.S_IFDIR | 0700, nkWriteTree},
	}
	for _, c := range cases {
		t.Run(c.role+c.path, func(t *testing.T) {
			rule, e := NormalSystemOrdinaryV2RolePlan(c.role)
			if e != nil {
				t.Fatal(e)
			}
			got, e := normalSystemOrdinaryV2PrivatePathRights(rule, c.path, unix.Stat_t{Uid: c.uid, Gid: c.gid, Mode: c.mode, Nlink: 1})
			if e != nil || got != c.rights {
				t.Fatalf("fixed installed path rejected or rights differ: %v", e)
			}
		})
	}
}

func TestNormalSystemOrdinaryV2PrivatePathRejectsChangedAuthority(t *testing.T) {
	for _, role := range []string{"network", "udev", "logs"} {
		for _, change := range []string{"uid", "gid", "mode", "type", "setuid", "setgid", "path", "role", "vector", "link"} {
			t.Run(role+"/"+change, func(t *testing.T) {
				rule, e := NormalSystemOrdinaryV2RolePlan(role)
				if e != nil {
					t.Fatal(e)
				}
				path := "/var/log/vector"
				st := unix.Stat_t{Uid: 60007, Gid: 60007, Mode: unix.S_IFDIR | 0700, Nlink: 1}
				if role == "network" {
					path = "/etc/resolv.conf"
					st = unix.Stat_t{Uid: 60003, Gid: 60003, Mode: unix.S_IFREG | 0644, Nlink: 1}
				}
				if role == "udev" {
					path = "/run/udev"
					st = unix.Stat_t{Uid: 0, Gid: 60004, Mode: unix.S_IFDIR | 01771, Nlink: 1}
				}
				switch change {
				case "uid":
					st.Uid++
				case "gid":
					st.Gid++
				case "mode":
					st.Mode ^= 0004
				case "type":
					st.Mode = (st.Mode & 07777) | unix.S_IFLNK
				case "setuid":
					st.Mode |= unix.S_ISUID
				case "setgid":
					st.Mode |= unix.S_ISGID
				case "path":
					path += "/child"
				case "role":
					rule.Role = "daemon"
				case "vector":
					rule.PrivatePaths = append(append([]string(nil), rule.PrivatePaths...), "/run")
				case "link":
					if role != "network" {
						path = "/run"
					} else {
						st.Nlink = 2
					}
				}
				if _, e := normalSystemOrdinaryV2PrivatePathRights(rule, path, st); e == nil {
					t.Fatal("changed original authority accepted")
				}
			})
		}
	}
	// 旧DEV私有0600 DNS不能冒称真实PG可读的0644；Root udev也不能丢sticky。
	net, _ := NormalSystemOrdinaryV2RolePlan("network")
	if _, e := normalSystemOrdinaryV2PrivatePathRights(net, "/etc/resolv.conf", unix.Stat_t{Uid: 60003, Gid: 60003, Mode: unix.S_IFREG | 0600, Nlink: 1}); e == nil {
		t.Fatal("private-only DNS accepted as real layout")
	}
	udev, _ := NormalSystemOrdinaryV2RolePlan("udev")
	if _, e := normalSystemOrdinaryV2PrivatePathRights(udev, "/run/udev", unix.Stat_t{Uid: 0, Gid: 60004, Mode: unix.S_IFDIR | 0771, Nlink: 1}); e == nil {
		t.Fatal("nonsticky stock parent accepted")
	}
}

func TestNormalSystemOrdinaryV2FixedWorkingDirectoryDropsInheritedCwd(t *testing.T) {
	if os.Getenv("DSGA_ORDINARY2_CWD_CHILD") == "1" {
		if e := normalSystemOrdinaryV2FixedWorkingDirectory(); e != nil {
			t.Fatal(e)
		}
		if cwd, e := os.Getwd(); e != nil || cwd != "/" {
			t.Fatalf("unexpected actual cwd: %q %v", cwd, e)
		}
		return
	}
	child := exec.Command(os.Args[0], "-test.run=^TestNormalSystemOrdinaryV2FixedWorkingDirectoryDropsInheritedCwd$", "-test.count=1")
	child.Dir = t.TempDir()
	child.Env = append(os.Environ(), "DSGA_ORDINARY2_CWD_CHILD=1")
	if out, e := child.CombinedOutput(); e != nil {
		t.Fatalf("actual cwd subprocess failed: %v %s", e, out)
	}
}
