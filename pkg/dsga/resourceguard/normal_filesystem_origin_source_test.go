package resourceguard

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func normalFilesystemFixtureMounts() string {
	rows := []string{"1 99 8:1 / / ro,relatime - ext4 /dev/vda rw,relatime"}
	id := 2
	for _, p := range normalFilesystemPersistentHoles {
		rows = append(rows, fmt.Sprintf("%d 1 8:1 %s %s rw,relatime - ext4 /dev/vda rw,relatime", id, p, p))
		id++
	}
	for _, p := range []struct{ path, fs, dev, opts string }{{"/proc", "proc", "0:2", "rw,nosuid,nodev,noexec"}, {"/sys", "sysfs", "0:3", "rw,nosuid,nodev,noexec"}, {"/sys/kernel/security", "securityfs", "0:20", "ro,nosuid,nodev,noexec"}, {"/dev", "devtmpfs", "0:4", "rw"}, {"/sys/fs/cgroup", "cgroup2", "0:5", "rw,nosuid,nodev,noexec"}, {"/dev/pts", "devpts", "0:6", "rw,nosuid,noexec"}, {"/run", "tmpfs", "0:7", "rw,nosuid,nodev,noexec"}, {"/tmp", "tmpfs", "0:8", "rw,nosuid,nodev,noexec"}, {"/dev/shm", "tmpfs", "0:9", "rw,nosuid,nodev,noexec"}, {"/neonvm/runtime", "iso9660", "11:0", "ro"}} {
		rows = append(rows, fmt.Sprintf("%d 1 %s / %s %s - %s %s %s", id, p.dev, p.path, p.opts, p.fs, p.fs, p.opts))
		id++
	}
	byPoint := map[string]string{}
	for _, row := range rows {
		f := strings.Fields(row)
		byPoint[f[4]] = f[0]
	}
	for i, row := range rows {
		f := strings.Fields(row)
		if f[4] == "/dev/shm" || f[4] == "/dev/pts" {
			f[1] = byPoint["/dev"]
		}
		if f[4] == "/sys/fs/cgroup" || f[4] == "/sys/kernel/security" {
			f[1] = byPoint["/sys"]
		}
		rows[i] = strings.Join(f, " ")
	}
	return strings.Join(rows, "\n") + "\n"
}
func TestNormalFilesystemMountParserRejectsAmbiguity(t *testing.T) {
	good := normalFilesystemFixtureMounts()
	out, e := normalFilesystemParseMounts([]byte(good))
	require.NoError(t, e)
	require.Len(t, out, 17)
	for name, bad := range map[string]string{
		"missing-newline": strings.TrimSuffix(good, "\n"), "empty": "", "double-space": strings.Replace(good, "1 99", "1  99", 1), "zero-id": strings.Replace(good, "1 99", "0 99", 1), "leading-zero": strings.Replace(good, "1 99", "01 99", 1), "duplicate-id": good + "1 99 8:1 / /extra ro - ext4 /dev/vda rw\n", "stacked-point": good + "17 99 8:1 / / ro - ext4 /dev/vda rw\n", "propagation": strings.Replace(good, " - ext4", " shared:1 - ext4", 1), "ambiguous-mode": strings.Replace(good, "ro,relatime", "ro,rw,relatime", 1), "duplicate-option": strings.Replace(good, "ro,relatime", "ro,relatime,ro", 1), "escaped-path": strings.Replace(good, "/var/log", "/var/lo\\147", 1), "parent-cycle": strings.Replace(good, "1 99", "1 2", 1), "noncanonical-device": strings.Replace(good, "8:1", "08:1", 1),
	} {
		t.Run(name, func(t *testing.T) { _, e := normalFilesystemParseMounts([]byte(bad)); require.Error(t, e) })
	}
}
func TestNormalFilesystemFixedGraphPreservesPersistentHoles(t *testing.T) {
	good := normalFilesystemFixtureMounts()
	out, e := normalFilesystemParseMounts([]byte(good))
	require.NoError(t, e)
	require.NoError(t, normalFilesystemValidateGraph(out))
	for name, bad := range map[string]string{
		"root-rw": strings.Replace(good, "/ / ro,", "/ / rw,", 1), "rw-full-root-alias": good + "18 1 8:1 / /run/neon-dsga/root-writable rw - ext4 /dev/vda rw\n", "foreign-data-device": strings.Replace(good, "2 1 8:1", "2 1 8:2", 1), "wrong-bind-root": strings.Replace(good, "3 1 8:1 /var/lib/neon-dsga", "3 1 8:1 /", 1), "data-readonly": strings.Replace(good, "/var/db/postgres rw,", "/var/db/postgres ro,", 1), "fuse": strings.ReplaceAll(good, "- ext4", "- fuse"), "dax": strings.Replace(good, "rw,relatime\n", "rw,relatime,dax=always\n", 1), "tmpfs-executable": strings.Replace(good, "/tmp rw,nosuid,nodev,noexec", "/tmp rw,nosuid,nodev", 1), "ISO-rw": strings.Replace(good, "/neonvm/runtime ro", "/neonvm/runtime rw", 1), "unexpected-RO-alias": good + "18 1 8:1 /usr /extra ro - ext4 /dev/vda rw\n",
	} {
		t.Run(name, func(t *testing.T) {
			out, e := normalFilesystemParseMounts([]byte(bad))
			require.NoError(t, e)
			require.Error(t, normalFilesystemValidateGraph(out))
		})
	}
}

func TestNormalFilesystemDAXAllowsOnlyExplicitNever(t *testing.T) {
	good := normalFilesystemFixtureMounts()
	for _, mode := range []string{"dax", "dax=always", "dax=inode", "dax=", "dax=unknown", "dax=NEVER"} {
		t.Run(mode, func(t *testing.T) {
			raw := strings.Replace(good, "rw,relatime\n", "rw,relatime,"+mode+"\n", 1)
			m, e := normalFilesystemParseMounts([]byte(raw))
			require.NoError(t, e)
			require.Error(t, normalFilesystemValidateGraph(m))
		})
	}
	m, e := normalFilesystemParseMounts([]byte(strings.Replace(good, "rw,relatime\n", "rw,relatime,dax=never\n", 1)))
	require.NoError(t, e)
	require.NoError(t, normalFilesystemValidateGraph(m))
}

func TestNormalFilesystemActual369MountInfo(t *testing.T) {
	raw := `26 1 254:0 / / ro,relatime - ext4 /dev/vda rw
27 26 0:23 / /proc rw,relatime - proc proc rw
28 26 0:22 / /sys rw,relatime - sysfs sysfs rw
29 26 0:6 / /dev rw,relatime - devtmpfs devtmpfs rw,size=243044k,nr_inodes=60761,mode=755,inode64
30 28 0:24 / /sys/fs/cgroup rw,relatime - cgroup2 cgroup2 rw
31 29 0:25 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,mode=600,ptmxmode=000
32 26 0:26 / /run rw,nosuid,nodev,noexec,relatime - tmpfs run rw,mode=755,inode64
33 26 0:27 / /tmp rw,nosuid,nodev,noexec,relatime - tmpfs tmp rw,inode64
34 29 0:28 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,inode64
35 26 254:16 / /neonvm/runtime ro,relatime - iso9660 /dev/vdb ro,nojoliet,check=s,map=n,blocksize=2048,fmode=644
37 26 254:0 /etc/neon-dsga /etc/neon-dsga rw,relatime - ext4 /dev/vda rw
38 26 254:0 /var/lib/neon-dsga /var/lib/neon-dsga rw,relatime - ext4 /dev/vda rw
39 26 254:0 /var/db/postgres /var/db/postgres rw,relatime - ext4 /dev/vda rw
40 26 254:0 /var/log /var/log rw,relatime - ext4 /dev/vda rw
41 26 254:0 /var/lib/chrony /var/lib/chrony rw,relatime - ext4 /dev/vda rw
42 26 254:0 /etc/resolv.conf /etc/resolv.conf rw,relatime - ext4 /dev/vda rw
`
	m, e := normalFilesystemParseMounts([]byte(raw))
	require.NoError(t, e)
	require.Len(t, m, 16)
	require.Error(t, normalFilesystemValidateGraph(m), "historical real16 lacks original LSM mount; not Layout2")
}
