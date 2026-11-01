package resourceguard

import (
	"fmt"
	"path"
	"strconv"
	"strings"
)

const normalFilesystemOriginScope = "original-normal-filesystem-origin-v1"

// Layout2是实际KernelPrep LSM来源必需的17mount图；旧16图不可升级为本来源。
const normalFilesystemLayoutVersion uint32 = 2

// 固定normal vminit的六个持久孔。PGDATA的父目录保留remove/create和warmup兄弟；
// 不把全根RW别名、请求路径或当前mountinfo摘要升级为原始来源。
var normalFilesystemPersistentHoles = [...]string{
	"/etc/neon-dsga", "/var/lib/neon-dsga", "/var/db/postgres", "/var/log", "/var/lib/chrony", "/etc/resolv.conf",
}

type normalFilesystemMount struct {
	ID           uint64
	Parent       uint64
	Device       string
	Root         string
	Point        string
	Options      map[string]bool
	FSType       string
	Source       string
	SuperOptions map[string]bool
}

// 严格解析内核mountinfo，拒重复ID/挂载点、传播域、隐藏控制符和非规范逃逸。
// mount options描述本mount，super options描述同superblock，bind根RO/孔RW可以共存。
func normalFilesystemParseMounts(raw []byte) ([]normalFilesystemMount, error) {
	if len(raw) == 0 || len(raw) > 1<<20 || raw[len(raw)-1] != '\n' {
		return nil, fmt.Errorf("filesystem mount inventory exceeds canonical bound")
	}
	rows := strings.Split(string(raw[:len(raw)-1]), "\n")
	if len(rows) > 256 {
		return nil, fmt.Errorf("filesystem mount inventory exceeds count bound")
	}
	ids := map[uint64]bool{}
	points := map[string]bool{}
	out := make([]normalFilesystemMount, 0, len(rows))
	number := func(s string) (uint64, error) {
		v, e := strconv.ParseUint(s, 10, 64)
		if e != nil || v == 0 || strconv.FormatUint(v, 10) != s {
			return 0, fmt.Errorf("filesystem mount identity is not canonical")
		}
		return v, nil
	}
	for _, line := range rows {
		if len(line) == 0 || len(line) > 16384 || strings.ContainsAny(line, "\r\t\x00") {
			return nil, fmt.Errorf("filesystem mount record is malformed")
		}
		fields := strings.Split(line, " ")
		for _, f := range fields {
			if f == "" {
				return nil, fmt.Errorf("filesystem mount record has hidden whitespace")
			}
		}
		if len(fields) != 10 || fields[6] != "-" {
			return nil, fmt.Errorf("filesystem mount propagation or optional fields are unsupported")
		}
		id, e := number(fields[0])
		if e != nil {
			return nil, e
		}
		parent, e := number(fields[1])
		if e != nil {
			return nil, e
		}
		if id == parent || ids[id] {
			return nil, fmt.Errorf("filesystem mount identity duplicate or recursive")
		}
		ids[id] = true
		dev := strings.Split(fields[2], ":")
		if len(dev) != 2 {
			return nil, fmt.Errorf("filesystem device identity malformed")
		}
		for _, n := range dev {
			v, e := strconv.ParseUint(n, 10, 32)
			if e != nil || strconv.FormatUint(v, 10) != n {
				return nil, fmt.Errorf("filesystem device identity noncanonical")
			}
		}
		root, e := normalFilesystemMountPath(fields[3])
		if e != nil {
			return nil, e
		}
		point, e := normalFilesystemMountPath(fields[4])
		if e != nil {
			return nil, e
		}
		if points[point] {
			return nil, fmt.Errorf("filesystem mount point is stacked or duplicated")
		}
		points[point] = true
		options, e := normalFilesystemMountOptions(fields[5])
		if e != nil {
			return nil, e
		}
		super, e := normalFilesystemMountOptions(fields[9])
		if e != nil {
			return nil, e
		}
		if fields[7] == "" || fields[8] == "" || strings.ContainsRune(fields[8], '\\') {
			return nil, fmt.Errorf("filesystem type/source unsupported")
		}
		out = append(out, normalFilesystemMount{id, parent, fields[2], root, point, options, fields[7], fields[8], super})
	}
	// 可见根的parent可能在namespace之外；可见记录间不允许环。
	parents := map[uint64]uint64{}
	for _, m := range out {
		parents[m.ID] = m.Parent
	}
	for _, m := range out {
		seen := map[uint64]bool{}
		for id := m.ID; parents[id] != 0; id = parents[id] {
			if seen[id] {
				return nil, fmt.Errorf("filesystem mount parent cycle")
			}
			seen[id] = true
		}
	}
	return out, nil
}

func normalFilesystemMountPath(raw string) (string, error) {
	// 路径中空格/反斜线虽可合法表示，本固定布局不含它们；拒绝而非模糊归一化。
	if raw == "" || raw[0] != '/' || path.Clean(raw) != raw || strings.ContainsAny(raw, "\\\x00\r\n\t ") {
		return "", fmt.Errorf("filesystem path is not fixed canonical form")
	}
	return raw, nil
}
func normalFilesystemMountOptions(raw string) (map[string]bool, error) {
	out := map[string]bool{}
	keys := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		key, _, _ := strings.Cut(part, "=")
		if part == "" || key == "" || keys[key] || strings.ContainsAny(part, "\\\x00\r\n\t ") {
			return nil, fmt.Errorf("filesystem mount option duplicate or malformed")
		}
		keys[key] = true
		out[part] = true
	}
	if out["ro"] == out["rw"] {
		return nil, fmt.Errorf("filesystem mount lacks one exact access mode")
	}
	return out, nil
}

// 只判实际固定挂载图；成功仍不是初始FD清退或未来writer受控的证明。
func normalFilesystemValidateGraph(mounts []normalFilesystemMount) error {
	by := map[string]normalFilesystemMount{}
	for _, m := range mounts {
		by[m.Point] = m
	}
	root, ok := by["/"]
	if !ok || root.Root != "/" || root.FSType != "ext4" || !root.Options["ro"] {
		return fmt.Errorf("filesystem original ext4 root is not readonly")
	}
	if normalFilesystemHasDAX(root) {
		return fmt.Errorf("filesystem DAX is unsupported")
	}
	fixed := map[string]string{"/": "ext4", "/proc": "proc", "/sys": "sysfs", "/sys/kernel/security": "securityfs", "/dev": "devtmpfs", "/sys/fs/cgroup": "cgroup2", "/dev/pts": "devpts", "/run": "tmpfs", "/tmp": "tmpfs", "/dev/shm": "tmpfs", "/neonvm/runtime": "iso9660"}
	for _, name := range normalFilesystemPersistentHoles {
		fixed[name] = "ext4"
		m, ok := by[name]
		if !ok || m.FSType != "ext4" || m.Device != root.Device || m.Root != name || !m.Options["rw"] || normalFilesystemHasDAX(m) {
			return fmt.Errorf("filesystem fixed persistent hole differs")
		}
	}
	for _, name := range []string{"/run", "/tmp", "/dev/shm"} {
		m, ok := by[name]
		if !ok || m.Root != "/" || !m.Options["rw"] || !m.Options["noexec"] || !m.Options["nosuid"] || !m.Options["nodev"] {
			return fmt.Errorf("filesystem temporary hole differs")
		}
	}
	for _, name := range []string{"/proc", "/sys", "/sys/kernel/security", "/dev", "/sys/fs/cgroup", "/dev/pts", "/neonvm/runtime"} {
		if _, ok := by[name]; !ok {
			return fmt.Errorf("filesystem fixed pseudo or ISO mount missing")
		}
	}
	if len(mounts) != 17 || !by["/neonvm/runtime"].Options["ro"] {
		return fmt.Errorf("filesystem original ISO is not readonly")
	}
	security := by["/sys/kernel/security"]
	if !security.Options["ro"] || !security.Options["nosuid"] || !security.Options["nodev"] || !security.Options["noexec"] || security.Root != "/" {
		return fmt.Errorf("filesystem fixed securityfs is not readonly/isolated")
	}
	for _, m := range mounts {
		if m.Point != "/" {
			parentPoint := "/"
			if m.Point == "/dev/pts" || m.Point == "/dev/shm" {
				parentPoint = "/dev"
			}
			if m.Point == "/sys/fs/cgroup" || m.Point == "/sys/kernel/security" {
				parentPoint = "/sys"
			}
			if m.Parent != by[parentPoint].ID {
				return fmt.Errorf("filesystem visible mount parent differs")
			}
		}
		fs, ok := fixed[m.Point]
		if !ok || fs != m.FSType {
			return fmt.Errorf("filesystem mount lies outside original fixed layout")
		}
		if normalFilesystemHasDAX(m) {
			return fmt.Errorf("filesystem DAX source unsupported")
		}
	}
	return nil
}

// DAX的inode模式也能绕开本profile普通folio收费，不能只拒always。
// 未声明dax沿已固定内核普通模式；任何显式模式只准never，未来值一律拒绝。
func normalFilesystemHasDAX(m normalFilesystemMount) bool {
	for _, options := range []map[string]bool{m.Options, m.SuperOptions} {
		for option := range options {
			if option == "dax" || strings.HasPrefix(option, "dax=") && option != "dax=never" {
				return true
			}
		}
	}
	return false
}
