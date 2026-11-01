//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type normalFilesystemObject struct {
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
	MountID    uint64 `json:"mount_id"`
	Filesystem int64  `json:"filesystem"`
	Readonly   bool   `json:"readonly"`
}
type normalFilesystemTask struct {
	pid                            int
	start                          uint64
	pidfd, procfd, namespace, root *os.File
	ns, rootObject                 normalFilesystemObject
}

func (t *normalFilesystemTask) close() {
	if t == nil {
		return
	}
	for _, f := range []*os.File{t.root, t.namespace, t.procfd, t.pidfd} {
		if f != nil {
			f.Close()
		}
	}
}
func normalFilesystemFDObject(fd int) (normalFilesystemObject, error) {
	var st unix.Stat_t
	var fs unix.Statfs_t
	var sx unix.Statx_t
	if e := unix.Fstat(fd, &st); e != nil {
		return normalFilesystemObject{}, e
	}
	if e := unix.Fstatfs(fd, &fs); e != nil {
		return normalFilesystemObject{}, e
	}
	if e := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_SYMLINK_NOFOLLOW, unix.STATX_BASIC_STATS|unix.STATX_MNT_ID, &sx); e != nil {
		return normalFilesystemObject{}, e
	}
	if sx.Attributes&unix.STATX_ATTR_DAX != 0 {
		return normalFilesystemObject{}, fmt.Errorf("filesystem actual DAX inode is unsupported")
	}
	if sx.Mask&unix.STATX_MNT_ID == 0 || sx.Mnt_id == 0 {
		return normalFilesystemObject{}, fmt.Errorf("filesystem original mount identity unavailable")
	}
	return normalFilesystemObject{Device: st.Dev, Inode: st.Ino, MountID: sx.Mnt_id, Filesystem: int64(fs.Type), Readonly: fs.Flags&unix.ST_RDONLY != 0}, nil
}
func normalFilesystemOpenAt(fd int, name string, flags int) (*os.File, error) {
	n, e := unix.Openat(fd, name, flags|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	return os.NewFile(uintptr(n), "filesystem-original-object"), nil
}
func normalFilesystemReadAt(fd int, name string, limit int64) ([]byte, error) {
	f, e := normalFilesystemOpenAt(fd, name, unix.O_RDONLY|unix.O_NOFOLLOW)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, limit+1))
	if e != nil || int64(len(b)) > limit {
		return nil, errors.Join(fmt.Errorf("filesystem proc inventory unreadable or exceeds bound"), e)
	}
	return b, nil
}

// 只有当前Root自身和Guest不复用的PID1可在这里首次pin；没有任意PID/角色参数。
// 运行期新Root角色只能由原role broker的opaque task移交，不能用此函数认领它。
func normalFilesystemPinInitialTask(pid1 bool) (*normalFilesystemTask, error) {
	pid, path := os.Getpid(), "/proc/self"
	if pid1 {
		pid, path = 1, "/proc/1"
	}
	fd, e := unix.PidfdOpen(pid, 0)
	if e != nil {
		return nil, e
	}
	t := &normalFilesystemTask{pid: pid, pidfd: os.NewFile(uintptr(fd), "filesystem-original-pidfd")}
	ok := false
	defer func() {
		if !ok {
			t.close()
		}
	}()
	fd, e = unix.Open(path, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	t.procfd = os.NewFile(uintptr(fd), "filesystem-original-proc")
	var fs unix.Statfs_t
	if unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		return nil, fmt.Errorf("filesystem task source is not procfs")
	}
	stat, e := normalFilesystemReadAt(fd, "stat", 64<<10)
	if e != nil {
		return nil, e
	}
	t.start, e = postmasterTicks(string(stat), pid)
	if e != nil {
		return nil, e
	}
	if t.namespace, e = normalFilesystemOpenAt(fd, "ns/mnt", unix.O_RDONLY); e != nil {
		return nil, e
	}
	if t.root, e = normalFilesystemOpenAt(fd, "root", unix.O_PATH|unix.O_DIRECTORY); e != nil {
		return nil, e
	}
	if t.ns, e = normalFilesystemFDObject(int(t.namespace.Fd())); e != nil {
		return nil, e
	}
	if t.rootObject, e = normalFilesystemFDObject(int(t.root.Fd())); e != nil {
		return nil, e
	}
	if e = t.verify(); e != nil {
		return nil, e
	}
	ok = true
	return t, nil
}
func (t *normalFilesystemTask) verify() error {
	if t == nil || t.pidfd == nil || t.procfd == nil {
		return fmt.Errorf("filesystem original task missing")
	}
	if e := arrayRegistryPIDFDAlive(int(t.pidfd.Fd())); e != nil {
		return e
	}
	if e := unix.PidfdSendSignal(int(t.pidfd.Fd()), 0, nil, 0); e != nil {
		return e
	}
	b, e := os.ReadFile("/proc/self/fdinfo/" + strconv.Itoa(int(t.pidfd.Fd())))
	if e != nil {
		return e
	}
	v, e := nativeTraceStatusValues(string(b), "Pid", 1)
	if e != nil || v[0] != uint64(t.pid) {
		return fmt.Errorf("filesystem original pidfd identity differs")
	}
	stat, e := normalFilesystemReadAt(int(t.procfd.Fd()), "stat", 64<<10)
	if e != nil {
		return e
	}
	ticks, e := postmasterTicks(string(stat), t.pid)
	if e != nil || ticks != t.start {
		return fmt.Errorf("filesystem original task start differs")
	}
	// 原namespace/root FD只是pin，不能把PID1后来setns/chroot当原引用仍有效。
	for _, item := range []struct {
		name string
		want normalFilesystemObject
	}{{"ns/mnt", t.ns}, {"root", t.rootObject}} {
		f, e := normalFilesystemOpenAt(int(t.procfd.Fd()), item.name, unix.O_PATH)
		if e != nil {
			return e
		}
		got, e := normalFilesystemFDObject(int(f.Fd()))
		f.Close()
		if e != nil || got != item.want {
			return fmt.Errorf("filesystem original task namespace/root changed")
		}
	}
	return nil
}
func normalFilesystemReadDirAt(fd int, name string, maximum int) ([]string, error) {
	f, e := normalFilesystemOpenAt(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	names, e := f.Readdirnames(maximum + 1)
	if e != nil && e != io.EOF {
		return nil, e
	}
	if len(names) > maximum {
		return nil, fmt.Errorf("filesystem task inventory exceeds bound")
	}
	sort.Strings(names)
	return names, nil
}

type normalFilesystemTaskInventory struct {
	PID                   int    `json:"pid"`
	StartTicks            uint64 `json:"start_ticks"`
	Threads               int    `json:"threads"`
	CheckedDescriptors    int    `json:"checked_descriptors"`
	CheckedSharedMappings int    `json:"checked_shared_mappings"`
}

// 读FD的实际mount对象；包括O_PATH、root/cwd、已删除/已卸载路径。不能用readlink
// 文本或可见mountinfo代替FD statfs+STATX_MNT_ID。只读ELF的private COW不改底层文件。
func normalFilesystemCheckHeldFD(fd int, allowed map[uint64]bool, sharedWritable bool) error {
	object, e := normalFilesystemFDObject(fd)
	if e != nil {
		return e
	}
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil {
		return e
	}
	if st.Mode&unix.S_IFMT == unix.S_IFBLK {
		return fmt.Errorf("filesystem initial task retains raw block device")
	}
	if object.Filesystem == unix.EXT4_SUPER_MAGIC && (sharedWritable || !object.Readonly) && !allowed[object.MountID] {
		return fmt.Errorf("filesystem initial task retains ext4 object outside original RW holes")
	}
	return nil
}
func (t *normalFilesystemTask) inspect(ctx context.Context, namespace, root normalFilesystemObject, allowed map[uint64]bool) (normalFilesystemTaskInventory, error) {
	out := normalFilesystemTaskInventory{PID: t.pid, StartTicks: t.start}
	if e := t.verify(); e != nil {
		return out, e
	}
	tids, e := normalFilesystemReadDirAt(int(t.procfd.Fd()), "task", 512)
	if e != nil {
		return out, e
	}
	for _, tid := range tids {
		if _, e = strconv.ParseUint(tid, 10, 32); e != nil {
			return out, e
		}
		if e = ctx.Err(); e != nil {
			return out, e
		}
		dir, e := normalFilesystemOpenAt(int(t.procfd.Fd()), "task/"+tid, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW)
		if e != nil {
			return out, e
		}
		e = func() error {
			defer dir.Close()
			for _, name := range []string{"ns/mnt", "root", "cwd"} {
				f, e := normalFilesystemOpenAt(int(dir.Fd()), name, unix.O_PATH)
				if e != nil {
					return e
				}
				o, e := normalFilesystemFDObject(int(f.Fd()))
				if e == nil && name == "ns/mnt" && (o.Device != namespace.Device || o.Inode != namespace.Inode) {
					e = fmt.Errorf("filesystem thread mount namespace differs")
				}
				if e == nil && name == "root" && o != root {
					e = fmt.Errorf("filesystem thread root differs from PID1")
				}
				if e == nil {
					e = normalFilesystemCheckHeldFD(int(f.Fd()), allowed, false)
				}
				f.Close()
				if e != nil {
					return e
				}
			}
			// 保留目录FD到本轮结束，避免观察者自己关闭fd目录产生假ENOENT。
			fds, e := normalFilesystemOpenAt(int(dir.Fd()), "fd", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW)
			if e != nil {
				return e
			}
			defer fds.Close()
			names, e := fds.Readdirnames(4097)
			if e != nil && e != io.EOF {
				return e
			}
			if len(names) > 4096 {
				return fmt.Errorf("filesystem inherited FD inventory exceeds bound")
			}
			for _, name := range names {
				if _, e = strconv.ParseUint(name, 10, 32); e != nil {
					return e
				}
				f, e := normalFilesystemOpenAt(int(fds.Fd()), name, unix.O_PATH)
				if e != nil {
					return fmt.Errorf("filesystem original FD changed during capture: %w", e)
				}
				e = normalFilesystemCheckHeldFD(int(f.Fd()), allowed, false)
				f.Close()
				if e != nil {
					return e
				}
				out.CheckedDescriptors++
			}
			maps, e := normalFilesystemReadAt(int(dir.Fd()), "maps", 1<<20)
			if e != nil {
				return e
			}
			for _, line := range strings.Split(strings.TrimSuffix(string(maps), "\n"), "\n") {
				fields := strings.Fields(line)
				if len(fields) < 5 || len(fields[1]) != 4 {
					return fmt.Errorf("filesystem original maps malformed")
				}
				if fields[1][1] != 'w' || fields[1][3] != 's' || fields[4] == "0" {
					continue
				}
				span := strings.Split(fields[0], "-")
				if len(span) != 2 {
					return fmt.Errorf("filesystem original maps range malformed")
				}
				for _, x := range span {
					if _, e = strconv.ParseUint(x, 16, 64); e != nil {
						return e
					}
				}
				// map_files仅TGID表存在；同TGID线程共享mm，仍沿原procFD魔术链接取真实文件。
				f, e := normalFilesystemOpenAt(int(t.procfd.Fd()), "map_files/"+fields[0], unix.O_PATH)
				if e != nil {
					return e
				}
				e = normalFilesystemCheckHeldFD(int(f.Fd()), allowed, true)
				f.Close()
				if e != nil {
					return e
				}
				out.CheckedSharedMappings++
			}
			return nil
		}()
		if e != nil {
			return out, e
		}
		out.Threads++
	}
	if e := t.verify(); e != nil {
		return out, e
	}
	return out, nil
}

// 这是额外否定census，不从扫描到的PID给任何角色资格。只有原PID1和self的
// pidfd/proc出生pin被允许；其它非kernel任务即便已僵死也不补认成可信角色。
func normalFilesystemRejectOtherInitialUsers() error {
	entries, e := os.ReadDir("/proc")
	if e != nil {
		return e
	}
	for _, entry := range entries {
		pid, e := strconv.Atoi(entry.Name())
		if e != nil || pid <= 0 || pid == 1 || pid == os.Getpid() {
			continue
		}
		b, e := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
		if e != nil {
			return fmt.Errorf("filesystem initial task census changed: %w", e)
		}
		end := strings.LastIndex(string(b), ") ")
		if end < 0 {
			return fmt.Errorf("filesystem initial task flags malformed")
		}
		fields := strings.Fields(string(b[end+2:]))
		if len(fields) < 7 {
			return fmt.Errorf("filesystem initial task flags missing")
		}
		flags, e := strconv.ParseUint(fields[6], 10, 64)
		if e != nil || flags&0x00200000 == 0 {
			return fmt.Errorf("filesystem capture occurs after another userspace role")
		}
	}
	return nil
}
