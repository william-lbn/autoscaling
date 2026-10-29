//go:build linux

package resourceguard

import (
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"

	"golang.org/x/sys/unix"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// /proc条目归普通进程所有，不使用只接受root所有文件的kernelText；路径只由
// 已核cgroup的规范PID生成，O_NOFOLLOW、长度上限和starttime前后读取封闭复用。
// 外层资源文件锁串行本服务，绝不拿HTTP取消把读/exec拆成放锁后的后台工作。
func boundedProcessText(pid int, name string) (string, error) {
	path := "/proc/" + strconv.Itoa(pid) + "/" + name
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return "", err
	}
	if len(raw) > 64*1024 {
		return "", fmt.Errorf("bounded process address space read failed")
	}
	return string(raw), nil
}

func inspectProcessAddressSpace(pid int, ceiling control.Uint64, uid uint64) (processAddressSample, error) {
	first, err := boundedProcessText(pid, "stat")
	if err != nil {
		return processAddressSample{}, err
	}
	start, err := postmasterTicks(first, pid)
	if err != nil {
		return processAddressSample{}, err
	}
	group, err := boundedProcessText(pid, "cgroup")
	if err != nil {
		return processAddressSample{}, err
	}
	if group != "0::/neon-postgres\n" {
		return processAddressSample{}, fmt.Errorf("address space process left original cgroup")
	}
	status, err := boundedProcessText(pid, "status")
	if err != nil {
		return processAddressSample{}, err
	}
	if err := processUID(status, uid); err != nil {
		return processAddressSample{}, err
	}
	limits, err := boundedProcessText(pid, "limits")
	if err != nil {
		return processAddressSample{}, err
	}
	hard, virtual, err := processAddressSpace(limits, status, ceiling)
	if err != nil {
		return processAddressSample{}, err
	}
	second, err := boundedProcessText(pid, "stat")
	if err != nil {
		return processAddressSample{}, err
	}
	end, err := postmasterTicks(second, pid)
	if err != nil || start != end {
		return processAddressSample{}, errAddressSpaceChurn
	}
	return processAddressSample{start: control.Uint64(start), hard: hard, virtual: virtual}, nil
}

func postgresUID() (uint64, error) {
	account, err := user.Lookup("postgres")
	if err != nil {
		return 0, fmt.Errorf("trusted postgres account unavailable")
	}
	uid, err := strconv.ParseUint(account.Uid, 10, 32)
	if err != nil || uid == 0 {
		return 0, fmt.Errorf("invalid postgres UID")
	}
	return uid, nil
}

// 仅覆盖固定PG domain；拒绝任何子cgroup以免只枚举本层却漏掉下层进程。
// 创建时先确认空组，入组/降权/exec持同一资源锁；之后只有受限PG的fork/clone/
// exec可新增成员，硬限及NNP不可被其放宽。root特权迁入仍须平台RBAC隔离，
// 此函数不把一个枚举快照宣称为跨恶意root写者的原子证明。
func readAddressSpaces(l Limits) (*AddressSpaceEvidence, error) {
	if l.AddressSpaceMaxBytes == 0 {
		return nil, nil
	}
	if err := validateAddressSpaceLimit(l.AddressSpaceMaxBytes, l.PidsMaxTasks); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(pgCgroup)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("address space domain has an unaccounted child cgroup or link")
		}
	}
	uid, err := postgresUID()
	if err != nil {
		return nil, err
	}
	return collectAddressSpaces(l, func() (string, error) {
		return kernelText(pgCgroup + "/cgroup.procs")
	}, func(pid int) (processAddressSample, error) {
		return inspectProcessAddressSpace(pid, l.AddressSpaceMaxBytes, uid)
	})
}
