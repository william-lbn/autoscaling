//go:build linux

package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"golang.org/x/sys/unix"
)

// 不扫描宿主树。仅私有 cgroup namespace 报 / 时，使用原 Pod/容器/QoS 两种规范路径。
func cpuGroupCandidates(source string, id *runnercpu.Identity, container, qos string) ([]string, error) {
	paths := []string{source}
	if source != "/" || id == nil {
		return paths, nil
	}
	if id.Validate() != nil {
		return nil, fmt.Errorf("original CPU container identity unavailable")
	}
	uid := id.RunnerPodUID
	if len(uid) != 36 || uid[8] != '-' || uid[13] != '-' || uid[18] != '-' || uid[23] != '-' {
		return nil, fmt.Errorf("original CPU Pod UID is not canonical")
	}
	compact := strings.ReplaceAll(uid, "-", "")
	decoded, err := hex.DecodeString(compact)
	if err != nil || len(decoded) != 16 || compact != strings.ToLower(compact) {
		return nil, fmt.Errorf("original CPU Pod UID is invalid")
	}
	prefix := "containerd://"
	if !strings.HasPrefix(container, prefix) {
		return nil, fmt.Errorf("CPU HostPath profile supports original containerd only")
	}
	cid := strings.TrimPrefix(container, prefix)
	decoded, err = hex.DecodeString(cid)
	if err != nil || len(decoded) != 32 || cid != strings.ToLower(cid) {
		return nil, fmt.Errorf("original CPU container ID is invalid")
	}
	qosPath := ""
	switch qos {
	case "Guaranteed":
	case "Burstable":
		qosPath = "burstable"
	case "BestEffort":
		qosPath = "besteffort"
	default:
		return nil, fmt.Errorf("original CPU Pod QoS unavailable")
	}
	systemdUID := strings.ReplaceAll(uid, "-", "_")
	if qosPath == "" {
		paths = append(paths, "/kubepods.slice/kubepods-pod"+systemdUID+".slice/cri-containerd-"+cid+".scope",
			"/kubepods/pod"+uid+"/"+cid)
	} else {
		paths = append(paths, "/kubepods.slice/kubepods-"+qosPath+".slice/kubepods-"+qosPath+"-pod"+systemdUID+".slice/cri-containerd-"+cid+".scope",
			"/kubepods/"+qosPath+"/pod"+uid+"/"+cid)
	}
	return paths, nil
}

func cpuGroupContains(fd int, pid int) error {
	raw, err := readCPUGroupFile(fd, "cgroup.procs")
	if err != nil {
		return err
	}
	wanted := strconv.Itoa(pid)
	for _, item := range strings.Fields(string(raw)) {
		if item == wanted {
			return nil
		}
	}
	return fmt.Errorf("original CPU process is not a direct member of the fixed cgroup")
}

func resolveOriginalCPUGroup(id *runnercpu.Identity, container, qos string) (*originalCPUGroup, error) {
	source, err := unifiedCPUPath()
	if err != nil {
		return nil, err
	}
	// 优先原命名空间路径；匹配真实本进程时不依赖运行时路径适配。
	paths := []string{source}
	direct, err := openCPUGroupPath(source)
	if err == nil {
		if source != "/" && cpuGroupContains(direct, os.Getpid()) == nil {
			var st unix.Stat_t
			err = unix.Fstat(direct, &st)
			if err == nil {
				return &originalCPUGroup{fd: direct, source: source, path: source, dev: uint64(st.Dev), ino: st.Ino}, nil
			}
		}
		unix.Close(direct)
	}
	paths, err = cpuGroupCandidates(source, id, container, qos)
	if err != nil {
		return nil, err
	}
	for _, candidate := range paths[1:] {
		fd, err := openCPUGroupPath(candidate)
		if err != nil {
			continue
		}
		var st unix.Stat_t
		if err = unix.Fstat(fd, &st); err == nil {
			err = cpuGroupContains(fd, os.Getpid())
		}
		if err != nil {
			unix.Close(fd)
			continue
		}
		return &originalCPUGroup{fd: fd, source: source, path: candidate, dev: uint64(st.Dev), ino: st.Ino}, nil
	}
	return nil, fmt.Errorf("original CPU cgroup cannot be matched without escaping or scanning Host hierarchy")
}

// 每次实读/实写都重核原成员。QEMU 只来自 RunQEMU 的 Start-before-Wait 观察能力。
func (g *originalCPUGroup) checkMembers() error {
	// 正常domain中同一进程所有线程同属该域；threaded/domain threaded不能据一个PID证明整个QEMU额度。
	kind, err := readCPUGroupFile(g.fd, "cgroup.type")
	if err != nil || strings.TrimSpace(string(kind)) != "domain" {
		return fmt.Errorf("original CPU group is not a complete process domain")
	}
	if err := cpuGroupContains(g.fd, os.Getpid()); err != nil {
		return err
	}
	if g.qemu != nil {
		return g.qemu(func(pid int, _ uint64) error {
			if pid <= 0 {
				return fmt.Errorf("original QEMU PID unavailable")
			}
			return cpuGroupContains(g.fd, pid)
		})
	}
	return nil // 仅启动前安装初始额度；此时 QEMU 尚未存在，也不返回就绪事实。
}

func (g *originalCPUGroup) fixedIdentity() (uint64, uint64, error) {
	current, err := unifiedCPUPath()
	if err != nil || current != g.source {
		return 0, 0, errors.Join(err, fmt.Errorf("Runner CPU cgroup moved"))
	}
	fd, err := openCPUGroupPath(g.path)
	if err != nil {
		return 0, 0, err
	}
	defer unix.Close(fd)
	var original, actual unix.Stat_t
	if err = unix.Fstat(g.fd, &original); err != nil {
		return 0, 0, err
	}
	if err = unix.Fstat(fd, &actual); err != nil {
		return 0, 0, err
	}
	if original.Dev != actual.Dev || original.Ino != actual.Ino || uint64(original.Dev) != g.dev || original.Ino != g.ino {
		return 0, 0, fmt.Errorf("original CPU cgroup changed dev/inode")
	}
	if err = g.checkMembers(); err != nil {
		return 0, 0, err
	}
	return g.dev, g.ino, nil
}
