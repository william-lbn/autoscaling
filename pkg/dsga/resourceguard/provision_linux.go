//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

func rootDirectory(path string, mode uint32) error {
	if err := secureParents(filepath.Dir(path)); err != nil {
		return err
	}
	if err := os.Mkdir(path, os.FileMode(mode)); err == nil {
		// 只为本次新建目录落实明确权限，不让调用者umask意外阻断可信启动；
		// 已有目录的权限/归属不自动修复或收养。
		if err := os.Chmod(path, os.FileMode(mode)); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	if err := secureParents(path); err != nil {
		return err
	}
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return err
	}
	if uint32(st.Mode)&0777 != mode {
		return fmt.Errorf("managed directory permissions differ: %s", path)
	}
	return nil
}

// sealFile只创建缺失的root文件，已有内容必须逐字节相等；不覆盖旧配置/日志，
// 不跟随符号链接。先写fsync，再fsync目录；部分写保留现场并使启动失败，不能
// 自动删除旧文件重来。它只在固定root目录使用，不接受请求里的任意文件路径。
func sealFile(path string, data []byte) error {
	if err := secureParents(filepath.Dir(path)); err != nil {
		return err
	}
	f, err := rootFile(path, false)
	if err == nil {
		defer f.Close()
		old, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
		if err != nil {
			return err
		}
		if !bytes.Equal(old, data) {
			return fmt.Errorf("sealed original resource input differs: %s", path)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	fd, err := unix.Open(path, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0444)
	if err != nil {
		return err
	}
	f = os.NewFile(uintptr(fd), path)
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// 启动检查只读，不能调用会创建缺失文件的sealFile。来源/封存/固定配置任一
// 丢失或不符即拒绝；不因旧资源记录仍Applied而收养另一份启动来源。
func matchSealedFile(path string, data []byte) error {
	f, err := rootFile(path, false)
	if err != nil {
		return err
	}
	defer f.Close()
	old, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return err
	}
	if !bytes.Equal(old, data) {
		return fmt.Errorf("sealed original resource source differs: %s", path)
	}
	return nil
}

func runtimeSource() (SourceConfig, []byte, error) {
	var source SourceConfig
	f, err := rootFile(FixedRuntimePath, false)
	if err != nil {
		return source, nil, err
	}
	defer f.Close()
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(f.Fd()), &fs); err != nil {
		return source, nil, err
	}
	if fs.Type != unix.ISOFS_SUPER_MAGIC || fs.Flags&unix.ST_RDONLY == 0 {
		return source, nil, fmt.Errorf("resource source must be the read-only runtime ISO")
	}
	data, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return source, nil, err
	}
	if len(data) > 64*1024 {
		return source, nil, fmt.Errorf("runtime resource source exceeds bound")
	}
	var d RuntimeDocument
	if err := json.Unmarshal(data, &d); err != nil {
		return source, nil, err
	}
	source, err = d.Source()
	return source, data, err
}

func sealedRuntimeSource() (SourceConfig, error) {
	s, raw, err := runtimeSource()
	if err != nil {
		return s, err
	}
	if err := matchSealedFile(FixedSealedRuntimePath, raw); err != nil {
		return s, err
	}
	config, err := json.Marshal(s.Config)
	if err != nil {
		return s, err
	}
	return s, matchSealedFile(FixedConfigPath, config)
}

func provisionCgroup() error {
	const root = "/sys/fs/cgroup"
	if err := secureParents(root); err != nil {
		return err
	}
	var st unix.Statfs_t
	if err := unix.Statfs(root, &st); err != nil {
		return err
	}
	if st.Type != unix.CGROUP2_SUPER_MAGIC {
		return fmt.Errorf("managed Guest requires cgroup v2")
	}
	// vminit先关闭根迁移权限。本层拒绝旧宽权限，而不是一边仍有旧PG运行一边
	// 静默收养/改权限。根线程入口同样只允许root，后续不调用旧cgconfigparser。
	for _, name := range []string{"cgroup.procs", "cgroup.threads"} {
		f, err := rootFile(filepath.Join(root, name), false)
		if err != nil {
			return err
		}
		f.Close()
	}
	controllers, err := kernelText(filepath.Join(root, "cgroup.controllers"))
	if err != nil {
		return err
	}
	hasMemory := false
	for _, c := range strings.Fields(controllers) {
		hasMemory = hasMemory || c == "memory"
	}
	if !hasMemory {
		return fmt.Errorf("Guest memory controller unavailable")
	}
	f, err := rootFile(filepath.Join(root, "cgroup.subtree_control"), true)
	if err != nil {
		return err
	}
	_, writeErr := f.Write([]byte("+memory\n"))
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	// 已有不受信/非root目录明确拒绝；崩溃留下的同一root目录可以继续完成原意图。
	return rootDirectory(pgCgroup, 0755)
}

// ProvisionGuest只用于产品sysinit，必须在普通进程入口之前执行。来源是Runner
// 生成的固定只读ISO，不接受stdin/环境/任意路径代替。这里不启动PG、不开普通
// SQL、不发VM许可；任何失败由vmstart的独立CheckGuest阻止后续普通进程启动。
func ProvisionGuest(ctx context.Context) (Evidence, error) {
	var evidence Evidence
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return evidence, fmt.Errorf("Guest resource provisioning requires real root")
	}
	source, data, err := runtimeSource()
	if err != nil {
		return evidence, err
	}
	if err := rootDirectory("/etc/neon-dsga", 0755); err != nil {
		return evidence, err
	}
	if err := sealFile(FixedSealedRuntimePath, data); err != nil {
		return evidence, err
	}
	configJSON, err := json.Marshal(source.Config)
	if err != nil {
		return evidence, err
	}
	if err := sealFile(FixedConfigPath, configJSON); err != nil {
		return evidence, err
	}
	if err := rootDirectory("/var/lib/neon-dsga", 0755); err != nil {
		return evidence, err
	}
	if err := rootDirectory(FixedStateDir, 0700); err != nil {
		return evidence, err
	}
	if err := provisionCgroup(); err != nil {
		return evidence, err
	}
	g, err := Open(source.Config)
	if err != nil {
		return evidence, err
	}
	id, err := g.InspectIdentity()
	if err != nil {
		return evidence, err
	}
	command := BootstrapCommand{ProtocolVersion: 1, Identity: id, IssuerEpoch: source.IssuerEpoch, BudgetCommitmentID: source.BudgetCommitmentID, Limits: source.Limits}
	return g.Bootstrap(ctx, command)
}

// CheckGuest只读原预算与实际约束；不补初始化、不重放未知写。初次vmstart以及
// compute_ctl respawn均复核，sysinit退出失败也不能让BusyBox继续启动普通入口。
func CheckGuest(ctx context.Context) error {
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return fmt.Errorf("Guest resource check requires real root")
	}
	source, err := sealedRuntimeSource()
	if err != nil {
		return err
	}
	g, err := Open(source.Config)
	if err != nil {
		return err
	}
	return g.launchWithSource(ctx, &source, func() error { return nil })
}

// 唯一保留的旧TLS供给目录也必须由root控制，不能经可被postgres替换的目录或
// 符号链接间接写入封存输入/系统权限文件。真实目录由官方TLS挂载流程先建立。
func CheckManagementTLSDirectory() error {
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return fmt.Errorf("management TLS directory requires real root")
	}
	return secureParents(FixedManagementTLSPath)
}
