package qualificationissuer

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// ReadTrustedFile 仅接受 root 拥有且不可被组/其他用户修改的普通原文件与父目录。
// 不跟随最后一级 symlink；可信配置仍须由独立平台管理者只读挂载，不接租户路径。
func ReadTrustedFile(path string, maximum int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || maximum < 1 || maximum > MaxArtifactBytes {
		return nil, fmt.Errorf("canonical fixed trusted input path required")
	}
	if err := protectedDirectory(filepath.Dir(path), 0); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("trusted input original file unavailable")
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("trusted input identity unavailable")
	}
	identity, ok := stat.Sys().(*syscall.Stat_t)
	if !ok || identity.Uid != 0 || !stat.Mode().IsRegular() || stat.Mode().Perm()&0022 != 0 || stat.Size() < 1 || stat.Size() > maximum {
		return nil, fmt.Errorf("trusted input is not a bounded root protected regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(raw)) > maximum {
		return nil, fmt.Errorf("trusted input unavailable or oversized")
	}
	return raw, nil
}

func LoadInputs(path string) (Config, Bundle, error) {
	var c Config
	var b Bundle
	raw, err := ReadTrustedFile(path, MaxReportBytes)
	if err != nil {
		return c, b, err
	}
	if json.Unmarshal(raw, &c) != nil {
		return c, b, fmt.Errorf("trusted issuer startup configuration invalid")
	}
	b.Report, err = ReadTrustedFile(c.ReportPath, MaxReportBytes)
	if err != nil {
		return c, b, err
	}
	b.LinuxArtifact, err = ReadTrustedFile(c.LinuxArtifactPath, MaxArtifactBytes)
	if err != nil {
		return c, b, err
	}
	b.ContinuousArtifact, err = ReadTrustedFile(c.ContinuousArtifactPath, MaxArtifactBytes)
	if err != nil {
		return c, b, err
	}
	if _, err = c.authenticate(b); err != nil {
		return c, b, err
	}
	return c, b, nil
}

// 与现有 Agent UDS 相同：专用目录和祖先不得由非管理者替换。root/self
// sticky 临时目录只用于受控本地验证，不允许任意可写非 sticky 祖先。
func protectedDirectory(path string, uid uint32) error {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return fmt.Errorf("trusted source directory contains symlinks")
	}
	current := path
	for {
		info, err := os.Lstat(current)
		if err != nil || !info.IsDir() {
			return fmt.Errorf("trusted source directory unavailable")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 && stat.Uid != uid || info.Mode().Perm()&0022 != 0 && (current == path || info.Mode()&os.ModeSticky == 0) {
			return fmt.Errorf("trusted source directory is replaceable")
		}
		if current == string(filepath.Separator) {
			break
		}
		current = filepath.Dir(current)
	}
	return nil
}

func originalAgentSocket(path string) ([2]uint64, error) {
	var zero [2]uint64
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm()&0077 != 0 {
		return zero, fmt.Errorf("original private Agent socket unavailable")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 && stat.Uid != uint32(os.Geteuid()) {
		return zero, fmt.Errorf("Agent socket owner is not trusted local service UID")
	}
	if err := protectedDirectory(filepath.Dir(path), stat.Uid); err != nil {
		return zero, err
	}
	return [2]uint64{uint64(stat.Dev), stat.Ino}, nil
}
