package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// normalPrepareTrustedComputeLauncher只刷新未发布构建根的固定trusted启动器。
// 原prepare-configs-only继续保护vmstart；本动作必须单独显式选择并先于base。
// 不签System/Native/Source，不变更任何ELF、普通角色入口或运行期许可。
func normalPrepareTrustedComputeLauncher(root, mode string, controls uint32, plan normalBuildPlan, baseSHA string) error {
	if mode != normalSplitV3BuildMode || controls != 2 || plan.ExecutionModel != resourceguard.TrustedControllerExecutionModel || baseSHA != "" {
		return fmt.Errorf("trusted Compute launcher preparation requires explicit pre-base trusted-controller-v1 controls2")
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	checked, err := decodeNormalBuildPlan(raw)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return fmt.Errorf("trusted Compute preparation requires a private canonical root")
	}
	st, err := os.Lstat(root)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("trusted Compute image root differs")
	}
	const logical = "/neonvm/bin/vmstart"
	if err = normalStockAssetParents(root, logical); err != nil {
		return err
	}
	path := filepath.Join(root, logical)
	st, err = os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0755 {
		return fmt.Errorf("original trusted Compute launcher is not a regular 0755 file")
	}
	original, ok := st.Sys().(*syscall.Stat_t)
	if !ok || original.Uid != 0 || original.Gid != 0 || original.Nlink != 1 {
		return fmt.Errorf("original trusted Compute launcher owner or link count differs")
	}
	generated := []byte(normalTrustedComputeScript(checked))
	if err = normalStockAssetWrite(root, logical, generated, true); err != nil {
		return err
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), "prepared-trusted-compute")
	defer f.Close()
	var actual syscall.Stat_t
	if syscall.Fstat(fd, &actual) != nil || actual.Mode&syscall.S_IFMT != syscall.S_IFREG || actual.Uid != 0 || actual.Gid != 0 || actual.Nlink != 1 {
		return fmt.Errorf("prepared trusted Compute launcher identity differs")
	}
	if err = f.Chmod(0755); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, generated) {
		return fmt.Errorf("prepared trusted Compute launcher bytes differ")
	}
	return nil
}
