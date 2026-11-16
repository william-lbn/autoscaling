package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const normalPGDataParent = "/var/db/postgres"

// PGDATA 在首次启动时由非 root Compute 建立。OCI 根层有可能统一属主，
// 因此生成最终磁盘前按锁定计划恢复这一目录，读回成品时只检查，不修补。
func (im *normalNativeImage) checkNormalPGDataParent(plan normalBuildPlan, prepare bool) error {
	if err := im.checkParents(normalPGDataParent); err != nil {
		return err
	}
	canonical, err := normalImageCanonical(im.root, normalPGDataParent)
	if err != nil || canonical != normalPGDataParent {
		return fmt.Errorf("normal PGDATA parent must be a canonical directory: %s", normalPGDataParent)
	}
	parent, err := os.OpenFile(filepath.Join(im.root, canonical), os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open normal PGDATA parent: %w", err)
	}
	defer parent.Close()
	info, err := parent.Stat()
	if err != nil {
		return fmt.Errorf("stat normal PGDATA parent: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode().Perm()&0700 != 0700 || info.Mode().Perm()&0022 != 0 {
		return fmt.Errorf("normal PGDATA parent must allow owner-only writes and owner traversal")
	}
	if stat.Uid == plan.ComputeUID && stat.Gid == plan.ComputeGID {
		return nil
	}
	if !prepare {
		return fmt.Errorf("normal PGDATA parent owner %d:%d differs from planned Compute %d:%d", stat.Uid, stat.Gid, plan.ComputeUID, plan.ComputeGID)
	}
	if err := parent.Chown(int(plan.ComputeUID), int(plan.ComputeGID)); err != nil {
		return fmt.Errorf("restore normal PGDATA parent owner: %w", err)
	}
	return nil
}
