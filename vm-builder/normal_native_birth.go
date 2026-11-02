package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

const normalNativeBuildPath = "/usr/share/neon-dsga/native-build.json"

// 版本只选择构建采集器；默认0完全沿旧流程，绝不由JSON或存在一个PG自动升级。
func validateNormalNativeBirthBuildSelection(mode, arch string, controls, version uint) error {
	if version == 0 {
		return nil
	}
	if version != 1 || mode != normalSplitV3BuildMode || arch != targetArchLinuxAmd64 || controls != 2 {
		return fmt.Errorf("native birth build1 requires explicit Linux amd64 System3 ordinary controls2")
	}
	return nil
}

type normalNativeImage struct{ root string }

func newNormalNativeImage(root string) (*normalNativeImage, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return nil, fmt.Errorf("native image requires isolated canonical rootdisk")
	}
	actual, e := filepath.EvalSymlinks(root)
	if e != nil || actual != root {
		return nil, fmt.Errorf("native rootdisk alias differs")
	}
	im := &normalNativeImage{root: root}
	if e = im.checkParents("/placeholder"); e != nil {
		return nil, e
	}
	return im, nil
}

// 仅核隔离image内部的实际祖先，不跟随Host绝对symlink；构建目录本身不要求RO挂载。
func (im *normalNativeImage) checkParents(logical string) error {
	for dir := filepath.Dir(logical); ; dir = filepath.Dir(dir) {
		parent, e := normalImageCanonical(im.root, filepath.Dir(dir))
		if e != nil {
			return e
		}
		logicalParent, e := os.Lstat(filepath.Join(im.root, parent, filepath.Base(dir)))
		if e != nil {
			return e
		}
		if st, ok := logicalParent.Sys().(*syscall.Stat_t); !ok || st.Uid != 0 || logicalParent.Mode()&os.ModeSymlink == 0 && (!logicalParent.IsDir() || logicalParent.Mode().Perm()&0022 != 0) {
			return fmt.Errorf("native image logical parent is foreign or writable: %s", dir)
		}
		canonical, e := normalImageCanonical(im.root, dir)
		if e != nil {
			return e
		}
		st, e := os.Stat(filepath.Join(im.root, canonical))
		if e != nil || !st.IsDir() || st.Mode().Perm()&0022 != 0 {
			return fmt.Errorf("native image directory is writable or not a directory: %s", dir)
		}
		if s, ok := st.Sys().(*syscall.Stat_t); !ok || s.Uid != 0 {
			return fmt.Errorf("native image directory is not root controlled: %s", dir)
		}
		if dir == "/" {
			break
		}
	}
	return nil
}
func (im *normalNativeImage) canonical(logical string) (string, error) {
	if !filepath.IsAbs(logical) || filepath.Clean(logical) != logical || strings.ContainsAny(logical, "\x00\r\n") {
		return "", fmt.Errorf("native image logical path differs")
	}
	if e := im.checkParents(logical); e != nil {
		return "", e
	}
	resolved, e := normalImageCanonical(im.root, logical)
	if e != nil {
		return "", e
	}
	if e = im.checkParents(resolved); e != nil {
		return "", e
	}
	return resolved, nil
}
func (im *normalNativeImage) lstat(logical string) (os.FileInfo, error) {
	if !filepath.IsAbs(logical) || filepath.Clean(logical) != logical || strings.ContainsAny(logical, "\x00\r\n") {
		return nil, fmt.Errorf("native image logical path differs")
	}
	if e := im.checkParents(logical); e != nil {
		return nil, e
	}
	parent, e := normalImageCanonical(im.root, filepath.Dir(logical))
	if e != nil {
		return nil, e
	}
	return os.Lstat(filepath.Join(im.root, parent, filepath.Base(logical)))
}
func (im *normalNativeImage) open(logical string) (*os.File, error) {
	canonical, e := im.canonical(logical)
	if e != nil || canonical != logical {
		return nil, fmt.Errorf("native image open requires canonical path: %s", logical)
	}
	return os.OpenFile(filepath.Join(im.root, canonical), os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
}
func (im *normalNativeImage) seed() ([]byte, os.FileInfo, error) {
	f, e := im.open(normalNativeBuildPath)
	if e != nil {
		return nil, nil, e
	}
	defer f.Close()
	before, e := f.Stat()
	if e != nil || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > 32<<10 || before.Mode().Perm()&0022 != 0 {
		return nil, nil, fmt.Errorf("native image seed is not bounded regular root input")
	}
	if st, ok := before.Sys().(*syscall.Stat_t); !ok || st.Uid != 0 {
		return nil, nil, fmt.Errorf("native image seed is not root controlled")
	}
	raw, e := io.ReadAll(io.NewSectionReader(f, 0, 32<<10+1))
	if e != nil || int64(len(raw)) != before.Size() {
		return nil, nil, fmt.Errorf("native image seed changed during read")
	}
	after, e := f.Stat()
	if e != nil || !resourceguard.NativeBirthBuildFileUnchanged(before, after) {
		return nil, nil, fmt.Errorf("native image seed identity changed")
	}
	return raw, after, nil
}

// finalSystem是将实际写出的原字节（包括newline）。所有检查/闭包收集成功后才发布；
// 任一写入失败令本Docker RUN失败，并移除本次System输出，不留可用的半个构建声明。
// 显式native1唯一命令入口先形成完整System3/metadata18c，再沿原事务发布。
func publishCompleteNormalNativeBirthImage(root string, input []byte) error {
	finalSystem, e := completeNormalSystemBuildManifest(root, input)
	if e != nil {
		return e
	}
	return publishNormalNativeBirthImage(root, append(finalSystem, '\n'))
}

func publishNormalNativeBirthImage(root string, finalSystem []byte) (err error) {
	im, e := newNormalNativeImage(root)
	if e != nil {
		return e
	}
	seed, seedIdentity, e := im.seed()
	if e != nil {
		return e
	}
	raw, e := resourceguard.BuildNativeBirthManifestJSON(seed, finalSystem, im.canonical, im.open, im.lstat)
	if e != nil {
		return e
	}
	systemPath := resourceguard.NormalSystemSourceManifestPath
	if e = im.checkParents(systemPath); e != nil {
		return e
	}
	parent, e := normalImageCanonical(root, filepath.Dir(systemPath))
	if e != nil || parent != filepath.Dir(systemPath) {
		return fmt.Errorf("native System output directory alias differs")
	}
	if _, e = im.lstat(systemPath); !os.IsNotExist(e) {
		return fmt.Errorf("native System output already exists or is unknown")
	}
	current, identity, e := im.seed()
	if e != nil || !bytes.Equal(seed, current) || !resourceguard.NativeBirthBuildFileUnchanged(seedIdentity, identity) {
		return fmt.Errorf("native seed changed before publication")
	}
	stage, e := os.CreateTemp(filepath.Dir(filepath.Join(root, normalNativeBuildPath)), ".native-build-pending-*")
	if e != nil {
		return e
	}
	stageName := stage.Name()
	defer os.Remove(stageName)
	defer stage.Close()
	if _, e = stage.Write(raw); e != nil {
		return e
	}
	if e = stage.Chmod(0444); e != nil {
		return e
	}
	if e = stage.Sync(); e != nil {
		return e
	}
	if e = stage.Close(); e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(root, systemPath), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0444)
	if e != nil {
		return e
	}
	created, e := f.Stat()
	if e != nil {
		f.Close()
		return e
	}
	defer func() {
		f.Close()
		if err != nil {
			if current, e := os.Lstat(filepath.Join(root, systemPath)); e == nil && os.SameFile(created, current) {
				os.Remove(filepath.Join(root, systemPath))
			}
		}
	}()
	if _, e = f.Write(finalSystem); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	current, identity, e = im.seed()
	if e != nil || !bytes.Equal(seed, current) || !resourceguard.NativeBirthBuildFileUnchanged(seedIdentity, identity) {
		return fmt.Errorf("native seed changed at publication")
	}
	return os.Rename(stageName, filepath.Join(root, normalNativeBuildPath))
}
