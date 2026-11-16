package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"
)

const normalOrdinaryInputMaxBytes = 256 << 20

type normalOrdinaryInput struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type normalOrdinaryInputReceipt struct {
	Kind              string                `json:"kind"`
	ProtocolVersion   uint32                `json:"protocol_version"`
	PackageLockSHA256 string                `json:"package_lock_sha256"`
	DerivedFileCount  int                   `json:"derived_file_count"`
	Inputs            []normalOrdinaryInput `json:"inputs"`
}

// 核实际APK、源码、patch和构建记录。最终root的每个衍生文件继续由原Collector
// 和打包后独立读回证明。SHA从发行锁传入，成功的输入读回不发放运行资格。
func verifyNormalOrdinaryInputs(sourceRoot string, raw []byte, expectedSHA string) (normalOrdinaryInputReceipt, error) {
	var receipt normalOrdinaryInputReceipt
	if sourceRoot == "" || !normalStockAssetDigest(expectedSHA) || fmt.Sprintf("%x", sha256.Sum256(raw)) != expectedSHA {
		return receipt, fmt.Errorf("ordinary source root or independent package lock digest differs")
	}
	lock, err := decodeNormalStockAssetPackageLock(raw)
	if err != nil {
		return receipt, err
	}
	root, err := os.OpenRoot(sourceRoot)
	if err != nil {
		return receipt, fmt.Errorf("open ordinary input root: %w", err)
	}
	defer root.Close()
	names := make([]string, 0, len(lock.Inputs))
	for name := range lock.Inputs {
		names = append(names, name)
	}
	sort.Strings(names)
	inputs := make([]normalOrdinaryInput, 0, len(names))
	for _, name := range names {
		entry, err := readNormalOrdinaryInput(root, name, lock.Inputs[name])
		if err != nil {
			return receipt, err
		}
		inputs = append(inputs, entry)
	}
	return normalOrdinaryInputReceipt{
		Kind: "normal-ordinary-input-readback", ProtocolVersion: 1, PackageLockSHA256: expectedSHA,
		DerivedFileCount: len(lock.Files), Inputs: inputs,
	}, nil
}

func readNormalOrdinaryInput(root *os.Root, name, expected string) (normalOrdinaryInput, error) {
	var entry normalOrdinaryInput
	if !fs.ValidPath(name) || !strings.HasPrefix(name, "apk/") && !strings.HasPrefix(name, "privsep-build/") || strings.Contains(name, "\\") {
		return entry, fmt.Errorf("invalid staged ordinary input path %q", name)
	}
	for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
		info, err := root.Lstat(parent)
		if err != nil || !info.IsDir() {
			return entry, fmt.Errorf("ordinary input parent must be an original directory: %s", parent)
		}
	}
	listed, err := root.Lstat(name)
	if err != nil || !listed.Mode().IsRegular() {
		return entry, fmt.Errorf("ordinary input must be an original regular file: %s", name)
	}
	file, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return entry, fmt.Errorf("open locked ordinary input %s: %w", name, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(listed, info) || info.Size() <= 0 || info.Size() > normalOrdinaryInputMaxBytes {
		return entry, fmt.Errorf("ordinary input must be a bounded regular file: %s", name)
	}
	hash := sha256.New()
	count, err := io.Copy(hash, io.LimitReader(file, normalOrdinaryInputMaxBytes+1))
	if err != nil || count != info.Size() || hex.EncodeToString(hash.Sum(nil)) != expected {
		return entry, fmt.Errorf("ordinary input byte count or digest differs: %s", name)
	}
	current, err := root.Lstat(name)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(info, current) {
		return entry, fmt.Errorf("ordinary input changed during read: %s", name)
	}
	return normalOrdinaryInput{Path: name, SHA256: expected, Bytes: count}, nil
}
