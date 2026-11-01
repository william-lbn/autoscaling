package main

import (
	"fmt"
	"io"
	"os"
)

// 构建选择显式独立于运行期能力；不读取环境，不从旧模式/库存自动升级。
func validateNormalOrdinaryV2BuildSelection(mode string, controls uint, lock []byte) error {
	if controls == 1 && len(lock) == 0 {
		return nil
	}
	if mode != normalSplitV3BuildMode || controls != 2 || len(lock) == 0 || len(lock) > 4<<20 {
		return fmt.Errorf("ordinary controls 2 require explicit System3 and bounded original package lock")
	}
	return nil
}
func readNormalOrdinaryV2BuildLock(mode string, controls uint, path string) ([]byte, error) {
	if controls == 1 && path == "" {
		return nil, nil
	}
	if mode != normalSplitV3BuildMode || controls != 2 || path == "" {
		return nil, fmt.Errorf("unsupported ordinary build selection")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Size() <= 0 || st.Size() > 4<<20 {
		return nil, fmt.Errorf("ordinary package lock must be bounded regular original input")
	}
	raw, e := io.ReadAll(io.LimitReader(f, 4<<20+1))
	if e != nil {
		return nil, e
	}
	if e = validateNormalOrdinaryV2BuildSelection(mode, controls, raw); e != nil {
		return nil, e
	}
	return raw, nil
}
