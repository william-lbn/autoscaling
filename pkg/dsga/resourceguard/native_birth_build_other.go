//go:build !linux

package resourceguard

import (
	"fmt"
	"os"
)

// 构建声明目前只支持原Linux amd64镜像，其他Host仍可编译CLI但不能调用此采集器。
func BuildNativeBirthManifestJSON(seedRaw, systemRaw []byte, canonical func(string) (string, error), open func(string) (*os.File, error), lstat func(string) (os.FileInfo, error)) ([]byte, error) {
	return nil, fmt.Errorf("native image collection requires Linux amd64")
}

func NativeBirthBuildFileUnchanged(a, b os.FileInfo) bool { return false }
