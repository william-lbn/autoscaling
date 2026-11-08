// 此工具仅为默认 Linux 文件 reader 测试物化真实只读输入，不输出 System/Kernel
// 来源或 normal 资格。inittab 调用当前源码的固定 v3 生成器，避免复制一份会漂移的表。
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: test-fixtures <new-output-directory>")
		os.Exit(2)
	}
	if err := generate(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func generate(directory string) error {
	if !filepath.IsAbs(directory) {
		return fmt.Errorf("fixture output directory must be absolute")
	}
	// 只向全新目录写入，不覆盖已有输入；Docker COPY 后由镜像真正提供 root owner。
	if err := os.Mkdir(directory, 0755); err != nil {
		return err
	}
	rows, err := resourceguard.NormalSystemSourceInitEntriesForVersion(3)
	if err != nil {
		return err
	}
	files := map[string]resourceguard.NormalSystemSourceFile{}
	for _, item := range []struct {
		name, data string
		large      bool
	}{
		{"small", "CI reader fixture only; not a Source or qualification\n", false},
		{"notELF", "CI negative ELF fixture\n", false},
		{"large0", "", true},
		{"large1", "", true},
		{"inittab", strings.Join(rows, "\n") + "\n", false},
	} {
		entry, err := writeFixture(directory, item.name, item.data, item.large)
		if err != nil {
			return err
		}
		files[item.name] = entry
	}
	raw, err := json.MarshalIndent(files, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(directory, "manifest.json"), append(raw, '\n'), 0444)
}

func writeFixture(directory, name, data string, large bool) (resourceguard.NormalSystemSourceFile, error) {
	var result resourceguard.NormalSystemSourceFile
	f, err := os.OpenFile(filepath.Join(directory, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return result, err
	}
	defer f.Close()
	if large {
		// 128MiB 未超过生产 reader 的 256MiB 上限；实际读取这些字节，给 /proc FD
		// 并发/取消观测充分窗口，不能以 mock hash 或延时回调代替真实文件读取。
		err = f.Truncate(128 << 20)
	} else {
		_, err = io.WriteString(f, data)
	}
	if err != nil {
		return result, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return result, err
	}
	hash := sha256.New()
	size, err := io.Copy(hash, f)
	if err != nil {
		return result, err
	}
	if err = f.Chmod(0444); err != nil {
		return result, err
	}
	if err = f.Close(); err != nil {
		return result, err
	}
	path := "/source-parallel/" + name
	if name == "inittab" {
		path = "/etc/inittab"
	}
	return resourceguard.NormalSystemSourceFile{
		Path: path, CanonicalPath: path, Kind: "config", Bytes: uint64(size), SHA256: hex.EncodeToString(hash.Sum(nil)),
	}, nil
}
