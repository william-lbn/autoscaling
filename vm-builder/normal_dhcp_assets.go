package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// 此二摘要锁定正式Alpine default.script输入及prepare-root.sh既有awk的唯一输出。
// 输入锁定已读回原生产盘的完整Alpine hook字节（原脚本已固定shebang/PATH）。
// 此阶段不从Builder6X元数据推断原盘的Alpine版本，也不把该hash当runtime Source
// 或DNS常数，不可用于证明Guest已取得DNS。更换Alpine版本必须同步实际原hook、
// 包锁和等价性测试，未知脚本绝不通过模式匹配猜测转换。
const normalAlpineDHCPHookSHA = "181f37e20fe2894d003cb120c6479e8a22060d993b15bfbc6165b7ecbdf1414a"
const normalPreparedDHCPHookSHA = "8a3296afeb02b556f12c0b0a6915f374ba71e9c02791a70194deb83bbd714e08"

func normalProductionDHCPHook(raw []byte, prepareOnly bool) ([]byte, error) {
	sum := fmt.Sprintf("%x", sha256.Sum256(raw))
	if sum == normalPreparedDHCPHookSHA {
		return append([]byte(nil), raw...), nil
	}
	if !prepareOnly || sum != normalAlpineDHCPHookSHA {
		return nil, fmt.Errorf("production DHCP hook differs from the locked Alpine or prepared bytes")
	}
	// 与embedded prepare-root.sh的awk逐项一致；真实测试执行该awk并逐字比输出，
	// 防止Docker正式路径与pre-base私有根路径各自漂移。
	var out strings.Builder
	inside := false
	counts := [4]int{}
	for _, line := range strings.SplitAfter(string(raw), "\n") {
		original := line
		if line == "resolvconf() {\n" {
			inside = true
		}
		if inside {
			switch {
			case line == "\techo -n > \"$RESOLV_CONF.$$\"\n":
				line = "\t{\n"
				counts[0]++
			case strings.HasSuffix(line, " >> \"$RESOLV_CONF.$$\"\n"):
				line = strings.TrimSuffix(line, " >> \"$RESOLV_CONF.$$\"\n") + "\n"
				counts[1]++
			case line == "\tchmod a+r \"$RESOLV_CONF.$$\"\n":
				counts[2]++
				continue
			case line == "\tmv -f \"$RESOLV_CONF.$$\" \"$RESOLV_CONF\"\n":
				line = "\t} > \"$RESOLV_CONF\" || exit 1\n"
				counts[3]++
			}
		}
		out.WriteString(line)
		if inside && original == "}\n" {
			inside = false
		}
	}
	prepared := []byte(out.String())
	if inside || counts != [4]int{1, 3, 1, 1} || fmt.Sprintf("%x", sha256.Sum256(prepared)) != normalPreparedDHCPHookSHA {
		return nil, fmt.Errorf("locked DHCP hook conversion drift")
	}
	return prepared, nil
}

// 这里只写尚未发布的构建根。沿原原子写/owner/fsync，再恢复脚本0755；
// 失败必须停止base/库存发布。运行期hook只原位写resolver，不替换bind inode。
func normalProductionDHCPHookWrite(root string, raw []byte) error {
	const logical = "/neonvm/bin/udhcpc.script"
	if e := normalStockAssetWrite(root, logical, raw, true); e != nil {
		return e
	}
	path := filepath.Join(root, logical)
	if e := os.Chmod(path, 0755); e != nil {
		return e
	}
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	if e = f.Sync(); e != nil {
		return e
	}
	got, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(got, raw) {
		return fmt.Errorf("prepared DHCP hook write differs: %v", e)
	}
	return nil
}
