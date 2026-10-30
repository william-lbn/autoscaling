package resourceguard

import (
	"fmt"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const maxPostgresEnvironmentBytes = 256 << 10

// postgresExecEnvironment 在固定 PG exec 前净化环境。降权后的动态 ELF 不可
// 依赖此前 SUID helper 的 loader 清理：root 直接调用和下一次普通 exec 都必须
// 排除 main/原生 AS 建立之前的外部代码注入。其余业务值（包括存储 token）原样
// 保留，错误中不回显键值。未来受限 profile 只能由原 root 来源显式加入。
func postgresExecEnvironment(original []string, limit control.Uint64) ([]string, error) {
	if validateAddressSpaceLimit(limit, 1) != nil {
		return nil, fmt.Errorf("invalid original PostgreSQL address space source")
	}
	result := make([]string, 0, len(original)+1)
	seen := make(map[string]struct{}, len(original))
	bytes := 0
	for _, entry := range original {
		if len(entry) >= maxPostgresEnvironmentBytes-bytes {
			return nil, fmt.Errorf("PostgreSQL environment exceeds fixed byte bound")
		}
		bytes += len(entry) + 1
		key, _, exists := strings.Cut(entry, "=")
		if !exists || !postgresEnvironmentKey(key) || strings.IndexByte(entry, 0) >= 0 {
			return nil, fmt.Errorf("invalid PostgreSQL environment entry")
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("ambiguous PostgreSQL environment entries")
		}
		seen[key] = struct{}{}
		if strings.HasPrefix(key, "LD_") || strings.HasPrefix(key, "NEON_DSGA_") ||
			strings.HasPrefix(key, "MALLOC_") {
			continue
		}
		switch key {
		case "GLIBC_TUNABLES", "GCONV_PATH", "LOCPATH", "NLSPATH", "GETCONF_DIR":
			continue
		}
		result = append(result, entry)
	}
	return addressSpaceEnvironment(result, limit), nil
}

func postgresEnvironmentKey(key string) bool {
	if key == "" {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}
