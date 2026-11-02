package resourceguard

import "fmt"

// nativeBirthABI只选择本fork已经实现的原生布局，不是上层能力声明或运行许可。
// 17保留原policy/WAL字节；18仍须原ISO、不可变ELF、四数组停点及全部过滤器
// 核验。相同sizeof不意味着可以用17的出生/安装回执替代18的实际来源。
type nativeBirthABI struct {
	pageBytes          uint32
	elementBytes       [4]uint64
	memoryControlScope string
}

func nativeBirthABIForPG(major uint32) (nativeBirthABI, error) {
	switch major {
	case 17:
		return nativeBirthABI{4096, [4]uint64{64, 8192, 16, 20}, normalPGMemoryControlScope}, nil
	case 18:
		// PG18 27b354的buf_internals/condition_variable布局；真实C审计器还会
		// 从原ELF自身sizeof重算每个range并回显，不能由JSON或大版本号补齐。
		return nativeBirthABI{4096, [4]uint64{64, 8192, 16, 20}, "original-pg18-memory-syscall-controls-v1-non-authorizing"}, nil
	default:
		return nativeBirthABI{}, fmt.Errorf("unsupported original native PG major ABI")
	}
}
