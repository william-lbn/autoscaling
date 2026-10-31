package resourceguard

import "fmt"

// RuntimeISOPreSystemAssetName 是ISO上的固定物理文件名。逻辑模板键仍为
// dsga-prebootstrap-system.json，原RuntimeDocument及其SHA不能因磁盘命名而改变。
// 当前ISO writer的30字节标识包含“;1”，长逻辑名会被截断，故必须显式映射。
const RuntimeISOPreSystemAssetName = "dsga-pre-system.json"

// RuntimeISOAssetName 只将六个已知逻辑资产键映射为固定ISO物理文件名。
// 不能接受路径、规范化别名或把短物理名再次当逻辑输入；不提供通用文件名裁剪，
// 避免未知资产经截断与现有文件碰撞。它不修改JSON内容或生成任何启动/降限资格。
func RuntimeISOAssetName(logical string) (string, error) {
	switch logical {
	case RuntimeResourceAssetName, "dsga-physical-kernel.json", "dsga-kernel-bootstrap.json",
		"dsga-normal-split.json", "dsga-native-birth.json":
		return logical, nil
	case "dsga-prebootstrap-system.json":
		return RuntimeISOPreSystemAssetName, nil
	default:
		return "", fmt.Errorf("unknown logical runtime ISO asset %q", logical)
	}
}
