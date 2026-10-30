package resourceguard

import "fmt"

// NormalV1PhysicalKernelProfileID 是首个受限 normal 真降限候选内核的独立构建身份。
// 常量本身不授予 Origin/预归组/PG 出生或预算资格；lookup 只给已核构建参考。
// 旧 component profile 不可通过改这个字符串升级成新 VM 的原启动历史。
const NormalV1PhysicalKernelProfileID = "pg17-linux-6.12.26-normal-v1"

// PhysicalKernelBuildReference 只引用独立审阅的构建收据，不是 Guest 测量证明。
// 所有返回值均为只读值副本；没有请求可注册的全局 map 或任意 release 前缀入口。
// KernelNotesSHA256 还须实际从原 Guest 内核读回；FS 规则/UMH 原启动和全部成本另验。
type PhysicalKernelBuildReference struct {
	ProfileID            string `json:"profile_id"`
	KernelRelease        string `json:"kernel_release"`
	KernelArtifactSHA256 string `json:"kernel_artifact_sha256"`
	KernelConfigSHA256   string `json:"kernel_config_sha256"`
	KernelNotesSHA256    string `json:"kernel_notes_sha256"`
}

// LookupPhysicalKernelBuild 仅返回本程序编译时已冻结的准确五元组。
// 两条目均保留 component-only 含义，不新增 VM 的降限能力。normal-v1 精确收据为
// pg17-normal-v1-kernel-20261030T165552Z/runtime-proof.json，旧/新真实 KVM UMH
// 正负控制及 Landlock 能力已核。新 config SHA 是实际 resolved .config；其输入 B20
// 到 resolved935 的无策略变更链另见构建清单，不把两个不同文件声称为相同 hash。
func LookupPhysicalKernelBuild(profileID string) (PhysicalKernelBuildReference, error) {
	switch profileID {
	case "pg17-linux-6.12.26-physical-component-v1":
		return PhysicalKernelBuildReference{
			ProfileID:            profileID,
			KernelRelease:        physicalKernelRelease,
			KernelArtifactSHA256: "0ab3f71456c783452a49d50752a9b1945c2da399588febed409637d7af6891a0",
			KernelConfigSHA256:   "639ea751eb4ae7743e2695368286d1aeb0417f6680d6ac24ae3b051948a94b96",
			KernelNotesSHA256:    "b52a6321c44be2b8d138e9ca990ec86413c3744a189a118ed69aa295cb8ef03d",
		}, nil
	case NormalV1PhysicalKernelProfileID:
		return PhysicalKernelBuildReference{
			ProfileID:            profileID,
			KernelRelease:        "6.12.26-william-normal-v1-b20ac4f85d74",
			KernelArtifactSHA256: "552ffabd096736e38d2d4b6c119533e4a26c2b9a47f6e04a8e1795a084f8d85d",
			KernelConfigSHA256:   "935e43773879d6a4f5c27c2ef966314de7e0e463a4a9bebefa3dc5dcf283ce5b",
			KernelNotesSHA256:    "3d22f15739bcd10735099d344c1c306f6bd7eccfbed8b3e4f1295e5893036d6b",
		}, nil
	default:
		return PhysicalKernelBuildReference{}, fmt.Errorf("physical kernel build profile is not recognized")
	}
}
