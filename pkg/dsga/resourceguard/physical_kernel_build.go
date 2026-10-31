package resourceguard

import "fmt"

// NormalV1PhysicalKernelProfileID 是首个受限 normal 真降限候选内核的独立构建身份。
// 常量本身不授予 Origin/预归组/PG 出生或预算资格；lookup 只给已核构建参考。
// 旧 component profile 不可通过改这个字符串升级成新 VM 的原启动历史。
const NormalV1PhysicalKernelProfileID = "pg17-linux-6.12.26-normal-v1"

// NormalV1PIDCappedPhysicalKernelProfileID 独立于原552能力组件，不移动旧五元组。
// 只有新准确构建、原early_param和真实只读hard事实共同成立，才有J的原硬来源。
// 它仍不是完整I、FS包络或normal降限许可。
const NormalV1PIDCappedPhysicalKernelProfileID = "pg17-linux-6.12.26-normal-v1-pid-capped"

// NormalSplitV1PhysicalKernelProfileID 版本中性，保持三个历史构建条目不变。
// 新0007仅保护两处完整ancestor取消段；并非完整force/I或normal降限授权。
const NormalSplitV1PhysicalKernelProfileID = "neon-linux-6.12.26-dsga-normal-split-v1"

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
// 所有条目均保留 component-only 含义，不新增 VM 的降限能力。normal-v1 精确收据为
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
	case NormalV1PIDCappedPhysicalKernelProfileID:
		// 新6patch构建及17项真实KVM含有效权限set_tid/namespace与非法early参数，
		// 见pg17-pid-capped-kernel-20261030T175217Z。resolved config是实际文件。
		return PhysicalKernelBuildReference{
			ProfileID:            profileID,
			KernelRelease:        "6.12.26-william-normal-v1-pidcap-946be0d25506",
			KernelArtifactSHA256: "3b5853b552a8adbe5de7772410c793928f0d4a86a2410d24aec806fecfea2608",
			KernelConfigSHA256:   "7733d4e430a2102052dd97c8b3fa64ac2b5904fa6a4f394b5653b9fa5cccbf25",
			KernelNotesSHA256:    "cbf448079077b712e5812d4c9c54a9ea3b4e3aacc76892699881c8aad57619dc",
		}, nil
	case NormalSplitV1PhysicalKernelProfileID:
		// 同一实际compiled layer核7patch、config和bzImage；Guest notes逐字节匹配。
		// 收据normal-split-kernel-20261030T224843Z/kernel-source-lock.json：
		// SHA256 49a8a0ea7e342547256340e3e10731d1198bfc35d84662583597e69f32c9b791。
		return PhysicalKernelBuildReference{
			ProfileID:            profileID,
			KernelRelease:        "6.12.26-william-normal-split-v1-e210f5e3ee94",
			KernelArtifactSHA256: "3693200bb10997988129955750382e586d13eef75e06f46aa3b9f56c6f8833f0",
			KernelConfigSHA256:   "11344757b0f979c7fee35e354bd706616c4283deaa67cc30c50747e4bca855be",
			KernelNotesSHA256:    "7cf45ced24c800ec8390fc7f04f07b8dc9e73ac0efa4bf8abd11d8513c8eb828",
		}, nil
	default:
		return PhysicalKernelBuildReference{}, fmt.Errorf("physical kernel build profile is not recognized")
	}
}

// StrictPageCounterIRQBoundary 只回答独立已核构建的编译属性，不接请求bool/scalar。
// 必须整个五元组逐值等于固定369构建：只复制profileID或混用3b config不会得到能力。
// compiled CONFIG_NEON_DSGA_STRICT_PAGE_COUNTER=y及实际0007源码绑定上述收据，
// 但本方法不证明该内核在Guest运行、原出生、force/I/FS闭合或任何降限资格；
// 后继消费者仍须真实notes、原Origin/Source及持续约束联合核验。
func (ref PhysicalKernelBuildReference) StrictPageCounterIRQBoundary() bool {
	expected, err := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	return err == nil && ref == expected
}

// 两个新准确build具有原early_param硬PID来源；旧552虽可晚写pid_max，不能补签。
func physicalKernelHasPIDHardSource(profile string) bool {
	return profile == NormalV1PIDCappedPhysicalKernelProfileID || profile == NormalSplitV1PhysicalKernelProfileID
}
