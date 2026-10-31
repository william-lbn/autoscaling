package resourceguard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// RuntimeBootRequirements 是内部版本化的静态启动计划，不是HTTP请求或资源账本。
// 字段私有，调用者不能把BootUpper改成观察值或修改必要参数；每次都从原准确文档
// 与完整有效资产派生。它不证明QMP base、Guest实际内核/阶段或normal许可已成立。
type RuntimeBootRequirements struct {
	version    uint32
	profile    string
	runtimeSHA string
	sourceSHA  string
	secretUID  string
	bootUpper  uint64
	pidLimit   uint32
	build      PhysicalKernelBuildReference
	args       []string
}

func (r RuntimeBootRequirements) ProtocolVersion() uint32                            { return r.version }
func (r RuntimeBootRequirements) Enabled() bool                                      { return r.profile != "" }
func (r RuntimeBootRequirements) ProfileID() string                                  { return r.profile }
func (r RuntimeBootRequirements) RuntimeDocumentSHA256() string                      { return r.runtimeSHA }
func (r RuntimeBootRequirements) ResourceSourceSHA256() string                       { return r.sourceSHA }
func (r RuntimeBootRequirements) ConfigSecretUID() string                            { return r.secretUID }
func (r RuntimeBootRequirements) BootMemoryUpperBoundBytes() uint64                  { return r.bootUpper }
func (r RuntimeBootRequirements) PIDMaxLimit() uint32                                { return r.pidLimit }
func (r RuntimeBootRequirements) KernelBuildReference() PhysicalKernelBuildReference { return r.build }
func (r RuntimeBootRequirements) RequiredKernelArgs() []string {
	return append([]string(nil), r.args...)
}

// DeriveRuntimeBootRequirements 只接受逻辑资产；resource也必须是同份原raw。
// 重新渲染并逐字节核整组，避免内部调用者漏掉/替换某阶段后静默退旧模式。
// v1保持旧启动选择，不发新profile参数；v2只支持准确369的完整五阶段前缀。
func DeriveRuntimeBootRequirements(raw []byte, assets map[string][]byte) (RuntimeBootRequirements, error) {
	var zero RuntimeBootRequirements
	if len(raw) == 0 || len(raw) > 64<<10 {
		return zero, fmt.Errorf("runtime boot source missing or oversized")
	}
	var d RuntimeDocument
	if err := json.Unmarshal(raw, &d); err != nil {
		return zero, err
	}
	source, err := d.Source()
	if err != nil {
		return zero, err
	}
	expected, err := RenderRuntimePolicyAssets(raw)
	if err != nil {
		return zero, err
	}
	expected[RuntimeResourceAssetName] = raw
	if len(expected) != len(assets) {
		return zero, fmt.Errorf("runtime boot assets differ from original document")
	}
	for name, want := range expected {
		got, ok := assets[name]
		if !ok || !bytes.Equal(got, want) {
			return zero, fmt.Errorf("runtime boot asset %q differs", name)
		}
	}
	next := RuntimeBootRequirements{version: 1, runtimeSHA: runtimePolicyDigest(raw), sourceSHA: d.SourceSHA256, secretUID: d.ConfigSecretUID}
	if d.ProtocolVersion == 1 {
		return next, nil
	}
	return deriveRuntimeBootRequirementsPlatform(next, d, source, expected)
}

// ValidateKernelCommandLine 必须在Runner拼好最终-append后、启动QEMU前调用。
// 新profile不能与旧auto-movable字符串叠加，追加参数也不能重复/别名覆盖原参数。
// 不验证QMP实际base，也不把命令行通过当内核原来源或任何预算阶段完成。
func (r RuntimeBootRequirements) ValidateKernelCommandLine(cmdline string) error {
	if r.version != 1 {
		return fmt.Errorf("unknown runtime boot requirements version")
	}
	if strings.IndexByte(cmdline, 0) >= 0 {
		return fmt.Errorf("NUL in kernel command line")
	}
	if !r.Enabled() {
		for _, token := range strings.Fields(cmdline) {
			key, _, _ := strings.Cut(token, "=")
			key = strings.ReplaceAll(key, "-", "_")
			if strings.HasPrefix(key, "neonvm.dsga.") || key == "neonvm.pid_max_limit" {
				return fmt.Errorf("new profile marker on runtime v1")
			}
		}
		return nil
	}
	if r.profile != NormalSplitV1PhysicalKernelProfileID || !r.build.StrictPageCounterIRQBoundary() || len(r.args) != 8 || r.bootUpper == 0 || r.pidLimit == 0 {
		return fmt.Errorf("incomplete runtime boot requirements")
	}
	// 准确6.12.26 x86 arch/x86/include/asm/setup.h的COMMAND_LINE_SIZE=2048，
	// 最后一字节留NUL。不能验证整段后让内核截断后半部必要原参数。
	if len(cmdline) > 2047 {
		return fmt.Errorf("normal-split kernel command line would truncate")
	}
	for _, c := range []byte(cmdline) {
		if c < 0x20 || c >= 0x7f || c == '\'' || c == '"' || c == '\\' {
			return fmt.Errorf("ambiguous normal-split kernel command line syntax")
		}
	}
	want := make(map[string]string, len(r.args))
	for _, arg := range r.args {
		key, _, _ := strings.Cut(arg, "=")
		want[key] = arg
	}
	seen := map[string]bool{}
	for _, token := range strings.Fields(cmdline) {
		if token == "--" {
			return fmt.Errorf("kernel argument separator hides required early parameters")
		}
		key, _, _ := strings.Cut(token, "=")
		canonical := strings.ReplaceAll(key, "-", "_")
		if required, ok := want[canonical]; ok {
			if key != canonical || token != required || seen[canonical] {
				return fmt.Errorf("conflicting original kernel argument %q", key)
			}
			seen[canonical] = true
			continue
		}
		if strings.HasPrefix(canonical, "neonvm.dsga.") || strings.HasPrefix(canonical, "memory_hotplug.") || canonical == "lsm" || canonical == "security" || canonical == "movablecore" || canonical == "movable_node" {
			return fmt.Errorf("unsupported normal-split boot override %q", key)
		}
	}
	if len(seen) != len(want) {
		return fmt.Errorf("required normal-split kernel argument missing")
	}
	return nil
}
