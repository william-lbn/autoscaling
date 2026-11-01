package resourceguard

import (
	"encoding/json"
	"fmt"
	"strings"
)

const normalSystemLifecycleScope = "original-system-lifecycle-source-component-v1"

type normalSystemLifecycleBinding struct {
	RootConsumer          nativeBirthConsumer `json:"root_consumer"`
	RuntimeDocumentSHA256 string              `json:"runtime_document_sha256"`
	NativePolicySHA256    string              `json:"native_policy_sha256"`
	NativeBuildSHA256     string              `json:"native_build_sha256"`
	SystemManifestSHA256  string              `json:"system_manifest_sha256"`
	GuestBootID           string              `json:"guest_boot_id"`
	VMUID                 string              `json:"vm_uid"`
	ComputeInstanceID     string              `json:"compute_instance_id"`
	SystemDevice          uint64              `json:"system_device"`
	SystemInode           uint64              `json:"system_inode"`
	SystemMemoryMaxBytes  uint64              `json:"system_memory_max_bytes"`
}

type normalSystemLifecycleELF struct {
	Path        string   `json:"path"`
	SHA256      string   `json:"sha256"`
	Device      uint64   `json:"device"`
	Inode       uint64   `json:"inode"`
	Interpreter string   `json:"interpreter"`
	Needed      []string `json:"needed"`
}

type normalSystemLifecycleObservation struct {
	Scope                            string                       `json:"scope"`
	Binding                          normalSystemLifecycleBinding `json:"binding"`
	OriginalMetadataBytesBound       bool                         `json:"original_metadata_bytes_bound"`
	ConditionalELFClosureVerified    bool                         `json:"conditional_elf_closure_verified"`
	ScriptInterpreterClosureVerified bool                         `json:"script_interpreter_closure_verified"`
	ConfigInventoryVerified          bool                         `json:"config_inventory_verified"`
	ELFs                             []normalSystemLifecycleELF   `json:"elfs"`
	ContinuousWritersClosed          bool                         `json:"continuous_writers_closed"`
	NormalDecreaseAuthorized         bool                         `json:"normal_decrease_authorized"`
	MissingSources                   []string                     `json:"missing_sources"`
}

type normalSystemRoleObservation struct {
	Role                         string                       `json:"role"`
	PID                          int                          `json:"pid"`
	StartTicks                   uint64                       `json:"start_ticks"`
	UID                          uint32                       `json:"uid"`
	GID                          uint32                       `json:"gid"`
	ExecutableSHA256             string                       `json:"executable_sha256"`
	ArgvSHA256                   string                       `json:"argv_sha256"`
	EnvironmentSHA256            string                       `json:"environment_sha256"`
	PIDFDDevice                  uint64                       `json:"pidfd_device"`
	PIDFDInode                   uint64                       `json:"pidfd_inode"`
	Binding                      normalSystemLifecycleBinding `json:"binding"`
	OriginalTaskObserved         bool                         `json:"original_task_observed"`
	FixedArgvEnvironmentObserved bool                         `json:"fixed_argv_environment_observed"`
	ContinuousWritersClosed      bool                         `json:"continuous_writers_closed"`
	NormalDecreaseAuthorized     bool                         `json:"normal_decrease_authorized"`
}

// 原始向量只用于本进程内比对，结果只输出JSON向量摘要，禁止在错误中带值。
func normalSystemVectorSHA(values []string) (string, error) {
	if values == nil || len(values) > 128 {
		return "", fmt.Errorf("fixed vector missing or exceeds bound")
	}
	total := 0
	for _, v := range values {
		total += len(v) + 1
		if len(v) > 4096 || strings.ContainsAny(v, "\x00\r\n") || total > 64<<10 {
			return "", fmt.Errorf("fixed vector exceeds canonical bound")
		}
	}
	b, e := json.Marshal(values)
	if e != nil {
		return "", e
	}
	return physicalDigest(b), nil
}
func normalSystemCleanEnvironment(env []string) error {
	seen := map[string]bool{}
	for _, v := range env {
		name, _, ok := strings.Cut(v, "=")
		if !ok || name == "" || seen[name] || strings.HasPrefix(name, "LD_") || name == "GLIBC_TUNABLES" || name == "MUSL_LOCPATH" || name == "ENV" || name == "BASH_ENV" || name == "IFS" || name == "SHELLOPTS" {
			return fmt.Errorf("fixed environment has unsupported control or duplicate")
		}
		for i, c := range name {
			if !(c >= 'A' && c <= 'Z' || c == '_' || i > 0 && c >= '0' && c <= '9') {
				return fmt.Errorf("fixed environment name unsupported")
			}
		}
		seen[name] = true
	}
	_, e := normalSystemVectorSHA(env)
	return e
}

// 只选择既有v2固定执行角色。Compute原向量由manifest两个SHA约束，不从请求
// 提供任意程序。其余角色只接受新launcher将实际采用的最小env-i；旧继承环境
// 不会被采样后反过来认成expected。短命角色的实际exec仍需后续出生hook。
func normalSystemFixedRoleArgv(role string) ([]string, error) {
	return normalSystemFixedRoleArgvForVersion(2, role)
}

// payload保持旧v2逐字节相同；显式版本只选择已有表，拒未知版本/任意程序。
func normalSystemFixedRoleArgvForVersion(version uint32, role string) ([]string, error) {
	if version != 2 && version != 3 {
		return nil, fmt.Errorf("unsupported fixed System role version")
	}
	roles := map[string][]string{
		"allow-start":    {"/neonvm/bin/busybox", "touch", "/run/neon-dsga/vmstart.allowed"},
		"kernel-version": {"/neonvm/bin/busybox", "cat", "/proc/version"},
		"network":        {"/neonvm/bin/udhcpc", "-t", "1", "-T", "1", "-A", "1", "-f", "-i", "eth0", "-O", "121", "-O", "119", "-s", "/neonvm/bin/udhcpc.script"},
		"udev":           {"/neonvm/bin/udevd"}, "udev-init": {"/neonvm/bin/sh", "/neonvm/bin/udev-init.sh"},
		"acpi":     {"/neonvm/bin/acpid", "-f", "-c", "/neonvm/acpi"},
		"logs":     {"/neonvm/bin/vector", "-c", "/neonvm/config/vector.yaml", "--config-dir", "/etc/vector", "--color", "never"},
		"clock":    {"/neonvm/bin/chronyd", "-n", "-f", "/neonvm/config/chrony.conf", "-l", "/var/log/chrony/chrony.log"},
		"daemon":   {"/neonvm/bin/neonvmd", "--addr=0.0.0.0:25183"},
		"resource": {"/neonvm/bin/dsga-resource-guard", "serve"},
	}
	value, ok := roles[role]
	if !ok {
		return nil, fmt.Errorf("unsupported fixed System role")
	}
	return append([]string(nil), value...), nil
}
