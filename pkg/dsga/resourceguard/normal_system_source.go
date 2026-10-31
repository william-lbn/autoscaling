package resourceguard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

const NormalSystemSourceProfileID = "neon-dsga-normal-system-source-v1"
const NormalSystemSourceManifestPath = "/neonvm/config/dsga-normal-system-source.json"
const normalSystemSourceScope = "fixed-system-startup-source-component-only"

// 新profile只固定受管System入口；不影响旧v1，也不把raw参数自称受信原来源。
// ImageSHA和argv/env摘要仍需原ISO/镜像/进程链绑定，不能仅凭格式正确发normal票据。
// 文件对象可用已固定alias，但CanonicalPath必须指向真正regular文件（如busybox）。
type NormalSystemSourceFile struct {
	Path          string `json:"path"`
	CanonicalPath string `json:"canonical_path"`
	Kind          string `json:"kind"`
	Bytes         uint64 `json:"bytes"`
	SHA256        string `json:"sha256"`
}
type NormalSystemSourceManifest struct {
	ProtocolVersion          uint32                       `json:"protocol_version"`
	ProfileID                string                       `json:"profile_id"`
	Kernel                   PhysicalKernelBuildReference `json:"kernel"`
	DeclaredImageSHA256      string                       `json:"declared_image_sha256"`
	ComputePath              string                       `json:"compute_path"`
	ComputeArgvSHA256        string                       `json:"compute_argv_sha256"`
	ComputeEnvironmentSHA256 string                       `json:"compute_environment_sha256"`
	Files                    []NormalSystemSourceFile     `json:"files"`
}
type NormalSystemSourceFileObservation struct {
	Path          string `json:"path"`
	CanonicalPath string `json:"canonical_path"`
	SHA256        string `json:"sha256"`
	Device        uint64 `json:"device"`
	Inode         uint64 `json:"inode"`
	Bytes         uint64 `json:"bytes"`
}
type NormalSystemSourceObservation struct {
	ConfigInventorySHA256        string                              `json:"config_inventory_sha256"`
	ConfigInventoryObserved      bool                                `json:"config_inventory_observed"`
	Scope                        string                              `json:"scope"`
	ManifestSHA256               string                              `json:"manifest_sha256"`
	ManifestCanonicalSHA256      string                              `json:"manifest_canonical_sha256"`
	GuestBootID                  string                              `json:"guest_boot_id"`
	Kernel                       PhysicalKernelBuildReference        `json:"kernel"`
	InittabSHA256                string                              `json:"inittab_sha256"`
	Files                        []NormalSystemSourceFileObservation `json:"files"`
	RuntimeOverridesAbsent       bool                                `json:"runtime_overrides_absent"`
	FixedInittabObserved         bool                                `json:"fixed_inittab_observed"`
	ReadonlyAncestorsObserved    bool                                `json:"readonly_ancestors_observed"`
	OriginalSourceAuthenticated  bool                                `json:"original_source_authenticated"`
	ELFDependencyClosureVerified bool                                `json:"elf_dependency_closure_verified"`
	FixedArgvEnvironmentExecuted bool                                `json:"fixed_argv_environment_executed"`
	AllSourcesClosed             bool                                `json:"all_sources_closed"`
	NormalDecreaseAuthorized     bool                                `json:"normal_decrease_authorized"`
	MissingSources               []string                            `json:"missing_sources"`
}

// 返回副本供最终Builder使用；无自定义条目、sshd、agetty、login或root交互。
// applet alias仍须在manifest中解析为真正busybox regular对象，不能把symlink当ELF。
func NormalSystemSourceInitEntries() []string {
	return []string{
		"::sysinit:/neonvm/bin/vminit",
		"::once:/neonvm/bin/touch /neonvm/vmstart.allowed",
		"::once:/neonvm/bin/cat /proc/version",
		"::respawn:/neonvm/bin/udhcpc -t 1 -T 1 -A 1 -f -i eth0 -O 121 -O 119 -s /neonvm/bin/udhcpc.script",
		"::respawn:/neonvm/bin/udevd",
		"::wait:/neonvm/bin/udev-init.sh",
		"::respawn:/neonvm/bin/acpid -f -c /neonvm/acpi",
		"::respawn:/neonvm/bin/vector -c /neonvm/config/vector.yaml --config-dir /etc/vector --color never",
		"::respawn:/neonvm/bin/chronyd -n -f /neonvm/config/chrony.conf -l /var/log/chrony/chrony.log",
		"::respawn:/neonvm/bin/neonvmd --addr=0.0.0.0:25183",
		"::respawn:/neonvm/bin/sh /neonvm/bin/dsga-resource-service",
		"::respawn:/neonvm/bin/vmstart",
		"::shutdown:/neonvm/bin/vmshutdown",
	}
}
func normalSystemRequiredPaths() map[string]string {
	return map[string]string{
		"/etc/inittab": "config", "/neonvm/bin/busybox": "exec",
		"/neonvm/bin/vminit": "script", "/neonvm/bin/vmstart": "script", "/neonvm/bin/dsga-resource-service": "script", "/neonvm/bin/vmshutdown": "script", "/neonvm/bin/udev-init.sh": "script", "/neonvm/bin/udhcpc.script": "script",
		"/neonvm/bin/touch": "exec", "/neonvm/bin/cat": "exec", "/neonvm/bin/sh": "exec", "/neonvm/bin/udhcpc": "exec", "/neonvm/bin/udevd": "exec", "/neonvm/bin/acpid": "exec", "/neonvm/bin/vector": "exec", "/neonvm/bin/chronyd": "exec", "/neonvm/bin/neonvmd": "exec", "/neonvm/bin/dsga-resource-guard": "exec", "/usr/local/bin/compute_ctl": "exec",
		"/neonvm/config/vector.yaml": "config", "/neonvm/config/chrony.conf": "config",
	}
}
func normalSystemCleanPath(p string) bool {
	return p != "" && len(p) <= 4096 && filepath.IsAbs(p) && filepath.Clean(p) == p && !strings.ContainsAny(p, "\x00\r\n") && p != "/"
}
func normalSystemAllowedFile(p, kind string) bool {
	if !normalSystemCleanPath(p) {
		return false
	}
	if k, ok := normalSystemRequiredPaths()[p]; ok {
		return k == kind
	}
	if kind == "loader" {
		for _, prefix := range []string{"/lib/", "/lib64/", "/usr/lib/", "/usr/lib64/", "/neonvm/lib/"} {
			if strings.HasPrefix(p, prefix) {
				return true
			}
		}
		return p == "/etc/ld.so.cache"
	}
	if kind == "config" {
		for _, prefix := range []string{"/neonvm/config/", "/neonvm/acpi/", "/etc/vector/", "/lib/udev/rules.d/", "/etc/udev/rules.d/"} {
			if strings.HasPrefix(p, prefix) {
				return true
			}
		}
	}
	return false
}
func (m NormalSystemSourceManifest) Validate() error {
	ref, e := LookupPhysicalKernelBuild(NormalSplitV1PhysicalKernelProfileID)
	if e != nil || m.ProtocolVersion != 1 || m.ProfileID != NormalSystemSourceProfileID || m.Kernel != ref || m.ComputePath != "/usr/local/bin/compute_ctl" {
		return fmt.Errorf("system source requires versioned fixed normal profile")
	}
	for _, d := range []string{m.DeclaredImageSHA256, m.ComputeArgvSHA256, m.ComputeEnvironmentSHA256} {
		if !physicalDigestValid(d) {
			return fmt.Errorf("system source digest absent")
		}
	}
	if len(m.Files) < len(normalSystemRequiredPaths()) || len(m.Files) > 256 {
		return fmt.Errorf("system source file inventory outside bound")
	}
	seen := map[string]bool{}
	var total uint64
	prev := ""
	for _, f := range m.Files {
		if !normalSystemAllowedFile(f.Path, f.Kind) || !normalSystemCleanPath(f.CanonicalPath) || !physicalDigestValid(f.SHA256) || f.Bytes == 0 || f.Bytes > 256<<20 || f.Path <= prev || seen[f.Path] {
			return fmt.Errorf("system source file is invalid/duplicate/unordered")
		}
		if f.Kind == "exec" {
			if f.CanonicalPath != f.Path && (f.CanonicalPath != "/neonvm/bin/busybox" || !normalSystemBusyboxApplet(f.Path)) {
				return fmt.Errorf("system executable alias is not fixed busybox")
			}
		}
		if f.Kind == "script" && f.CanonicalPath != f.Path {
			return fmt.Errorf("system script alias is unsupported")
		}
		if f.Kind == "loader" && !normalSystemAllowedFile(f.CanonicalPath, "loader") {
			return fmt.Errorf("system loader canonical path is unsupported")
		}
		total += f.Bytes
		if total > 512<<20 {
			return fmt.Errorf("system source aggregate file inventory exceeds bound")
		}
		seen[f.Path] = true
		prev = f.Path
	}
	for p := range normalSystemRequiredPaths() {
		if !seen[p] {
			return fmt.Errorf("system source required file absent: %s", p)
		}
	}
	return nil
}

// 限长/限深度并拒重复key；普通DisallowUnknownFields本身不会拒重复key。
func DecodeNormalSystemSourceManifest(raw []byte) (NormalSystemSourceManifest, error) {
	var m NormalSystemSourceManifest
	if len(raw) == 0 || len(raw) > 1<<20 {
		return m, fmt.Errorf("system manifest outside bound")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 32 {
			return fmt.Errorf("system manifest depth exceeds bound")
		}
		v, e := d.Token()
		if e != nil {
			return e
		}
		delim, ok := v.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return fmt.Errorf("system manifest duplicate key")
				}
				seen[s] = true
				if e = walk(depth + 1); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		case '[':
			for d.More() {
				if e = walk(depth + 1); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		default:
			return fmt.Errorf("system manifest delimiter invalid")
		}
	}
	if e := walk(0); e != nil {
		return m, e
	}
	if _, e := d.Token(); e != io.EOF {
		return m, fmt.Errorf("system manifest trailing data")
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e := d.Decode(&m); e != nil {
		return m, e
	}
	return m, m.Validate()
}
func normalSystemCheckInittab(raw []byte) error {
	if len(raw) == 0 || len(raw) > 64<<10 || bytes.ContainsRune(raw, '\r') {
		return fmt.Errorf("system inittab outside fixed format")
	}
	var entries []string
	for _, line := range strings.Split(string(raw), "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "#") {
			continue
		}
		if trim != line {
			return fmt.Errorf("system inittab entry has hidden whitespace")
		}
		entries = append(entries, line)
	}
	want := NormalSystemSourceInitEntries()
	if len(entries) != len(want) {
		return fmt.Errorf("system inittab contains missing/custom/interactive entry")
	}
	for i := range want {
		if entries[i] != want[i] {
			return fmt.Errorf("system inittab entry %d is not the fixed managed service", i)
		}
	}
	return nil
}

// 这些程序读取glob目录，必须比对完整库存，不能只检查manifest选列的文件。
func NormalSystemSourceConfigDirectories() []string {
	return []string{"/neonvm/acpi", "/etc/vector", "/lib/udev/rules.d", "/etc/udev/rules.d"}
}

func normalSystemBusyboxApplet(p string) bool {
	switch p {
	case "/neonvm/bin/touch", "/neonvm/bin/cat", "/neonvm/bin/sh", "/neonvm/bin/udhcpc", "/neonvm/bin/acpid":
		return true
	}
	return false
}
