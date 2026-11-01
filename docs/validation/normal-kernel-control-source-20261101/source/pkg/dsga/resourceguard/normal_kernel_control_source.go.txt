package resourceguard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

const normalKernelControlManifestPath = "/neonvm/config/dsga-kernel-control-source.json"

// Builder输出的是静态非授权能力模板；运行期必须被原RO System库存准确绑定。
const NormalKernelControlSourceManifestPath = normalKernelControlManifestPath

func NormalKernelControlSourceManifestJSON() ([]byte, error) {
	return json.Marshal(normalKernelControlExpectedManifest())
}

const normalKernelControlProfile = "neon-normal-ordinary-kernel-controls-v1"
const normalKernelControlScope = "fixed-ordinary-role-kernel-control-source-v1"

// ABI6的FS权利逐位固定；规则没有网络/CPU/存储调优授权。禁止任何/dev整树读写。
const (
	nkExecute uint64 = 1 << iota
	nkWrite
	nkRead
	nkReadDir
	nkRemoveDir
	nkRemoveFile
	nkMakeChar
	nkMakeDir
	nkMakeReg
	nkMakeSock
	nkMakeFIFO
	nkMakeBlock
	nkMakeSym
	nkRefer
	nkTruncate
	nkIoctlDev
)
const nkHandled = (1 << 16) - 1
const nkReadTree = nkRead | nkReadDir | nkExecute
const nkWriteTree = nkWrite | nkRead | nkReadDir | nkRemoveDir | nkRemoveFile | nkMakeDir | nkMakeReg | nkMakeSock | nkMakeFIFO | nkMakeSym | nkRefer | nkTruncate

type normalKernelControlPathRule struct {
	Path     string `json:"path"`
	Rights   uint64 `json:"rights"`
	Kind     string `json:"kind"`
	Optional bool   `json:"optional"`
}
type normalKernelControlRoleRule struct {
	Role              string                        `json:"role"`
	ArgvSHA256        string                        `json:"argv_sha256"`
	EnvironmentSHA256 string                        `json:"environment_sha256"`
	KeepCapabilities  uint64                        `json:"keep_capabilities"`
	Paths             []normalKernelControlPathRule `json:"paths"`
}
type normalKernelControlManifest struct {
	ProtocolVersion uint32                        `json:"protocol_version"`
	Scope           string                        `json:"scope"`
	ProfileID       string                        `json:"profile_id"`
	Roles           []normalKernelControlRoleRule `json:"roles"`
}

// 原System manifest列入此静态文件即可选新能力；本文件不反向引用System SHA或
// 动态Runtime/SecretUID，避免镜像库存自引用。旧manifest没有此文件，不能隐式启用。
func normalKernelControlExpectedManifest() normalKernelControlManifest {
	roles := []string{"acpi", "allow-start", "clock", "daemon", "kernel-version", "logs", "network", "udev", "udev-init"}
	m := normalKernelControlManifest{ProtocolVersion: 1, Scope: normalKernelControlScope, ProfileID: normalKernelControlProfile}
	for _, role := range roles {
		argv, e := normalSystemFixedRoleArgv(role)
		if e != nil {
			panic("fixed role table inconsistent")
		}
		ah, _ := normalSystemVectorSHA(argv)
		eh, _ := normalSystemVectorSHA([]string{"PATH=/neonvm/bin"})
		q := normalKernelControlRoleRule{Role: role, ArgvSHA256: ah, EnvironmentSHA256: eh}
		// 只保各既有服务必要cap；没有SYS_ADMIN/NICE/RESOURCE/PTRACE/RAWIO/BPF等。
		switch role {
		case "network":
			q.KeepCapabilities = (1 << 12) | (1 << 13) // NET_ADMIN/NET_RAW
		case "clock":
			q.KeepCapabilities = 1 << 25 // SYS_TIME
		case "udev", "udev-init":
			q.KeepCapabilities = 1 << 27 // MKNOD；Landlock仍限创建与设备打开
		case "acpi":
			q.KeepCapabilities = 1 << 22 // SYS_BOOT仅既有poweroff来源
		}
		q.Paths = append(q.Paths, normalKernelControlPathRule{"/", nkReadDir, "dir", false})
		for _, p := range []string{"/etc", "/neonvm", "/lib", "/usr", "/proc", "/sys", "/var/log", "/var/lib/chrony"} {
			q.Paths = append(q.Paths, normalKernelControlPathRule{p, nkReadTree, "dir", false})
		}
		// glibc路径可不存在；存在时canonical对象仍固定，不能动态选择另一棵可写树。
		q.Paths = append(q.Paths, normalKernelControlPathRule{"/lib64", nkReadTree, "dir", true})
		for _, p := range []string{"/run", "/tmp"} {
			q.Paths = append(q.Paths, normalKernelControlPathRule{p, nkWriteTree, "dir", false})
		}
		for _, p := range []string{"/dev/null", "/dev/zero", "/dev/random", "/dev/urandom", "/dev/console"} {
			q.Paths = append(q.Paths, normalKernelControlPathRule{p, nkRead | nkWrite, "char", false})
		}
		switch role {
		case "network":
			q.Paths = append(q.Paths, normalKernelControlPathRule{"/etc/resolv.conf", nkRead | nkWrite | nkTruncate, "file", false})
		case "clock":
			q.Paths = append(q.Paths, normalKernelControlPathRule{"/dev/ptp0", nkRead | nkWrite | nkIoctlDev, "ptp", true})
			for _, p := range []string{"/var/log/chrony", "/var/lib/chrony"} {
				q.Paths = append(q.Paths, normalKernelControlPathRule{p, nkWriteTree, "dir", false})
			}
		case "logs":
			q.Paths = append(q.Paths, normalKernelControlPathRule{"/var/log", nkWriteTree, "dir", false}, normalKernelControlPathRule{"/dev/virtio-ports/tech.neon.log.0", nkRead | nkWrite, "virtio-log", true})
		case "udev", "udev-init":
			q.Paths = append(q.Paths, normalKernelControlPathRule{"/dev", nkReadDir | nkMakeDir | nkMakeChar | nkMakeSym | nkRemoveFile | nkRemoveDir, "dir", false})
		}
		sort.Slice(q.Paths, func(i, j int) bool {
			if q.Paths[i].Path != q.Paths[j].Path {
				return q.Paths[i].Path < q.Paths[j].Path
			}
			return q.Paths[i].Rights < q.Paths[j].Rights
		})
		m.Roles = append(m.Roles, q)
	}
	return m
}
func decodeNormalKernelControlManifest(raw []byte) (normalKernelControlManifest, error) {
	var m normalKernelControlManifest
	if len(raw) == 0 || len(raw) > 64<<10 {
		return m, fmt.Errorf("kernel controls manifest exceeds bound")
	}
	if e := normalKernelControlStrictObject(raw, &m); e != nil {
		return m, e
	}
	expected := normalKernelControlExpectedManifest()
	want, e := json.Marshal(expected)
	if e != nil {
		return m, e
	}
	actual, e := json.Marshal(m)
	if e != nil || !bytes.Equal(actual, want) {
		return m, fmt.Errorf("kernel controls require exact fixed role/path/cap table")
	}
	return m, nil
}
func normalKernelControlStrictObject(raw []byte, out any) error {
	// 复用逐层精确键/重复拒绝；不是宽松json.Unmarshal。
	return normalKernelControlDecodeObject(raw, out)
}
func (m normalKernelControlManifest) role(name string) (normalKernelControlRoleRule, error) {
	for _, r := range m.Roles {
		if r.Role == name {
			return r, nil
		}
	}
	return normalKernelControlRoleRule{}, fmt.Errorf("role does not have ordinary kernel control capability")
}
