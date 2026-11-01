package resourceguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"strconv"
	"strings"
)

const NormalSystemOrdinaryV2ProfileID = "neon-normal-ordinary-services-v2"
const NormalSystemOrdinaryV2Scope = "fixed-nonroot-ordinary-services-contract-v2"

// 同一固定资产的显式新版本；旧ordinary1解码器/JSON均保持不变。
const NormalSystemOrdinaryV2ManifestPath = NormalKernelControlSourceManifestPath
const NormalSystemOrdinaryV2PackageLockPath = "/neonvm/config/dsga-ordinary-v2-package-lock.json"

var ErrNormalSystemOrdinaryV2Unsupported = errors.New("ordinary v2 requires original Gate installer, role lease and fixed Root actions")

// 静态非授权合同。没有ready/verified/sourceSHA等可自报资格；真实producer须
// 从原RO System文件和clone pidfd、实际exec-stop/权限/五cap集合独立核验。
type NormalSystemOrdinaryV2Role struct {
	Role             string         `json:"role"`
	User             string         `json:"user"`
	Group            string         `json:"group"`
	UID              uint32         `json:"uid"`
	GID              uint32         `json:"gid"`
	KeepCapabilities control.Uint64 `json:"keep_capabilities"`
	Argv             []string       `json:"argv"`
	Environment      []string       `json:"environment"`
	PrivatePaths     []string       `json:"private_paths"`
	RootActions      []string       `json:"root_actions"`
}
type NormalSystemOrdinaryV2Manifest struct {
	ProtocolVersion uint32                       `json:"protocol_version"`
	Scope           string                       `json:"scope"`
	ProfileID       string                       `json:"profile_id"`
	PackageLockPath string                       `json:"package_lock_path"`
	Roles           []NormalSystemOrdinaryV2Role `json:"roles"`
}

func (v *NormalSystemOrdinaryV2Role) UnmarshalJSON(b []byte) error {
	type plain NormalSystemOrdinaryV2Role
	var n plain
	if e := control.UnmarshalObject(b, &n); e != nil {
		return e
	}
	*v = NormalSystemOrdinaryV2Role(n)
	return nil
}
func (v *NormalSystemOrdinaryV2Manifest) UnmarshalJSON(b []byte) error {
	type plain NormalSystemOrdinaryV2Manifest
	var n plain
	if e := control.UnmarshalObject(b, &n); e != nil {
		return e
	}
	*v = NormalSystemOrdinaryV2Manifest(n)
	return nil
}
func normalSystemOrdinaryV2Expected() NormalSystemOrdinaryV2Manifest {
	roles := []string{"allow-start", "kernel-version", "network", "udev", "udev-init", "acpi", "logs", "clock", "daemon"}
	m := NormalSystemOrdinaryV2Manifest{ProtocolVersion: 2, Scope: NormalSystemOrdinaryV2Scope, ProfileID: NormalSystemOrdinaryV2ProfileID, PackageLockPath: NormalSystemOrdinaryV2PackageLockPath}
	for i, name := range roles {
		argv, e := normalSystemFixedRoleArgvForVersion(3, name)
		if e != nil {
			panic("fixed ordinary role table inconsistent")
		}
		q := NormalSystemOrdinaryV2Role{Role: name, User: "neon-" + name, Group: "neon-" + name, UID: uint32(60001 + i), GID: uint32(60001 + i), Argv: argv, Environment: []string{"PATH=/neonvm/bin"}, PrivatePaths: []string{}, RootActions: []string{}}
		switch name {
		case "allow-start":
			q.Argv = []string{"/neonvm/bin/busybox", "touch", "/run/neon-ordinary/allow-start/vmstart.allowed"}
			q.PrivatePaths = []string{"/run/neon-ordinary/allow-start"}
		case "network":
			q.KeepCapabilities = (1 << 12) | (1 << 13)
			q.PrivatePaths = []string{"/etc/resolv.conf", "/run/neon-ordinary/network"}
		case "udev":
			q.Argv = []string{"/neonvm/bin/udevd", "--neon-fixed-devices-v1"}
			q.PrivatePaths = []string{"/run/udev"}
		case "udev-init":
			q.Argv = []string{"/neonvm/bin/dsga-resource-guard", "ordinary-v2-device-check"}
		case "acpi":
			q.Argv = []string{"/neonvm/bin/acpid", "-f", "-n", "-c", "/neonvm/acpi", "-s", "/run/neon-ordinary/acpi/acpid.socket", "--neon-fixed-power-v1"}
			q.PrivatePaths = []string{"/run/neon-ordinary/acpi"}
			q.RootActions = []string{"pid1-graceful-poweroff"}
		case "logs":
			q.PrivatePaths = []string{"/run/neon-ordinary/logs", "/var/log/vector"}
		case "clock":
			q.KeepCapabilities = (1 << 25) | (1 << 10)
			q.Argv = []string{"/neonvm/bin/chronyd", "-U", "-u", "neon-clock", "-n", "-f", "/neonvm/config/chrony.conf", "-l", "/var/log/chrony/chrony.log"}
			q.PrivatePaths = []string{"/run/chrony", "/var/lib/chrony", "/var/log/chrony"}
		case "daemon":
			q.RootActions = []string{"replace-management-tls", "management-tls-checksum"}
		}
		m.Roles = append(m.Roles, q)
	}
	return m
}
func NormalSystemOrdinaryV2ManifestJSON() ([]byte, error) {
	return json.Marshal(normalSystemOrdinaryV2Expected())
}
func DecodeNormalSystemOrdinaryV2Manifest(raw []byte) (NormalSystemOrdinaryV2Manifest, error) {
	var m NormalSystemOrdinaryV2Manifest
	if len(raw) == 0 || len(raw) > 32<<10 {
		return m, fmt.Errorf("ordinary v2 manifest exceeds bound")
	}
	if e := json.Unmarshal(raw, &m); e != nil {
		return m, e
	}
	want, _ := NormalSystemOrdinaryV2ManifestJSON()
	got, e := json.Marshal(m)
	if e != nil || !bytes.Equal(want, got) {
		return m, fmt.Errorf("ordinary v2 requires exact fixed identity/capability/argv table")
	}
	return m, nil
}
func NormalSystemOrdinaryV2RolePlan(role string) (NormalSystemOrdinaryV2Role, error) {
	for _, p := range normalSystemOrdinaryV2Expected().Roles {
		if p.Role == role {
			return p, nil
		}
	}
	return NormalSystemOrdinaryV2Role{}, fmt.Errorf("unknown ordinary v2 role")
}

// 第一批没有生产安装/Root动作消费者，始终Unsupported；静态manifest解码成功
// 不得被调用者当Source或放行票据。后继只能以真实opaque生产链替代该缺口。
func RequireNormalSystemOrdinaryV2Production() error { return ErrNormalSystemOrdinaryV2Unsupported }

func ordinaryV2AccountNumber(s string) (uint32, error) {
	v, e := strconv.ParseUint(s, 10, 32)
	if e != nil || strconv.FormatUint(v, 10) != s {
		return 0, fmt.Errorf("noncanonical account ID")
	}
	return uint32(v), nil
}
func validateOrdinaryV2Accounts(passwd, group []byte, pgUID, pgGID uint32, installed bool) error {
	if len(passwd) == 0 || len(group) == 0 || len(passwd) > 64<<10 || len(group) > 64<<10 || pgUID == 0 || pgGID == 0 {
		return fmt.Errorf("original bounded account source required")
	}
	plans := normalSystemOrdinaryV2Expected().Roles
	reserved := map[uint32]string{}
	names := map[string]NormalSystemOrdinaryV2Role{}
	for _, q := range plans {
		if q.UID == pgUID || q.GID == pgGID {
			return fmt.Errorf("ordinary identity collides with original PG")
		}
		reserved[q.UID] = q.User
		names[q.User] = q
	}
	seenNames := map[string]bool{}
	seenRoles := map[string]bool{}
	rootSeen, pgSeen := false, false
	for _, line := range strings.Split(strings.TrimSuffix(string(passwd), "\n"), "\n") {
		f := strings.Split(line, ":")
		if len(f) != 7 || f[0] == "" || seenNames[f[0]] {
			return fmt.Errorf("invalid or duplicate passwd entry")
		}
		seenNames[f[0]] = true
		uid, e := ordinaryV2AccountNumber(f[2])
		if e != nil {
			return e
		}
		gid, e := ordinaryV2AccountNumber(f[3])
		if e != nil {
			return e
		}
		if uid == 0 {
			if f[0] != "root" || gid != 0 || rootSeen {
				return fmt.Errorf("ambiguous original root identity")
			}
			rootSeen = true
		}
		if uid == pgUID || f[0] == "postgres" {
			if uid != pgUID || gid != pgGID || f[0] != "postgres" || pgSeen {
				return fmt.Errorf("original PG account differs")
			}
			pgSeen = true
		}
		wanted, byName := names[f[0]]
		owner, byID := reserved[uid]
		// 主GID也会授予目录/socket DAC权限；镜像中其它账号即使UID不冲突，
		// 也不能先占用未来role GID。installed检查同样拒第三方主组成员。
		if groupOwner, byGID := reserved[gid]; byGID && (!byName || groupOwner != f[0]) {
			return fmt.Errorf("ordinary primary group identity collision")
		}
		if byName || byID {
			if !installed || !byName || owner != f[0] || uid != wanted.UID || gid != wanted.GID {
				return fmt.Errorf("ordinary passwd identity collision")
			}
			seenRoles[f[0]] = true
		}
	}
	if !rootSeen || !pgSeen {
		return fmt.Errorf("original root/PG accounts absent")
	}
	seenNames = map[string]bool{}
	seenGroups := map[string]bool{}
	rootSeen, pgSeen = false, false
	for _, line := range strings.Split(strings.TrimSuffix(string(group), "\n"), "\n") {
		f := strings.Split(line, ":")
		if len(f) != 4 || f[0] == "" || seenNames[f[0]] {
			return fmt.Errorf("invalid or duplicate group entry")
		}
		seenNames[f[0]] = true
		gid, e := ordinaryV2AccountNumber(f[2])
		if e != nil {
			return e
		}
		if gid == 0 {
			if f[0] != "root" || rootSeen {
				return fmt.Errorf("ambiguous original root group")
			}
			rootSeen = true
		}
		if gid == pgGID || f[0] == "postgres" {
			if gid != pgGID || f[0] != "postgres" || pgSeen {
				return fmt.Errorf("original PG group differs")
			}
			pgSeen = true
		}
		wanted, byName := names[f[0]]
		owner, byID := reserved[gid]
		if byName || byID {
			if !installed || !byName || owner != f[0] || gid != wanted.GID || f[3] != "" {
				return fmt.Errorf("ordinary group identity collision")
			}
			seenGroups[f[0]] = true
		}
		for _, member := range strings.Split(f[3], ",") {
			if _, ok := names[member]; ok {
				return fmt.Errorf("ordinary account must have no supplementary group membership")
			}
		}
	}
	if !rootSeen || !pgSeen {
		return fmt.Errorf("original root/PG groups absent")
	}
	if installed && (len(seenRoles) != len(plans) || len(seenGroups) != len(plans)) {
		return fmt.Errorf("ordinary installed account set incomplete")
	}
	return nil
}
func ValidateNormalSystemOrdinaryV2AccountsBeforeInstall(passwd, group []byte, pgUID, pgGID uint32) error {
	return validateOrdinaryV2Accounts(passwd, group, pgUID, pgGID, false)
}
func ValidateNormalSystemOrdinaryV2InstalledAccounts(passwd, group []byte, pgUID, pgGID uint32) error {
	return validateOrdinaryV2Accounts(passwd, group, pgUID, pgGID, true)
}
