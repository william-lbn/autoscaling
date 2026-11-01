package main

import (
	"fmt"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"os"
	"path/filepath"
	"strings"
)

// 新独立helper只形成可审计划，不写原rootdisk/账号/设备，也不接旧mode。
type ordinaryV2BuildPlan struct {
	Manifest        []byte
	PasswdAdditions []byte
	GroupAdditions  []byte
}

func ordinaryV2AccountPlan(passwd, group []byte, pgUID, pgGID uint32) (ordinaryV2BuildPlan, error) {
	var zero ordinaryV2BuildPlan
	if e := resourceguard.ValidateNormalSystemOrdinaryV2AccountsBeforeInstall(passwd, group, pgUID, pgGID); e != nil {
		return zero, e
	}
	raw, e := resourceguard.NormalSystemOrdinaryV2ManifestJSON()
	if e != nil {
		return zero, e
	}
	m, e := resourceguard.DecodeNormalSystemOrdinaryV2Manifest(raw)
	if e != nil {
		return zero, e
	}
	out := ordinaryV2BuildPlan{Manifest: raw}
	for _, p := range m.Roles {
		out.PasswdAdditions = append(out.PasswdAdditions, []byte(fmt.Sprintf("%s:x:%d:%d:ordinary service:/nonexistent:/neonvm/bin/nologin\n", p.User, p.UID, p.GID))...)
		out.GroupAdditions = append(out.GroupAdditions, []byte(fmt.Sprintf("%s:x:%d:\n", p.Group, p.GID))...)
	}
	if e = resourceguard.ValidateNormalSystemOrdinaryV2InstalledAccounts(append(append([]byte(nil), passwd...), out.PasswdAdditions...), append(append([]byte(nil), group...), out.GroupAdditions...), pgUID, pgGID); e != nil {
		return zero, e
	}
	return out, nil
}
func ordinaryV2AccountPlanForRoot(root string, pgUID, pgGID uint32) (ordinaryV2BuildPlan, error) {
	var z ordinaryV2BuildPlan
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return z, fmt.Errorf("original canonical rootdisk required")
	}
	for _, name := range []string{"etc/passwd", "etc/group"} {
		path := filepath.Join(root, name)
		got, e := filepath.EvalSymlinks(path)
		if e != nil || got != path || !strings.HasPrefix(got, root+string(os.PathSeparator)) {
			return z, fmt.Errorf("account source alias differs")
		}
		st, e := os.Lstat(path)
		if e != nil || !st.Mode().IsRegular() || st.Size() > 64<<10 {
			return z, fmt.Errorf("account source not bounded regular file")
		}
	}
	p, e := os.ReadFile(filepath.Join(root, "etc/passwd"))
	if e != nil {
		return z, e
	}
	g, e := os.ReadFile(filepath.Join(root, "etc/group"))
	if e != nil {
		return z, e
	}
	return ordinaryV2AccountPlan(p, g, pgUID, pgGID)
}
