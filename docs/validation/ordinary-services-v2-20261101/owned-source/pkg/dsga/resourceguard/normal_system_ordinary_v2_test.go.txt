package resourceguard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestNormalSystemOrdinaryV2ExactStaticContract(t *testing.T) {
	b, e := NormalSystemOrdinaryV2ManifestJSON()
	if e != nil {
		t.Fatal(e)
	}
	m, e := DecodeNormalSystemOrdinaryV2Manifest(b)
	if e != nil || len(m.Roles) != 9 {
		t.Fatal(e)
	}
	for i, p := range m.Roles {
		if p.UID != uint32(60001+i) || p.GID != p.UID || p.KeepCapabilities&(1<<27) != 0 {
			t.Fatal("identity/MKNOD drift")
		}
	}
	m.Roles[0].Argv[0] = "bad"
	again, _ := NormalSystemOrdinaryV2ManifestJSON()
	if !bytes.Equal(again, b) {
		t.Fatal("caller mutated fixed table")
	}
	if !errors.Is(RequireNormalSystemOrdinaryV2Production(), ErrNormalSystemOrdinaryV2Unsupported) {
		t.Fatal("unimplemented installer was authorized")
	}
}
func TestNormalSystemOrdinaryV2StrictNestedDecode(t *testing.T) {
	b, _ := NormalSystemOrdinaryV2ManifestJSON()
	for _, mode := range []string{"top-alias", "role-alias", "duplicate", "unknown", "uid", "mknod", "root-action", "null-argv", "old-version"} {
		t.Run(mode, func(t *testing.T) {
			raw := string(b)
			switch mode {
			case "top-alias":
				raw = strings.Replace(raw, `"protocol_version":`, `"Protocol_Version":`, 1)
			case "role-alias":
				raw = strings.Replace(raw, `"uid":`, `"UID":`, 1)
			case "duplicate":
				raw = strings.Replace(raw, `"uid":60001`, `"uid":60001,"uid":60001`, 1)
			case "unknown":
				raw = strings.Replace(raw, `"uid":60001`, `"uid":60001,"verified":true`, 1)
			case "uid":
				raw = strings.Replace(raw, `"uid":60001`, `"uid":0`, 1)
			case "mknod":
				raw = strings.Replace(raw, `"keep_capabilities":"0"`, `"keep_capabilities":"134217728"`, 1)
			case "root-action":
				raw = strings.Replace(raw, `"root_actions":[]`, `"root_actions":["arbitrary-root"]`, 1)
			case "null-argv":
				raw = strings.Replace(raw, `"argv":["/neonvm/bin/busybox","touch","/run/neon-ordinary/allow-start/vmstart.allowed"]`, `"argv":null`, 1)
			case "old-version":
				raw = strings.Replace(raw, `"protocol_version":2`, `"protocol_version":1`, 1)
			}
			if raw == string(b) {
				t.Fatal("case did not alter actual contract")
			}
			if _, e := DecodeNormalSystemOrdinaryV2Manifest([]byte(raw)); e == nil {
				t.Fatal("tamper accepted")
			}
		})
	}
}
func ordinaryV2TestAccounts(installed bool) ([]byte, []byte) {
	p := "root:x:0:0:root:/:/bin/sh\npostgres:x:1000:1000:PG:/var/db/postgres:/bin/sh\n"
	g := "root:x:0:\npostgres:x:1000:\n"
	if installed {
		for _, q := range normalSystemOrdinaryV2Expected().Roles {
			p += fmt.Sprintf("%s:x:%d:%d:ordinary:/nonexistent:/neonvm/bin/nologin\n", q.User, q.UID, q.GID)
			g += fmt.Sprintf("%s:x:%d:\n", q.Group, q.GID)
		}
	}
	return []byte(p), []byte(g)
}
func TestNormalSystemOrdinaryV2AccountCollisionAndInstalledSet(t *testing.T) {
	p, g := ordinaryV2TestAccounts(false)
	if e := ValidateNormalSystemOrdinaryV2AccountsBeforeInstall(p, g, 1000, 1000); e != nil {
		t.Fatal(e)
	}
	ip, ig := ordinaryV2TestAccounts(true)
	if e := ValidateNormalSystemOrdinaryV2InstalledAccounts(ip, ig, 1000, 1000); e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"name", "uid", "group-name", "gid", "future-member", "PG-member", "PG-id", "missing-installed", "partial-installed", "root-alias", "foreign-primary-gid", "installed-foreign-primary-gid"} {
		t.Run(mode, func(t *testing.T) {
			pp, gg := append([]byte(nil), p...), append([]byte(nil), g...)
			var e error
			switch mode {
			case "name":
				pp = append(pp, []byte("neon-clock:x:123:123:x:/:/bin/sh\n")...)
			case "uid":
				pp = append(pp, []byte("occupied:x:60001:123:x:/:/bin/sh\n")...)
			case "group-name":
				gg = append(gg, []byte("neon-logs:x:123:\n")...)
			case "gid":
				gg = append(gg, []byte("occupied:x:60002:\n")...)
			case "future-member":
				gg = append(gg, []byte("wheel:x:10:neon-network\n")...)
			case "PG-member":
				gg = []byte(strings.Replace(string(g), "postgres:x:1000:", "postgres:x:1000:neon-daemon", 1))
			case "PG-id":
				e = ValidateNormalSystemOrdinaryV2AccountsBeforeInstall(p, g, 60001, 1000)
			case "missing-installed":
				e = ValidateNormalSystemOrdinaryV2InstalledAccounts(p, g, 1000, 1000)
			case "partial-installed":
				e = ValidateNormalSystemOrdinaryV2InstalledAccounts(ip, g, 1000, 1000)
			case "root-alias":
				pp = append(pp, []byte("root-alias:x:0:0:x:/:/bin/sh\n")...)
			case "foreign-primary-gid":
				pp = append(pp, []byte("foreign:x:2000:60006:x:/:/bin/sh\n")...)
			case "installed-foreign-primary-gid":
				foreign := append(append([]byte(nil), ip...), []byte("foreign:x:2000:60006:x:/:/bin/sh\n")...)
				e = ValidateNormalSystemOrdinaryV2InstalledAccounts(foreign, ig, 1000, 1000)
			}
			if e == nil {
				e = ValidateNormalSystemOrdinaryV2AccountsBeforeInstall(pp, gg, 1000, 1000)
			}
			if e == nil {
				t.Fatal("collision/partial installed accepted")
			}
		})
	}
}
func TestNormalSystemOrdinaryV2LegacyManifestBytesUntouched(t *testing.T) {
	old, e := NormalKernelControlSourceManifestJSON()
	if e != nil {
		t.Fatal(e)
	}
	var v map[string]any
	if json.Unmarshal(old, &v) != nil || v["protocol_version"] != float64(1) {
		t.Fatal("legacy changed")
	}
	if _, e := DecodeNormalSystemOrdinaryV2Manifest(old); e == nil {
		t.Fatal("legacy implicitly enabled v2")
	}
	modern, _ := NormalSystemOrdinaryV2ManifestJSON()
	if _, e := decodeNormalKernelControlManifest(modern); e == nil {
		t.Fatal("legacy decoder silently enabled modern unsupported installer")
	}
}
