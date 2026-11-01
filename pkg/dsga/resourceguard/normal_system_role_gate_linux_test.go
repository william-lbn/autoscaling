//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalSystemRoleGateStrictOriginalWire(t *testing.T) {
	c := normalSystemRoleGateCommand{ProtocolVersion: 1, Role: "resource", GateNonce: strings.Repeat("ab", 32), OriginalNonce: strings.Repeat("cd", 32), PendingSHA256: strings.Repeat("ef", 32)}
	raw, e := normalSystemRoleGateEncode(c)
	require.NoError(t, e)
	got, e := normalSystemRoleGateDecode(raw)
	require.NoError(t, e)
	require.Equal(t, c, got)
	for _, mode := range []string{"short", "tail", "case", "duplicate", "path", "env", "missing", "null", "unknown-role", "zero-nonce"} {
		t.Run(mode, func(t *testing.T) {
			b := append([]byte(nil), raw...)
			n := int(binary.BigEndian.Uint32(b[8:12]))
			text := string(b[12 : 12+n])
			switch mode {
			case "short":
				b = b[:len(b)-1]
			case "tail":
				b[len(b)-1] = 1
			case "case":
				text = strings.Replace(text, "protocol_version", "Protocol_Version", 1)
			case "duplicate":
				text = strings.Replace(text, `"protocol_version":1`, `"protocol_version":1,"protocol_version":1`, 1)
			case "path":
				text = strings.Replace(text, `"protocol_version":1`, `"protocol_version":1,"path":"/bin/sh"`, 1)
			case "env":
				text = strings.Replace(text, `"protocol_version":1`, `"protocol_version":1,"env":["PRIVATE_VALUE_NEVER_EMIT"]`, 1)
			case "missing":
				text = strings.Replace(text, `"protocol_version":1,`, "", 1)
			case "null":
				text = strings.Replace(text, `"protocol_version":1`, `"protocol_version":null`, 1)
			case "unknown-role":
				text = strings.Replace(text, "resource", "shell", 1)
			case "zero-nonce":
				text = strings.Replace(text, c.GateNonce, strings.Repeat("0", 64), 1)
			}
			if mode != "short" && mode != "tail" {
				b = make([]byte, normalSystemRoleGateWireBytes)
				copy(b, "NSYSGO01")
				binary.BigEndian.PutUint32(b[8:12], uint32(len(text)))
				copy(b[12:], text)
			}
			_, e := normalSystemRoleGateDecode(b)
			require.Error(t, e)
			require.NotContains(t, e.Error(), "PRIVATE_VALUE_NEVER_EMIT")
		})
	}
}
func TestNormalSystemRoleGateNoOriginalReferenceNoSpawn(t *testing.T) {
	_, e := normalSystemRoleGateReferenceOf(nil)
	require.ErrorIs(t, e, ErrUnknown)
	require.Error(t, RunNormalSystemRoleExecGate(nil))
	require.Error(t, RunNormalSystemRoleExecGate(context.Background()))
	require.Error(t, normalSystemRoleGateThreadAlive(-1))
}
func TestNormalSystemRoleEntryASLRAndAmbiguity(t *testing.T) {
	geometry := normalSystemRoleEntryGeometry{kind: 3, entry: 0x1234, offset: 0x1234, device: 1, inode: 7}
	maps := "7f0000001000-7f0000003000 r-xp 00001000 00:01 7 /original-loader\n"
	pc, base, e := normalSystemRoleMappedEntry(maps, geometry)
	require.NoError(t, e)
	require.Equal(t, uint64(0x7f0000001234), pc)
	require.Equal(t, uint64(0x7f0000000000), base)
	for _, mode := range []string{"missing", "wrong-inode", "wrong-offset", "ambiguous", "unreadable-x", "unmapped-entry", "noncanonical-base", "exec-fixed-bias"} {
		t.Run(mode, func(t *testing.T) {
			m := maps
			g := geometry
			switch mode {
			case "missing":
				m = ""
			case "wrong-inode":
				g.inode = 8
			case "wrong-offset":
				m = strings.Replace(m, " 00001000 ", " 00002000 ", 1)
			case "ambiguous":
				m += maps
			case "unreadable-x":
				m = strings.Replace(m, "r-xp", "r--p", 1)
			case "unmapped-entry":
				g.offset = 0x9999
			case "noncanonical-base":
				m = strings.Replace(m, "00001000-", "00001001-", 1)
			case "exec-fixed-bias":
				g.kind = 2
			}
			_, _, e := normalSystemRoleMappedEntry(m, g)
			require.Error(t, e)
		})
	}
}

func TestNormalSystemRoleBrokerLifetimeDeclaration(t *testing.T) {
	a, _ := normalSystemVectorSHA(normalSystemRoleBrokerArgv())
	env, _ := normalSystemVectorSHA(normalSystemRoleBrokerEnvironment())
	valid := normalSystemRoleBrokerDeclaration{ProtocolVersion: 1, Scope: normalSystemRoleBrokerDeclarationScope, ExecutablePath: normalSystemRoleBrokerExecutable, ExecutableSHA256: strings.Repeat("ab", 32), ArgvSHA256: a, EnvironmentSHA256: env, NamespaceProtocolVersion: 1, GateProtocolVersion: 1, Lifetime: normalSystemRoleBrokerLifetime, ComputeBuildPlanSHA256: strings.Repeat("cd", 32)}
	raw, e := json.Marshal(valid)
	require.NoError(t, e)
	_, e = decodeNormalSystemRoleBrokerDeclaration(raw)
	require.NoError(t, e)
	for name, edit := range map[string]func(*normalSystemRoleBrokerDeclaration){"generic lifetime": func(d *normalSystemRoleBrokerDeclaration) { d.Lifetime = "always-detach" }, "no gate ABI": func(d *normalSystemRoleBrokerDeclaration) { d.GateProtocolVersion = 0 }, "wrong namespace ABI": func(d *normalSystemRoleBrokerDeclaration) { d.NamespaceProtocolVersion = 2 }, "wrong path": func(d *normalSystemRoleBrokerDeclaration) { d.ExecutablePath = "/bin/sh" }, "wrong argv": func(d *normalSystemRoleBrokerDeclaration) { d.ArgvSHA256 = strings.Repeat("ef", 32) }, "zero source": func(d *normalSystemRoleBrokerDeclaration) { d.ComputeBuildPlanSHA256 = strings.Repeat("00", 32) }} {
		t.Run(name, func(t *testing.T) {
			d := valid
			edit(&d)
			b, e := json.Marshal(d)
			require.NoError(t, e)
			_, e = decodeNormalSystemRoleBrokerDeclaration(b)
			require.Error(t, e)
		})
	}
	for name, b := range map[string][]byte{"wrong case": bytes.Replace(raw, []byte("\"lifetime\""), []byte("\"Lifetime\""), 1), "unknown": append(append([]byte{}, raw[:len(raw)-1]...), []byte(",\"extra\":1}")...), "alias duplicate": append(append([]byte{}, raw[:len(raw)-1]...), []byte(",\"Lifetime\":\"initial-root-to-pid1-after-durable-release\"}")...), "null": []byte("null"), "empty": []byte("{}")} {
		t.Run(name, func(t *testing.T) { _, e := decodeNormalSystemRoleBrokerDeclaration(b); require.Error(t, e) })
	}
	_, e = normalSystemFixedRoleArgv(normalSystemRoleBrokerKind)
	require.Error(t, e)
}
