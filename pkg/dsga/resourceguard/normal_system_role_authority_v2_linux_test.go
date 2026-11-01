//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNormalSystemRoleAuthorityDeclarationVersions(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	one, e := NormalSystemRoleBrokerDeclarationJSON(sha, sha)
	require.NoError(t, e)
	d, e := decodeNormalSystemRoleBrokerDeclaration(one)
	require.NoError(t, e)
	require.Equal(t, uint32(1), d.ProtocolVersion)
	require.False(t, d.brokerOwnsAuthority())
	require.NotContains(t, string(one), "resource_authority_mode")
	two, e := NormalSystemRoleBrokerDeclarationV2JSON(sha, sha)
	require.NoError(t, e)
	d, e = decodeNormalSystemRoleBrokerDeclaration(two)
	require.NoError(t, e)
	require.Equal(t, uint32(2), d.ProtocolVersion)
	require.True(t, d.brokerOwnsAuthority())
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(two, &fields))
	for _, mode := range []string{"missing", "null", "empty", "wrong", "wrong-case", "duplicate", "alias", "v1-present", "v3", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			var next map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(two, &next))
			switch mode {
			case "missing":
				delete(next, "resource_authority_mode")
			case "null":
				next["resource_authority_mode"] = json.RawMessage("null")
			case "empty":
				next["resource_authority_mode"] = json.RawMessage(`""`)
			case "wrong":
				next["resource_authority_mode"] = json.RawMessage(`"new-guard"`)
			case "wrong-case":
				next["Resource_Authority_Mode"] = next["resource_authority_mode"]
				delete(next, "resource_authority_mode")
			case "alias":
				next["Resource_Authority_Mode"] = next["resource_authority_mode"]
			case "v1-present":
				next["protocol_version"] = json.RawMessage("1")
				next["scope"] = json.RawMessage(`"fixed-system-role-broker-declaration-v1"`)
			case "v3":
				next["protocol_version"] = json.RawMessage("3")
			case "unknown":
				next["trusted"] = json.RawMessage("true")
			}
			raw, e := json.Marshal(next)
			require.NoError(t, e)
			if mode == "duplicate" {
				raw = append(raw[:len(raw)-1], []byte(`,"resource_authority_mode":"broker-owned-same-guard-v1"}`)...)
			}
			_, e = decodeNormalSystemRoleBrokerDeclaration(raw)
			require.Error(t, e)
		})
	}
}
func TestNormalSystemRoleAuthorityCandidateHTTPHasNoBusinessEffects(t *testing.T) {
	g, _, _ := fixtureGuard(t)
	before, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
	require.NoError(t, e)
	a := &normalAuthorityAdmission{guard: g, nonce: [32]byte{1}, done: make(chan struct{})}
	var calls atomic.Int32
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusNoContent) })
	handler := a.httpHandler(next)
	for _, path := range []string{"/dsg/v1/resource/apply", "/dsg/v1/resource/array-authorize", normalAuthorityProbePath, normalAuthorityProbePath + "?"} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"protocol_version":1}`))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		require.NotEqual(t, http.StatusNoContent, response.Code)
	}
	require.Zero(t, calls.Load())
	after, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
	require.NoError(t, e)
	require.Equal(t, before, after)
	require.NoError(t, a.activate())
	r := httptest.NewRecorder()
	handler.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/business", nil))
	require.Equal(t, http.StatusNoContent, r.Code)
	require.Equal(t, int32(1), calls.Load())
	a.fail()
	require.ErrorIs(t, a.activate(), ErrUnknown)
	r = httptest.NewRecorder()
	handler.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/business", nil))
	require.Equal(t, http.StatusServiceUnavailable, r.Code)
	require.Equal(t, int32(1), calls.Load())
}
func TestNormalSystemRoleAuthorityCandidateNativeHasNoBirthEffects(t *testing.T) {
	g, _, _ := fixtureGuard(t)
	before, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
	require.NoError(t, e)
	a := &normalAuthorityAdmission{guard: g, nonce: [32]byte{1}, done: make(chan struct{})}
	sender, receiver := roleServiceTestPair(t)
	frame := make([]byte, 80)
	copy(frame, arrayPreexecBeginMagic)
	_, e = sender.Write(frame)
	require.NoError(t, e)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	handled, e := a.native(ctx, g, receiver, 1)
	require.True(t, handled)
	require.ErrorIs(t, e, ErrUnknown)
	after, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
	require.NoError(t, e)
	require.Equal(t, before, after)
	entries, e := os.ReadDir(g.config.StateDir)
	require.NoError(t, e)
	for _, entry := range entries {
		require.False(t, strings.Contains(entry.Name(), "birth") || strings.Contains(entry.Name(), "array-populate"))
	}
	foreign := &Guard{config: g.config, kernel: g.kernel}
	require.True(t, lookupNormalSystemAuthority(foreign) == nil)
	_, e = foreign.normalSystemOriginalComputeLeaseLocked(ctx, nil, nil)
	require.ErrorIs(t, e, ErrUnknown)
}

func TestNormalSystemRoleAuthorityHTTPPeerTypeBoundary(t *testing.T) {
	for _, kind := range []int{unix.SOCK_STREAM, unix.SOCK_SEQPACKET} {
		t.Run(fmt.Sprintf("type-%d", kind), func(t *testing.T) {
			fds, e := unix.Socketpair(unix.AF_UNIX, kind|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, e)
			var connections []*net.UnixConn
			for _, fd := range fds {
				f := os.NewFile(uintptr(fd), "test HTTP actual peer")
				c, e := net.FileConn(f)
				f.Close()
				require.NoError(t, e)
				u := c.(*net.UnixConn)
				connections = append(connections, u)
				defer u.Close()
			}
			peer, fd, e := normalAuthorityHTTPPeer(connections[0])
			if kind == unix.SOCK_SEQPACKET {
				require.Error(t, e)
				require.Nil(t, peer)
				require.Equal(t, -1, fd)
				return
			}
			require.NoError(t, e)
			defer unix.Close(fd)
			require.Equal(t, int32(os.Getpid()), peer.Pid)
			require.Equal(t, uint32(os.Getuid()), peer.Uid)
			require.NoError(t, arrayRegistryPIDFDAlive(fd))
			flags, e := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
			require.NoError(t, e)
			require.NotZero(t, flags&unix.FD_CLOEXEC)
		})
	}
}

// 只直测生产getter调用的三前缀读回门，不构造原task或发放Compute/预算许可。
func TestNormalSystemRoleAuthorityComputeLeaseThreeOriginalPrefixes(t *testing.T) {
	for _, phase := range []string{"pending", "exec-stopped", "released"} {
		for _, mutation := range []string{"deleted", "same-bytes-new-inode", "changed-bytes"} {
			t.Run(phase+"/"+mutation, func(t *testing.T) {
				g, _, _ := fixtureGuard(t)
				require.NoError(t, g.withRecord(context.Background(), func(_ *record, state *lockedState) error {
					lease := &normalSystemOriginalComputeLease{pending: normalSystemRoleJournalEntry{Nonce: strings.Repeat("c1", 32), Role: "compute"}}
					for i, p := range [3]string{"pending", "exec-stopped", "released"} {
						name := normalSystemRoleStageName(lease.pending.Nonce, p)
						require.NoError(t, state.createPhysicalFile(name, []byte(`{"test_only":"original-`+p+`"}`)))
						w, e := readNormalSystemRoleFileWitness(state, name)
						require.NoError(t, e)
						lease.prefixes[i] = w
					}
					require.NoError(t, lease.verifyOriginalPrefixesLocked(state))
					name := normalSystemRoleStageName(lease.pending.Nonce, phase)
					full := filepath.Join(g.config.StateDir, name)
					raw, e := os.ReadFile(full)
					require.NoError(t, e)
					switch mutation {
					case "deleted":
						require.NoError(t, os.Remove(full))
					case "same-bytes-new-inode":
						replacement := "compute-prefix-fixture-replacement.json"
						require.NoError(t, state.createPhysicalFile(replacement, raw))
						require.NoError(t, os.Rename(filepath.Join(g.config.StateDir, replacement), full))
					case "changed-bytes":
						require.NoError(t, os.WriteFile(full, []byte(`{"test_only":"tampered"}`), 0600))
					}
					before, e := os.ReadDir(g.config.StateDir)
					require.NoError(t, e)
					require.ErrorIs(t, lease.verifyOriginalPrefixesLocked(state), ErrUnknown)
					after, e := os.ReadDir(g.config.StateDir)
					require.NoError(t, e)
					require.Equal(t, before, after)
					if mutation == "deleted" {
						_, e = os.Stat(full)
						require.True(t, os.IsNotExist(e))
					}
					if mutation == "same-bytes-new-inode" {
						now, e := readNormalSystemRoleFileWitness(state, name)
						require.NoError(t, e)
						require.Equal(t, physicalDigest(raw), now.SHA256)
						require.NotEqual(t, lease.prefixes[map[string]int{"pending": 0, "exec-stopped": 1, "released": 2}[phase]].Inode, now.Inode)
					}
					return nil
				}))
			})
		}
	}
}
