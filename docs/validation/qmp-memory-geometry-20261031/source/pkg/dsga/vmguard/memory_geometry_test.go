package vmguard

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/stretchr/testify/require"
)

func geometryInputFixture() memoryGeometryInputs {
	return memoryGeometryInputs{
		Version:     json.RawMessage(`{"qemu":{"major":8,"minor":1,"micro":5},"package":""}`),
		Machine:     json.RawMessage(`{"size":268435456,"slots":1,"max-size":1073741824}`),
		Devices:     json.RawMessage(`[{"type":"virtio-mem","data":{"id":"vm0","memdev":"/objects/vmem0","requested-size":16777216,"size":0,"max-size":805306368,"block-size":8388608,"node":0,"memaddr":4294967296}}]`),
		Summary:     json.RawMessage(`{"base-memory":268435456,"plugged-memory":0}`),
		Backends:    json.RawMessage(`[{"id":"vmem0","size":805306368,"host-nodes":[],"policy":"default"},{"id":"pc.ram","size":268435456,"host-nodes":[],"policy":"default"}]`),
		BackendSize: json.RawMessage(`805306368`),
	}
}

func geometryIdentityFixture() Identity {
	id := fixtureIdentity()
	id.MinimumBytes = 512 << 20
	id.MaximumBytes = 1 << 30
	id.BlockBytes = VirtioBlockBytes
	return id
}

func TestOriginalMemoryGeometryDecodesActualQEMUSchema(t *testing.T) {
	id := geometryIdentityFixture()
	got, err := decodeOriginalMemoryGeometry(id, 256<<20, geometryInputFixture())
	require.NoError(t, err)
	require.Equal(t, id, got.Identity)
	require.Equal(t, control.Uint64(1<<30), got.MaximumBytes)
	require.Equal(t, control.Uint64(768<<20), got.BackendBytes)
	require.Equal(t, control.Uint64(256<<20), got.ActualBytes)
	require.Equal(t, control.Uint64(272<<20), got.RequestedBytes)
	require.NotEqual(t, got.RequestedBytes, got.MaximumBytes)
	require.Error(t, (Observation{RequestedBytes: got.RequestedBytes, ActualBytes: got.ActualBytes}).validate(id), "earlyboot geometry cannot bypass production minimum")
}

func TestOriginalMemoryGeometryRejectsIncompleteOrChangedSources(t *testing.T) {
	original := geometryInputFixture()
	fields := []struct {
		name string
		ptr  func(*memoryGeometryInputs) *json.RawMessage
	}{
		{"version", func(x *memoryGeometryInputs) *json.RawMessage { return &x.Version }},
		{"machine", func(x *memoryGeometryInputs) *json.RawMessage { return &x.Machine }},
		{"devices", func(x *memoryGeometryInputs) *json.RawMessage { return &x.Devices }},
		{"summary", func(x *memoryGeometryInputs) *json.RawMessage { return &x.Summary }},
		{"backends", func(x *memoryGeometryInputs) *json.RawMessage { return &x.Backends }},
		{"backend-size", func(x *memoryGeometryInputs) *json.RawMessage { return &x.BackendSize }},
	}
	for _, f := range fields {
		for _, bad := range []string{"null", "{}", "[]", "-1", "1.5", "1e9", `"805306368"`, `{"size":1,"size":2}`} {
			t.Run(f.name+"/"+bad, func(t *testing.T) {
				in := original
				*f.ptr(&in) = json.RawMessage(bad)
				_, err := decodeOriginalMemoryGeometry(geometryIdentityFixture(), 256<<20, in)
				require.Error(t, err)
			})
		}
	}
	cases := []struct{ name, field, old, new string }{
		{"version-drift", "version", `"micro":5`, `"micro":6`},
		{"machine-maximum", "machine", "1073741824", "1082130432"},
		{"machine-slots", "machine", `"slots":1`, `"slots":2`},
		{"machine-base", "machine", "268435456", "276824064"},
		{"wrong-node", "devices", `"node":0`, `"node":1`},
		{"no-node", "devices", `"node":0,`, ``},
		{"wrong-device", "devices", `"vm0"`, `"other"`},
		{"wrong-memdev", "devices", `"/objects/vmem0"`, `"/objects/other"`},
		{"max-not-request", "devices", `"max-size":805306368`, `"max-size":16777216`},
		{"unaligned-request", "devices", "16777216", "16777217"},
		{"actual-over-max", "devices", `"size":0`, `"size":813694976`},
		{"bad-addr", "devices", "4294967296", "1"},
		{"summary-drift", "summary", `"plugged-memory":0`, `"plugged-memory":8388608`},
		{"backend-size-drift", "backends", "805306368", "813694976"},
		{"base-backend-drift", "backends", "268435456", "276824064"},
		{"unknown-backend", "backends", `"pc.ram"`, `"another"`},
		{"duplicate-backend", "backends", `"pc.ram"`, `"vmem0"`},
		{"numa-policy", "backends", `"default"`, `"bind"`},
		{"numa-hostnodes", "backends", `"host-nodes":[]`, `"host-nodes":[0]`},
		{"null-hostnodes", "backends", `"host-nodes":[]`, `"host-nodes":null`},
		{"qom-size-drift", "backend-size", "805306368", "813694976"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := original
			for _, f := range fields {
				if f.name == tc.field {
					require.Contains(t, string(*f.ptr(&in)), tc.old)
					*f.ptr(&in) = json.RawMessage(strings.ReplaceAll(string(*f.ptr(&in)), tc.old, tc.new))
				}
			}
			_, err := decodeOriginalMemoryGeometry(geometryIdentityFixture(), 256<<20, in)
			require.Error(t, err)
		})
	}
	for _, base := range []control.Uint64{0, 1, 256<<20 + 1, 520 << 20} {
		t.Run("invalid-base/"+fmt.Sprint(base), func(t *testing.T) {
			_, err := decodeOriginalMemoryGeometry(geometryIdentityFixture(), base, original)
			require.Error(t, err)
		})
	}
}

func TestOriginalMemoryGeometryUnixReadsOnlyAndBoundsFrames(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(map[bool]string{false: "readonly", true: "bad-summary"}[bad], func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "qmp-geometry-")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			socket := filepath.Join(dir, "qmp.sock")
			l, err := net.Listen("unix", socket)
			require.NoError(t, err)
			var wg sync.WaitGroup
			var commands []string
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := l.Accept()
				if err != nil {
					return
				}
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(3 * time.Second))
				enc := json.NewEncoder(c)
				_ = enc.Encode(map[string]any{"QMP": map[string]any{}})
				scan := bufio.NewScanner(c)
				in := geometryInputFixture()
				for scan.Scan() {
					var cmd struct {
						Execute string `json:"execute"`
						ID      string `json:"id"`
						Args    struct {
							Path     string `json:"path"`
							Property string `json:"property"`
						} `json:"arguments"`
					}
					if json.Unmarshal(scan.Bytes(), &cmd) != nil {
						return
					}
					commands = append(commands, cmd.Execute)
					raw := json.RawMessage(`{}`)
					switch cmd.Execute {
					case "qmp_capabilities":
					case "query-uuid":
						u, _ := geometryIdentityFixture().QEMUUUID()
						raw, _ = json.Marshal(map[string]string{"UUID": u})
					case "query-version":
						raw = in.Version
					case "query-memory-devices":
						raw = in.Devices
					case "query-memory-size-summary":
						raw = in.Summary
						if bad {
							raw = json.RawMessage(`{"base-memory":268435456,"plugged-memory":8388608}`)
						}
					case "query-memdev":
						raw = in.Backends
					case "qom-get":
						if cmd.Args.Path == "/machine" && cmd.Args.Property == "memory" {
							raw = in.Machine
						} else if cmd.Args.Path == "/objects/vmem0" && cmd.Args.Property == "size" {
							raw = in.BackendSize
						} else {
							return
						}
					default:
						return
					}
					if enc.Encode(map[string]any{"return": raw, "id": cmd.ID}) != nil {
						return
					}
				}
			}()
			q := nativeQMP{id: geometryIdentityFixture(), path: socket, timeout: time.Second}
			_, err = q.readOriginalMemoryGeometry(context.Background(), 256<<20)
			if bad {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			l.Close()
			wg.Wait()
			require.Equal(t, []string{"qmp_capabilities", "query-uuid", "query-version", "qom-get", "query-memory-devices", "query-memory-size-summary", "query-memdev", "qom-get"}, commands)
		})
	}
}
