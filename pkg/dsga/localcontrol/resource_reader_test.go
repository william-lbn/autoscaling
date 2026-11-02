package localcontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/stretchr/testify/require"
)

// 通过真实UDS验证资源读取socket的服务端权限，不把只读mount当动作限制。
func TestLocalControlResourceReaderUDSRefusesOtherActionsBeforeDispatch(t *testing.T) {
	dir, err := os.MkdirTemp("", "dsga-ro-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	registry := NewRegistry()
	cmd := commandFixture()
	var calls atomic.Int32
	registry.entries[cmd.Instance.VMUID] = &transportEndpoint{done: make(chan struct{}), call: func(_ context.Context, got control.CoordinationCommand) (owner.CoordinationResult, error) {
		require.Equal(t, control.CoordinationResourceState, got.Action)
		calls.Add(1)
		return owner.CoordinationResult{ProtocolVersion: 1, Instance: got.Instance, OwnerEpoch: got.ExpectedOwnerEpoch}, nil
	}}
	path := filepath.Join(dir, "read.sock")
	server, err := ListenResourceRead(path, registry)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- server.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		select {
		case err := <-stopped:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Error("resource reader did not stop")
		}
	})
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	discovery, err := client.Get("http://unix" + OwnerPath + "?vm_uid=" + cmd.Instance.VMUID)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, discovery.StatusCode)
	require.NoError(t, discovery.Body.Close())
	for _, action := range []control.CoordinationAction{control.CoordinationResourceState, control.CoordinationState, control.CoordinationRepairHeld, control.CoordinationReplayInitial} {
		t.Run(string(action), func(t *testing.T) {
			command := cmd
			command.Action = action
			if action == control.CoordinationRepairHeld || action == control.CoordinationReplayInitial {
				command.Selector = strings.Repeat("ab", 32)
			}
			require.NoError(t, command.Validate(), "负例必须是原合同中的合法命令")
			raw, err := json.Marshal(command)
			require.NoError(t, err)
			response, err := client.Post("http://unix"+CommandPath, "application/json", bytes.NewReader(raw))
			require.NoError(t, err)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, "no-store", response.Header.Get("Cache-Control"))
			if action == control.CoordinationResourceState {
				require.Equal(t, http.StatusOK, response.StatusCode)
			} else {
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				require.Contains(t, string(body), "resource_read_only")
			}
		})
	}
	require.EqualValues(t, 1, calls.Load(), "有效管理写请求也必须在Submit前拒绝")
}
