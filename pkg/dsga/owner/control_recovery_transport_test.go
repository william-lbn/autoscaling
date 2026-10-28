package owner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/localcontrol"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
)

func TestPrivateControlSocketRecoversRootBeforePGExists(t *testing.T) {
	owner.WithResourceRecoveryEndpointTestFixture(t, func(ctx context.Context, endpoint *owner.ControlEndpoint) {
		base, err := filepath.EvalSymlinks(os.TempDir())
		require.NoError(t, err)
		if len(base) > 40 {
			base = "/private/tmp"
		}
		dir, err := os.MkdirTemp(base, "dsga-root-")
		require.NoError(t, err)
		defer os.RemoveAll(dir)
		registry := localcontrol.NewRegistry()
		unregister, err := registry.Register(endpoint)
		require.NoError(t, err)
		defer unregister()
		path := filepath.Join(dir, "agent.sock")
		server, err := localcontrol.Listen(path, registry)
		require.NoError(t, err)
		serverCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- server.Run(serverCtx) }()
		defer func() { stop(); require.NoError(t, <-done) }()
		transport := new(http.Transport)
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: time.Second}
		instance, epoch := endpoint.Binding()
		discovery, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://agent"+localcontrol.OwnerPath+"?vm_uid="+instance.VMUID, nil)
		require.NoError(t, err)
		response, err := client.Do(discovery)
		require.NoError(t, err)
		var binding localcontrol.OwnerBinding
		decodeErr := json.NewDecoder(response.Body).Decode(&binding)
		require.NoError(t, response.Body.Close())
		require.NoError(t, decodeErr)
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Zero(t, binding.Instance.PGBootID, "不能为尚未建立的PG伪造出生")
		require.Equal(t, control.CoordinationBinding(instance), binding.Instance)
		require.Equal(t, epoch, binding.OwnerEpoch)
		submit := func(command control.CoordinationCommand) (int, []byte) {
			t.Helper()
			body, err := json.Marshal(command)
			require.NoError(t, err)
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://agent"+localcontrol.CommandPath, bytes.NewReader(body))
			require.NoError(t, err)
			request.Header.Set("Content-Type", "application/json")
			response, err := client.Do(request)
			require.NoError(t, err)
			data, readErr := io.ReadAll(response.Body)
			require.NoError(t, response.Body.Close())
			require.NoError(t, readErr)
			return response.StatusCode, data
		}
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: binding.Instance, ExpectedOwnerEpoch: epoch, Action: control.CoordinationResourceState, Request: nil, Key: nil, Selector: ""}
		status, data := submit(command)
		require.Equal(t, http.StatusOK, status, string(data))
		var result owner.CoordinationResult
		require.NoError(t, json.Unmarshal(data, &result))
		require.NotEmpty(t, result.HeldSourceSHA256)
		require.Nil(t, result.State.PGInstance)
		command.Action = control.CoordinationState
		status, _ = submit(command)
		require.Equal(t, http.StatusBadRequest, status, "零boot不能进入业务/PG管理路径")
		command.Action, command.Selector = control.CoordinationRepairHeld, result.HeldSourceSHA256
		status, data = submit(command)
		require.Contains(t, []int{http.StatusOK, http.StatusConflict}, status, string(data))
		select {
		case <-endpoint.Done():
		case <-ctx.Done():
			t.Fatal("confirmed Root did not finish recovery loop")
		}
	})
}
