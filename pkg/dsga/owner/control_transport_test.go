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
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/localcontrol"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
)

// 真实私有UDS→HTTP→正式注册端点→同一Run→真实UID/RV日志。PG/Lease API仍为
// 明确协议夹具，本组合以零派发取消终结，不假称执行了Linux/PG/VM物理动作。
func TestPrivateControlSocketToRealOwnerWAL(t *testing.T) {
	owner.WithControlEndpointTestFixture(t, func(ctx context.Context, endpoint *owner.ControlEndpoint, request control.Request) {
		base, err := filepath.EvalSymlinks(os.TempDir())
		require.NoError(t, err)
		if len(base) > 40 {
			base = "/private/tmp"
		}
		dir, err := os.MkdirTemp(base, "dsga-wire-")
		require.NoError(t, err)
		defer os.RemoveAll(dir)
		registry := localcontrol.NewRegistry()
		unregister, err := registry.Register(endpoint)
		require.NoError(t, err)
		defer unregister()
		_, err = registry.Register(endpoint)
		require.Error(t, err, "another registration must not replace a live owner")
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
		// 发现只返回本地注册绑定，不包含URL/凭据或执行许可；后续POST仍
		// 显式携带该绑定，再由真实Session和领域fence核验。
		discovery, err := http.NewRequestWithContext(ctx, "GET", "http://agent"+localcontrol.OwnerPath+"?vm_uid="+request.Key.Instance.VMUID, nil)
		require.NoError(t, err)
		response, err := client.Do(discovery)
		require.NoError(t, err)
		var binding localcontrol.OwnerBinding
		decodeErr := json.NewDecoder(response.Body).Decode(&binding)
		require.NoError(t, response.Body.Close())
		require.NoError(t, decodeErr)
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, control.CoordinationBinding(request.Key.Instance), binding.Instance)
		submit := func(command control.CoordinationCommand) owner.CoordinationResult {
			t.Helper()
			body, err := json.Marshal(command)
			require.NoError(t, err)
			r, err := http.NewRequestWithContext(ctx, "POST", "http://agent"+localcontrol.CommandPath, bytes.NewReader(body))
			require.NoError(t, err)
			r.Header.Set("Content-Type", "application/json")
			response, err := client.Do(r)
			require.NoError(t, err)
			raw, readErr := io.ReadAll(response.Body)
			require.NoError(t, response.Body.Close())
			require.NoError(t, readErr)
			require.Equal(t, http.StatusOK, response.StatusCode, string(raw))
			var value owner.CoordinationResult
			require.NoError(t, json.Unmarshal(raw, &value))
			require.NotNil(t, value.State)
			return value
		}
		_, epoch := endpoint.Binding()
		require.Equal(t, epoch, binding.OwnerEpoch)
		command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(request.Key.Instance), ExpectedOwnerEpoch: epoch, Action: control.CoordinationBegin, Request: &request, Key: nil, Selector: ""}
		value := submit(command)
		require.Equal(t, journal.Accepted, value.Disposition)
		require.Equal(t, request, value.State.Active.Request)
		value = submit(command)
		require.Equal(t, journal.Replayed, value.Disposition)
		command.Action, command.Request, command.Key = control.CoordinationCancel, nil, &request.Key
		value = submit(command)
		require.True(t, value.State.Active.AbortRequested)
		command.Action, command.Key = control.CoordinationState, nil
		require.Eventually(t, func() bool {
			value = submit(command)
			return value.State.Active == nil
		}, time.Second, 10*time.Millisecond)
		require.Len(t, value.State.History, 1)
		require.Equal(t, journal.AbortedBeforeDispatch, value.State.History[0].Outcome)
		require.Equal(t, request.Key, *value.State.HighWater)
	})
}
