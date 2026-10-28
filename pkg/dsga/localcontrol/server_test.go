package localcontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
)

// HTTP/UDS传输夹具不模拟原生完成。真实Session/WAL与管理命令组合在owner包
// 单独验；此处只核请求严格性、身份路由、期限、有界并发及错误不泄密。
type transportEndpoint struct {
	done chan struct{}
	call func(context.Context, control.CoordinationCommand) (owner.CoordinationResult, error)
}

func (e *transportEndpoint) Done() <-chan struct{} { return e.done }

func (e *transportEndpoint) Binding() (control.Instance, control.Uint64) {
	c := commandFixture()
	return c.Instance, c.ExpectedOwnerEpoch
}

func (e *transportEndpoint) Submit(ctx context.Context, c control.CoordinationCommand) (owner.CoordinationResult, error) {
	return e.call(ctx, c)
}

func commandFixture() control.CoordinationCommand {
	return control.CoordinationCommand{ProtocolVersion: 1, Instance: control.Instance{VMUID: "vm-local", ComputeInstanceID: "compute-local", PGBootID: 3}, ExpectedOwnerEpoch: 7, Action: control.CoordinationState, Request: nil, Key: nil, Selector: ""}
}

func TestLocalControlStrictRoutingAndBoundedBody(t *testing.T) {
	registry := NewRegistry()
	command := commandFixture()
	body, err := json.Marshal(command)
	require.NoError(t, err)
	endpoint := &transportEndpoint{done: make(chan struct{}), call: func(ctx context.Context, got control.CoordinationCommand) (owner.CoordinationResult, error) {
		require.Equal(t, command, got)
		deadline, bounded := ctx.Deadline()
		require.True(t, bounded)
		require.WithinDuration(t, time.Now().Add(callTimeout), deadline, time.Second)
		return owner.CoordinationResult{ProtocolVersion: 1, Instance: got.Instance, OwnerEpoch: got.ExpectedOwnerEpoch, Progress: pgexec.Idle, Disposition: "", State: nil}, nil
	}}
	registry.entries[command.Instance.VMUID] = endpoint
	handler := newHandler(registry)
	for _, tc := range []struct {
		name, method, path, contentType, body string
		status                                int
	}{
		{"valid", "POST", CommandPath, "application/json", string(body), http.StatusOK},
		{"get", "GET", CommandPath, "application/json", string(body), http.StatusMethodNotAllowed},
		{"route", "POST", "/", "application/json", string(body), http.StatusNotFound},
		{"query", "POST", CommandPath + "?x=1", "application/json", string(body), http.StatusNotFound},
		{"bare-query", "POST", CommandPath + "?", "application/json", string(body), http.StatusNotFound},
		{"encoded-route", "POST", "/dsga/v1/%63ommand", "application/json", string(body), http.StatusNotFound},
		{"type", "POST", CommandPath, "text/plain", string(body), http.StatusUnsupportedMediaType},
		{"duplicate", "POST", CommandPath, "application/json", strings.Replace(string(body), "{", `{"action":"begin",`, 1), http.StatusBadRequest},
		{"unknown-field", "POST", CommandPath, "application/json", strings.Replace(string(body), "{", `{"pid":123,`, 1), http.StatusBadRequest},
		{"missing-instance", "POST", CommandPath, "application/json", `{}`, http.StatusBadRequest},
		{"oversized-whitespace", "POST", CommandPath, "application/json", strings.Repeat(" ", maxBody) + string(body), http.StatusRequestEntityTooLarge},
		{"unknown-vm", "POST", CommandPath, "application/json", strings.Replace(string(body), "vm-local", "other-vm", 1), http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		})
	}
	endpoint.call = func(context.Context, control.CoordinationCommand) (owner.CoordinationResult, error) {
		return *new(owner.CoordinationResult), errors.New("credential secret must not appear")
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", CommandPath, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.NotContains(t, w.Body.String(), "secret")
	require.Contains(t, w.Body.String(), "inspect_original_journal")
	for _, tc := range []struct {
		query  string
		status int
	}{
		{"?vm_uid=vm-local", http.StatusOK},
		{"", http.StatusBadRequest},
		{"?vm_uid=vm-local&vm_uid=vm-local", http.StatusBadRequest},
		{"?vm_uid=vm-local&unexpected=1", http.StatusBadRequest},
		{"?vm_uid=%ZZ", http.StatusBadRequest},
		{"?vm_uid=other-vm", http.StatusConflict},
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", OwnerPath+tc.query, nil))
		require.Equal(t, tc.status, w.Code)
		if tc.status == http.StatusOK {
			var binding OwnerBinding
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &binding))
			require.Equal(t, command.Instance, binding.Instance)
			require.Equal(t, command.ExpectedOwnerEpoch, binding.OwnerEpoch)
		}
	}
	close(endpoint.done)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", OwnerPath+"?vm_uid=vm-local", nil))
	require.Equal(t, http.StatusConflict, w.Code, "closed owner cannot be discovered as active")
}

func TestLocalControlAdmissionIsBoundedAndCancellationReleasesWaiters(t *testing.T) {
	registry := NewRegistry()
	command := commandFixture()
	body, err := json.Marshal(command)
	require.NoError(t, err)
	entered := make(chan struct{}, maxInFlight)
	registry.entries[command.Instance.VMUID] = &transportEndpoint{done: make(chan struct{}), call: func(ctx context.Context, _ control.CoordinationCommand) (owner.CoordinationResult, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return *new(owner.CoordinationResult), ctx.Err()
	}}
	handler := newHandler(registry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for range maxInFlight {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest("POST", CommandPath, bytes.NewReader(body)).WithContext(ctx)
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			require.Equal(t, http.StatusServiceUnavailable, w.Code)
		}()
	}
	for range maxInFlight {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("management requests did not reach bounded waiters")
		}
	}
	r := httptest.NewRequest("POST", CommandPath, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	cancel()
	wg.Wait()
}

func socketDir(t *testing.T) string {
	t.Helper()
	// Mac t.TempDir路径可能超过Unix socket上限；使用已解析的系统临时目录
	// 创建独占0700子目录，仍走与Linux相同的所有者/祖先检查。
	base, err := filepath.EvalSymlinks(os.TempDir())
	require.NoError(t, err)
	if len(base) > 40 {
		base = "/private/tmp"
	}
	dir, err := os.MkdirTemp(base, "dsga-ctl-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	return dir
}

func TestPrivateUnixSocketLifecycleAndFilesystemBoundary(t *testing.T) {
	dir := socketDir(t)
	path := filepath.Join(dir, "control.sock")
	registry := NewRegistry()
	for _, mode := range []os.FileMode{0o755, 0o770, 0o777} {
		require.NoError(t, os.Chmod(dir, mode))
		_, err := Listen(path, registry)
		require.Error(t, err)
	}
	require.NoError(t, os.Chmod(dir, 0o700))
	link := filepath.Join(dir, "link")
	require.NoError(t, os.Symlink(dir, link))
	_, err := Listen(filepath.Join(link, "control.sock"), registry)
	require.Error(t, err)
	for _, invalid := range []string{"relative.sock", path + "/../socket", "/" + strings.Repeat("x", 100)} {
		require.Error(t, ValidateSocketPath(invalid))
	}
	server, err := Listen(path, registry)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	info, err := os.Lstat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	_, err = Listen(path, registry)
	require.Error(t, err, "must not unlink an existing listener")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	transport := new(http.Transport)
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	body, err := json.Marshal(commandFixture())
	require.NoError(t, err)
	r, err := http.NewRequestWithContext(ctx, "POST", "http://agent"+CommandPath, bytes.NewReader(body))
	require.NoError(t, err)
	r.Header.Set("Content-Type", "application/json")
	response, err := client.Do(r)
	require.NoError(t, err)
	_, readErr := io.Copy(io.Discard, response.Body)
	require.NoError(t, response.Body.Close())
	require.NoError(t, readErr)
	require.Equal(t, http.StatusConflict, response.StatusCode, "real UDS reaches strict handler without an owner")
	cancel()
	require.NoError(t, <-done)
	_, err = os.Lstat(path)
	require.True(t, os.IsNotExist(err))
	// 也覆盖Serve尚未开始、Agent其他初始化失败的关闭路径。
	beforeRun, err := Listen(path, registry)
	require.NoError(t, err)
	require.NoError(t, beforeRun.Close())
	_, err = os.Lstat(path)
	require.True(t, os.IsNotExist(err))
}
