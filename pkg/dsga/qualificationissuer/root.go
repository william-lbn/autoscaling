package qualificationissuer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/localcontrol"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

// RootReader 读取同一个可信 Agent 的现有串行 API；不是可序列化的来源或授权票据。
type RootReader interface {
	Read(context.Context, journal.Binding) (localcontrol.OwnerBinding, resourceguard.View, error)
}
type LocalRoot struct {
	client    *http.Client
	transport *http.Transport
	path      string
	socket    [2]uint64
}

// NewLocalRoot 仅连接固定 UDS，不接受报告提供 URL、Bearer、伪造 Session 或远程 Root。
func NewLocalRoot(path string, timeout time.Duration) (*LocalRoot, error) {
	if localcontrol.ValidateSocketPath(path) != nil || timeout <= 0 || timeout > 120*time.Second {
		return nil, fmt.Errorf("fixed Agent socket and finite timeout required")
	}
	socket, err := originalAgentSocket(path)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	return &LocalRoot{client: &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("Agent redirect forbidden") }}, transport: transport, path: path, socket: socket}, nil
}
func (r *LocalRoot) Close() { r.transport.CloseIdleConnections() }
func (r *LocalRoot) call(ctx context.Context, method, path string, body []byte, into any) error {
	current, err := originalAgentSocket(r.path)
	if err != nil || current != r.socket {
		return fmt.Errorf("original Agent socket identity changed")
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://dsga-agent"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("trusted Agent read unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "no-store" {
		return fmt.Errorf("trusted Agent read unconfirmed")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, journal.MaxRawBytes+1))
	if err != nil || len(raw) > journal.MaxRawBytes {
		return fmt.Errorf("trusted Agent response unavailable or oversized")
	}
	if control.ValidateJSON(raw, journal.MaxRawBytes, 16384) != nil || json.Unmarshal(raw, into) != nil {
		return fmt.Errorf("trusted Agent response invalid")
	}
	return nil
}
func (r *LocalRoot) Read(ctx context.Context, b journal.Binding) (localcontrol.OwnerBinding, resourceguard.View, error) {
	var binding localcontrol.OwnerBinding
	var result owner.CoordinationResult
	if err := r.call(ctx, http.MethodGet, localcontrol.OwnerPath+"?vm_uid="+url.QueryEscape(string(b.VMUID)), nil, &binding); err != nil {
		return binding, resourceguard.View{}, err
	}
	if binding.ProtocolVersion != 1 || binding.Instance.Validate() != nil || binding.Instance.VMUID != string(b.VMUID) || binding.OwnerEpoch == 0 {
		return binding, resourceguard.View{}, fmt.Errorf("Agent owner discovery changed original VM")
	}
	command := control.CoordinationCommand{ProtocolVersion: 1, Instance: binding.Instance, ExpectedOwnerEpoch: binding.OwnerEpoch, Action: control.CoordinationResourceState}
	raw, err := json.Marshal(command)
	if err != nil {
		return binding, resourceguard.View{}, err
	}
	if err = r.call(ctx, http.MethodPost, localcontrol.CommandPath, raw, &result); err != nil {
		return binding, resourceguard.View{}, err
	}
	if result.ProtocolVersion != 1 || result.Instance != binding.Instance || result.OwnerEpoch != binding.OwnerEpoch || result.ResourceState == nil || result.ResourceState.Validate() != nil {
		return binding, resourceguard.View{}, fmt.Errorf("Agent resource state does not match discovered owner")
	}
	return binding, *result.ResourceState, nil
}
