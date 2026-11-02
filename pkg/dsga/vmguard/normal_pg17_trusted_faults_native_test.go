//go:build dsga_native_trusted_control && dsga_native_stock_preparation && linux && dsga_native_initial_lifecycle && dsga_native_memory_origin_wire && dsga_native_memory_commit2 && dsga_native_role_transfer2 && dsga_native_ordinary2

package vmguard_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/compute"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// 故障只作用于DEV客户端的响应交付，不生成身份、预算或内核完成事实。
// 两个非空值均保留完整三轮正常链；取消的原意图额外归档，不替代真实缩容。
func trustedPG17FaultMode() (string, error) {
	mode := os.Getenv("DSGA_PG17_FAULT_CASE")
	switch mode {
	case "", "busy-cancel", "normal-reply-loss", "sql-busy-drain", trustedPG17ControllerTakeoverMode:
		return mode, nil
	}
	return "", errors.New("unknown fixed trusted PG17 fault case")
}

// 最早入口保存实际进程读取的闭集mode，不从父配置自报替代现场。
func trustedPG17WriteModeReceipt(dir, mode string) error {
	if mode != "" && mode != "busy-cancel" && mode != "normal-reply-loss" && mode != "sql-busy-drain" && mode != trustedPG17ControllerTakeoverMode {
		return errors.New("invalid fixed mode receipt")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	b, e := json.Marshal(map[string]any{"protocol_version": 1, "actual_mode": mode, "host_pid": os.Getpid(), "scope": "DEV assembly only; no execution authority"})
	if e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(dir, "actual-fault-mode.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	if _, e = f.Write(append(b, '\n')); e != nil {
		return e
	}
	return f.Sync()
}
func TestTrustedPG17ActualModeReceipt(t *testing.T) {
	for _, mode := range []string{"busy-cancel", "normal-reply-loss", "sql-busy-drain", trustedPG17ControllerTakeoverMode} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("DSGA_PG17_FAULT_CASE", mode)
			actual, e := trustedPG17FaultMode()
			if e != nil {
				t.Fatal("fixed actual mode unavailable")
			}
			if e = trustedPG17WriteModeReceipt(dir, actual); e != nil {
				t.Fatal("actual mode receipt failed")
			}
			raw, e := os.ReadFile(filepath.Join(dir, "actual-fault-mode.json"))
			if e != nil {
				t.Fatal("actual mode receipt unreadable")
			}
			var receipt struct {
				ProtocolVersion uint32 `json:"protocol_version"`
				ActualMode      string `json:"actual_mode"`
				HostPID         int    `json:"host_pid"`
			}
			if json.Unmarshal(raw, &receipt) != nil || receipt.ProtocolVersion != 1 || receipt.ActualMode != mode || receipt.HostPID != os.Getpid() {
				t.Fatal("actual mode receipt differs from process")
			}
			st, e := os.Stat(filepath.Join(dir, "actual-fault-mode.json"))
			if e != nil || st.Mode().Perm() != 0600 {
				t.Fatal("mode receipt permissions differ")
			}
		})
	}
}

type trustedPG17FaultTransport struct {
	inner       *http.Transport
	mode        string
	applyCalls  atomic.Uint32
	dropped     atomic.Bool
	holdObserve atomic.Bool
	recovered   atomic.Bool
}

func (f *trustedPG17FaultTransport) CloseIdleConnections() { f.inner.CloseIdleConnections() }

// SDK会Clone具体Transport。使用可被Clone保留的原DialTLSContext进入本DEV中继，
// 只负责交付/丢失实际HTTP响应；不更改鉴权头、命令、Root或PG来源。
func (f *trustedPG17FaultTransport) transportForSDK() (*http.Transport, func(), error) {
	if f.mode != "normal-reply-loss" {
		return f.inner, func() {}, nil
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	proxy := &httputil.ReverseProxy{Director: func(r *http.Request) { r.URL.Scheme = "https"; r.URL.Host = "trusted-compute.invalid" }, Transport: f, ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
		// 故障必须表现为实际丢失连接，不向SDK伪造成功或Root业务错误。
		if h, ok := w.(http.Hijacker); ok {
			c, _, e := h.Hijack()
			if e == nil {
				_ = c.Close()
				return
			}
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: 2 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	closeRelay := func() { _ = server.Close(); _ = listener.Close(); <-done }
	transport := &http.Transport{DialTLSContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}}
	return transport, closeRelay, nil
}

func (f *trustedPG17FaultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	const apply = "/dsg/v1/resource/normal-apply"
	const observe = "/dsg/v1/resource/normal-observe"
	if f.mode == "normal-reply-loss" && f.dropped.Load() && f.holdObserve.Load() && r.URL.Path == observe {
		return nil, errors.New("DEV original normal observation temporarily unavailable")
	}
	response, err := f.inner.RoundTrip(r)
	if r.URL.Path != apply {
		return response, err
	}
	call := f.applyCalls.Add(1)
	if f.mode != "normal-reply-loss" || call != 2 || err != nil || response.StatusCode != http.StatusOK {
		return response, err
	}
	// 第二次正式normal Apply是released-shrink；必须真实成功收完响应才制造丢回复。
	// 原Root已执行，客户端仍按丢回复路径保留写前Unknown，不能据此伪造完成。
	data, readErr := io.ReadAll(io.LimitReader(response.Body, 65537))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(data) > 65536 {
		return nil, errors.New("DEV cannot capture bounded original normal reply")
	}
	f.holdObserve.Store(true)
	f.dropped.Store(true)
	return nil, errors.New("DEV completed original normal reply lost")
}

func trustedPG17BusyCancel(ctx context.Context, f *vmguard.NativePG17TrustedHostFixture, session *owner.Session, worker *owner.PGWorker, pg *compute.Client, root *resourceguard.Client) error {
	beforePG, err := pg.Memory(ctx)
	if err != nil {
		return err
	}
	beforeRoot, err := root.InspectState(ctx)
	if err != nil {
		return err
	}
	limits, err := beforeRoot.ConfirmedLimits()
	if err != nil {
		return err
	}
	instance, _ := worker.Binding()
	request := control.Request{ProtocolVersion: 1, Key: control.Key{Instance: instance, IssuerEpoch: session.Actor().Epoch, Revision: 1, OperationID: control.OperationID{Low: 1}}, Kind: control.Grow, ExpectedCapacityGeneration: beforePG.Observed.CapacityGeneration, TargetBuffers: 1024, TargetVMMemoryBytes: 8 << 30, BudgetCommitmentID: "trusted-cancel-before-dispatch", DeadlineUnixMillis: control.Uint64(time.Now().Add(time.Minute).UnixMilli())}
	snapshot, err := f.Store.Load(ctx)
	if err != nil {
		return err
	}
	accepted, disposition, err := f.Store.Begin(ctx, snapshot, session.Actor(), request, uint64(time.Now().UnixMilli()))
	if err != nil {
		return err
	}
	if disposition != journal.Accepted {
		return errors.New("DEV cancellation original Begin not accepted")
	}
	other := request
	other.Key.Revision++
	other.Key.OperationID.Low++
	other.TargetBuffers = 2048
	busy, disposition, err := f.Store.Begin(ctx, accepted, session.Actor(), other, uint64(time.Now().UnixMilli()))
	if err != nil {
		return err
	}
	if disposition != journal.Busy || !reflect.DeepEqual(accepted.State(), busy.State()) {
		return errors.New("DEV concurrent intent did not retain original Busy state")
	}
	cancelled, err := f.Store.RequestAbort(ctx, busy, session.Actor(), request.Key)
	if err != nil {
		return err
	}
	if !cancelled.State().Active.AbortRequested {
		return errors.New("DEV original cancellation not persisted")
	}
	progress, err := worker.Advance(ctx)
	if err != nil {
		return err
	}
	if progress != pgexec.Recorded {
		return errors.New("DEV undispatched cancellation not recorded")
	}
	after, err := f.Store.Load(ctx)
	if err != nil {
		return err
	}
	m := after.State()
	if m.Active != nil || len(m.History) != 1 || m.History[0].Outcome != journal.AbortedBeforeDispatch || !reflect.DeepEqual(m.InitialHeld, accepted.State().InitialHeld) {
		return errors.New("DEV cancellation did not retain initial original budget")
	}
	for _, step := range m.History[0].Operation.Steps {
		if step.Phase != journal.Planned || step.Command != nil || step.DispatchedBy != nil {
			return errors.New("DEV cancelled undispatched operation gained a dispatch")
		}
	}
	afterPG, err := pg.Memory(ctx)
	if err != nil {
		return err
	}
	afterRoot, err := root.InspectState(ctx)
	if err != nil {
		return err
	}
	afterLimits, err := afterRoot.ConfirmedLimits()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(limits, afterLimits) || beforePG.Observed.HighBuffers != afterPG.Observed.HighBuffers || beforePG.Observed.LowBuffers != afterPG.Observed.LowBuffers || beforePG.Observed.CapacityGeneration != afterPG.Observed.CapacityGeneration {
		return errors.New("DEV undispatched cancellation changed actual caps or PG generation")
	}
	return trustedDriverSave("fault-busy-cancel", map[string]any{"case": "busy-cancel", "scope": "control serialization Busy plus original undispatched cancellation; not foreground SQL drain", "state": m, "root_before": beforeRoot, "root_after": afterRoot, "pg_before": beforePG, "pg_after": afterPG, "actual_limits_unchanged": true, "E": 0})
}

// 只读原生租约；DeadlineNS是Guest单调时钟，HostUnixNano仅为采样时间，不混算。
func trustedPG17LeaseObservation(ctx context.Context, client *compute.Client, key control.Key, stage string) (control.AdmissionLeaseResponse, error) {
	request := control.AdmissionLeaseRequest{ProtocolVersion: 1, Instance: key.Instance, IssuerEpoch: key.IssuerEpoch, ExpectedCapacityGeneration: 1, ExpectedAdmissionRevision: 0, ExpectedDeadlineNS: 0, Action: "observe"}
	fact, err := client.AdmissionLease(ctx, request)
	if err != nil {
		return fact, err
	}
	if err = trustedDriverSave("fault-native-lease-"+stage, map[string]any{"fact": fact, "host_unix_nano": time.Now().UnixNano(), "deadline_clock": "Guest monotonic; not Host wall", "renew_sent": false, "E": 0}); err != nil {
		return fact, err
	}
	return fact, nil
}

// 只接受同一原Key/命令经生产CAS记录的完成；查询与显式选择可正常竞合。
func trustedPG17OriginalReadyConfirmed(before, after journal.Model) bool {
	if before.Validate() != nil || after.Validate() != nil || before.Active == nil || after.Owner != before.Owner {
		return false
	}
	original := before.Active
	var current *journal.Operation
	if after.Active != nil && after.Active.Request == original.Request {
		current = after.Active
	}
	if current == nil {
		for i := range after.History {
			r := &after.History[i]
			if r.Outcome == journal.Succeeded && r.Operation.Request == original.Request {
				current = &r.Operation
				break
			}
		}
	}
	if current == nil || current.Origin != original.Origin {
		return false
	}
	var old *journal.Step
	for i := range original.Steps {
		if original.Steps[i].Action == journal.GuestReady {
			old = &original.Steps[i]
			break
		}
	}
	if old == nil {
		return false
	}
	for i := range current.Steps {
		step := &current.Steps[i]
		if step.Action == journal.GuestReady {
			return step.Phase == journal.Settled && step.Observation != nil && step.Observation.Result == journal.Completed && step.TargetID == old.TargetID && step.CommandSHA256 == old.CommandSHA256 && bytes.Equal(step.Command, old.Command) && reflect.DeepEqual(step.DispatchedBy, old.DispatchedBy)
		}
	}
	return false
}

func trustedPG17RecoverReplyLoss(ctx context.Context, f *vmguard.NativePG17TrustedHostFixture, session *owner.Session, rc resourceguard.ClientConfig, pc compute.Config, qualification budget.QualificationSource, leaseClient *compute.Client, transport *trustedPG17FaultTransport) (*owner.ResourceWorker, *owner.PGWorker, error) {
	snapshot, err := f.Store.Load(ctx)
	if err != nil {
		return nil, nil, err
	}
	before := snapshot.State()
	if !transport.dropped.Load() || !transport.holdObserve.Load() || transport.applyCalls.Load() != 2 || before.Active == nil || before.Active.Request.Kind != control.Shrink {
		return nil, nil, errors.New("DEV original released-shrink reply loss not captured")
	}
	unknown := false
	for _, step := range before.Active.Steps {
		if step.Action == journal.GuestReady && step.Phase == journal.Unconfirmed && step.Observation == nil {
			unknown = true
		}
	}
	if !unknown {
		return nil, nil, errors.New("DEV lost normal reply did not retain original GuestReady Unknown")
	}
	if err = trustedDriverSave("fault-reply-loss-before-restart", map[string]any{"state": before, "normal_apply_calls": transport.applyCalls.Load(), "actual_root_reply_completed_then_lost": true, "E": 0}); err != nil {
		return nil, nil, err
	}
	resources, err := session.NewResourceRecoveryWorker(ctx, rc)
	if err != nil {
		return nil, nil, err
	}
	rootView, err := resources.Inspect(ctx)
	if err != nil {
		return nil, nil, err
	}
	var original resourceguard.Command
	for _, step := range before.Active.Steps {
		if step.Action == journal.GuestReady {
			if err = json.Unmarshal(step.Command, &original); err != nil {
				return nil, nil, err
			}
		}
	}
	if !rootView.Applied || rootView.Original == nil || rootView.Held == nil || !reflect.DeepEqual(*rootView.Original, original) || !reflect.DeepEqual(*rootView.Held, original) {
		return nil, nil, errors.New("DEV actual Root original complete command differs after lost reply")
	}
	if _, err = rootView.ConfirmedLimits(); err != nil {
		return nil, nil, err
	}
	retained, err := resources.PrepareRetainedPGRecovery(ctx)
	if err != nil {
		resources.CloseIdleConnections()
		return nil, nil, err
	}
	retainedState, err := f.Store.Load(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !reflect.DeepEqual(before.Active, retainedState.State().Active) {
		return nil, nil, errors.New("DEV read-only resource recovery handoff changed active original")
	}
	// false表示原Observe已确认；true表示同一Pending必须交给PG恢复，两者均是原合同。
	worker, err := session.NewProductionPGWorker(ctx, pc, resources, f.Core.CoreV1(), qualification)
	if err != nil {
		resources.CloseIdleConnections()
		return nil, nil, err
	}
	key := before.Active.Request.Key
	// 重建后的业务控制仍由正式Run/ControlEndpoint串行处理；租约过期不可旁路PG/VM门。
	endpoint := worker.NewControlEndpoint()
	runCtx, stopRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	var expiredTick atomic.Bool
	go func() {
		runDone <- worker.Run(runCtx, owner.PGLoopOptions{PollInterval: 100 * time.Millisecond, Control: endpoint, Observe: func(tick owner.Tick) {
			if errors.Is(tick.Err, owner.ErrServiceSafety) && expiredTick.CompareAndSwap(false, true) {
				_ = trustedDriverSave("fault-rebuilt-run-safety", map[string]any{"service_safety": true, "progress": tick.Progress, "E": 0})
			}
		}})
	}()
	defer func() { stopRun(); <-runDone }()
	command := control.CoordinationCommand{ProtocolVersion: 1, Instance: control.CoordinationBinding(key.Instance), ExpectedOwnerEpoch: session.Actor().Epoch, Action: control.CoordinationConfirmNormalQuotaDurability, Key: &key}
	_, err = endpoint.Submit(ctx, command)
	if err != nil {
		return nil, nil, err
	}
	confirmed, err := f.Store.Load(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !reflect.DeepEqual(before.Active, confirmed.State().Active) && !trustedPG17OriginalReadyConfirmed(before, confirmed.State()) {
		return nil, nil, errors.New("DEV confirmation did not preserve original operation or its confirmed receipt")
	}
	lease, err := trustedPG17LeaseObservation(ctx, leaseClient, key, "after-rebuild")
	if err != nil {
		return nil, nil, err
	}
	if lease.Expired {
		return nil, nil, owner.ErrServiceSafety
	}
	transport.holdObserve.Store(false)
	selected, err := f.Store.Load(ctx)
	if err != nil {
		return nil, nil, err
	}
	explicitSelected := false
	var selectorError error
	if !trustedPG17OriginalReadyConfirmed(before, selected.State()) {
		explicitSelected = true
		command.Action = control.CoordinationRecoverNormalQuota
		_, selectorError = endpoint.Submit(ctx, command)
	}
	after, err := f.Store.Load(ctx)
	if err != nil {
		return nil, nil, err
	}
	if selectorError != nil && (errors.Is(selectorError, owner.ErrServiceSafety) || errors.Is(selectorError, owner.ErrLeadershipLost) || errors.Is(selectorError, context.Canceled) || errors.Is(selectorError, context.DeadlineExceeded)) {
		return nil, nil, selectorError
	}
	if !trustedPG17OriginalReadyConfirmed(before, after.State()) {
		if selectorError != nil {
			return nil, nil, selectorError
		}
		return nil, nil, errors.New("DEV same original GuestReady remains unconfirmed after selected recovery")
	}
	if transport.applyCalls.Load() != 2 {
		return nil, nil, errors.New("DEV recovery resent normal Apply")
	}
	transport.recovered.Store(true)
	if err = trustedDriverSave("fault-reply-loss-after-restart", map[string]any{"before": before, "after": after.State(), "same_session_epoch": session.Actor().Epoch, "normal_apply_calls": transport.applyCalls.Load(), "new_resource_and_PG_clients": true, "retained_unknown_handoff": retained, "actual_complete_root_before_recovery": rootView, "explicit_original_durability": true, "explicit_recover_selected": explicitSelected, "selector_race_error_present": selectorError != nil, "original_ready_read_confirmed": true, "E": 0}); err != nil {
		return nil, nil, err
	}
	return resources, worker, nil
}

// 这里只测DEV丢回复边界，不把HTTP fixture当Root/PG物理验收。
func TestTrustedPG17FaultTransportKeepsDeliveryBoundary(t *testing.T) {
	t.Run("success-after-delivery", func(t *testing.T) {
		var hits atomic.Uint32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()
		fault := &trustedPG17FaultTransport{inner: &http.Transport{}, mode: "normal-reply-loss"}
		defer fault.CloseIdleConnections()
		invoke := func(path string) (*http.Response, error) {
			request, e := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader("{}"))
			if e != nil {
				t.Fatal("fixture request construction failed")
			}
			return fault.RoundTrip(request)
		}
		first, e := invoke("/dsg/v1/resource/normal-apply")
		if e != nil || first == nil {
			t.Fatal("first actual reply was not delivered")
		}
		_, _ = io.Copy(io.Discard, first.Body)
		_ = first.Body.Close()
		lost, e := invoke("/dsg/v1/resource/normal-apply")
		if e == nil || lost != nil || hits.Load() != 2 || !fault.dropped.Load() || !fault.holdObserve.Load() {
			t.Fatal("second actual success did not enter bounded lost reply")
		}
		blocked, e := invoke("/dsg/v1/resource/normal-observe")
		if e == nil || blocked != nil || hits.Load() != 2 {
			t.Fatal("observation hold reached remote peer")
		}
		fault.holdObserve.Store(false)
		observed, e := invoke("/dsg/v1/resource/normal-observe")
		if e != nil || observed == nil || hits.Load() != 3 {
			t.Fatal("explicit release did not restore original observation")
		}
		_ = observed.Body.Close()
	})
	t.Run("remote-error-kept", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusConflict) }))
		defer server.Close()
		fault := &trustedPG17FaultTransport{inner: &http.Transport{}, mode: "normal-reply-loss"}
		defer fault.CloseIdleConnections()
		for i := 0; i < 2; i++ {
			r, _ := http.NewRequest(http.MethodPost, server.URL+"/dsg/v1/resource/normal-apply", nil)
			reply, e := fault.RoundTrip(r)
			if e != nil || reply == nil || reply.StatusCode != http.StatusConflict {
				t.Fatal("original remote rejection was changed")
			}
			_ = reply.Body.Close()
		}
		if fault.dropped.Load() || fault.holdObserve.Load() {
			t.Fatal("remote rejection was counted as delivered success")
		}
	})
}
func TestTrustedPG17FaultCaseIsClosed(t *testing.T) {
	for _, mode := range []string{"", "busy-cancel", "normal-reply-loss", "unrecognized"} {
		t.Run(map[string]string{"": "normal", "busy-cancel": "busy", "normal-reply-loss": "loss", "unrecognized": "reject"}[mode], func(t *testing.T) {
			t.Setenv("DSGA_PG17_FAULT_CASE", mode)
			actual, e := trustedPG17FaultMode()
			if mode == "unrecognized" {
				if e == nil {
					t.Fatal("unknown fault case accepted")
				}
			} else if e != nil || actual != mode {
				t.Fatal("fixed fault case changed")
			}
		})
	}
}

// 必须经过SDK同样的Clone再实际收发，防只测RoundTripper却漏掉池装配。
func TestTrustedPG17FaultTransportCloneRetainsDelivery(t *testing.T) {
	var hits atomic.Uint32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()
	inner := &http.Transport{DialTLSContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(upstream.URL, "http://"))
	}}
	fault := &trustedPG17FaultTransport{inner: inner, mode: "normal-reply-loss"}
	defer fault.CloseIdleConnections()
	sdk, closeRelay, e := fault.transportForSDK()
	if e != nil {
		t.Fatal("fixed SDK relay construction failed")
	}
	defer closeRelay()
	client := &http.Client{Transport: sdk.Clone(), Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	invoke := func(path string) (*http.Response, error) {
		r, _ := http.NewRequest(http.MethodPost, "https://trusted-compute.invalid"+path, strings.NewReader("{}"))
		return client.Do(r)
	}
	delivered, e := invoke("/dsg/v1/resource/normal-apply")
	if e != nil || delivered == nil {
		t.Fatal("cloned SDK did not deliver actual first reply")
	}
	_, _ = io.Copy(io.Discard, delivered.Body)
	_ = delivered.Body.Close()
	lost, e := invoke("/dsg/v1/resource/normal-apply")
	if e == nil || lost != nil || !fault.dropped.Load() || hits.Load() != 2 {
		t.Fatal("cloned SDK did not preserve real second-reply loss")
	}
	fault.holdObserve.Store(false)
	observed, e := invoke("/dsg/v1/resource/normal-observe")
	if e != nil || observed == nil || hits.Load() != 3 {
		t.Fatal("cloned SDK did not restore original observation")
	}
	_ = observed.Body.Close()
}

// 复用同次真实WAL验证合法读确认，不发送HTTP/Root/PG/VM命令。
func TestTrustedPG17OriginalReadyReadConfirmation(t *testing.T) {
	dir := os.Getenv("DSGA_G5_RECOVERY_ARTIFACT_DIR")
	if dir == "" {
		t.Fatal("original recovery artifacts required")
	}
	b, e := os.ReadFile(filepath.Join(dir, "before.json"))
	if e != nil {
		t.Fatal("original before artifact unavailable")
	}
	var wrapper struct {
		State journal.Model `json:"state"`
	}
	if json.Unmarshal(b, &wrapper) != nil {
		t.Fatal("original before model invalid")
	}
	b, e = os.ReadFile(filepath.Join(dir, "after.json"))
	if e != nil {
		t.Fatal("original after artifact unavailable")
	}
	var after journal.Model
	if json.Unmarshal(b, &after) != nil {
		t.Fatal("original after model invalid")
	}
	t.Run("actual-read-confirmed", func(t *testing.T) {
		if !trustedPG17OriginalReadyConfirmed(wrapper.State, after) {
			t.Fatal("same original completed GuestReady not recognized")
		}
	})
	t.Run("still-unknown", func(t *testing.T) {
		if trustedPG17OriginalReadyConfirmed(wrapper.State, wrapper.State) {
			t.Fatal("original Unknown was promoted")
		}
	})
	t.Run("different-key", func(t *testing.T) {
		changed := after.Clone()
		if changed.Active == nil {
			t.Fatal("expected original active artifact")
		}
		changed.Active.Request.Key.OperationID.Low++
		if trustedPG17OriginalReadyConfirmed(wrapper.State, changed) {
			t.Fatal("another key was adopted")
		}
	})
}

// 原关闭尚未完成时只读三次真实 Work2、Root 与 Runner 发布，未就绪不签降限。
func trustedPG17SQLBusyUnchanged(ctx context.Context, f *vmguard.NativePG17TrustedHostFixture, pg *compute.Client, root *resourceguard.Client, model journal.Model, request control.Request) (bool, error) {
	if model.Validate() != nil || model.Active == nil || model.Active.Request != request || len(model.Active.Steps) < 2 {
		return false, errors.New("DEV SQL busy original request changed")
	}
	step := model.Active.Steps[1]
	if step.Action != journal.Quiesce || step.Phase == journal.Planned {
		return false, nil
	}
	if step.Phase != journal.Unconfirmed || step.Observation != nil {
		return false, errors.New("DEV SQL busy Quiesce advanced before foreground drained")
	}
	var original budget.QuiesceCommand
	if json.Unmarshal(step.Command, &original) != nil {
		return false, errors.New("DEV SQL busy original closing command invalid")
	}
	var prepared budget.Prepared
	if json.Unmarshal(model.Active.Steps[0].Observation.Data, &prepared) != nil {
		return false, errors.New("DEV SQL busy prepared budget invalid")
	}
	samples := make([]map[string]any, 0, 3)
	for i := 0; i < 3; i++ {
		dsm, err := pg.DSMState(ctx)
		if err != nil {
			return false, err
		}
		c := dsm.Prepared.Connections
		if c.Work.Observed.AdmissionOpen {
			return false, nil
		}
		if !c.Acknowledged || !c.PublicConnectionsBlocked {
			return false, nil
		}
		if c.Work.ForegroundIdle || c.Work.FrontendExecutes == 0 {
			return false, errors.New("DEV real SQL did not retain foreground execution")
		}
		q := budget.Quiesced{ProtocolVersion: 1, Request: request, Close: original.Close, DSM: dsm, PGAggregate: original.PGAggregate}
		ready, err := q.ReadyFor(prepared, model.Owner.Epoch)
		if err != nil {
			return false, err
		}
		if ready {
			return false, errors.New("DEV executing SQL unexpectedly authorized Quiesce")
		}
		view, err := root.InspectState(ctx)
		if err != nil {
			return false, err
		}
		if err = original.PGAggregate.Match(view); err != nil {
			return false, err
		}
		if _, err = view.ConfirmedLimits(); err != nil {
			return false, err
		}
		_, _, pub, err := vmguard.ReadPublished(ctx, f.Core.CoreV1(), f.Store.Binding(), nil)
		if err != nil {
			return false, err
		}
		if pub.Actual == nil || pub.Actual.RequestedBytes != 8<<30 || pub.Actual.ActualBytes != 8<<30 {
			return false, errors.New("DEV SQL busy actual QMP memory changed")
		}
		fresh, err := f.Store.Load(ctx)
		if err != nil {
			return false, err
		}
		m := fresh.State()
		if m.Active == nil || m.Active.Request != request || m.Active.Steps[1].Phase != journal.Unconfirmed || !bytes.Equal(m.Active.Steps[1].Command, step.Command) {
			return false, errors.New("DEV SQL busy original Quiesce not retained")
		}
		for _, later := range m.Active.Steps[2:] {
			if later.Phase != journal.Planned || len(later.Command) != 0 || later.Observation != nil {
				return false, errors.New("DEV SQL busy dispatched a downstream effect")
			}
		}
		samples = append(samples, map[string]any{"dsm": dsm, "root": view, "runner_publication": pub, "state": m})
		if i < 2 {
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
	err := trustedDriverSave("fault-real-sql-busy", map[string]any{"case": "sql-busy-drain", "actual_samples": samples, "normal_apply_or_vm_shrink_dispatched": false, "scope": "actual PG SQL Work2 busy; no buffer-pin claim", "E": 0})
	return err == nil, err
}
