package localcontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/stretchr/testify/require"
)

// 只测正式HTTP入口的排队/执行共用ctx，不模拟原生恢复或延长领域租约。
func TestLocalControlLifecycleRecoveryHasDedicatedBoundedContext(t *testing.T) {
	for _, tc := range []struct {
		action control.CoordinationAction
		want   time.Duration
	}{
		{control.CoordinationRecoverServiceLifecycle, 300 * time.Second},
		{control.CoordinationState, 130 * time.Second},
		{control.CoordinationReopenService, 130 * time.Second},
		{control.CoordinationRearmService, 130 * time.Second},
		{control.CoordinationObserveServiceReopen, 130 * time.Second},
	} {
		t.Run(string(tc.action), func(t *testing.T) {
			command := commandFixture()
			command.Action = tc.action
			if tc.action != control.CoordinationState {
				command.Selector = strings.Repeat("a", 64)
			}
			require.NoError(t, command.Validate())
			body, err := json.Marshal(command)
			require.NoError(t, err)
			registry := NewRegistry()
			registry.entries[command.Instance.VMUID] = &transportEndpoint{done: make(chan struct{}), call: func(ctx context.Context, got control.CoordinationCommand) (owner.CoordinationResult, error) {
				require.Equal(t, command, got)
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.WithinDuration(t, time.Now().Add(tc.want), deadline, time.Second, "selected management deadline includes endpoint queue and execution")
				return owner.CoordinationResult{}, nil
			}}
			r := httptest.NewRequest(http.MethodPost, CommandPath, bytes.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			newHandler(registry).ServeHTTP(w, r)
			require.Equal(t, http.StatusOK, w.Code)
		})
	}
}

func TestLocalControlRecoveryKeepsCallerDeadlineAndCancellation(t *testing.T) {
	for _, action := range []control.CoordinationAction{control.CoordinationRecoverServiceLifecycle, control.CoordinationState} {
		t.Run(string(action), func(t *testing.T) {
			command := commandFixture()
			command.Action = action
			if action == control.CoordinationRecoverServiceLifecycle {
				command.Selector = strings.Repeat("b", 64)
			}
			require.NoError(t, command.Validate())
			body, err := json.Marshal(command)
			require.NoError(t, err)
			parentDeadline := time.Now().Add(45 * time.Second)
			parent, cancel := context.WithDeadline(context.Background(), parentDeadline)
			defer cancel()
			entered := make(chan context.Context, 1)
			observed := make(chan error, 1)
			registry := NewRegistry()
			registry.entries[command.Instance.VMUID] = &transportEndpoint{done: make(chan struct{}), call: func(ctx context.Context, _ control.CoordinationCommand) (owner.CoordinationResult, error) {
				entered <- ctx
				<-ctx.Done()
				observed <- ctx.Err()
				return owner.CoordinationResult{}, ctx.Err()
			}}
			r := httptest.NewRequest(http.MethodPost, CommandPath, bytes.NewReader(body)).WithContext(parent)
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			finished := make(chan struct{})
			go func() { defer close(finished); newHandler(registry).ServeHTTP(w, r) }()
			select {
			case got := <-entered:
				deadline, ok := got.Deadline()
				require.True(t, ok)
				require.Equal(t, parentDeadline, deadline)
			case <-time.After(2 * time.Second):
				t.Fatal("request did not reach original endpoint")
			}
			cancel()
			select {
			case err := <-observed:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(2 * time.Second):
				t.Fatal("caller cancellation did not reach endpoint")
			}
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Fatal("cancelled handler did not finish")
			}
			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			require.Contains(t, w.Body.String(), "inspect_original_journal")
		})
	}
}

func TestLocalControlRecoveryStillRejectsInvalidActionsBeforeSubmit(t *testing.T) {
	registry := NewRegistry()
	calls := 0
	registry.entries[commandFixture().Instance.VMUID] = &transportEndpoint{done: make(chan struct{}), call: func(context.Context, control.CoordinationCommand) (owner.CoordinationResult, error) {
		calls++
		return owner.CoordinationResult{}, nil
	}}
	handler := newHandler(registry)
	for _, tc := range []struct {
		action   control.CoordinationAction
		selector string
	}{
		{"recover_service_lifecycle_extra", strings.Repeat("a", 64)},
		{control.CoordinationRecoverServiceLifecycle, ""},
		{control.CoordinationRecoverServiceLifecycle, "not-a-sha"},
	} {
		command := commandFixture()
		command.Action = tc.action
		command.Selector = tc.selector
		body, err := json.Marshal(command)
		require.NoError(t, err)
		r := httptest.NewRequest(http.MethodPost, CommandPath, bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		require.Equal(t, http.StatusBadRequest, w.Code)
	}
	require.Zero(t, calls)
}

func TestLocalControlWriteDeadlineCoversRecoveryAndKeepsBounds(t *testing.T) {
	server, err := Listen(filepath.Join(socketDir(t), "recovery.sock"), NewRegistry())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	require.Equal(t, 315*time.Second, server.http.WriteTimeout)
	require.Equal(t, 10*time.Second, server.http.ReadTimeout)
	require.Equal(t, 5*time.Second, server.http.ReadHeaderTimeout)
	require.Equal(t, 15*time.Second, server.http.IdleTimeout)
	require.Equal(t, 8*1024, server.http.MaxHeaderBytes)
	require.Equal(t, 16, maxInFlight)
	require.Equal(t, 16*1024, maxBody)
}
