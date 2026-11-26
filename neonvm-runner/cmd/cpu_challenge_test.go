package main

import (
	"context"
	"encoding/json"
	"errors"
	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/api/runnercpu"
	"go.uber.org/zap"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCPUReadChallengeBindsRealReadAndDoesNotChangeQuotaOrGeneration(t *testing.T) {
	state, group, _ := managedCPUFixture()
	before := state.intent
	nonce := strings.Repeat("a", 32)
	response := httptest.NewRecorder()
	handleCPUCurrent(zap.NewNop(), response, httptest.NewRequest(http.MethodGet, "/cpu_current?observation_nonce="+nonce, nil), managedCPUCallbacks(state).get)
	var got api.VCPUCgroup
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &got) != nil || got.Managed == nil ||
		got.Managed.ObservationNonce != nonce || got.Managed.ObservedBootTimeNS == 0 || got.Managed.Validate() != nil {
		t.Fatalf("challenge not backed by current CPU sources: %d %s", response.Code, response.Body.String())
	}
	if len(group.writes) != 0 || state.intent.Generation != before.Generation || state.intent.TargetMillicores != before.TargetMillicores {
		t.Fatal("read-only challenge wrote quota or raised original intent floor")
	}
	calls := 0
	for _, query := range []string{"", strings.Repeat("a", 31), strings.Repeat("A", 32), strings.Repeat("0", 32),
		nonce + "&observation_nonce=" + nonce, strings.Repeat("a", 4097)} {
		response = httptest.NewRecorder()
		handleCPUCurrent(zap.NewNop(), response, httptest.NewRequest(http.MethodGet, "/cpu_current?observation_nonce="+query, nil),
			func(*zap.Logger) (*api.VCPUCgroup, int, error) { calls++; return nil, 500, errors.New("must not read") })
		if response.Code != 400 {
			t.Fatalf("noncanonical nonce accepted %d", response.Code)
		}
	}
	if calls != 0 {
		t.Fatal("invalid challenge reached original source")
	}
}

func TestCPUExactDurablePendingCanFinishAfterCurrentVMGenerationAdvances(t *testing.T) {
	state, group, online := managedCPUFixture()
	group.writeLost = true
	if _, err := state.change(context.Background(), cpuCommand(2, 1500)); err == nil {
		t.Fatal("lost write accepted")
	}
	group.writeLost = false
	// 新原VM代次前进。只有已fsync的旧intent可恢复；只读Node来源不能自授新目标。
	state.authorize = func(context.Context, runnercpu.Command, vmv1.MilliCPU) error {
		return errors.New("old command no longer equals current VM target")
	}
	before := len(group.writes)
	if _, err := state.change(context.Background(), cpuCommand(3, 500)); err == nil || len(group.writes) != before {
		t.Fatal("new command replaced pending")
	}
	if _, err := state.change(context.Background(), cpuCommand(2, 1500)); err != nil || *online != 2000 || state.intent.Pending {
		t.Fatal("exact original durable pending failed", err)
	}
	if _, err := state.change(context.Background(), cpuCommand(2, 1500)); err == nil {
		t.Fatal("settled old command bypassed current VM authority")
	}
	state.intent.Pending = true
	state.verifyCommitment = func(context.Context) (runnercpu.NodeCommitment, error) {
		return runnercpu.NodeCommitment{}, errors.New("original hold lost")
	}
	before = len(group.writes)
	if _, err := state.change(context.Background(), cpuCommand(2, 1500)); err == nil || len(group.writes) != before {
		t.Fatal("missing original hold allowed recovery")
	}
	state, group, _ = managedCPUFixture()
	before = len(group.writes)
	if _, err := state.change(context.Background(), cpuCommand(2, 250)); err == nil || len(group.writes) != before {
		t.Fatal("target below frozen minimum received quota")
	}
}
