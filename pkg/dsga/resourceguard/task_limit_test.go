package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func taskFixture(t *testing.T) (*Guard, *fixtureKernel, BootstrapCommand, Command) {
	t.Helper()
	g, k, b, c := freshBootstrapFixture(t)
	b.Limits.PidsMaxTasks = 64
	c.PidsMaxTasks = 64
	// 启动前真实pids.max可为max；Read先返回原文件，不能由请求伪造读回值。
	k.e.TaskLimit = &TaskLimitEvidence{MaxTasks: ^control.Uint64(0)}
	return g, k, b, c
}

func TestFiniteTasksBootstrapWALUnknownAndOriginalReadback(t *testing.T) {
	g, k, b, _ := taskFixture(t)
	ctx := context.Background()
	k.fail = true
	if _, err := g.Bootstrap(ctx, b); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	if k.writes != 1 || k.e.TaskLimit.MaxTasks != 64 {
		t.Fatal("first partial task effect missing")
	}
	raw, err := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var held record
	if json.Unmarshal(raw, &held) != nil || held.Bootstrap.Applied || held.Bootstrap.Original.Limits.PidsMaxTasks != 64 {
		t.Fatal("original finite intent lost")
	}
	if _, err = g.Bootstrap(ctx, b); !errors.Is(err, ErrUnknown) || k.writes != 1 {
		t.Fatal("unknown bootstrap silently rewrote", err)
	}
	changed := b
	changed.Limits.PidsMaxTasks++
	if _, err = g.ReplayBootstrap(ctx, changed); err == nil || k.writes != 1 {
		t.Fatal("changed replay admitted")
	}
	k.fail = false
	// 原第一项写已生效，其他条件由夹具显式补成已落实，原查询才可完成；不补写内核。
	k.e.SwapMaxBytes = 0
	k.e.HugeOvercommitDisabled = true
	k.e.HugeSurplusPages = 0
	k.e.HugePools = append([]HugePool(nil), b.Limits.HugePools...)
	k.e.WholePoolUpperBytes = 16 << 20
	if _, err = g.ObserveBootstrap(ctx, b); err != nil || k.writes != 1 {
		t.Fatal("original readback failed", err)
	}
}

func TestFiniteTasksFreezeAcrossRuntimeAndRecovery(t *testing.T) {
	g, k, b, c := taskFixture(t)
	ctx := context.Background()
	if _, err := g.Bootstrap(ctx, b); err != nil {
		t.Fatal(err)
	}
	birth := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
	stop := errors.New("fixture exec boundary")
	if err := g.launchWithBirth(ctx, nil, &birth, func() error { k.process = &birth; return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	k.e.CgroupPopulated = true
	k.e.TaskLimit.CurrentTasks = 4
	for _, n := range []control.Uint64{0, 63, 65} {
		next := c
		next.PidsMaxTasks = n
		if _, err := g.Apply(ctx, next); err == nil || k.writes != 1 {
			t.Fatal("changed cap reached kernel", n, err)
		}
	}
	if _, err := g.Apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	view, err := g.InspectState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if limits, err := view.ConfirmedLimits(); err != nil || limits.PidsMaxTasks != 64 {
		t.Fatal("held cap lost", err)
	}
	// 重建Guard仅复用真实磁盘状态，不能借新对象/owner把有限来源洗成0。
	restored := &Guard{config: g.config, kernel: k}
	if _, err = restored.Fence(ctx, 2); err != nil {
		t.Fatal(err)
	}
	next := c
	next.IssuerEpoch = 2
	next.Revision++
	if _, err = restored.Apply(ctx, next); err != nil {
		t.Fatal(err)
	}
	k.e.TaskLimit.CurrentTasks = 65
	writes := k.writes
	tooSoon := next
	tooSoon.Revision++
	if _, err = restored.Apply(ctx, tooSoon); err == nil || k.writes != writes {
		t.Fatal("excess tasks reached another resource write", err)
	}
	if _, err = restored.Observe(ctx, next); !errors.Is(err, ErrUnknown) {
		t.Fatal("migration excess certified", err)
	}
	if _, err = restored.InspectState(ctx); err != nil {
		t.Fatal(err)
	} // state可查，Confirmed不可通过。
	view, err = restored.InspectState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = view.ConfirmedLimits(); !errors.Is(err, ErrUnknown) {
		t.Fatal("excess limits confirmed", err)
	}
	k.e.CgroupPopulated = false
	k.e.TaskLimit.CurrentTasks = 1
	called := false
	if err = restored.launch(ctx, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("unreleased task permitted new birth")
	}
}

func TestFiniteTasksStrictEvidenceAndLegacyEncoding(t *testing.T) {
	g, k, c := fixtureGuard(t)
	if _, err := g.Apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	valid, err := k.Read(c)
	if err != nil {
		t.Fatal(err)
	}
	l := c.limits()
	raw, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "pids_max_tasks") {
		t.Fatal("legacy bytes changed")
	}
	var legacy Limits
	if err = json.Unmarshal(raw, &legacy); err != nil || legacy.PidsMaxTasks != 0 {
		t.Fatal(err)
	}
	raw, err = json.Marshal(k.e)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "task_limit") {
		t.Fatal("legacy evidence gained field")
	}
	l.PidsMaxTasks = MaxPidsTasks + 1
	if l.Validate() == nil {
		t.Fatal("unsupported finite range accepted")
	}
	c.PidsMaxTasks = 64
	cases := []struct {
		name  string
		tasks *TaskLimitEvidence
	}{
		{"absent", nil}, {"zero-max", &TaskLimitEvidence{}}, {"unlimited", &TaskLimitEvidence{MaxTasks: ^control.Uint64(0)}},
		{"drift", &TaskLimitEvidence{MaxTasks: 65, CurrentTasks: 4}}, {"exceeded", &TaskLimitEvidence{MaxTasks: 64, CurrentTasks: 65}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := valid
			e.TaskLimit = tc.tasks
			if e.ValidateFor(c) == nil {
				t.Fatal("invalid task evidence accepted")
			}
		})
	}
	e := valid
	e.TaskLimit = &TaskLimitEvidence{MaxTasks: 64, CurrentTasks: 4}
	if err = e.ValidateFor(c); err != nil {
		t.Fatal(err)
	}
	c.PidsMaxTasks = 0
	if e.ValidateFor(c) == nil {
		t.Fatal("bounded evidence attached to legacy source")
	}
	for _, s := range []string{`{}`, `{"max_tasks":"64"}`, `{"max_tasks":"64","current_tasks":null}`, `{"max_tasks":"64","current_tasks":"4","other":true}`, `{"max_tasks":"64","max_tasks":"64","current_tasks":"4"}`} {
		var fact TaskLimitEvidence
		if json.Unmarshal([]byte(s), &fact) == nil {
			t.Fatal("malformed task evidence accepted", s)
		}
	}
}

func TestFiniteTasksControllerSelection(t *testing.T) {
	for _, tc := range []struct {
		name, available, want string
		n                     control.Uint64
		ok                    bool
	}{
		{"legacy", "memory", "+memory\n", 0, true}, {"finite", "cpu pids memory", "+memory +pids\n", 64, true},
		{"missing-pids", "cpu memory", "", 64, false}, {"missing-memory", "cpu pids", "", 64, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := requiredControllers(tc.available, Limits{PidsMaxTasks: tc.n})
			if (err == nil) != tc.ok || got != tc.want {
				t.Fatal(got, err)
			}
		})
	}
}
