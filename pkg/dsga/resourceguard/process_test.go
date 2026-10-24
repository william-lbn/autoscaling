package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// 此处验证真实Mac日志/fsync/固定HTTP客户端与出生状态机；内核和exec明确为
// fixture，Linux UID/exe/cgroup实际读回和原位exec留目标平台，不算作本机通过。
func TestProcessBirthPersistsBeforeExecAndNeverAdoptsCallerPID(t *testing.T) {
	g, k, b, c := freshBootstrapFixture(t)
	ctx := context.Background()
	if _, err := g.Bootstrap(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Apply(ctx, c); err == nil {
		t.Fatal("request-supplied PID adopted without original birth")
	}
	p := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
	marker := errors.New("fixture exec did not occur")
	if err := g.launchWithBirth(ctx, nil, &p, func() error {
		var r record
		raw, err := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		if json.Unmarshal(raw, &r) != nil || r.Birth == nil || *r.Birth != p {
			t.Fatal("exec boundary reached before durable birth")
		}
		return marker
	}); !errors.Is(err, marker) {
		t.Fatal(err)
	}
	if _, err := g.InspectProcess(ctx); err == nil {
		t.Fatal("pre-exec record mistaken for actual PG")
	}
	k.mu.Lock()
	k.process = &p
	k.mu.Unlock()
	if got, err := g.InspectProcess(ctx); err != nil || got != p {
		t.Fatal("original live PG not confirmed", err)
	}
	wrong := c
	wrong.PostmasterPID++
	if _, err := g.Apply(ctx, wrong); err == nil {
		t.Fatal("another same-group process accepted as original Postmaster")
	}
	wrong = c
	wrong.PostmasterStartTicks++
	if _, err := g.Apply(ctx, wrong); err == nil {
		t.Fatal("reused PID accepted")
	}
	if _, err := g.Apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	k.mu.Lock()
	k.process = nil
	k.mu.Unlock()
	if _, err := g.InspectProcess(ctx); err == nil {
		t.Fatal("dead PG adopted")
	}
}

func TestProcessBirthHTTPAndClientUseOriginalRecord(t *testing.T) {
	h, key, g, k, c := resourceHTTPFixture(t)
	// HTTP夹具初始bare日志只用于协议测试；本用例按真正原创建重建独立fixture
	// 并经过Bootstrap+出生，不手写一个“已执行”的生产原意图。
	if err := os.Remove(filepath.Join(g.config.StateDir, "state.json")); err != nil {
		t.Fatal(err)
	}
	b := BootstrapCommand{ProtocolVersion: 1, Identity: c.Identity, IssuerEpoch: 1, BudgetCommitmentID: "bootstrap-budget", Limits: c.limits()}
	if _, err := g.Bootstrap(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	client := resourceClientFixture(t, h, c, resourceToken(t, h, key, nil), nil)
	if _, err := client.InspectProcess(context.Background()); err == nil {
		t.Fatal("missing original birth synthesized")
	}
	p := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
	if err := g.launchWithBirth(context.Background(), nil, &p, func() error { k.mu.Lock(); k.process = &p; k.mu.Unlock(); return nil }); err != nil {
		t.Fatal(err)
	}
	got, err := client.InspectProcess(context.Background())
	if err != nil || got != p {
		t.Fatal("root process fact changed by HTTP client", err)
	}
	if _, err := client.Apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if client.Pending() != nil {
		t.Fatal("confirmed original resource command retained unknown")
	}
}

func TestProcessBirthUIDAndInvocationRejectSpecialOrPrivilegedProcess(t *testing.T) {
	if processUID("Name:\tpostgres\nUid:\t1000\t1000\t1000\t1000\n", 1000) != nil {
		t.Fatal("ordinary PG UID rejected")
	}
	for _, s := range []string{"", "Uid:\t1000 1000 0 1000\n", "Uid:\t1000 1000 1000\n", "Uid:\t01000 1000 1000 1000\n", "Uid:\t1000 1000 1000 1000\nUid:\t1000 1000 1000 1000\n"} {
		if processUID(s, 1000) == nil {
			t.Fatal("invalid process privileges accepted")
		}
	}
	if server, err := launchInvocation([]string{"--sync-safekeepers"}); err != nil || server {
		t.Fatal("storage sync mistaken for Postmaster")
	}
	if server, err := launchInvocation([]string{"-D", "/pgdata", "-c", "shared_buffers=2048"}); err != nil || !server {
		t.Fatal("ordinary server invocation rejected")
	}
	for _, args := range [][]string{nil, {"--single"}, {"-D", "/pgdata", "--single"}, {"-D", "/pgdata", "--sync-safekeepers"}, {"-D", "/pgdata", "-C", "shared_buffers"}, {"-D", "/pgdata", "-c", "--single"}, {"-D", "/pgdata", "-p", "5432"}} {
		if _, err := launchInvocation(args); err == nil {
			t.Fatal("special mode acquired Postmaster birth", args)
		}
	}
}
