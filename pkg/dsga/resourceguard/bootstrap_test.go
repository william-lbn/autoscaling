package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func freshBootstrapFixture(t *testing.T) (*Guard, *fixtureKernel, BootstrapCommand, Command) {
	t.Helper()
	g, k, c := fixtureGuard(t)
	// 仅清理本用例刚创建的测试初始记录，模拟独立原VM尚未初始化；生产无此清理。
	if err := os.Remove(filepath.Join(g.config.StateDir, "state.json")); err != nil {
		t.Fatal(err)
	}
	b := BootstrapCommand{ProtocolVersion: 1, Identity: c.Identity, IssuerEpoch: c.IssuerEpoch, BudgetCommitmentID: "bootstrap-budget", Limits: c.limits()}
	return g, k, b, c
}

func TestBoundedBootstrapHoldsActualLimitsBeforePGBirth(t *testing.T) {
	g, k, b, c := freshBootstrapFixture(t)
	ctx := context.Background()
	if _, err := g.ObserveBootstrap(ctx, b); !errors.Is(err, ErrUnknown) {
		t.Fatal("absent bootstrap is not unknown", err)
	}
	if _, err := g.Bootstrap(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Bootstrap(ctx, b); err != nil || k.writes != 1 {
		t.Fatal("confirmed bootstrap rewrote limits", err)
	}
	called := 0
	marker := errors.New("fixture exec boundary")
	p := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
	if err := g.launchWithBirth(ctx, nil, &p, func() error { k.mu.Lock(); k.process = &p; k.mu.Unlock(); called++; return marker }); !errors.Is(err, marker) || called != 1 {
		t.Fatal("confirmed limits did not reach birth boundary", err)
	}
	k.e.MemoryMaxBytes++
	if err := g.launch(ctx, func() error { called++; return nil }); err == nil || called != 1 {
		t.Fatal("drifted actual limits allowed birth")
	}
	k.e.MemoryMaxBytes--
	k.e.CgroupPopulated = true
	if err := g.launch(ctx, func() error { called++; return nil }); err == nil || called != 1 {
		t.Fatal("nonempty group allowed another birth")
	}
	k.e.CgroupPopulated = false
	// 运行期已改变额度后，下一次出生必须核验当前Held，而不是原较大启动额度。
	c.MemoryMaxBytes = 48 << 20
	if _, err := g.Apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := g.launch(ctx, func() error { called++; return marker }); !errors.Is(err, marker) || called != 2 {
		t.Fatal("current held constraints were replaced with old bootstrap", err)
	}
	if _, err := g.ObserveBootstrap(ctx, b); err == nil {
		t.Fatal("runtime started but old bootstrap still returned success")
	}
	next := c
	next.Revision++
	next.MemoryMaxBytes = 40 << 20
	next.HugePools = append([]HugePool(nil), c.HugePools...)
	next.HugePools[0].Pages = 4
	k.fail = true
	if _, err := g.Apply(ctx, next); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	if err := g.launch(ctx, func() error { called++; return nil }); err == nil || called != 2 {
		t.Fatal("unknown resource write allowed fresh PG birth")
	}
}

func TestBoundedBootstrapUnknownRequiresOriginalReadbackOrReplay(t *testing.T) {
	g, k, b, _ := freshBootstrapFixture(t)
	ctx := context.Background()
	k.fail = true
	if _, err := g.Bootstrap(ctx, b); !errors.Is(err, ErrUnknown) {
		t.Fatal("partial write not unknown", err)
	}
	if _, err := g.Bootstrap(ctx, b); !errors.Is(err, ErrUnknown) || k.writes != 1 {
		t.Fatal("ordinary bootstrap retried unknown", err)
	}
	if err := g.launch(ctx, func() error { t.Fatal("unknown bootstrap reached exec"); return nil }); err == nil {
		t.Fatal("unknown bootstrap allowed birth")
	}
	other := b
	other.BudgetCommitmentID = "another-bootstrap"
	if _, err := g.ReplayBootstrap(ctx, other); err == nil {
		t.Fatal("changed bootstrap identity replayed")
	}
	k.fail = false
	if _, err := g.ReplayBootstrap(ctx, b); err != nil {
		t.Fatal(err)
	}
	// 完成记录丢失：保持磁盘原请求与真实限额；新owner仅查询，不能更改旧epoch写。
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.load()
	if err != nil {
		t.Fatal(err)
	}
	r.Bootstrap.Applied = false
	if err := s.save(r); err != nil {
		t.Fatal(err)
	}
	s.close()
	if _, err := g.Fence(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := g.ReplayBootstrap(ctx, b); err == nil {
		t.Fatal("new owner wrote old bootstrap epoch")
	}
	if _, err := g.ObserveBootstrap(ctx, b); err != nil || k.writes != 2 {
		t.Fatal("original readback changed kernel", err)
	}
	if err := g.launch(ctx, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedBootstrapRefusesExistingPGOrBareJournal(t *testing.T) {
	g, k, b, _ := freshBootstrapFixture(t)
	ctx := context.Background()
	k.e.CgroupPopulated = true
	if _, err := g.Bootstrap(ctx, b); err == nil || k.writes != 0 {
		t.Fatal("live group was treated as original empty bootstrap")
	}
	if _, err := os.Stat(filepath.Join(g.config.StateDir, "state.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unsafe precheck created original journal", err)
	}
	k.e.CgroupPopulated = false
	if _, err := g.Initialize(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Bootstrap(ctx, b); err == nil {
		t.Fatal("bare old journal upgraded as original creation")
	}
	if err := g.launch(ctx, func() error { t.Fatal("bare journal reached exec"); return nil }); err == nil {
		t.Fatal("floor alone authorized PG birth")
	}
	data, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	var parsed BootstrapCommand
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}
}
