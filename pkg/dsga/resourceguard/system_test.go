package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// Mac上真实锁/WAL/fsync；kernel与进程迁移明确是模型，不能宣称执行了Linux。
func systemFixture(t *testing.T) (*Guard, *fixtureKernel, BootstrapCommand, Command) {
	t.Helper()
	g, k, b, c := freshBootstrapFixture(t)
	g.config.SystemCgroup = true
	k.id.SystemCgroupDevice = k.id.CgroupDevice
	k.id.SystemCgroupInode = 101
	b.Identity = k.id
	c.Identity = k.id
	b.Limits.SystemMemoryMaxBytes = 16 << 20
	c.SystemMemoryMaxBytes = 16 << 20
	k.e.System = &SystemEvidence{Scope: SystemChargedScope, MemoryMaxBytes: ^control.Uint64(0), SwapMaxBytes: ^control.Uint64(0), RootUserspaceCount: 3}
	return g, k, b, c
}
func fixturePlacement(k *fixtureKernel) func(context.Context) error {
	return func(context.Context) error {
		k.e.System.PID1Member = true
		k.e.System.ProcessCount = 3
		k.e.System.RootUserspaceCount = 0
		return nil
	}
}

func TestSystemPlacementDurableBeforeBirthAndImmutableThroughRuntime(t *testing.T) {
	g, k, b, c := systemFixture(t)
	ctx := context.Background()
	if _, err := g.Bootstrap(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := g.launch(ctx, func() error { t.Fatal("birth before placement"); return nil }); err == nil {
		t.Fatal("unconfirmed placement admitted birth")
	}
	v, err := g.InspectState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = v.ConfirmedLimits(); !errors.Is(err, ErrUnknown) {
		t.Fatal("quota confused with placement", err)
	}
	if _, err = g.placeSystem(ctx, fixturePlacement(k)); err != nil {
		t.Fatal(err)
	}
	// 新Guard必须从磁盘恢复确认，不能只依赖本进程内字段。
	g = &Guard{config: g.config, kernel: k}
	v, err = g.InspectState(ctx)
	if err != nil || !v.SystemPlacementObserved {
		t.Fatal(err)
	}
	if _, err = v.ConfirmedLimits(); err != nil {
		t.Fatal(err)
	}
	if _, err = g.placeSystem(ctx, func(context.Context) error { t.Fatal("confirmed placement replayed"); return nil }); err != nil {
		t.Fatal(err)
	}
	birth := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
	if err = g.launchWithBirth(ctx, nil, &birth, func() error { k.process = &birth; return nil }); err != nil {
		t.Fatal(err)
	}
	k.e.CgroupPopulated = true
	if _, err = g.Apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	before := k.writes
	for _, cap := range []control.Uint64{0, 8 << 20, 24 << 20} {
		next := c
		next.Revision++
		next.SystemMemoryMaxBytes = cap
		if _, err = g.Apply(ctx, next); err == nil || k.writes != before {
			t.Fatal("runtime changed immutable system cap", cap, err)
		}
	}
	k.e.System.RootUserspaceCount = 1
	next := c
	next.Revision++
	if _, err = g.Apply(ctx, next); err == nil || k.writes != before {
		t.Fatal("placement drift reached resource write")
	}
	k.e.System.RootUserspaceCount = 0
	k.e.System.MemoryMaxBytes++
	if _, err = g.Observe(ctx, c); err == nil {
		t.Fatal("drifted cap confirmed retained command")
	}
	if _, err = g.placeSystem(ctx, fixturePlacement(k)); err == nil {
		t.Fatal("runtime repaired boot assignment")
	}
}
func TestSystemPlacementPartialAndPersistenceFailureKeepOriginalIntent(t *testing.T) {
	for _, mode := range []string{"partial", "persistence", "readback"} {
		t.Run(mode, func(t *testing.T) {
			g, k, b, _ := systemFixture(t)
			ctx := context.Background()
			if _, err := g.Bootstrap(ctx, b); err != nil {
				t.Fatal(err)
			}
			calls := 0
			_, err := g.placeSystem(ctx, func(context.Context) error {
				calls++
				k.e.System.PID1Member = true
				k.e.System.ProcessCount = 1
				if mode == "partial" {
					return fmt.Errorf("fixture partial migration")
				}
				k.e.System.RootUserspaceCount = 0
				if mode == "persistence" {
					return os.Chmod(g.config.StateDir, 0500)
				}
				k.e.System.SwapCurrentBytes = 1
				return nil
			})
			if mode == "persistence" {
				if e := os.Chmod(g.config.StateDir, 0700); e != nil {
					t.Fatal(e)
				}
			}
			if !errors.Is(err, ErrUnknown) || calls != 1 {
				t.Fatal("partial placement not unknown", err)
			}
			raw, e := os.ReadFile(filepath.Join(g.config.StateDir, "state.json"))
			if e != nil {
				t.Fatal(e)
			}
			var r record
			if e = json.Unmarshal(raw, &r); e != nil || r.Bootstrap.SystemPlacementObserved || !reflect.DeepEqual(r.Bootstrap.Original, b) {
				t.Fatal("original intent/unknown lost", e)
			}
			if e = g.launch(ctx, func() error { t.Fatal("birth after partial placement"); return nil }); e == nil {
				t.Fatal("unknown admitted birth")
			}
			k.e.System.SwapCurrentBytes = 0
			g = &Guard{config: g.config, kernel: k}
			if _, e = g.placeSystem(ctx, fixturePlacement(k)); e != nil {
				t.Fatal("same original scope could not recover", e)
			}
		})
	}
}
func TestSystemScopeStrictLegacyAndUnsupportedSources(t *testing.T) {
	g, k, b, c := systemFixture(t)
	ctx := context.Background()
	legacy := c
	legacy.SystemMemoryMaxBytes = 0
	legacy.Identity.SystemCgroupDevice = 0
	legacy.Identity.SystemCgroupInode = 0
	raw, _ := json.Marshal(legacy)
	if strings.Contains(string(raw), "system_") {
		t.Fatal("legacy wire changed")
	}
	if legacy.Validate() != nil {
		t.Fatal("legacy source rejected")
	}
	bad := b
	bad.Limits.SystemMemoryMaxBytes = bad.Limits.OutsideUpperBytes + 1
	if bad.Validate() == nil {
		t.Fatal("outside budget exceeded")
	}
	bad = b
	bad.Identity.SystemCgroupInode = 0
	if bad.Validate() == nil {
		t.Fatal("partial system identity")
	}
	source := sourceFixture(c)
	source.Config.SystemCgroup = true
	if source.Validate() != nil {
		t.Fatal("new source rejected")
	}
	source.Config.SystemCgroup = false
	if source.Validate() == nil {
		t.Fatal("mode/cap mismatch")
	}
	source.Config.SystemCgroup = true
	source.Limits.SystemMemoryMaxBytes++
	if source.Validate() == nil {
		t.Fatal("unaligned source accepted")
	}
	k.e.System.ProcessCount = 1
	if _, err := g.Bootstrap(ctx, b); err == nil || k.writes != 0 {
		t.Fatal("initial cap applied to populated system group")
	}
	k.e.System.ProcessCount = 0
	if _, err := g.Bootstrap(ctx, b); err != nil {
		t.Fatal(err)
	}
	valid := *k.e.System
	for _, mutate := range []func(*SystemEvidence){func(s *SystemEvidence) { s.Scope = "full-system-outside" }, func(s *SystemEvidence) { s.MemoryCurrentBytes = s.MemoryMaxBytes + 1 }, func(s *SystemEvidence) { s.SwapMaxBytes = 1 }} {
		copy := valid
		mutate(&copy)
		k.e.System = &copy
		if _, err := g.ObserveBootstrap(ctx, b); err == nil {
			t.Fatal("bad system evidence accepted")
		}
	}
	encoded, _ := json.Marshal(valid)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(encoded, &fields)
	for key := range fields {
		var copy map[string]json.RawMessage
		_ = json.Unmarshal(encoded, &copy)
		delete(copy, key)
		missing, _ := json.Marshal(copy)
		var decoded SystemEvidence
		if json.Unmarshal(missing, &decoded) == nil {
			t.Fatal("missing system field accepted", key)
		}
	}
}

type placementModel struct {
	list   []systemProcess
	calls  []int
	stuck  bool
	cancel context.CancelFunc
}

func (p *placementModel) rootUserspace() ([]systemProcess, error) {
	return append([]systemProcess(nil), p.list...), nil
}
func (p *placementModel) moveSystem(proc systemProcess) error {
	p.calls = append(p.calls, proc.pid)
	if p.cancel != nil {
		p.cancel()
	}
	if !p.stuck {
		var next []systemProcess
		for _, q := range p.list {
			if q.pid != proc.pid {
				next = append(next, q)
			}
		}
		p.list = next
	}
	return nil
}
func TestSystemBootPlacementOrdersDeduplicatesAndBoundsCancellation(t *testing.T) {
	p := &placementModel{list: []systemProcess{{7, 1}, {1, 0}, {7, 1}, {4, 2}}}
	if err := moveBootUserspace(context.Background(), p); err != nil || !reflect.DeepEqual(p.calls, []int{1, 4, 7}) {
		t.Fatal(p.calls, err)
	}
	p = &placementModel{list: []systemProcess{{1, 0}}, stuck: true}
	if err := moveBootUserspace(context.Background(), p); err == nil || len(p.calls) != 4 {
		t.Fatal("unbounded boot retry", p.calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p = &placementModel{list: []systemProcess{{2, 1}, {1, 0}}, cancel: cancel}
	if err := moveBootUserspace(ctx, p); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(p.calls, []int{1}) {
		t.Fatal("cancel allowed later migration", p.calls, err)
	}
	p = &placementModel{list: []systemProcess{{2, 1}, {2, 2}}}
	if err := moveBootUserspace(context.Background(), p); err == nil {
		t.Fatal("changed PID identity accepted")
	}
}
func TestSystemBootStatDistinguishesKernelThreadsAndZeroTickInit(t *testing.T) {
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = "S"
	text := "1 (init (spaces)) " + strings.Join(fields, " ")
	p, k, err := bootProcessStat(text, 1)
	if err != nil || k || p.ticks != 0 {
		t.Fatal(p, k, err)
	}
	fields[6] = "2097152"
	_, k, err = bootProcessStat("2 (kthreadd) "+strings.Join(fields, " "), 2)
	if err != nil || !k {
		t.Fatal(k, err)
	}
	for _, bad := range []string{"2 (wrong pid) " + strings.Join(fields, " "), "1 (short) S", text + strings.Repeat(" ", 64*1024)} {
		if _, _, err = bootProcessStat(bad, 1); err == nil {
			t.Fatal("invalid boot stat accepted")
		}
	}
}

func TestSystemBootstrapPartialCapRetainsUnknownAndRequiresOriginalReplay(t *testing.T) {
	g, k, b, _ := systemFixture(t)
	ctx := context.Background()
	k.fail = true
	if _, err := g.Bootstrap(ctx, b); !errors.Is(err, ErrUnknown) || k.writes != 1 || k.e.System.MemoryMaxBytes != b.Limits.SystemMemoryMaxBytes {
		t.Fatal("partial cap not retained", err)
	}
	if _, err := g.Bootstrap(ctx, b); !errors.Is(err, ErrUnknown) || k.writes != 1 {
		t.Fatal("ordinary retry rewrote unknown cap", err)
	}
	if _, err := g.placeSystem(ctx, func(context.Context) error { t.Fatal("unconfirmed quota migrated userspace"); return nil }); err == nil {
		t.Fatal("partial quota reached migration")
	}
	bad := b
	bad.Limits.SystemMemoryMaxBytes = 8 << 20
	if _, err := g.ReplayBootstrap(ctx, bad); err == nil || k.writes != 1 {
		t.Fatal("changed cap replay admitted")
	}
	k.fail = false
	if _, err := g.ReplayBootstrap(ctx, b); err != nil || k.writes != 2 {
		t.Fatal("original explicit replay failed", err)
	}
	if _, err := g.placeSystem(ctx, fixturePlacement(k)); err != nil {
		t.Fatal(err)
	}
}
