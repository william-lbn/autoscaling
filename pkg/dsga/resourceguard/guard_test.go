package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 仅协议夹具：文件锁/写前日志/fsync是真实Mac文件操作，下面的内核明确是假。
// 本测试不证明cgroup、HugeTLB、排空、真实物理回收或Linux内核写语义。
type fixtureKernel struct {
	mu       sync.Mutex
	id       Identity
	e        Evidence
	writes   int
	fail     bool
	entered  chan struct{}
	release  chan struct{}
	stateDir string
}

func (k *fixtureKernel) Identity() (Identity, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.id, nil
}
func (k *fixtureKernel) Read(c Command) (Evidence, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e := k.e
	e.ProtocolVersion = 1
	e.Identity = k.id
	e.HugePools = append([]HugePool(nil), e.HugePools...)
	return e, nil
}
func (k *fixtureKernel) Apply(c Command) error {
	// 每次外部副作用之前，必须已能从磁盘读取完整原命令及未知状态。
	raw, err := os.ReadFile(filepath.Join(k.stateDir, "state.json"))
	if err != nil {
		return err
	}
	var r record
	if err := json.Unmarshal(raw, &r); err != nil {
		return err
	}
	if r.Applied || r.Original == nil || !reflect.DeepEqual(*r.Original, c) {
		return fmt.Errorf("external write preceded durable original intent")
	}
	if k.entered != nil {
		close(k.entered)
		<-k.release
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.writes++
	k.e.MemoryMaxBytes = c.MemoryMaxBytes
	if k.fail {
		return fmt.Errorf("fixture failure after first effect")
	}
	k.e.SwapMaxBytes = 0
	k.e.HugePools = append([]HugePool(nil), c.HugePools...)
	k.e.HugeOvercommitDisabled = true
	k.e.HugeSurplusPages = 0
	k.e.WholePoolUpperBytes = 0
	for _, p := range c.HugePools {
		k.e.WholePoolUpperBytes += p.PageSizeKB * 1024 * p.Pages
	}
	return nil
}

func fixtureGuard(t *testing.T) (*Guard, *fixtureKernel, Command) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	id := Identity{VMUID: "vm-one", ComputeInstanceID: "compute-one", GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 100}
	c := Command{ProtocolVersion: 1, Identity: id, IssuerEpoch: 1, Revision: 1, BudgetCommitmentID: "budget-one", PostmasterPID: 123, PostmasterStartTicks: 123456, MemoryMaxBytes: 64 << 20, OutsideUpperBytes: 32 << 20, HugePools: []HugePool{{PageSizeKB: 2048, Pages: 8}, {PageSizeKB: 1048576, Pages: 0}}}
	k := &fixtureKernel{id: id, stateDir: dir, e: Evidence{Identity: id, MemoryCurrentBytes: 16 << 20, MemoryMaxBytes: 128 << 20, GuestMemTotalBytes: 256 << 20, SwapMaxBytes: control.Uint64(math.MaxUint64), HugePools: []HugePool{{PageSizeKB: 2048, Pages: 16}, {PageSizeKB: 1048576, Pages: 0}}}}
	g := &Guard{config: Config{VMUID: id.VMUID, ComputeInstanceID: id.ComputeInstanceID, StateDir: dir}, kernel: k}
	if _, err := g.Initialize(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	return g, k, c
}

func TestResourceGuardDurableUnknownAndExplicitOriginalReplay(t *testing.T) {
	g, k, c := fixtureGuard(t)
	ctx := context.Background()
	k.fail = true
	if _, err := g.Apply(ctx, c); err == nil {
		t.Fatal("partial kernel failure accepted")
	}
	// 真实重建客户端；原意图/floor必须从原目录恢复，不能借重启重置状态。
	g = &Guard{config: g.config, kernel: k}
	if _, err := g.Observe(ctx, c); !errors.Is(err, ErrUnknown) {
		t.Fatalf("partial write is not an unknown: %v", err)
	}
	if _, err := g.Apply(ctx, c); !errors.Is(err, ErrUnknown) || k.writes != 1 {
		t.Fatalf("ordinary retry resent unknown: %v", err)
	}
	other := c
	other.BudgetCommitmentID = "another-budget"
	if _, err := g.ReplayOriginal(ctx, other); err == nil {
		t.Fatal("changed original replayed")
	}
	k.fail = false
	if _, err := g.ReplayOriginal(ctx, c); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Apply(ctx, c); err != nil || k.writes != 2 {
		t.Fatalf("confirmed exact replay wrote again: %v", err)
	}
	// 新PG进程只能通过下一完整revision更新；资源epoch及既有约束不随PG变化。
	next := c
	next.Revision = 2
	next.PostmasterPID = 124
	next.PostmasterStartTicks++
	if _, err := g.Apply(ctx, next); err != nil {
		t.Fatal(err)
	}
	// 下一轮写到一半时，上一份已生效承诺必须仍在日志内，不能只剩新目标。
	pending := next
	pending.Revision = 3
	pending.MemoryMaxBytes = 48 << 20
	pending.HugePools = append([]HugePool(nil), next.HugePools...)
	pending.HugePools[0].Pages = 4
	k.fail = true
	if _, err := g.Apply(ctx, pending); !errors.Is(err, ErrUnknown) {
		t.Fatalf("write failure lost uncertainty: %v", err)
	}
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.load()
	s.close()
	if err != nil {
		t.Fatal(err)
	}
	if r.Applied || r.Held == nil || !reflect.DeepEqual(*r.Held, next) || r.Original == nil || !reflect.DeepEqual(*r.Original, pending) {
		t.Fatal("partial update discarded held constraints")
	}
	if _, err := g.Apply(ctx, c); err == nil {
		t.Fatal("stale revision resurrected")
	}
}

func TestResourceGuardNewOwnerRetainsOriginalReadback(t *testing.T) {
	g, k, c := fixtureGuard(t)
	ctx := context.Background()
	if _, err := g.Apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	// 模拟最终日志落盘前进程丢失；内核已完成，但调用者没有可用的成功回复。
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.load()
	if err != nil {
		t.Fatal(err)
	}
	r.Applied = false
	r.Held = nil
	if err := s.save(r); err != nil {
		t.Fatal(err)
	}
	s.close()
	if _, err := g.Fence(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := g.ReplayOriginal(ctx, c); err == nil {
		t.Fatal("new owner resent old epoch")
	}
	if _, err := g.Observe(ctx, c); err != nil {
		t.Fatal(err)
	}
	if k.writes != 1 {
		t.Fatal("readback wrote kernel")
	}
	next := c
	next.IssuerEpoch = 2
	next.Revision = 2
	if _, err := g.Apply(ctx, next); err != nil {
		t.Fatal(err)
	}
	k.id.GuestBootID = "87654321-1234-1234-1234-123456789abc"
	if _, err := g.Fence(ctx, 3); err == nil {
		t.Fatal("Guest reboot silently reset original resource journal")
	}
}

func TestResourceGuardInFlightWriteKeepsCrossProcessFence(t *testing.T) {
	g, k, c := fixtureGuard(t)
	ctx := context.Background()
	k.entered = make(chan struct{})
	k.release = make(chan struct{})
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := g.Apply(callCtx, c); done <- err }()
	select {
	case <-k.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write never started")
	}
	cancel() // 已开始的内核调用不能因HTTP/ctx取消提前释放资源锁。
	second := &Guard{config: g.config, kernel: k}
	waitCtx, stop := context.WithTimeout(ctx, 40*time.Millisecond)
	defer stop()
	if _, err := second.Fence(waitCtx, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("new owner crossed pending kernel call: %v", err)
	}
	close(k.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write did not finish")
	}
	if _, err := second.Fence(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Apply(ctx, c); err == nil {
		t.Fatal("old owner wrote after higher durable floor")
	}
}

func TestResourceGuardBoundsIdentityAndStrictPersistence(t *testing.T) {
	g, k, c := fixtureGuard(t)
	ctx := context.Background()
	for _, mutate := range []func(*Command){
		func(c *Command) { c.HugePools = append(c.HugePools, c.HugePools[0]) },
		func(c *Command) { c.HugePools[0].Pages = control.Uint64(math.MaxUint64) },
		func(c *Command) { c.MemoryMaxBytes = control.Uint64(math.MaxUint64) },
		func(c *Command) { c.Identity.CgroupInode++ },
		func(c *Command) { c.MemoryMaxBytes = 1 },
	} {
		x := c
		x.HugePools = append([]HugePool(nil), c.HugePools...)
		mutate(&x)
		if _, err := g.Apply(ctx, x); err == nil {
			t.Fatal("unsafe command accepted", x)
		}
	}
	if k.writes != 0 {
		t.Fatal("invalid command reached kernel")
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(string(raw), `"revision":"1"`, `"revision":1`, 1), strings.Replace(string(raw), `"revision":"1"`, `"revision":"1","revision":"1"`, 1), strings.Replace(string(raw), `"revision":"1"`, `"revision":"1","extra":false`, 1)} {
		var parsed Command
		if err := json.Unmarshal([]byte(bad), &parsed); err == nil {
			t.Fatal("invalid command JSON accepted")
		}
	}
	if err := os.WriteFile(filepath.Join(g.config.StateDir, "state.json"), []byte(`{"version":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Fence(ctx, 2); err == nil {
		t.Fatal("corrupted journal replaced")
	}
}

func TestResourceGuardRejectsStateSymlinkAndPublicPermissions(t *testing.T) {
	g, _, _ := fixtureGuard(t)
	state := filepath.Join(g.config.StateDir, "state.json")
	if err := os.Chmod(state, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Fence(context.Background(), 2); err == nil {
		t.Fatal("public journal accepted")
	}
	if err := os.Remove(state); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "external")
	if err := os.WriteFile(outside, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, state); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Fence(context.Background(), 2); err == nil {
		t.Fatal("journal symlink followed")
	}
}

func TestResourceGuardMissingStateCannotResetFloor(t *testing.T) {
	g, _, _ := fixtureGuard(t)
	if _, err := g.Initialize(context.Background(), 2); err == nil {
		t.Fatal("existing original journal reinitialized")
	}
	if err := os.Remove(filepath.Join(g.config.StateDir, "state.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Fence(context.Background(), 1); err == nil {
		t.Fatal("missing journal silently recreated by ordinary recovery")
	}
}

func TestResourceGuardProcIdentityParsing(t *testing.T) {
	suffix := []string{"S"}
	for len(suffix) < 19 {
		suffix = append(suffix, "0")
	}
	suffix = append(suffix, "18446744073709551615")
	text := "123 (postgres (check)) " + strings.Join(suffix, " ")
	if ticks, err := postmasterTicks(text, 123); err != nil || ticks != math.MaxUint64 {
		t.Fatalf("valid comm or uint64 lost: %d %v", ticks, err)
	}
	for _, s := range []string{strings.Replace(text, "123 (", "124 (", 1), "123 (postgres) S 0", strings.Replace(text, "18446744073709551615", "18446744073709551616", 1), strings.Replace(text, "18446744073709551615", "0", 1)} {
		if _, err := postmasterTicks(s, 123); err == nil {
			t.Fatal("bad process incarnation accepted")
		}
	}
}
