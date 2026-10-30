//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 这些是明确的 U 模型，只验证原锁/磁盘事务与源绑定，不冒充真实 kernel/ISO/Guest。
type originFixtureKernel struct {
	*fixtureKernel
	first             PhysicalKernelObservation
	empty             bool
	active, maxActive atomic.Int32
}

func (k *originFixtureKernel) physicalOriginRead() (PhysicalKernelObservation, error) {
	n := k.active.Add(1)
	defer k.active.Add(-1)
	for old := k.maxActive.Load(); n > old && !k.maxActive.CompareAndSwap(old, n); old = k.maxActive.Load() {
	}
	time.Sleep(time.Millisecond)
	o := k.first
	o.Zones = append([]PhysicalKernelZone(nil), o.Zones...)
	return o, nil
}
func (k *originFixtureKernel) physicalOriginPGEmpty() error {
	if !k.empty {
		return errors.New("U fixture PG is populated")
	}
	return nil
}
func originFixture(t *testing.T) (*Guard, *originFixtureKernel, physicalOriginInputs) {
	t.Helper()
	g, k, b, _ := freshBootstrapFixture(t)
	o := physicalFixture(t)
	o.Identity = b.Identity
	fk := &originFixtureKernel{fixtureKernel: k, first: o, empty: true}
	g.kernel = fk
	policy := PhysicalKernelOriginPolicy{ProtocolVersion: 1, ProfileID: physicalOriginProfileID, KernelRelease: physicalKernelRelease, KernelNotesSHA256: o.KernelNotesSHA256, KernelArtifactSHA256: physicalKernelArtifactSHA, KernelConfigSHA256: physicalKernelConfigSHA, RuntimeDocumentSHA256: physicalDigest([]byte("U runtime ISO document"))}
	in := physicalOriginInputs{policy: policy, policySHA: physicalDigest([]byte("U policy exact bytes")), sourceSHA: physicalDigest([]byte("U resource exact bytes")), runtimeSHA: policy.RuntimeDocumentSHA256, incarnation: "11111111222233334444555555555555", secretUID: "secret-original", config: g.config, bootstrap: b, bootMarker: true}
	return g, fk, in
}
func originData(t *testing.T, g *Guard) []byte {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(g.config.StateDir, physicalOriginDataName))
	if e != nil {
		t.Fatal(e)
	}
	return b
}

func TestPhysicalKernelOriginCapturesAndKeepsOriginal(t *testing.T) {
	g, k, in := originFixture(t)
	ctx := context.Background()
	o, err := g.physicalOrigin(ctx, in, physicalOriginCapture)
	if err != nil {
		t.Fatal(err)
	}
	raw := originData(t, g)
	ref, err := o.ReferenceSHA256()
	if err != nil || ref != physicalDigest(raw) {
		t.Fatal("component reference differs from exact original bytes", ref, err)
	}

	if o.VMShrinkPermit || o.Nonce == "" {
		t.Fatal(o)
	}
	if _, err = os.Stat(filepath.Join(g.config.StateDir, "state.json")); !os.IsNotExist(err) {
		t.Fatal("component fabricated resource bootstrap", err)
	}
	for _, op := range []physicalOriginOperation{physicalOriginCapture, physicalOriginObserve, physicalOriginReplay} {
		got, err := g.physicalOrigin(ctx, in, op)
		if err != nil || got.Nonce != o.Nonce {
			t.Fatal(got, err)
		}
	}
	if string(raw) != string(originData(t, g)) {
		t.Fatal("retry replaced original bytes")
	}
	k.first.Zones[1].PresentPages++
	if _, err = g.physicalOrigin(ctx, in, physicalOriginObserve); err != nil {
		t.Fatal("MOVABLE change replaced fixed origin", err)
	}
	if string(raw) != string(originData(t, g)) {
		t.Fatal("MOVABLE observation rewrote original")
	}
	// 原 Bootstrap 后仍只能核同一来源，不得重新生成 nonce/起始物理事实。
	if _, err = g.Bootstrap(ctx, in.bootstrap); err != nil {
		t.Fatal(err)
	}
	if got, err := g.physicalOrigin(ctx, in, physicalOriginObserve); err != nil || got.Nonce != o.Nonce {
		t.Fatal(got, err)
	}
}

func TestPhysicalKernelOriginRejectsSourceAndHistoryDrift(t *testing.T) {
	for _, scenario := range []string{"old-journal", "populated", "missing-marker", "wrong-policy-runtime", "kernel-notes", "fixed-zone", "cpu", "source-bytes", "cgroup", "wrong-config-artifact"} {
		t.Run(scenario, func(t *testing.T) {
			g, k, in := originFixture(t)
			ctx := context.Background()
			beforeCapture := scenario == "old-journal" || scenario == "populated" || scenario == "missing-marker" || scenario == "wrong-policy-runtime" || scenario == "wrong-config-artifact"
			if !beforeCapture {
				if _, e := g.physicalOrigin(ctx, in, physicalOriginCapture); e != nil {
					t.Fatal(e)
				}
			}
			switch scenario {
			case "old-journal":
				if _, e := g.Bootstrap(ctx, in.bootstrap); e != nil {
					t.Fatal(e)
				}
			case "populated":
				k.empty = false
			case "missing-marker":
				in.bootMarker = false
			case "wrong-policy-runtime":
				in.runtimeSHA = physicalDigest([]byte("other ISO"))
			case "wrong-config-artifact":
				in.policy.KernelConfigSHA256 = physicalDigest([]byte("other config"))
			case "kernel-notes":
				k.first.KernelNotesSHA256 = physicalDigest([]byte("other kernel"))
			case "fixed-zone":
				k.first.Zones[0].PresentPages++
				k.first.NonMovableUpperBytes += 4096
			case "cpu":
				k.first.PossibleCPUs++
				k.first.CPUChargeStockUpperBytes += 64 * 4096
			case "source-bytes":
				in.sourceSHA = physicalDigest([]byte("other resource source"))
			case "cgroup":
				k.first.Identity.CgroupInode++
			}
			var before []byte
			if !beforeCapture {
				before = originData(t, g)
			}
			if _, e := g.physicalOrigin(ctx, in, physicalOriginCapture); e == nil {
				t.Fatal("accepted source/history drift")
			}
			if beforeCapture {
				if _, e := os.Stat(filepath.Join(g.config.StateDir, physicalOriginDataName)); !os.IsNotExist(e) {
					t.Fatal("rejection created new origin", e)
				}
			} else if string(before) != string(originData(t, g)) {
				t.Fatal("rejection changed old origin")
			}
		})
	}
}

func TestPhysicalKernelOriginPendingRequiresExplicitOriginalReplay(t *testing.T) {
	for _, scenario := range []string{"replay-original", "pending-after-resource-init", "populated-replay", "malformed-data", "noncanonical-data", "wrong-confirmation", "orphan-confirmation", "symlink-data"} {
		t.Run(scenario, func(t *testing.T) {
			g, k, in := originFixture(t)
			ctx := context.Background()
			o, e := g.physicalOrigin(ctx, in, physicalOriginCapture)
			if e != nil {
				t.Fatal(e)
			}
			raw := originData(t, g)
			confirm := filepath.Join(g.config.StateDir, physicalOriginConfirmName)
			data := filepath.Join(g.config.StateDir, physicalOriginDataName)
			if e = os.Remove(confirm); e != nil {
				t.Fatal(e)
			}
			switch scenario {
			case "pending-after-resource-init":
				if _, e = g.Bootstrap(ctx, in.bootstrap); e != nil {
					t.Fatal(e)
				}
			case "populated-replay":
				k.empty = false
			case "malformed-data":
				if e = os.WriteFile(data, []byte("{\"partial\":"), 0600); e != nil {
					t.Fatal(e)
				}
			case "noncanonical-data":
				if e = os.WriteFile(data, append(append([]byte{}, raw...), '\n'), 0600); e != nil {
					t.Fatal(e)
				}
				b, _ := json.Marshal(physicalOriginConfirmation{ProtocolVersion: 1, OriginSHA256: physicalDigest(append(append([]byte{}, raw...), '\n'))})
				if e = os.WriteFile(confirm, b, 0600); e != nil {
					t.Fatal(e)
				}
			case "wrong-confirmation":
				b, _ := json.Marshal(physicalOriginConfirmation{ProtocolVersion: 1, OriginSHA256: physicalDigest([]byte("wrong"))})
				if e = os.WriteFile(confirm, b, 0600); e != nil {
					t.Fatal(e)
				}
			case "orphan-confirmation":
				if e = os.Rename(data, confirm); e != nil {
					t.Fatal(e)
				}
			case "symlink-data":
				if e = os.Remove(data); e != nil {
					t.Fatal(e)
				}
				target := filepath.Join(t.TempDir(), "target")
				if e = os.WriteFile(target, raw, 0600); e != nil {
					t.Fatal(e)
				}
				if e = os.Symlink(target, data); e != nil {
					t.Fatal(e)
				}
			}
			for _, op := range []physicalOriginOperation{physicalOriginCapture, physicalOriginObserve} {
				if _, e = g.physicalOrigin(ctx, in, op); e == nil {
					t.Fatal("ordinary call repaired pending or malformed source")
				}
			}
			got, e := g.physicalOrigin(ctx, in, physicalOriginReplay)
			if scenario == "replay-original" {
				if e != nil || got.Nonce != o.Nonce || string(raw) != string(originData(t, g)) {
					t.Fatal("explicit replay changed original", got, e)
				}
			} else if e == nil {
				t.Fatal("unsafe replay accepted")
			}
		})
	}
}

func TestPhysicalKernelOriginSerializesWithOriginalResourceLock(t *testing.T) {
	g, k, in := originFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan PhysicalKernelOrigin, 12)
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o, e := g.physicalOrigin(ctx, in, physicalOriginCapture)
			if e != nil {
				errs <- e
			} else {
				results <- o
			}
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	nonce := ""
	for o := range results {
		if nonce == "" {
			nonce = o.Nonce
		}
		if nonce != o.Nonce {
			t.Fatal("parallel callers created separate origins")
		}
	}
	if k.maxActive.Load() != 1 {
		t.Fatal("component observer escaped original flock", k.maxActive.Load())
	}
	held, e := lockState(ctx, g.config.StateDir)
	if e != nil {
		t.Fatal(e)
	}
	defer held.close()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e = g.physicalOrigin(canceled, in, physicalOriginCapture); !errors.Is(e, context.Canceled) {
		t.Fatal("canceled lock attempt mutated capture", e)
	}
}
