//go:build linux

package resourceguard

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

type normalMemoryExchangeTrap struct{ reads, writes int }

func (t *normalMemoryExchangeTrap) Read([]byte) (int, error)  { t.reads++; return 0, io.ErrClosedPipe }
func (t *normalMemoryExchangeTrap) Write([]byte) (int, error) { t.writes++; return 0, io.ErrClosedPipe }
func TestNormalMemoryExchangePlanMustMatchOriginalBeforeEffects(t *testing.T) {
	c, raw, assets, _ := initialPrebootFixture(t)
	plan, e := DeriveRuntimeBootRequirements(raw, assets)
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"runtime", "resource", "secret", "build", "unsupported"} {
		t.Run(mode, func(t *testing.T) {
			challenge := c
			p := plan
			switch mode {
			case "runtime":
				challenge.RuntimeSHA256 = physicalDigest([]byte("another"))
			case "resource":
				challenge.ResourceSHA256 = physicalDigest([]byte("another"))
			case "secret":
				challenge.ConfigSecretUID = "another"
			case "build":
				challenge.KernelBuildSHA256 = physicalDigest([]byte("another"))
			case "unsupported":
				p.memoryOriginProtocol = 3
			}
			trap := &normalMemoryExchangeTrap{}
			e := initialGuestExchangeWithRequirements(context.Background(), trap, &Guard{}, challenge, false, p)
			if e == nil {
				t.Fatal("foreign original plan accepted")
			}
			if trap.reads != 0 || trap.writes != 0 {
				t.Fatal("foreign plan reached semantic exchange")
			}
		})
	}
}
func TestNormalMemoryExchangeRejectsMissingOriginalEndpoint(t *testing.T) {
	c, _, _, _ := initialPrebootFixture(t)
	if e := initialGuestExchangeWithRequirements(nil, &normalMemoryExchangeTrap{}, &Guard{}, c, false, RuntimeBootRequirements{}); e == nil {
		t.Fatal("nil context accepted")
	}
	if e := initialGuestExchangeWithRequirements(context.Background(), nil, &Guard{}, c, false, RuntimeBootRequirements{}); e == nil {
		t.Fatal("nil original endpoint accepted")
	}
	if e := initialGuestExchangeWithRequirements(context.Background(), &normalMemoryExchangeTrap{}, nil, bootprotocol.InitialPrebootChallenge{}, false, RuntimeBootRequirements{}); e == nil {
		t.Fatal("nil original Guard accepted")
	}
}

// 实际持久state/Bootstrap事实触发原split拒绝；不是mock函数调用表。
func TestNormalMemoryInitialSplitMustPrecedePersistentBootstrap(t *testing.T) {
	ctx := context.Background()
	installBootstrap := func(f *splitFixture) {
		t.Helper()
		c := BootstrapCommand{ProtocolVersion: 1, Identity: f.k.snapshot.Identity, IssuerEpoch: f.in.source.IssuerEpoch, BudgetCommitmentID: f.in.source.BudgetCommitmentID, Limits: f.in.source.Limits}
		if e := c.Validate(); e != nil {
			t.Fatal(e)
		}
		r := &record{Version: 1, Identity: c.Identity, Floor: c.IssuerEpoch, Bootstrap: &bootstrapRecord{Original: c}}
		if e := r.validate(c.Identity); e != nil {
			t.Fatal(e)
		}
		s, e := lockState(ctx, f.dir)
		if e != nil {
			t.Fatal(e)
		}
		defer s.close()
		if e = s.save(r); e != nil {
			t.Fatal(e)
		}
		actual, e := s.load()
		if e != nil || actual == nil || actual.Bootstrap == nil {
			t.Fatalf("bootstrap state not really visible: %v", e)
		}
	}
	t.Run("bootstrap-first-must-reject-new-split", func(t *testing.T) {
		f := newSplitFixture(t)
		installBootstrap(f)
		before, e := os.ReadFile(filepath.Join(f.dir, "state.json"))
		if e != nil {
			t.Fatal(e)
		}
		if _, e = f.run(ctx, normalSplitPrepare); e == nil {
			t.Fatal("historical Bootstrap adopted new split")
		}
		if len(f.k.writes) != 0 {
			t.Fatal("reverse order had topology effects")
		}
		if _, e = os.Stat(filepath.Join(f.dir, normalSplitIntentName)); !os.IsNotExist(e) {
			t.Fatal("reverse order wrote split intent")
		}
		after, e := os.ReadFile(filepath.Join(f.dir, "state.json"))
		if e != nil {
			t.Fatal(e)
		}
		if string(before) != string(after) {
			t.Fatal("split rewrote original Bootstrap")
		}
	})
	t.Run("split-first-preserves-both-originals", func(t *testing.T) {
		f := newSplitFixture(t)
		prepared, e := f.run(ctx, normalSplitPrepare)
		if e != nil {
			t.Fatal(e)
		}
		if !prepared.DurabilityConfirmed {
			t.Fatal("split not complete before Bootstrap")
		}
		installBootstrap(f)
		observed, e := f.run(ctx, normalSplitObserve)
		if e != nil {
			t.Fatal(e)
		}
		if observed.Original.Nonce != prepared.Original.Nonce {
			t.Fatal("post Bootstrap observation replaced split origin")
		}
		if len(f.k.writes) != 5 {
			t.Fatal("post Bootstrap observation replayed topology")
		}
	})
}
