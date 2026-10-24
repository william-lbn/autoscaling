package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestResourceViewPreservesUnknownInsteadOfBorrowingHeldCompletion(t *testing.T) {
	g, k, b, c := freshBootstrapFixture(t)
	ctx := context.Background()
	if _, err := g.Bootstrap(ctx, b); err != nil {
		t.Fatal(err)
	}
	v, err := g.InspectState(ctx)
	if err != nil || v.Original != nil {
		t.Fatal("initial resource state differs", err)
	}
	if _, err := v.ConfirmedLimits(); err != nil {
		t.Fatal(err)
	}
	drift := v
	drift.Actual.MemoryCurrentBytes = drift.Actual.MemoryMaxBytes + 1
	if _, err := drift.ConfirmedLimits(); !errors.Is(err, ErrUnknown) {
		t.Fatal("transient occupancy must retain unknown instead of rotating owner", err)
	}
	p := ProcessIdentity{ProtocolVersion: 1, Identity: c.Identity, PostmasterPID: c.PostmasterPID, PostmasterStartTicks: c.PostmasterStartTicks}
	if err := g.launchWithBirth(ctx, nil, &p, func() error { k.mu.Lock(); k.process = &p; k.mu.Unlock(); return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	next := cloneCommand(c)
	next.Revision++
	next.MemoryMaxBytes -= 1 << 20
	k.mu.Lock()
	k.fail = true
	k.mu.Unlock()
	if _, err := g.Apply(ctx, next); !errors.Is(err, ErrUnknown) {
		t.Fatal("partial resource write not retained", err)
	}
	v, err = g.InspectState(ctx)
	if err != nil || v.Original == nil || v.Held == nil || v.Original.Revision != 2 || v.Held.Revision != 1 || v.Applied {
		t.Fatal("view lost original or held", err)
	}
	if _, err := v.ConfirmedLimits(); !errors.Is(err, ErrUnknown) {
		t.Fatal("old held snapshot turned into new completion")
	}
	raw, _ := json.Marshal(v)
	var restored View
	if err := json.Unmarshal(raw, &restored); err != nil || restored.Original.PostmasterPID != c.PostmasterPID {
		t.Fatal("strict retained source roundtrip failed", err)
	}
}

func TestResourceViewDiscoveryAndClientDoNotSilentlyRebind(t *testing.T) {
	h, key, g, _, c := resourceHTTPFixture(t)
	// 只重建本用例的初始测试日志，真实HTTP不提供初始化/删除功能。
	fresh, _, b, _ := freshBootstrapFixture(t)
	if _, err := fresh.Bootstrap(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	h.guard = fresh
	g = fresh
	client := resourceClientFixture(t, h, c, resourceToken(t, h, key, nil), nil)
	config := client.config
	config.Identity = Identity{VMUID: c.Identity.VMUID, ComputeInstanceID: c.Identity.ComputeInstanceID}
	id, err := DiscoverIdentity(context.Background(), config)
	if err != nil || id != c.Identity {
		t.Fatal("read-only resource discovery failed", err)
	}
	if _, err := client.InspectState(context.Background()); err != nil {
		t.Fatal(err)
	}
	config.Identity = c.Identity
	if _, err := DiscoverIdentity(context.Background(), config); err == nil {
		t.Fatal("existing Guest silently rediscovered")
	}
	if got, err := g.InspectIdentity(); err != nil || got != id {
		t.Fatal("discovery modified resource identity")
	}
}
