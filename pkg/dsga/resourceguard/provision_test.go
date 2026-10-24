package resourceguard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sourceFixture(c Command) SourceConfig {
	return SourceConfig{ProtocolVersion: 1, Config: Config{VMUID: c.Identity.VMUID, ComputeInstanceID: c.Identity.ComputeInstanceID, StateDir: FixedStateDir}, IssuerEpoch: 1, BudgetCommitmentID: "bootstrap-budget", Limits: c.limits(), ControlPublicKey: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))}
}

func TestResourceSourceStrictBytesAndBounds(t *testing.T) {
	_, _, c := fixtureGuard(t)
	s := sourceFixture(c)
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	d := RuntimeDocument{ProtocolVersion: 1, Incarnation: "1234567890abcdef1234567890abcdef", ConfigSecretUID: "original-config-uid", SourceJSON: raw, SourceSHA256: hex.EncodeToString(sum[:])}
	if _, err := d.Source(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*SourceConfig){func(s *SourceConfig) { s.IssuerEpoch = 2 }, func(s *SourceConfig) { s.Config.StateDir = "/tmp/recreated" }, func(s *SourceConfig) { s.ControlPublicKey += "=" }, func(s *SourceConfig) { s.ControlPublicKey = base64.RawURLEncoding.EncodeToString(make([]byte, 32)) }, func(s *SourceConfig) { s.Limits.MemoryMaxBytes++ }} {
		x := s
		mutate(&x)
		if x.Validate() == nil {
			t.Fatal("invalid source accepted")
		}
	}
	d.SourceJSON = append([]byte(nil), raw...)
	d.SourceJSON[0] = '['
	if _, err := d.Source(); err == nil {
		t.Fatal("changed original bytes accepted")
	}
	for _, bad := range []string{strings.Replace(string(raw), `"issuer_epoch":"1"`, `"issuer_epoch":1`, 1), strings.Replace(string(raw), `"issuer_epoch":"1"`, `"issuer_epoch":"1","issuer_epoch":"1"`, 1), strings.Replace(string(raw), `"issuer_epoch":"1"`, `"issuer_epoch":"1","extra":true`, 1)} {
		var next SourceConfig
		if json.Unmarshal([]byte(bad), &next) == nil {
			t.Fatal("noncanonical source input accepted")
		}
	}
}

func TestResourceSourceManagedModeCannotFallBackOnResidualState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "marker")
	if managedGuestPaths([]string{path}) {
		t.Fatal("fresh missing marker is managed")
	}
	if err := os.Symlink("missing-target", path); err != nil {
		t.Fatal(err)
	}
	if !managedGuestPaths([]string{path}) {
		t.Fatal("dangling original marker fell back")
	}
	if !managedKernelArgs("root=/dev/vda "+ManagedKernelArg+" rw") || managedKernelArgs("root=/dev/vda neonvm.dsga=0 rw") {
		t.Fatal("managed kernel mode parsed incorrectly")
	}
}

func TestResourceSourceBirthBindsOriginalBudget(t *testing.T) {
	g, _, b, c := freshBootstrapFixture(t)
	if _, err := g.Bootstrap(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	s := sourceFixture(c)
	state, err := lockState(context.Background(), g.config.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := state.load()
	state.close()
	if err != nil {
		t.Fatal(err)
	}
	if err := sameBootstrapSource(s, r.Bootstrap.Original); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*SourceConfig){func(s *SourceConfig) { s.BudgetCommitmentID = "another-budget" }, func(s *SourceConfig) { s.Limits.MemoryMaxBytes += 64 << 10 }, func(s *SourceConfig) { s.Config.ComputeInstanceID = "another-compute" }} {
		x := s
		mutate(&x)
		if sameBootstrapSource(x, r.Bootstrap.Original) == nil {
			t.Fatal("changed source adopted old bootstrap")
		}
	}
}
