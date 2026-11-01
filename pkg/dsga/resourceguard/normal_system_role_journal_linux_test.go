//go:build linux

package resourceguard

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalSystemRoleJournalOriginalDurabilityWitness(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires actual root-owned persistent fixture")
	}
	for _, mode := range []string{"original", "new-broker", "same-bytes-new-inode", "missing-prefix", "unknown-stage", "visible-without-witness"} {
		t.Run(mode, func(t *testing.T) {
			path := t.TempDir()
			dir, e := os.Open(path)
			require.NoError(t, e)
			defer dir.Close()
			state := &lockedState{dir: dir}
			binding := normalSystemLifecycleBinding{SystemManifestSHA256: strings.Repeat("ab", 32)}
			pending := normalSystemRoleJournalEntry{ProtocolVersion: 1, Scope: normalSystemRoleJournalScope, Nonce: strings.Repeat("cd", 32), Stage: "pending", Role: "resource", PlanSHA256: strings.Repeat("ef", 32), Binding: binding, Namespace: []normalSystemRoleNamespaceIdentity{{Name: "mnt", Device: 1, Inode: 2}}}
			journal := &normalSystemRoleJournal{state: state, pending: pending, seen: map[string]normalSystemRoleFileWitness{}}
			require.NoError(t, journal.append(context.Background(), "pending", nil, false))
			observed := normalSystemRoleObservation{Role: "resource", Binding: binding, OriginalTaskObserved: true, FixedArgvEnvironmentObserved: true}
			require.NoError(t, journal.persistOriginalSystemRoleStopped(context.Background(), observed, pending.Namespace, pending.PlanSHA256))
			require.NoError(t, journal.append(context.Background(), "exited", &observed, true))
			run := &normalSystemRoleOriginalRun{pending: pending, terminal: true, seen: journal.seen}
			broker := &normalSystemRoleBrokerComponent{source: &normalSystemLifecycleSource{binding: binding}, runs: map[string]*normalSystemRoleOriginalRun{pending.Nonce: run}}
			target := normalSystemRoleStageName(pending.Nonce, "pending")
			switch mode {
			case "new-broker":
				broker.runs = map[string]*normalSystemRoleOriginalRun{}
			case "same-bytes-new-inode":
				raw, e := os.ReadFile(filepath.Join(path, target))
				require.NoError(t, e)
				require.NoError(t, os.Rename(filepath.Join(path, target), filepath.Join(path, "old-original")))
				require.NoError(t, os.WriteFile(filepath.Join(path, target), raw, 0600))
			case "missing-prefix":
				require.NoError(t, os.Remove(filepath.Join(path, target)))
			case "unknown-stage":
				require.NoError(t, os.WriteFile(filepath.Join(path, normalSystemRoleStageName(pending.Nonce, "future")), []byte("{}"), 0600))
			case "visible-without-witness":
				run.seen = map[string]normalSystemRoleFileWitness{}
			}
			e = broker.rejectUnknownHistoryLocked(state)
			if mode == "original" {
				require.NoError(t, e)
			} else {
				require.ErrorIs(t, e, ErrUnknown)
			}
		})
	}
}
func TestNormalSystemRoleStoppedWriterCannotChangeBinding(t *testing.T) {
	j := &normalSystemRoleJournal{pending: normalSystemRoleJournalEntry{Role: "resource", PlanSHA256: strings.Repeat("ab", 32)}}
	e := j.persistOriginalSystemRoleStopped(context.Background(), normalSystemRoleObservation{Role: "compute", OriginalTaskObserved: true, FixedArgvEnvironmentObserved: true}, nil, strings.Repeat("ab", 32))
	require.ErrorIs(t, e, ErrUnknown)
}
