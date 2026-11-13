package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/neondatabase/autoscaling/pkg/api/runnerdiagnostic"
	"github.com/stretchr/testify/require"
)

func TestRunnerTerminationDiagnosticWritesOnlyFixedProtocol(t *testing.T) {
	file := filepath.Join(t.TempDir(), "termination-log")
	require.NoError(t, os.WriteFile(file, []byte("previous private-token"), 0600))
	require.NoError(t, writeRunnerFailureAt(file, runnerdiagnostic.At(runnerdiagnostic.StageInitialFreeze, errors.New("private-token"))))
	raw, err := os.ReadFile(file)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "private-token")
	report, err := runnerdiagnostic.Parse(raw)
	require.NoError(t, err)
	require.Equal(t, runnerdiagnostic.StageInitialFreeze, report.Stage)
	require.Equal(t, runnerdiagnostic.CodeFailed, report.Code)
}

func TestRunnerTerminationDiagnosticRejectsReplacementLinkAndMissingMount(t *testing.T) {
	dir := t.TempDir()
	original, link := filepath.Join(dir, "original"), filepath.Join(dir, "termination-log")
	require.NoError(t, os.WriteFile(original, []byte("unchanged"), 0600))
	require.NoError(t, os.Symlink(original, link))
	require.Error(t, writeRunnerFailureAt(link, errors.New("private-token")))
	raw, err := os.ReadFile(original)
	require.NoError(t, err)
	require.Equal(t, "unchanged", string(raw))
	require.Error(t, writeRunnerFailureAt(filepath.Join(dir, "missing"), errors.New("private-token")))
	_, err = os.Stat(filepath.Join(dir, "missing"))
	require.True(t, os.IsNotExist(err))
}
