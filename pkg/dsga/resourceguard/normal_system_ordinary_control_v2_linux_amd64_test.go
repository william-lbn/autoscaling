//go:build linux && amd64

package resourceguard

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestNormalSystemOrdinaryV2ExecStatusPreservesOnlyRoleCapabilities(t *testing.T) {
	for _, role := range []string{"allow-start", "network", "clock"} {
		t.Run(role, func(t *testing.T) {
			q, e := NormalSystemOrdinaryV2RolePlan(role)
			require.NoError(t, e)
			status := fmt.Sprintf("Uid:\t%d %d %d %d\nGid:\t%d %d %d %d\nGroups:\t\nNoNewPrivs:\t1\nSeccomp:\t2\nThreads:\t1\n", q.UID, q.UID, q.UID, q.UID, q.GID, q.GID, q.GID, q.GID)
			for _, key := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
				status += fmt.Sprintf("%s:\t%016x\n", key, uint64(q.KeepCapabilities))
			}
			require.NoError(t, normalSystemOrdinaryV2ExecStatus(status, q))
			for _, mode := range []string{"wrong UID", "saved root", "supplementary group", "NNP0", "no filter", "second thread", "ambient lost", "bounding excess", "duplicate cap"} {
				t.Run(mode, func(t *testing.T) {
					s := status
					switch mode {
					case "wrong UID":
						s = strings.Replace(s, fmt.Sprintf("Uid:\t%d", q.UID), "Uid:\t0", 1)
					case "saved root":
						s = strings.Replace(s, fmt.Sprintf("Uid:\t%d %d %d %d", q.UID, q.UID, q.UID, q.UID), fmt.Sprintf("Uid:\t%d %d 0 %d", q.UID, q.UID, q.UID), 1)
					case "supplementary group":
						s = strings.Replace(s, "Groups:\t\n", "Groups:\t1000\n", 1)
					case "NNP0":
						s = strings.Replace(s, "NoNewPrivs:\t1", "NoNewPrivs:\t0", 1)
					case "no filter":
						s = strings.Replace(s, "Seccomp:\t2", "Seccomp:\t0", 1)
					case "second thread":
						s = strings.Replace(s, "Threads:\t1", "Threads:\t2", 1)
					case "ambient lost":
						s = strings.Replace(s, fmt.Sprintf("CapAmb:\t%016x", uint64(q.KeepCapabilities)), fmt.Sprintf("CapAmb:\t%016x", uint64(q.KeepCapabilities)^(1<<12)), 1)
					case "bounding excess":
						s = strings.Replace(s, fmt.Sprintf("CapBnd:\t%016x", uint64(q.KeepCapabilities)), fmt.Sprintf("CapBnd:\t%016x", uint64(q.KeepCapabilities)|(1<<21)), 1)
					case "duplicate cap":
						s += "CapEff:\t0000000000000000\n"
					}
					require.Error(t, normalSystemOrdinaryV2ExecStatus(s, q))
				})
			}
		})
	}
}
func TestNormalSystemOrdinaryV2InstallerHasNoUnboundFallback(t *testing.T) {
	for _, role := range []string{"network", "clock", "resource", "compute", "ordinary-role-broker", "/bin/sh"} {
		_, e := newNormalSystemOrdinaryV2ControlPlan(context.Background(), nil, role)
		require.ErrorIs(t, e, ErrUnknown)
	}
	require.ErrorIs(t, normalSystemOrdinaryV2Apply(context.Background(), nil), ErrUnknown)
	require.ErrorIs(t, normalSystemOrdinaryV2ExactBuild(nil), ErrUnknown)
}

func TestNormalSystemOrdinaryV2InheritedDescriptors(t *testing.T) {
	if mode := os.Getenv("DSGA_ORDINARY2_FD_DEV"); mode != "" {
		e := normalSystemOrdinaryV2CheckInheritedDescriptors()
		if e != nil {
			os.Exit(125)
		}
		os.Exit(0)
	}
	executable, e := os.Executable()
	require.NoError(t, e)
	for _, extra := range []bool{false, true} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			cmd := exec.Command(executable, "-test.run=^TestNormalSystemOrdinaryV2InheritedDescriptors$")
			cmd.Env = append(os.Environ(), "DSGA_ORDINARY2_FD_DEV=bounded-child")
			if extra {
				f, e := os.CreateTemp(t.TempDir(), "unknown")
				require.NoError(t, e)
				defer f.Close()
				cmd.ExtraFiles = []*os.File{f}
			}
			e := cmd.Run()
			if extra {
				var x *exec.ExitError
				require.ErrorAs(t, e, &x)
				require.Equal(t, 125, x.ExitCode())
			} else {
				require.NoError(t, e)
			}
		})
	}
}
