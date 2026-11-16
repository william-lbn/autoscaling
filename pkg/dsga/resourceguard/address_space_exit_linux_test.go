//go:build linux

package resourceguard

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	addressSpaceStatDuringExit = "3351847 (exit-status) R 3351846 3351846 3351846 0 -1 4194380 10 0 0 0 0 0 0 0 20 0 1 0 149018770 0 0 18446744073709551615 0 0 0 0 0 0 0 4096 0 0 0 0 17 10 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
	addressSpaceGroup          = "0::/neon-postgres\n"
)

func TestAddressSpaceProcRetryRequiresOriginalExitEvidence(t *testing.T) {
	// A real Linux status can still report R after exit_mm. Only the independent
	// stat sample's kernel flag or a changed incarnation may trigger a retry.
	stableStat := strings.Replace(addressSpaceStatDuringExit, " 4194380 ", " 4194376 ", 1)
	missingSize := "State:\tR (running)\n" + strings.ReplaceAll(processStatusText, "VmSize:\t65536 kB\n", "")
	zeroSize := strings.Replace(processStatusText, "65536 kB", "0 kB", 1)
	for _, tc := range []struct {
		name, status, lastStat string
		want                   error
	}{
		{"valid", processStatusText, stableStat, nil},
		{"running-exit-mm", missingSize, addressSpaceStatDuringExit, errAddressSpaceChurn},
		{"exiting-zero-size", zeroSize, addressSpaceStatDuringExit, errAddressSpaceChurn},
		{"stable-no-size", missingSize, stableStat, errAddressSpaceMappingUnavailable},
		{"reuse", missingSize, strings.Replace(stableStat, "149018770", "149018771", 1), errAddressSpaceChurn},
		{"bad-flags", missingSize, strings.Replace(stableStat, "4194376", "bad", 1), errAddressSpaceMappingUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count := 0
			read := func(file string) (string, error) {
				switch file {
				case "stat":
					count++
					if count == 1 {
						return stableStat, nil
					}
					return tc.lastStat, nil
				case "status":
					return tc.status, nil
				case "limits":
					return processLimitsText, nil
				case "cgroup":
					return addressSpaceGroup, nil
				default:
					t.Fatalf("unexpected /proc read %q", file)
					return "", nil
				}
			}
			result, err := inspectAddressSpaceFromProc(3351847, finiteAS, 1000, addressSpaceGroup, read)
			require.Equal(t, 2, count)
			if tc.want == nil {
				require.NoError(t, err)
				require.NotZero(t, result.virtual)
				return
			}
			require.ErrorIs(t, err, tc.want)
			require.Zero(t, result)
		})
	}

	statusWithPrivileges := strings.Replace(missingSize, "NoNewPrivs:\t1", "NoNewPrivs:\t0", 1)
	_, _, err := processAddressSpace(processLimitsText, statusWithPrivileges, finiteAS)
	require.ErrorContains(t, err, "can gain new privileges")
	_, _, err = processAddressSpace(strings.ReplaceAll(processLimitsText, "134217728", "unlimited"), missingSize, finiteAS)
	require.ErrorContains(t, err, "ceiling changed or is unlimited")
}
