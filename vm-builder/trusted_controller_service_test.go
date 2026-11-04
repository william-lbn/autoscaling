package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrustedControllerServiceSeparatesPGEmptyAndSystemReadiness(t *testing.T) {
	oldGate := "/neonvm/bin/dsga-resource-guard check-guest >/dev/null || exit 1\n"
	require.Equal(t, 1, strings.Count(scriptNormalResourceService, oldGate), "ordinary default must retain its exact old gate")
	want := strings.Replace(scriptNormalResourceService, oldGate, "case \"$1\" in\n allow-start|compute) /neonvm/bin/dsga-resource-guard check-guest >/dev/null || exit 1 ;;\n *) /neonvm/bin/dsga-resource-guard check-system-service \"$1\" >/dev/null || exit 1 ;;\nesac\n", 1)
	require.Equal(t, want, scriptTrustedControllerResourceService, "only role readiness guard may differ; all argc/role/argv vectors stay fixed")
	require.Contains(t, scriptTrustedControllerResourceService, "[ \"$#\" -eq 1 ] || exit 1")
	require.Contains(t, scriptTrustedControllerResourceService, "*) exit 1 ;; esac")
	require.Equal(t, 1, strings.Count(scriptTrustedControllerResourceService, "compute) exec /neonvm/bin/vmstart ;;"))
}
