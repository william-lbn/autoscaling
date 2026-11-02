//go:build linux && dsga_native_trusted_control && dsga_native_role_transfer2 && dsga_native_role_service && dsga_native_system_lifecycle

package resourceguard

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const trustedControlNativePG18OptIn = "isolated18c-real-compute-normal-pg18-v1"

// PGmajor是此DEV用例的严格预期，不是Root预算/来源授权。
func trustedControlNativeEntrypoint(opt string) string {
	switch opt {
	case trustedControlNativeOptIn:
		return "TestNativePG17TrustedControl"
	case trustedControlNativePG18OptIn:
		return "TestNativePG18TrustedControl"
	default:
		return ""
	}
}

func TestNativePG18TrustedControl(t *testing.T) {
	require.Equal(t, trustedControlNativePG18OptIn, os.Getenv("DSGA_TRUSTED_CONTROL_NATIVE"))
	trustedControlNativeRun(t, 18)
}

func TestTrustedControlNativeMajorEntrypoints(t *testing.T) {
	for _, tt := range []struct{ name, opt, target string }{
		{"original17", trustedControlNativeOptIn, "TestNativePG17TrustedControl"},
		{"explicit18", trustedControlNativePG18OptIn, "TestNativePG18TrustedControl"},
		{"unknown-closed", "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.target, trustedControlNativeEntrypoint(tt.opt)) })
	}
}
