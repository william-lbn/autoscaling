//go:build linux && amd64

package resourceguard

import (
	"context"
	"strings"
	"testing"
	"unsafe"
)

func TestNormalKernelSignalExecCVectorLifetimeAndBounds(t *testing.T) {
	argv := []string{"/neonvm/bin/busybox", "cat", "/proc/version"}
	env := []string{"PATH=/neonvm/bin"}
	image, e := newNormalKernelSignalExecImage(argv, env)
	if e != nil {
		t.Fatal(e)
	}
	if image.Path != image.argumentPointers[0] || image.Argv != &image.argumentPointers[0] || image.Environ != &image.environmentPointers[0] || image.argumentPointers[len(argv)] != nil || image.environmentPointers[len(env)] != nil {
		t.Fatal("C vectors not terminated or typed lifetime root differs")
	}
	for i, s := range argv {
		if string(image.argumentBytes[i]) != s+"\x00" {
			t.Fatal("C argv bytes differ")
		}
	}
	if string(image.environmentBytes[0]) != env[0]+"\x00" {
		t.Fatal("C env bytes differ")
	}
	for name, in := range map[string]struct{ a, e []string }{
		"missing": {nil, env}, "relative": {[]string{"busybox"}, env}, "alias": {[]string{"/neonvm/../bin/sh"}, env},
		"nil-env": {argv, nil}, "NUL": {[]string{"/neonvm/bin/busybox", "x\x00y"}, env}, "newline-env": {argv, []string{"X=x\ny"}},
		"oversize": {[]string{"/neonvm/bin/busybox", strings.Repeat("x", 8193)}, env},
	} {
		t.Run(name, func(t *testing.T) {
			if _, e := newNormalKernelSignalExecImage(in.a, in.e); e == nil {
				t.Fatal("malformed final vectors accepted")
			}
		})
	}
}
func TestNormalKernelSignalScopeWireAndPrivateMissingSource(t *testing.T) {
	var attr normalKernelSignalRulesetAttr
	var image normalKernelSignalExecImage
	if unsafe.Sizeof(attr) != 24 || unsafe.Offsetof(attr.Scoped) != 16 || normalKernelSignalScope != 2 || unsafe.Offsetof(image.Path) != 0 || unsafe.Offsetof(image.Argv) != 8 || unsafe.Offsetof(image.Environ) != 16 {
		t.Fatal("exact ABI6/ASM pointer layout differs")
	}
	for _, role := range []string{"resource", "compute", "sysinit", "shutdown", "system-role-broker", "unknown"} {
		t.Run(role, func(t *testing.T) {
			if _, e := normalKernelControlExpectedManifest().role(role); e == nil {
				t.Fatal("privileged/unknown role entered ordinary scope")
			}
			if e := execNormalSystemOrdinaryRoleWithKernelControls(context.Background(), nil, role); e == nil {
				t.Fatal("nil opaque original source accepted")
			}
		})
	}
	if e := execNormalKernelOrdinaryRoleSignalScoped(context.Background(), nil); e == nil {
		t.Fatal("nil original plan accepted")
	}
}
