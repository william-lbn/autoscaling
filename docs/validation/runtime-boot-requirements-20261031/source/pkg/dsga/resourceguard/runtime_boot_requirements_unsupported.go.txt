//go:build !linux

package resourceguard

import "fmt"

func deriveRuntimeBootRequirementsPlatform(RuntimeBootRequirements, RuntimeDocument, SourceConfig, map[string][]byte) (RuntimeBootRequirements, error) {
	return RuntimeBootRequirements{}, fmt.Errorf("runtime normal-split boot requirements unsupported on non-Linux")
}
