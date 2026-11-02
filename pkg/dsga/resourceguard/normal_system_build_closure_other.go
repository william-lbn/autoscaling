//go:build !linux

package resourceguard

import (
	"fmt"
	"os"
)

func BuildNormalSystemClosureManifestJSON(input []byte, canonical func(string) (string, error), open func(string) (*os.File, error), lstat func(string) (os.FileInfo, error)) ([]byte, error) {
	return nil, fmt.Errorf("System closure build requires Linux amd64")
}
