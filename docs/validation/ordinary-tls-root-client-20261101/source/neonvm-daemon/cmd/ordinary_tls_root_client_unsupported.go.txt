//go:build !linux

package main

import "os"

func ordinaryTLSNeedsRootDelegate() bool { return os.Getuid() != 0 || os.Geteuid() != 0 }
func newOrdinaryTLSRootDelegate() (ordinaryTLSRootDelegate, error) {
	return nil, errOrdinaryTLSRootDelegateUnavailable
}
