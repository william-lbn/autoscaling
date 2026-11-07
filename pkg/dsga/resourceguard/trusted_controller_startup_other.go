//go:build !linux

package resourceguard

import "context"

func WaitTrustedControllerReady(context.Context) error { return ErrUnknown }

func RuntimeComputeWorkload() (string, error) { return "", ErrUnknown }
