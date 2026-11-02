//go:build linux

package resourceguard

import (
	"context"
	"fmt"
	"os"
)

// 旧入口保来源/Open行为；新broker不调用此wrapper，直接传原g给私有构造器。
func ServeGuest(ctx context.Context) error {
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return fmt.Errorf("resource service requires real root")
	}
	source, e := sealedRuntimeSource()
	if e != nil {
		return e
	}
	if source.Config.ExecutionModel != TrustedControllerExecutionModel {
		if e := rejectNormalStandaloneBrokerOwnedAuthority(); e != nil {
			return e
		}
	}
	g, e := Open(source.Config)
	if e != nil {
		return e
	}
	instance, e := prepareNormalGuestResourceInstance(ctx, g, source, nil)
	if e != nil {
		return e
	}
	defer instance.close()
	return instance.wait()
}
