//go:build dsga_native_trusted_control && dsga_native_stock_preparation && linux && dsga_native_initial_lifecycle && dsga_native_memory_origin_wire && dsga_native_memory_commit2 && dsga_native_role_transfer2 && dsga_native_ordinary2

package vmguard

import (
	"context"
	"time"
)

// 只复用原固定nativeQMP受界Read；不Tick、不Set、不写WAL、不授VM许可。
func (f *NativePG17TrustedHostFixture) ReadTrustedTakeoverMemory(ctx context.Context) (Observation, error) {
	if f == nil || f.Runner == nil || f.Runner.engine == nil || f.Runner.engine.native == nil {
		return Observation{}, ErrUnknown
	}
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return f.Runner.engine.native.Read(bounded)
}
