//go:build !linux

package resourceguard

import (
	"context"
	"fmt"
)

// 保持既有非Linux构建；显式PreSys仅能由Linux真实原来源实现，不从模型补资格。
func (g *Guard) bootstrapPreSystemLocked(_ context.Context, _ *lockedState, _ BootstrapCommand, _ bool, _ bool) (*bootstrapPreSystemReference, error) {
	return nil, fmt.Errorf("original pre-System bootstrap source requires Linux")
}
