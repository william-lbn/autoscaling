//go:build !linux

package resourceguard

// 非Linux没有该生产launcher/registry；原旧模式仍走各自拒绝/验证路径。
func (g *Guard) rejectArrayPendingLocked(s *lockedState, r *record) error { return nil }
