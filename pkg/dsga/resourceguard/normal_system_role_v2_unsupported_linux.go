//go:build linux && !amd64

package resourceguard

import "context"

// 非已证架构不能用旧Root角色替代能力2。
func normalSystemRoleV2InspectTarget(context.Context, *normalSystemLifecycleSource, *normalSystemOriginalTask, normalSystemRoleLaunchSpec) (normalSystemRoleObservation, error) {
	return normalSystemRoleObservation{}, ErrUnknown
}
func execNormalSystemOrdinaryRoleWithKernelControlsV2(context.Context, *normalSystemLifecycleSource, string) error {
	return ErrUnknown
}

func inspectNormalSystemRoleV2ControlsAtStop(ctx context.Context, s *normalSystemLifecycleSource, task *normalSystemOriginalTask, spec normalSystemRoleLaunchSpec, control *nativeTraceOriginalControl, tid int) (normalSystemRoleV2ControlWitness, error) {
	return nil, ErrUnknown
}
