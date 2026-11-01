//go:build linux && amd64

package resourceguard

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNormalKernelControlMemoryAuthorityRejectsAbsentOrAmbiguousSource(t *testing.T) {
	g := &Guard{}
	// 非nil lock外壳只能进入类型门，不构造实际锁/权限；两源/缺源须在读写前拒。
	state := &lockedState{}
	for _, pair := range []struct {
		original *normalMemoryOriginSource
		broker   *normalMemoryBrokerConsumerSource
	}{
		{}, {&normalMemoryOriginSource{}, &normalMemoryBrokerConsumerSource{}},
	} {
		_, _, e := normalKernelControlMemoryBindingLocked(context.Background(), state, g, pair.original, pair.broker)
		require.ErrorIs(t, e, ErrUnknown)
	}
}

func TestNormalKernelControlBrokerCannotAdoptMemoryReferenceOrOtherGuard(t *testing.T) {
	g := &Guard{}
	// raw历史值/新结构对象没有private消费见证，不能产生R2当前Authority。
	raw := &normalMemoryBrokerConsumerSource{g: g, filesystem: &normalFilesystemOriginSource{}, proof: &normalSystemRoleBrokerExecProof{}}
	_, _, e := normalKernelControlMemoryBindingLocked(context.Background(), &lockedState{}, g, nil, raw)
	require.ErrorIs(t, e, ErrUnknown)
	other := &Guard{}
	_, e = captureNormalKernelControlSourceForBrokerLocked(context.Background(), &lockedState{}, g, &normalFilesystemOriginSource{g: other}, &normalSystemLifecycleSource{}, nil)
	require.ErrorIs(t, e, ErrUnknown)
	_, e = captureNormalKernelControlSourceLocked(context.Background(), &lockedState{}, g, nil, &normalSystemLifecycleSource{}, nil)
	require.ErrorIs(t, e, ErrUnknown)
}
