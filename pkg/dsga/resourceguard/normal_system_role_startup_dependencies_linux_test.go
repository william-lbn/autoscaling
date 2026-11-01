//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"testing"
)

// 缺原对象不能进入等待或获得许可；实际原lease/socket/Wait0正例由真PID1组件验证。
func TestNormalFixedStartupRequiresOriginalSources(t *testing.T) {
	t.Run("noPublisherAuthority", func(t *testing.T) {
		if _, e := normalStartupPublisher(&normalOrdinaryUdevV2{}); !errors.Is(e, ErrUnknown) {
			t.Fatalf("raw source: %v", e)
		}
	})
	t.Run("noActionSource", func(t *testing.T) {
		if e := (&normalOrdinaryActionsV2{}).awaitOriginalUdevReady(context.Background(), nil, nil); !errors.Is(e, ErrUnknown) {
			t.Fatalf("raw action: %v", e)
		}
	})
	t.Run("noOriginalComputeBroker", func(t *testing.T) {
		if e := (&normalOrdinaryUdevV2{}).computeStartupLocked(context.Background(), nil, nil, nil); !errors.Is(e, ErrUnknown) {
			t.Fatalf("raw compute: %v", e)
		}
	})
	t.Run("canceledReadiness", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if e := (&normalOrdinaryUdevV2{}).ensureOriginalUdevReadyLocked(ctx, nil, nil); !errors.Is(e, ErrUnknown) {
			t.Fatalf("canceled: %v", e)
		}
	})
}
