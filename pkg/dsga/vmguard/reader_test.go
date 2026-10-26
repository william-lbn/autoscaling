package vmguard

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 只把实际启动/异步floor确认窗口当等待；用生产出生、发布和读者验证。
// 底层QEMU与Kube为明确协议夹具，身份冲突、坏发布不能被“重试”吞掉。
func TestRunnerReaderWaitsOnlyForOriginalStartupAndLaggingFloor(t *testing.T) {
	for _, mode := range []string{"first-qmp", "lease-not-yet-pinned", "lagging-floor", "conflicting-holder", "wrong-lease", "malformed-publication"} {
		t.Run(mode, func(t *testing.T) {
			f := newRunnerFixture(t)
			ctx := context.Background()
			actor, lease := f.actor, "lease-uid"
			f.runner.engine.native = &memoryFixture{mu: sync.Mutex{}, dir: f.config.StateDir, actual: Observation{RequestedBytes: 1 << 30, ActualBytes: 1 << 30}, writes: 0, apply: false, err: nil, entered: nil, release: nil}
			if mode == "lease-not-yet-pinned" {
				require.NoError(t, f.runner.publish(ctx))
			} else if mode != "first-qmp" {
				require.NoError(t, f.runner.Tick(ctx))
			}
			switch mode {
			case "lagging-floor":
				actor = journal.Owner{HolderID: "current-session", Epoch: f.actor.Epoch + 1}
			case "conflicting-holder":
				actor.HolderID = "different-holder-same-epoch"
			case "wrong-lease":
				lease = "same-name-new-lease-uid"
			case "malformed-publication":
				cm, err := f.core.CoreV1().ConfigMaps("testing").Get(ctx, f.runner.birth.StateClaim.Name, metav1.GetOptions{})
				require.NoError(t, err)
				var pub Publication
				require.NoError(t, json.Unmarshal([]byte(cm.Data[ClaimKey]), &pub))
				pub.ProtocolVersion++
				raw, err := json.Marshal(pub)
				require.NoError(t, err)
				cm.Data[ClaimKey] = string(raw)
				_, err = f.core.CoreV1().ConfigMaps("testing").Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			reader, err := NewReader(f.core.CoreV1(), f.store.Binding(), lease)
			require.NoError(t, err)
			_, _, _, err = reader.Inspect(ctx, &f.runner.engine.binding, actor)
			if mode == "first-qmp" || mode == "lease-not-yet-pinned" || mode == "lagging-floor" {
				require.ErrorIs(t, err, ErrUnknown)
			} else {
				require.Error(t, err)
				require.NotErrorIs(t, err, ErrUnknown)
			}
		})
	}
}
