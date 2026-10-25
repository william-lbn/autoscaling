package owner

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
)

// 首次发现是实际SDK/TLS读取，不能等首个Resize才保存boot。此前若只重造
// worker而没有HWM，新PG会被当作另一次首次发现，掩盖前一个实例的准入寿命。
func TestPGWorkerRejectsBootReplacementBeforeAnyResize(t *testing.T) {
	_, store, coord := newAPI(t)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var replacementErr error
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		request := pgRequest(t, session.Actor())
		request.Key.Instance.PGBootID = 11
		config := pgConfig(t, request, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) })
		config.Instance.PGBootID = 0
		worker, err := session.NewPGWorker(ctx, config)
		if err != nil {
			return err
		}
		defer worker.CloseIdleConnections()
		request.Key.Instance.PGBootID = 12
		replacementConfig := pgConfig(t, request, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) })
		replacementConfig.Instance.PGBootID = 0
		replacement, observedErr := session.NewPGWorker(ctx, replacementConfig)
		replacementErr = observedErr
		if replacement != nil {
			replacement.CloseIdleConnections()
		}
		return nil
	})
	require.NoError(t, err)
	require.Error(t, replacementErr, "没有Resize/HWM也不能接受另一个PG boot")
}

// 使用实际Owner/能力协商/typed Store，不发送任何PG管理写。CAS回复丢失后
// 原绑定必须已经在VM账本中，重建只认它；跨boot须独立受控恢复。
func TestPGWorkerFirstBindingPersistsBeforeReturningUsableClient(t *testing.T) {
	for _, loseReply := range []bool{false, true} {
		t.Run(map[bool]string{false: "confirmed", true: "lost-reply"}[loseReply], func(t *testing.T) {
			api, store, coord := newAPI(t)
			elector, err := New(coord, store, testOptions())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancel()
			err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
				request := pgRequest(t, session.Actor())
				request.Key.Instance.PGBootID = 11
				config := pgConfig(t, request, func(w http.ResponseWriter, _ *http.Request) {
					t.Error("首次身份冻结不能发送PG写操作")
					w.WriteHeader(500)
				})
				config.Instance.PGBootID = 0
				api.mu.Lock()
				api.loseJournalReply = loseReply
				api.mu.Unlock()
				worker, err := session.NewPGWorker(ctx, config)
				if loseReply {
					var unknown *journal.UnconfirmedWrite
					require.ErrorAs(t, err, &unknown)
					require.Nil(t, worker, "没有CAS确认不能返回可派发worker")
				} else {
					require.NoError(t, err)
					worker.CloseIdleConnections()
				}
				api.mu.Lock()
				api.loseJournalReply = false
				api.mu.Unlock()
				saved, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, &request.Key.Instance, saved.State().PGInstance)
				require.EqualValues(t, 2, saved.State().ProtocolVersion)
				require.Nil(t, saved.State().Active)
				require.Nil(t, saved.State().HighWater)
				sequence := saved.State().Sequence
				recovered, err := session.NewPGWorker(ctx, config)
				require.NoError(t, err)
				defer recovered.CloseIdleConnections()
				again, err := store.Load(ctx)
				require.NoError(t, err)
				require.Equal(t, sequence, again.State().Sequence, "恢复同一绑定不能重新写入")
				// 显式传入新boot也不能通过已认证固定Compute绕过原绑定。
				var tokenCalled bool
				foreign := config
				foreign.Instance.PGBootID = 12
				foreign.Token = func(context.Context) (string, error) { tokenCalled = true; return "fixture", nil }
				_, err = session.NewPGWorker(ctx, foreign)
				require.Error(t, err)
				require.False(t, tokenCalled)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// 参数错误必须先于能力HTTP和首次固定，不能用一次无效构造污染永久身份。
func TestPGWorkerInvalidResourcesCannotFreezeFirstPGIdentity(t *testing.T) {
	_, store, coord := newAPI(t)
	elector, err := New(coord, store, testOptions())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = elector.Run(ctx, func(ctx context.Context, session *Session) error {
		request := pgRequest(t, session.Actor())
		config := pgConfig(t, request, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) })
		config.Instance.PGBootID = 0
		var tokenCalled bool
		config.Token = func(context.Context) (string, error) { tokenCalled = true; return "fixture", nil }
		for _, mode := range []string{"nil", "multiple", "foreign-session"} {
			t.Run(mode, func(t *testing.T) {
				var args []*ResourceWorker
				switch mode {
				case "nil":
					args = []*ResourceWorker{nil}
				case "multiple":
					args = []*ResourceWorker{nil, nil}
				case "foreign-session":
					args = []*ResourceWorker{{session: nil}}
				}
				worker, err := session.NewPGWorker(ctx, config, args...)
				require.Error(t, err)
				require.Nil(t, worker)
				require.False(t, tokenCalled)
				current, err := store.Load(ctx)
				require.NoError(t, err)
				require.Nil(t, current.State().PGInstance)
				require.EqualValues(t, 1, current.State().ProtocolVersion)
			})
		}
		return nil
	})
	require.NoError(t, err)
}
