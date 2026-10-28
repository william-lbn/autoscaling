package journal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 真实typed client与RFC6902 peer验证两个提交顺序；不是etcd/网络分区认证。
func TestUnacceptedBeginFenceOrdersDelayedCASAndRecoversLostReply(t *testing.T) {
	for _, mode := range []string{"fence-first", "begin-first", "fence-reply-lost"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			api, store := newStoreAPI(t)
			s := initializeStore(t, api, store)
			r := testRequest(s.State(), 1, control.Grow)
			r.DeadlineUnixMillis = 100
			var err error
			s, err = store.BindPG(ctx, s, s.State().Owner, r.Key.Instance)
			require.NoError(t, err)
			original := s
			if mode == "begin-first" {
				_, disposition, err := store.Begin(ctx, original, original.State().Owner, r, 1)
				require.NoError(t, err)
				require.Equal(t, Accepted, disposition)
			}
			if mode == "fence-reply-lost" {
				api.mu.Lock()
				api.failAfterCommit = true
				api.mu.Unlock()
			}
			_, err = store.FenceUnacceptedBegin(ctx, original, original.State().Owner, r, 100)
			if mode == "fence-first" {
				require.NoError(t, err)
			} else {
				var unknown *UnconfirmedWrite
				require.ErrorAs(t, err, &unknown)
			}
			fresh, err := store.Load(ctx)
			require.NoError(t, err)
			require.NotEqual(t, original.ResourceVersion(), fresh.ResourceVersion())
			if mode == "begin-first" {
				require.Equal(t, Replayed, fresh.State().Lookup(r))
				require.NotNil(t, fresh.State().Active)
			} else {
				require.Equal(t, Stale, fresh.State().Lookup(r))
				want := original.State()
				want.Sequence++
				require.Equal(t, want, fresh.State(), "屏障不补效果、预算、历史或高水位")
				_, _, err = store.Begin(ctx, original, original.State().Owner, r, 1)
				require.Error(t, err, "此前已发送但迟到的旧RV Begin必须失败")
			}
		})
	}
}

func TestUnacceptedBeginFenceRejectsUnexpiredAndDifferentIdentityWithoutWrite(t *testing.T) {
	api, store := newStoreAPI(t)
	ctx := context.Background()
	s := initializeStore(t, api, store)
	r := testRequest(s.State(), 1, control.Grow)
	r.DeadlineUnixMillis = 100
	s, err := store.BindPG(ctx, s, s.State().Owner, r.Key.Instance)
	require.NoError(t, err)
	for _, mode := range []string{"unexpired", "other-owner", "other-boot", "other-epoch"} {
		t.Run(mode, func(t *testing.T) {
			request, actor, now := r, s.State().Owner, uint64(100)
			switch mode {
			case "unexpired":
				now = 99
			case "other-owner":
				actor.HolderID += "-other"
			case "other-boot":
				request.Key.Instance.PGBootID++
			case "other-epoch":
				request.Key.IssuerEpoch++
			}
			api.mu.Lock()
			patches := api.patches
			api.mu.Unlock()
			_, err := store.FenceUnacceptedBegin(ctx, s, actor, request, now)
			require.Error(t, err)
			api.mu.Lock()
			require.Equal(t, patches, api.patches)
			api.mu.Unlock()
		})
	}
}
