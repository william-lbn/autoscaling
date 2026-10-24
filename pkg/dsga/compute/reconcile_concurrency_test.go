package compute

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTerminalReconcileCannotClearAnExecutingOriginal(t *testing.T) {
	for _, mode := range []string{"already-executing", "begins-during-read"} {
		t.Run(mode, func(t *testing.T) {
			intent, saved := fixtures(t)
			readEntered, execEntered := make(chan struct{}), make(chan struct{})
			releaseRead, releaseExec := make(chan struct{}), make(chan struct{})
			finishRead, finishExec := sync.OnceFunc(func() { close(releaseRead) }), sync.OnceFunc(func() { close(releaseExec) })
			defer finishRead()
			defer finishExec()
			var reads atomic.Int32
			_, config := newTLSClient(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/dsg/v1/resize":
					close(execEntered)
					<-releaseExec
				case "/dsg/v1/resize/status":
					reads.Add(1)
					close(readEntered)
					<-releaseRead
				default:
					t.Error("unexpected reconcile concurrency path", r.URL.Path)
				}
				reply(w, saved)
			})
			config.Pending = &intent
			client, err := New(context.Background(), config)
			require.NoError(t, err)
			defer client.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			execDone, readDone := make(chan error, 1), make(chan error, 1)
			execute := func() { _, err := client.Resize(ctx, intent); execDone <- err }
			observe := func() {
				_, err := client.ReconcileTerminal(ctx, intent, saved, terminalFence(intent, config.IssuerEpoch, saved))
				readDone <- err
			}
			if mode == "already-executing" {
				go execute()
				<-execEntered
				go observe()
			} else {
				go observe()
				<-readEntered
				go execute()
				<-execEntered
				finishRead()
			}
			require.Error(t, <-readDone)
			require.Equal(t, &intent, client.Pending(), "任何执行在途时只读核对都不能清栅栏")
			if mode == "already-executing" {
				require.Zero(t, reads.Load(), "先检查本地执行状态，不发送查询")
			}
			finishExec()
			require.NoError(t, <-execDone)
			require.Nil(t, client.Pending(), "实际Resize自己的完整终态才能结束该执行")
		})
	}
}
