//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
)

func TestNormalMemoryCommit2GuestNoDurabilityFromWireOrMissingSource(t *testing.T) {
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	s, e := lockState(context.Background(), dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	if e = normalMemoryAcceptGuestCommit2Locked(nil, s, &Guard{}, nil, bootprotocol.NormalMemoryOriginHostCommit{}); !errors.Is(e, ErrUnknown) {
		t.Fatal("unbound wire was accepted", e)
	}
	var source *normalMemoryOriginSource
	if e = source.recheckCommit2Locked(s); !errors.Is(e, ErrUnknown) {
		t.Fatal("nil source became confirmed", e)
	}
	if e = (&normalMemoryOriginSource{}).recheckCommit2Locked(s); !errors.Is(e, ErrUnknown) {
		t.Fatal("empty private object became confirmed", e)
	}
	for _, name := range []string{normalMemoryGuestIntentFile, normalMemoryGuestCompleteFile, normalMemoryGuestCommitFile} {
		if _, e = os.Stat(filepath.Join(dir, name)); !os.IsNotExist(e) {
			t.Fatal("unbound inputs wrote history", name, e)
		}
	}
}
func TestNormalMemoryCommit2GuestCannotReplaceOriginalRoot(t *testing.T) {
	// 只验证新Root门：未构造合格Source，不能将此模型当真实捕获/恢复正控。
	source := &normalMemoryOriginSource{intent: normalMemoryOriginIntent{OriginalRoot: nativeBirthConsumer{InstanceNonce: "original", PID: 2, StartTicks: control.Uint64(3)}}, currentRoot: nativeBirthConsumer{InstanceNonce: "new", PID: 4, StartTicks: control.Uint64(5)}}
	if _, e := normalMemoryReadGuestCommit2Locked(nil, source); !errors.Is(e, ErrUnknown) {
		t.Fatal("new Root borrowed original producer", e)
	}
	if _, e := normalMemoryGuestExchangeV2(nil, &normalMemoryExchangeTrap{}, nil, bootprotocol.InitialPrebootReceipt{}, bootprotocol.InitialMovableReceipt{}, bootprotocol.InitialCompletion{}); !errors.Is(e, ErrUnknown) {
		t.Fatal("unbound channel accepted", e)
	}
}
