//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func stockPreparationWaitFixture() []normalOrdinaryStockUdevWait {
	return []normalOrdinaryStockUdevWait{{Stage: normalOrdinaryStockUdevDaemon, PID: 21, PIDFSDevice: 7, PIDFSInode: 101}, {Stage: normalOrdinaryStockUdevControlReady, PID: 22, PIDFSDevice: 7, PIDFSInode: 102}, {Stage: normalOrdinaryStockUdevTriggerDevices, PID: 23, PIDFSDevice: 7, PIDFSInode: 103}, {Stage: normalOrdinaryStockUdevSettle, PID: 24, PIDFSDevice: 7, PIDFSInode: 104}}
}

// 纯Wait资料验证不是实际cmd.Wait或Source正控；真实主ctor仅消费原Programs.run。
func TestNormalOrdinaryStockPreparationWaitReceipt(t *testing.T) {
	waits := stockPreparationWaitFixture()
	secret := []byte("test-only-not-logged-value")
	raw, e := normalOrdinaryStockUdevPreparationWaitBytes(waits, secret)
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(raw, secret) || !bytes.Contains(raw, []byte(physicalDigest(secret))) {
		t.Fatal("log values escaped hash-only receipt")
	}
	for _, name := range []string{"missing-settle", "extra-orphan", "duplicate-stage", "missing-pidfs", "same-object", "unknown-stage", "nonzero-exit", "wait-error", "log-overflow"} {
		t.Run(name, func(t *testing.T) {
			w := stockPreparationWaitFixture()
			log := []byte{}
			switch name {
			case "missing-settle":
				w = w[:3]
			case "extra-orphan":
				w = append(w, normalOrdinaryStockUdevWait{PID: 25, ExitCode: 0})
			case "duplicate-stage":
				w[3].Stage = w[2].Stage
			case "missing-pidfs":
				w[1].PIDFSInode = 0
			case "same-object":
				w[1].PIDFSInode = w[0].PIDFSInode
			case "unknown-stage":
				w[2].Stage = 99
			case "nonzero-exit":
				w[3].ExitCode = 7
			case "wait-error":
				w[1].WaitError = "wait ECHILD"
			case "log-overflow":
				log = make([]byte, (32<<10)+1)
			}
			if _, e := normalOrdinaryStockUdevPreparationWaitBytes(w, log); e == nil {
				t.Fatal("incomplete Wait accepted")
			}
		})
	}
}
func TestNormalOrdinaryStockPreparationSingleUseTombstone(t *testing.T) {
	g := &Guard{}
	q := &normalOrdinaryUdevV2{g: g}
	defer normalOrdinaryStockUdevPreparations.Delete(g)
	var mu sync.Mutex
	wins := 0
	var first *normalOrdinaryStockUdevPreparation
	var wg sync.WaitGroup
	for n := 0; n < 20; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, e := normalOrdinaryStockUdevReservePreparation(q)
			if e == nil {
				mu.Lock()
				wins++
				first = p
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 || first == nil {
		t.Fatal(wins)
	}
	first.close()
	if _, e := normalOrdinaryStockUdevReservePreparation(q); e == nil {
		t.Fatal("failure/close permitted recapture epoch")
	}
	if _, e := first.referenceLocked(context.Background(), nil); e == nil {
		t.Fatal("reserved raw slot created source")
	}
}
func stockPreparationStore(t *testing.T) *lockedState {
	t.Helper()
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	s, e := lockState(context.Background(), dir)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.close)
	return s
}
func TestNormalOrdinaryStockPreparationHistoryNeverImplicitlyReplayed(t *testing.T) {
	for _, stage := range []string{"intent-only", "complete-only", "symlink"} {
		t.Run(stage, func(t *testing.T) {
			s := stockPreparationStore(t)
			if e := normalOrdinaryStockUdevPreparationRejectHistory(s); e != nil {
				t.Fatal(e)
			}
			name := normalOrdinaryStockUdevPreparationIntentName
			if stage == "complete-only" {
				name = normalOrdinaryStockUdevPreparationCompleteName
			}
			if stage == "symlink" {
				if e := os.Symlink("missing", filepath.Join(s.dir.Name(), name)); e != nil {
					t.Fatal(e)
				}
			} else if e := s.createPhysicalFile(name, []byte("{}")); e != nil {
				t.Fatal(e)
			}
			if e := normalOrdinaryStockUdevPreparationRejectHistory(s); !errors.Is(e, ErrUnknown) {
				t.Fatal("visible/partial history reopened", e)
			}
			if _, e := os.Lstat(filepath.Join(s.dir.Name(), name)); e != nil {
				t.Fatal("partial history deleted", e)
			}
		})
	}
}
func TestNormalOrdinaryStockPreparationOriginalPrefixIdentity(t *testing.T) {
	// 这里只验通用文件见证比较，不从造出的JSON生成真实准备对象。
	for _, which := range []string{"intent-replace", "complete-replace", "intent-bytes", "complete-bytes"} {
		t.Run(which, func(t *testing.T) {
			s := stockPreparationStore(t)
			p := &normalOrdinaryStockUdevPreparation{}
			names := []string{normalOrdinaryStockUdevPreparationIntentName, normalOrdinaryStockUdevPreparationCompleteName}
			for n, name := range names {
				if e := s.createPhysicalFile(name, []byte("{}")); e != nil {
					t.Fatal(e)
				}
				w, e := readNormalSystemRoleFileWitness(s, name)
				if e != nil {
					t.Fatal(e)
				}
				p.history[n] = w
			}
			if e := p.verifyHistoryLocked(s); e != nil {
				t.Fatal(e)
			}
			index := 0
			if which == "complete-replace" || which == "complete-bytes" {
				index = 1
			}
			target := filepath.Join(s.dir.Name(), names[index])
			if which == "intent-replace" || which == "complete-replace" {
				if e := os.Rename(target, target+".old"); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(target, []byte("{}"), 0600); e != nil {
					t.Fatal(e)
				}
			} else if e := os.WriteFile(target, []byte("{\"x\":1}"), 0600); e != nil {
				t.Fatal(e)
			}
			if e := p.verifyHistoryLocked(s); !errors.Is(e, ErrUnknown) {
				t.Fatal("original inode/bytes drift accepted", e)
			}
		})
	}
}
func TestNormalOrdinaryStockPreparationCannotAdoptRawInputs(t *testing.T) {
	g := &Guard{}
	if p, e := g.normalOrdinaryUdevV2PreparationLocked(context.Background(), nil, nil, nil, nil, nil); e == nil || p != nil {
		t.Fatal("raw inputs made producer")
	}
	if _, e := originalNormalOrdinaryStockUdevPreparationSource(g, &normalFilesystemOriginSource{}, &normalSystemRoleBrokerExecProof{}, &normalSystemResourceAuthority{}); e == nil {
		t.Fatal("foreign objects accepted")
	}
	if p, e := (*normalOrdinaryStockUdevPreparation)(nil).duplicateOriginalRules(context.Background()); e == nil || p != nil {
		t.Fatal("unbound rule reader")
	}
	if _, e := (&normalOrdinaryStockUdevPreparation{complete: true}).projectionForOriginalDeviceLocked(context.Background(), nil, &normalOrdinaryUdevV2CatalogEntry{}); e == nil {
		t.Fatal("caller ready flag created sealed projection")
	}
}
