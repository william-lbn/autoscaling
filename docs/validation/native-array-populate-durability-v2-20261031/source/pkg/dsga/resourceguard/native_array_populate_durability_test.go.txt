//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

func arrayPopulateBindFixtureState(t *testing.T, s *lockedState, p *arrayPopulatePending) {
	t.Helper()
	key, err := arrayPopulateStateKey(s, p.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	p.StateDevice, p.StateInode = control.Uint64(key.StateDevice), control.Uint64(key.StateInode)
	p.LockDevice, p.LockInode = control.Uint64(key.LockDevice), control.Uint64(key.LockInode)
}

func arrayPopulateDurabilityFixture(t *testing.T) (*lockedState, arrayPopulatePending, arrayPopulateComplete) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := lockState(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	p := arrayPopulateFixture(t)
	arrayPopulateBindFixtureState(t, s, &p)
	z := arrayPopulateWritePrefix(t, s, p)
	return s, p, z
}
func arrayPopulateWritePrefix(t *testing.T, s *lockedState, p arrayPopulatePending) arrayPopulateComplete {
	t.Helper()
	raw, err := arrayPopulateAppend(s, p.Nonce, "pending", p)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = arrayPopulateAppend(s, p.Nonce, "child", arrayPopulateChild{ProtocolVersion: 1, PendingSHA256: physicalDigest(raw), PID: 555, StartTicks: 777, PIDFDDevice: 22, PIDFDInode: 88})
	if err != nil {
		t.Fatal(err)
	}
	raw, err = arrayPopulateAppend(s, p.Nonce, "prepared", arrayPopulatePrepared{ProtocolVersion: 1, TracerTID: 555, ChildSHA256: physicalDigest(raw), StatusSHA256: strings.Repeat("ab", 32), FilterSHA256: strings.Repeat("cd", 32), VmSizeBytes: 2 << 20, LockedBytes: 80 << 10})
	if err != nil {
		t.Fatal(err)
	}
	return arrayPopulateComplete{ProtocolVersion: 1, PreparedSHA256: physicalDigest(raw), PID: 555, ExitObserved: true, Progress: [4]bool{true, true, true, true}}
}
func arrayPopulateVisibleComplete(t *testing.T, s *lockedState, p arrayPopulatePending, z arrayPopulateComplete) {
	t.Helper()
	raw, err := json.Marshal(z)
	if err != nil {
		t.Fatal(err)
	}
	// 明确模型：直接写出完整可见JSON而不给持久成功见证，不声称真实磁盘故障。
	if err := os.WriteFile(filepath.Join(s.dir.Name(), arrayPopulateName(p.Nonce, "complete")), raw, 0600); err != nil {
		t.Fatal(err)
	}
}
func arrayPopulateSnapshot(t *testing.T, s *lockedState) map[string]string {
	t.Helper()
	names, err := os.ReadDir(s.dir.Name())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range names {
		if !strings.HasPrefix(e.Name(), "array-populate.") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.dir.Name(), e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		st, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = physicalDigest(raw) + "/" + st.ModTime().UTC().String()
	}
	return out
}
func arrayPopulateExpectUnknown(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrUnknown) {
		t.Fatalf("Unknown required: %v", err)
	}
}

// PG17-U-ARRAY-DURABILITY-001：复现完整JSON可见但原完成同步失败，普通门禁零写。
// 注入的是确定性IO模型；没有把它称作Guest/硬件断电持久故障实验。
func TestNativeArrayPopulateDurabilityVisibleFailure(t *testing.T) {
	for _, mode := range []string{"complete-create-sync", "prefix-resync", "directory-sync"} {
		t.Run(mode, func(t *testing.T) {
			s, p, z := arrayPopulateDurabilityFixture(t)
			disk := arrayPopulateDiskDurability()
			switch mode {
			case "complete-create-sync":
				disk.create = func(s *lockedState, name string, raw []byte) error {
					if err := os.WriteFile(filepath.Join(s.dir.Name(), name), raw, 0600); err != nil {
						return err
					}
					return unix.EIO // createPhysicalFile写完后f.Sync失败的精确可见性模型。
				}
			case "prefix-resync":
				disk.syncFile = func(*lockedState, string) error { return unix.EIO }
			case "directory-sync":
				disk.syncDir = func(*lockedState) error { return unix.EIO }
			}
			_, err := arrayPopulateAppendWithDurability(s, p.Nonce, "complete", z, disk)
			arrayPopulateExpectUnknown(t, err)
			if _, err := arrayPopulateCompleteKeyLocked(s, p.Nonce); err != nil {
				t.Fatal("fixture complete must be visible", err)
			}
			before := arrayPopulateSnapshot(t, s)
			for i := 0; i < 3; i++ {
				arrayPopulateExpectUnknown(t, rejectOutstandingArrayPopulateLocked(s))
			}
			if !reflect.DeepEqual(before, arrayPopulateSnapshot(t, s)) {
				t.Fatal("ordinary gate wrote or changed original files")
			}
			if err := recoverArrayPopulateCompletedLocked(s, p.Nonce); err != nil {
				t.Fatal(err)
			}
			if err := rejectOutstandingArrayPopulateLocked(s); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, arrayPopulateSnapshot(t, s)) {
				t.Fatal("recovery changed nonce or JSON bytes")
			}
		})
	}
}

// PG17-U-ARRAY-DURABILITY-002：每一前缀/目录同步失败均保Unknown；精确顺序之后才登记。
func TestNativeArrayPopulateDurabilityRecoveryFailures(t *testing.T) {
	for _, fail := range []string{"pending", "child", "prepared", "complete", "directory"} {
		t.Run(fail, func(t *testing.T) {
			s, p, z := arrayPopulateDurabilityFixture(t)
			arrayPopulateVisibleComplete(t, s, p, z)
			disk := arrayPopulateDiskDurability()
			realFile, realDir := disk.syncFile, disk.syncDir
			var seen []string
			disk.syncFile = func(s *lockedState, name string) error {
				phase := strings.Split(name, ".")[2]
				seen = append(seen, phase)
				if phase == fail {
					return unix.EIO
				}
				return realFile(s, name)
			}
			disk.syncDir = func(s *lockedState) error {
				seen = append(seen, "directory")
				if fail == "directory" {
					return unix.EIO
				}
				return realDir(s)
			}
			arrayPopulateExpectUnknown(t, arrayPopulateConfirmDurableLocked(s, p.Nonce, disk))
			arrayPopulateExpectUnknown(t, rejectOutstandingArrayPopulateLocked(s))
			if len(seen) == 0 || seen[len(seen)-1] != fail {
				t.Fatal("sync did not stop at original failure", seen)
			}
			var successful []string
			disk = arrayPopulateDiskDurability()
			realFile, realDir = disk.syncFile, disk.syncDir
			disk.syncFile = func(s *lockedState, name string) error {
				successful = append(successful, strings.Split(name, ".")[2])
				return realFile(s, name)
			}
			disk.syncDir = func(s *lockedState) error { successful = append(successful, "directory"); return realDir(s) }
			if err := arrayPopulateConfirmDurableLocked(s, p.Nonce, disk); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(successful, []string{"pending", "child", "prepared", "complete", "directory"}) {
				t.Fatal(successful)
			}
			if err := rejectOutstandingArrayPopulateLocked(s); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("changed-after-sync", func(t *testing.T) {
		s, p, z := arrayPopulateDurabilityFixture(t)
		arrayPopulateVisibleComplete(t, s, p, z)
		disk := arrayPopulateDiskDurability()
		disk.syncDir = func(s *lockedState) error {
			path := filepath.Join(s.dir.Name(), arrayPopulateName(p.Nonce, "complete"))
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			// JSON仍合法但完整字节改变，不能借同步前摘要登记。
			return os.WriteFile(path, append(raw, '\n'), 0600)
		}
		arrayPopulateExpectUnknown(t, arrayPopulateConfirmDurableLocked(s, p.Nonce, disk))
		arrayPopulateExpectUnknown(t, rejectOutstandingArrayPopulateLocked(s))
	})
}

// PG17-U-ARRAY-DURABILITY-003：真实新Go进程没有旧Root内存见证；仅显式Recover解除。
// 它验证实际进程/文件fsync语义，PG身份与ExitObserved在此仍为模型。
func TestNativeArrayPopulateDurabilityProcessRecovery(t *testing.T) {
	if dir := os.Getenv("DSGA_ARRAY_DURABILITY_MODEL_DIR"); dir != "" {
		s, err := lockState(context.Background(), dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.close()
		before := arrayPopulateSnapshot(t, s)
		arrayPopulateExpectUnknown(t, rejectOutstandingArrayPopulateLocked(s))
		if !reflect.DeepEqual(before, arrayPopulateSnapshot(t, s)) {
			t.Fatal("new Root ordinary gate wrote")
		}
		if err := recoverArrayPopulateCompletedLocked(s, os.Getenv("DSGA_ARRAY_DURABILITY_MODEL_NONCE")); err != nil {
			t.Fatal(err)
		}
		if err := rejectOutstandingArrayPopulateLocked(s); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, arrayPopulateSnapshot(t, s)) {
			t.Fatal("new Root rewrote chain")
		}
		return
	}
	s, p, z := arrayPopulateDurabilityFixture(t)
	if _, err := arrayPopulateAppend(s, p.Nonce, "complete", z); err != nil {
		t.Fatal(err)
	}
	if err := rejectOutstandingArrayPopulateLocked(s); err != nil {
		t.Fatal(err)
	}
	before := arrayPopulateSnapshot(t, s)
	dir := s.dir.Name()
	s.close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestNativeArrayPopulateDurabilityProcessRecovery$", "-test.timeout=10s")
	cmd.Env = append(os.Environ(), "DSGA_ARRAY_DURABILITY_MODEL_DIR="+dir, "DSGA_ARRAY_DURABILITY_MODEL_NONCE="+p.Nonce)
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("new Root recovery failed: %v %s", err, raw)
	}
	reopened, err := lockState(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if !reflect.DeepEqual(before, arrayPopulateSnapshot(t, reopened)) {
		t.Fatal("process recovery changed original files")
	}
}

// PG17-U-ARRAY-DURABILITY-004：partial不补、错误/非退出链不认，失败恢复没有持久副作用。
func TestNativeArrayPopulateDurabilityPartialAndInvalid(t *testing.T) {
	for _, mode := range []string{"missing-complete", "missing-prepared", "not-exited", "wrong-pid", "wrong-prefix"} {
		t.Run(mode, func(t *testing.T) {
			s, p, z := arrayPopulateDurabilityFixture(t)
			if mode != "missing-complete" {
				if mode == "not-exited" {
					z.ExitObserved = false
				}
				if mode == "wrong-pid" {
					z.PID++
				}
				if mode == "wrong-prefix" {
					z.PreparedSHA256 = strings.Repeat("aa", 32)
				}
				arrayPopulateVisibleComplete(t, s, p, z)
			}
			if mode == "missing-prepared" {
				if err := os.Remove(filepath.Join(s.dir.Name(), arrayPopulateName(p.Nonce, "prepared"))); err != nil {
					t.Fatal(err)
				}
			}
			before := arrayPopulateSnapshot(t, s)
			arrayPopulateExpectUnknown(t, recoverArrayPopulateCompletedLocked(s, p.Nonce))
			arrayPopulateExpectUnknown(t, rejectOutstandingArrayPopulateLocked(s))
			if !reflect.DeepEqual(before, arrayPopulateSnapshot(t, s)) {
				t.Fatal("partial recovered or changed")
			}
		})
	}
}

// PG17-U-ARRAY-DURABILITY-005：原目录/锁/nonce/完整四摘要互相隔离，既有见证不可借给新链。
func TestNativeArrayPopulateDurabilityWitnessIsolation(t *testing.T) {
	s, p, z := arrayPopulateDurabilityFixture(t)
	if _, err := arrayPopulateAppend(s, p.Nonce, "complete", z); err != nil {
		t.Fatal(err)
	}
	t.Run("new-nonce", func(t *testing.T) {
		next := p
		next.Nonce = strings.Repeat("03", 32)
		completion := arrayPopulateWritePrefix(t, s, next)
		arrayPopulateVisibleComplete(t, s, next, completion)
		arrayPopulateExpectUnknown(t, rejectOutstandingArrayPopulateLocked(s))
		if err := recoverArrayPopulateCompletedLocked(s, next.Nonce); err != nil {
			t.Fatal(err)
		}
		if err := rejectOutstandingArrayPopulateLocked(s); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("changed-complete-bytes", func(t *testing.T) {
		path := filepath.Join(s.dir.Name(), arrayPopulateName(p.Nonce, "complete"))
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(bytes.Clone(raw), '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		arrayPopulateExpectUnknown(t, rejectOutstandingArrayPopulateLocked(s))
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		arrayPopulateExpectUnknown(t, rejectOutstandingArrayPopulateLocked(s))
		if err := recoverArrayPopulateCompletedLocked(s, p.Nonce); err != nil {
			t.Fatal(err)
		}
		if err := rejectOutstandingArrayPopulateLocked(s); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("same-bytes-new-inode", func(t *testing.T) {
		path := filepath.Join(s.dir.Name(), arrayPopulateName(p.Nonce, "complete"))
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		backup := filepath.Join(s.dir.Name(), "original-complete-preserved")
		if err := os.Rename(path, backup); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		arrayPopulateExpectUnknown(t, rejectOutstandingArrayPopulateLocked(s))
		if err := recoverArrayPopulateCompletedLocked(s, p.Nonce); err != nil {
			t.Fatal(err)
		}
		if err := rejectOutstandingArrayPopulateLocked(s); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("copied-chain-other-state", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
		other, err := lockState(context.Background(), dir)
		if err != nil {
			t.Fatal(err)
		}
		defer other.close()
		for name := range arrayPopulateSnapshot(t, s) {
			raw, err := os.ReadFile(filepath.Join(s.dir.Name(), name))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
		arrayPopulateExpectUnknown(t, rejectOutstandingArrayPopulateLocked(other))
		arrayPopulateExpectUnknown(t, recoverArrayPopulateCompletedLocked(other, p.Nonce))
	})
}
