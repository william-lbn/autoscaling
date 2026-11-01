//go:build linux

package resourceguard

import (
	"context"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"testing"
)

func managementTLSRootFixture(t *testing.T) (string, *os.File) {
	t.Helper()
	p, e := os.MkdirTemp("/dev/shm", "neon-management-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { os.RemoveAll(p) })
	if e = os.Chmod(p, 0755); e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(p)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.Close() })
	return p, f
}
func TestNormalOrdinaryManagementTLSOriginalTmpfs(t *testing.T) {
	p, f := managementTLSRootFixture(t)
	if e := prepareNormalOrdinaryManagementTLSAtRun(context.Background(), f); e != nil {
		t.Fatal(e)
	}
	for _, n := range []string{"neon", "neon/management"} {
		st, e := os.Stat(filepath.Join(p, n))
		if e != nil || !st.IsDir() || st.Mode().Perm() != 0755 {
			t.Fatal(n, e)
		}
	}
	// 第二次调用不因原Root目录已存在而收养，外层WAL负责单次事务。
	if e := prepareNormalOrdinaryManagementTLSAtRun(context.Background(), f); e == nil {
		t.Fatal("existing preparation adopted")
	}
}
func TestNormalOrdinaryManagementTLSRejectsForeignOrPartialInput(t *testing.T) {
	for _, kind := range []string{"symlink", "preexisting", "canceled", "bad-mode", "not-tmpfs"} {
		t.Run(kind, func(t *testing.T) {
			p, f := managementTLSRootFixture(t)
			ctx := context.Background()
			switch kind {
			case "symlink":
				if e := os.Symlink("/", filepath.Join(p, "neon")); e != nil {
					t.Fatal(e)
				}
			case "preexisting":
				if e := os.Mkdir(filepath.Join(p, "neon"), 0755); e != nil {
					t.Fatal(e)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "bad-mode":
				if e := os.Chmod(p, 0777); e != nil {
					t.Fatal(e)
				}
			case "not-tmpfs":
				var e error
				f, e = os.Open(t.TempDir())
				if e != nil {
					t.Fatal(e)
				}
				defer f.Close()
				if e = f.Chmod(0755); e != nil {
					t.Fatal(e)
				}
			}
			if e := prepareNormalOrdinaryManagementTLSAtRun(ctx, f); e == nil {
				t.Fatal("foreign source accepted")
			}
			if kind == "symlink" {
				s, e := os.Lstat(filepath.Join(p, "neon"))
				if e != nil || s.Mode()&os.ModeSymlink == 0 {
					t.Fatal("foreign alias changed")
				}
			}
		})
	}
}
func TestNormalOrdinaryManagementTLSPartialFailureKeepsTombstone(t *testing.T) {
	p, f := managementTLSRootFixture(t)
	// 原低层只给固定literal；这里直接造部分前缀验证不会补后半或删前缀。
	if e := unix.Mkdirat(int(f.Fd()), "neon", 0700); e != nil {
		t.Fatal(e)
	}
	if e := prepareNormalOrdinaryManagementTLSAtRun(context.Background(), f); e == nil {
		t.Fatal("partial source replayed")
	}
	if _, e := os.Stat(filepath.Join(p, "neon")); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(p, "neon/management")); !os.IsNotExist(e) {
		t.Fatal("partial directory completed")
	}
}
