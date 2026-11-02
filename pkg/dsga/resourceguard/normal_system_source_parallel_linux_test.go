//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// 独立只读根容器提供真实文件；只验本次并发文件reader，不假装完整Source或kernel资格。
func normalSystemParallelFixture(t *testing.T) map[string]NormalSystemSourceFile {
	t.Helper()
	raw, e := os.ReadFile("/source-parallel/manifest.json")
	if e != nil {
		t.Fatal(e)
	}
	var files map[string]NormalSystemSourceFile
	if e = json.Unmarshal(raw, &files); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"small", "notELF", "large0", "large1", "inittab"} {
		if files[name].Path == "" {
			t.Fatalf("fixture missing %s", name)
		}
	}
	return files
}
func normalSystemParallelOpenFDs(paths []string) (int, error) {
	wanted := map[string]bool{}
	for _, p := range paths {
		wanted[p] = true
	}
	entries, e := os.ReadDir("/proc/self/fd")
	if e != nil {
		return 0, e
	}
	count := 0
	for _, entry := range entries {
		target, e := os.Readlink("/proc/self/fd/" + entry.Name())
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return 0, e
		}
		if wanted[target] {
			count++
		}
	}
	return count, nil
}
func normalSystemParallelAssertClosed(t *testing.T, files []NormalSystemSourceFile) {
	t.Helper()
	paths := []string{}
	for _, f := range files {
		paths = append(paths, f.CanonicalPath)
	}
	n, e := normalSystemParallelOpenFDs(paths)
	if e != nil || n != 0 {
		t.Fatalf("reader returned with original file FD still open: %d %v", n, e)
	}
}

func TestNormalSystemSourceParallelFilesOriginalRO(t *testing.T) {
	f := normalSystemParallelFixture(t)
	t.Run("orderedFullRead", func(t *testing.T) {
		input := []NormalSystemSourceFile{f["large0"], f["small"], f["inittab"]}
		got, tab, fixed, e := normalSystemInspectSourceFiles(context.Background(), 3, input)
		if e != nil {
			t.Fatal(e)
		}
		if len(got) != 3 || !fixed || tab != f["inittab"].SHA256 {
			t.Fatalf("incomplete original observations: %d %t %q", len(got), fixed, tab)
		}
		for i, x := range input {
			if got[i].Path != x.Path || got[i].SHA256 != x.SHA256 || got[i].Bytes != x.Bytes {
				t.Fatalf("manifest order/bytes differ at %d", i)
			}
		}
		normalSystemParallelAssertClosed(t, input)
	})
	t.Run("wrongDigestJoinsAll", func(t *testing.T) {
		bad := f["small"]
		bad.SHA256 = physicalDigest([]byte("not-original-config"))
		input := []NormalSystemSourceFile{f["large0"], bad, f["large1"]}
		got, tab, fixed, e := normalSystemInspectSourceFiles(context.Background(), 3, input)
		if e == nil || got != nil || tab != "" || fixed {
			t.Fatal("failed reader exposed partial observation")
		}
		normalSystemParallelAssertClosed(t, input)
	})
	t.Run("wrongCanonical", func(t *testing.T) {
		bad := f["small"]
		bad.CanonicalPath = "/source-parallel/foreign"
		got, _, _, e := normalSystemInspectSourceFiles(context.Background(), 3, []NormalSystemSourceFile{bad})
		if e == nil || got != nil {
			t.Fatal("foreign canonical accepted")
		}
		normalSystemParallelAssertClosed(t, []NormalSystemSourceFile{bad})
	})
	t.Run("nonELF", func(t *testing.T) {
		bad := f["notELF"]
		bad.Kind = "loader"
		got, _, _, e := normalSystemInspectSourceFiles(context.Background(), 3, []NormalSystemSourceFile{bad})
		if e == nil || got != nil {
			t.Fatal("non ELF accepted")
		}
		normalSystemParallelAssertClosed(t, []NormalSystemSourceFile{bad})
	})
	t.Run("nonExecutable", func(t *testing.T) {
		bad := f["large0"]
		bad.Kind = "exec"
		got, _, _, e := normalSystemInspectSourceFiles(context.Background(), 3, []NormalSystemSourceFile{bad})
		if e == nil || got != nil {
			t.Fatal("non executable accepted")
		}
		normalSystemParallelAssertClosed(t, []NormalSystemSourceFile{bad})
	})
	t.Run("inittabWrongVersion", func(t *testing.T) {
		got, _, _, e := normalSystemInspectSourceFiles(context.Background(), 1, []NormalSystemSourceFile{f["inittab"]})
		if e == nil || got != nil {
			t.Fatal("other version inittab accepted")
		}
		normalSystemParallelAssertClosed(t, []NormalSystemSourceFile{f["inittab"]})
	})
}

type normalSystemParallelAnswer struct {
	observations []NormalSystemSourceFileObservation
	err          error
}

func TestNormalSystemSourceParallelFilesActualOverlap(t *testing.T) {
	f := normalSystemParallelFixture(t)
	input := []NormalSystemSourceFile{f["large0"], f["large1"]}
	paths := []string{input[0].CanonicalPath, input[1].CanonicalPath}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan normalSystemParallelAnswer, 1)
	go func() {
		got, _, _, e := normalSystemInspectSourceFiles(ctx, 3, input)
		done <- normalSystemParallelAnswer{got, e}
	}()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	maximumObserved := 0
	for {
		n, e := normalSystemParallelOpenFDs(paths)
		if e != nil {
			cancel()
			<-done
			t.Fatal(e)
		}
		if n > maximumObserved {
			maximumObserved = n
		}
		select {
		case answer := <-done:
			if answer.err != nil || len(answer.observations) != 2 {
				t.Fatalf("actual overlap failed: %v", answer.err)
			}
			normalSystemParallelAssertClosed(t, input)
			// /proc的多次观测不能证明严格上限；固定两个worker的上限由实际代码独审。
			if maximumObserved < 2 {
				t.Fatalf("did not observe actual simultaneous two file FDs: %d", maximumObserved)
			}
			fmt.Printf("ACTUAL_SOURCE_PARALLEL_FILE_OVERLAP observed_open_fds=%d synthetic_verifier=false after_return_open=0\n", maximumObserved)
			return
		case <-ticker.C:
		case <-deadline.C:
			cancel()
			<-done
			t.Fatal("actual parallel reader deadline")
		}
	}
}
func TestNormalSystemSourceParallelFilesCancellationJoin(t *testing.T) {
	f := normalSystemParallelFixture(t)
	input := []NormalSystemSourceFile{f["large0"], f["large1"]}
	paths := []string{input[0].CanonicalPath, input[1].CanonicalPath}
	t.Run("alreadyCanceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		got, _, _, e := normalSystemInspectSourceFiles(ctx, 3, input)
		if !errors.Is(e, context.Canceled) || got != nil {
			t.Fatalf("canceled context: %v", e)
		}
		normalSystemParallelAssertClosed(t, input)
	})
	t.Run("cancelDuringActualHash", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan normalSystemParallelAnswer, 1)
		go func() {
			got, _, _, e := normalSystemInspectSourceFiles(ctx, 3, input)
			done <- normalSystemParallelAnswer{got, e}
		}()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		observed := false
		for !observed {
			n, e := normalSystemParallelOpenFDs(paths)
			if e != nil {
				cancel()
				<-done
				t.Fatal(e)
			}
			if n > 0 {
				observed = true
				cancel()
				break
			}
			select {
			case answer := <-done:
				t.Fatalf("actual hash completed before cancellation observation: %v", answer.err)
			case <-ticker.C:
			case <-deadline.C:
				cancel()
				<-done
				t.Fatal("actual open FD not observed")
			}
		}
		answer := <-done
		if !errors.Is(answer.err, context.Canceled) || answer.observations != nil {
			t.Fatalf("actual cancellation did not return zero: %v", answer.err)
		}
		normalSystemParallelAssertClosed(t, input)
		fmt.Println("ACTUAL_SOURCE_PARALLEL_CANCEL actual_open_file_observed=true joined=true after_return_open=0")
	})
}
