//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type nativeStoppedModel struct {
	log                            []string
	fail                           string
	verifications                  int
	cleanupConfirmed               bool
	cleanupEntered, cleanupRelease chan struct{}
}

func (m *nativeStoppedModel) step(name string) error {
	m.log = append(m.log, name)
	if name == m.fail {
		return errors.New("explicit-model-fault")
	}
	return nil
}
func (m *nativeStoppedModel) verify(tid int) error {
	m.verifications++
	name := "verify-initial"
	if m.verifications == 2 {
		name = "verify-stopped"
	} else if m.verifications == 3 {
		name = "verify-final"
	}
	if tid == 0 && m.verifications != 1 {
		return errors.New("unexpected-zero-tracer")
	}
	return m.step(name)
}
func (m *nativeStoppedModel) seize() error     { return m.step("seize-options-zero") }
func (m *nativeStoppedModel) interrupt() error { return m.step("interrupt") }
func (m *nativeStoppedModel) waitInterruptStop(context.Context) error {
	return m.step("wait-original-exact-event")
}
func (m *nativeStoppedModel) exitKill() error { return m.step("set-exitkill") }
func (m *nativeStoppedModel) detach() error   { return m.step("detach") }
func (m *nativeStoppedModel) cleanup(ctx context.Context, confirmed bool) (bool, string, error) {
	m.cleanupConfirmed = confirmed
	m.log = append(m.log, "synchronous-cleanup")
	if m.cleanupEntered != nil {
		close(m.cleanupEntered)
		select {
		case <-m.cleanupRelease:
		case <-ctx.Done():
			return true, "retire", ctx.Err()
		}
	}
	return true, "explicit-model-retire", nil
}
func TestNativeTraceOriginalStoppedModelOrdering(t *testing.T) {
	m := &nativeStoppedModel{}
	tr, err, retire := nativeTraceOriginalStoppedFlow(context.Background(), m, 123, func(context.Context) error { return m.step("fixed-stopped-callback") })
	want := []string{"verify-initial", "seize-options-zero", "interrupt", "wait-original-exact-event", "verify-stopped", "set-exitkill", "fixed-stopped-callback", "verify-final", "detach"}
	if err != nil || retire || !tr.OriginalConfirmed || !tr.ExitKillInstalled || !tr.CallbackCompleted || !tr.Detached || !reflect.DeepEqual(m.log, want) {
		t.Fatalf("ordering/facts differ: %+v %v %v %v", tr, err, retire, m.log)
	}
}
func TestNativeTraceOriginalStoppedModelFaults(t *testing.T) {
	for _, test := range []struct {
		name               string
		cleanup, confirmed bool
	}{
		{"verify-initial", false, false}, {"seize-options-zero", false, false},
		{"interrupt", true, false}, {"wait-original-exact-event", true, false}, {"verify-stopped", true, false},
		{"set-exitkill", true, true}, {"fixed-stopped-callback", true, true}, {"verify-final", true, true}, {"detach", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := &nativeStoppedModel{fail: test.name}
			tr, err, retire := nativeTraceOriginalStoppedFlow(context.Background(), m, 123, func(context.Context) error { return m.step("fixed-stopped-callback") })
			if err == nil || tr.Detached || retire != test.cleanup || m.cleanupConfirmed != test.confirmed {
				t.Fatalf("fault escaped original cleanup: %+v %v %v %+v", tr, err, retire, m)
			}
			cleaned := len(m.log) > 0 && m.log[len(m.log)-1] == "synchronous-cleanup"
			if cleaned != test.cleanup || test.cleanup && !errors.Is(err, ErrUnknown) {
				t.Fatal("partial cleanup was not Unknown")
			}
			if !test.confirmed && tr.ExitKillInstalled {
				t.Fatal("tentative target gained EXITKILL")
			}
		})
	}
}
func TestNativeTraceOriginalStoppedCancellationJoinsCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &nativeStoppedModel{cleanupEntered: make(chan struct{}), cleanupRelease: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err, _ := nativeTraceOriginalStoppedFlow(ctx, m, 123, func(context.Context) error { cancel(); return nil })
		done <- err
	}()
	<-m.cleanupEntered
	select {
	case <-done:
		t.Fatal("returned before cleanup joined")
	default:
	}
	close(m.cleanupRelease)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrUnknown) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not complete")
	}
}
func TestNativeTraceOriginalStoppedRejectsMissingSource(t *testing.T) {
	if target, err := newNativeTraceOriginalStoppedTarget(nativeTraceOriginalTargetSource{}); err == nil || target != nil {
		t.Fatal("missing original source accepted")
	}
	if _, err := nativeTraceWithOriginalStoppedTarget(context.Background(), nil, nil); err == nil {
		t.Fatal("unbounded missing target accepted")
	}
}
