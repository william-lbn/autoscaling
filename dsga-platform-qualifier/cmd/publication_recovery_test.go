package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/qualificationissuer"
	"k8s.io/client-go/rest"
)

func recoveryConfig() qualificationissuer.Config {
	return qualificationissuer.Config{ProtocolVersion: 1, Binding: journal.Binding{Namespace: "site", Name: "vm", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32)}, NodeName: "worker", NodeUID: "node-uid", ReportSHA256: strings.Repeat("b", 64), TimeoutSeconds: 1}
}
func TestAutomaticPublicationRecoversOnlyOriginalReturnedUID(t *testing.T) {
	c := recoveryConfig()
	ref := budget.ObjectRef{Name: "dsga-q-" + c.ReportSHA256[:56], UID: "original-created-uid"}
	calls, creates := 0, 0
	result, err := publishAutomaticWithInterval(context.Background(), c, func(ctx context.Context, next qualificationissuer.Config) (qualificationissuer.Result, error) {
		calls++
		if next.ResumeClaimUID == "" {
			creates++
			return qualificationissuer.Result{}, &qualificationissuer.Unconfirmed{Claim: ref, Err: fmt.Errorf("original platform sources changed after publication")}
		}
		if next.ResumeClaimUID != ref.UID || next.ReportSHA256 != c.ReportSHA256 || next.Binding != c.Binding {
			t.Fatal("recovery changed original input")
		}
		if calls == 2 {
			return qualificationissuer.Result{}, fmt.Errorf("original platform sources changed before publication")
		}
		return qualificationissuer.Result{Source: budget.QualificationSource{Claim: ref, SHA256: strings.Repeat("c", 64)}, ReportSHA256: c.ReportSHA256}, nil
	}, time.Millisecond)
	if err != nil || creates != 1 || calls != 3 || result.Source.Claim != ref {
		t.Fatalf("original publication not reconciled: creates=%d calls=%d err=%v", creates, calls, err)
	}
}
func TestAutomaticPublicationNeverAdoptsUnknownCreateIdentity(t *testing.T) {
	for _, ref := range []budget.ObjectRef{{Name: "dsga-q-" + strings.Repeat("b", 56)}, {Name: "foreign", UID: "foreign-uid"}} {
		calls := 0
		_, err := publishAutomaticWithInterval(context.Background(), recoveryConfig(), func(context.Context, qualificationissuer.Config) (qualificationissuer.Result, error) {
			calls++
			return qualificationissuer.Result{}, &qualificationissuer.Unconfirmed{Claim: ref, Err: errors.New("unknown create")}
		}, time.Millisecond)
		if err == nil || calls != 1 {
			t.Fatal("unknown CREATE was replayed or adopted")
		}
	}
}
func TestAutomaticPublicationRetainsOriginalOnRecoveryDrift(t *testing.T) {
	c := recoveryConfig()
	ref := budget.ObjectRef{Name: "dsga-q-" + c.ReportSHA256[:56], UID: "original"}
	calls := 0
	_, err := publishAutomaticWithInterval(context.Background(), c, func(context.Context, qualificationissuer.Config) (qualificationissuer.Result, error) {
		calls++
		if calls == 1 {
			return qualificationissuer.Result{}, &qualificationissuer.Unconfirmed{Claim: ref, Err: errors.New("unknown")}
		}
		return qualificationissuer.Result{Source: budget.QualificationSource{Claim: budget.ObjectRef{Name: ref.Name, UID: "replacement"}}, ReportSHA256: c.ReportSHA256}, nil
	}, time.Millisecond)
	var unknown *qualificationissuer.Unconfirmed
	if !errors.As(err, &unknown) || unknown.Claim != ref || calls != 2 {
		t.Fatal("recovery accepted replacement identity or lost original")
	}
}
func TestAutomaticPublicationUsesOneBoundedRecoveryContext(t *testing.T) {
	c := recoveryConfig()
	ref := budget.ObjectRef{Name: "dsga-q-" + c.ReportSHA256[:56], UID: "original"}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := publishAutomaticWithInterval(ctx, c, func(ctx context.Context, next qualificationissuer.Config) (qualificationissuer.Result, error) {
		calls++
		cancel()
		return qualificationissuer.Result{}, &qualificationissuer.Unconfirmed{Claim: ref, Err: errors.New("original platform sources changed after publication")}
	}, time.Second)
	var unknown *qualificationissuer.Unconfirmed
	if !errors.As(err, &unknown) || unknown.Claim != ref || !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal("cancelled original recovery was extended or lost")
	}
}
func TestQualifierReadDefaultsRespectExplicitLimits(t *testing.T) {
	defaults := &rest.Config{}
	configureQualifierReads(defaults)
	if defaults.QPS <= 0 || defaults.Burst <= 0 {
		t.Fatal("unbounded read defaults")
	}
	explicit := &rest.Config{QPS: 7, Burst: 11}
	configureQualifierReads(explicit)
	if explicit.QPS != 7 || explicit.Burst != 11 {
		t.Fatal("explicit client policy changed")
	}
}
