package agent

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vmv1 "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/dsga/credential"
	"github.com/neondatabase/autoscaling/pkg/dsga/owner"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/util"
)

// 原Agent+真实OwnerElector+原TLS凭据客户端；只让既有夹具的issuer响应超时。
// 必须在外层workerFailure隐藏正文前留下精确固定阶段，原错误继续原样返回。
func TestDSGAOwnerCallbackReportsOriginalCredentialStage(t *testing.T) {
	s, vm, pod, _, entered := initialAgentFixture(t)
	s.config.DSGA.RequestTimeoutSeconds = 1
	vm.Status.Node, vm.Status.PodIP, vm.Status.Phase = pod.Spec.NodeName, pod.Status.PodIP, vmv1.VmRunning
	_, err := s.vmClient.NeonvmV1().VirtualMachines(vm.Namespace).UpdateStatus(context.Background(), vm, metav1.UpdateOptions{})
	require.NoError(t, err)
	info := api.VmInfo{Name: vm.Name, Namespace: vm.Namespace, UID: vm.UID}
	runner := s.newRunner(info, util.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, pod.Status.PodIP)
	runner.dsgaPodUID = string(pod.UID)
	runner.status = new(lockedPodStatus)
	runner.status.startTime = time.Now()
	core, observed := observer.New(zap.WarnLevel)
	logger := zap.New(core)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	done := make(chan error, 1)
	_, updated := util.NewCondChannelPair()
	go func() { _, err := runner.runDSGA(ctx, logger, updated); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("original credential client did not reach TLS issuer")
	}
	select {
	case err = <-done:
	case <-ctx.Done():
		t.Fatal("original callback did not return its finite failure")
	}
	var typed *credential.Error
	require.ErrorAs(t, err, &typed, "original unwrap chain remains intact")
	require.Equal(t, "issuer_unavailable", typed.Code)
	entries := observed.FilterMessage("DSGA owner callback stopped").All()
	require.Len(t, entries, 1)
	require.Equal(t, map[string]interface{}{"stage": "credentials_identity", "code": "credential_issuer_unavailable"}, entries[0].ContextMap())
	require.Equal(t, "DSGA owner worker failed; reconcile original effects", err.Error())
}

type dsgaCyclicFailure struct{}

func (e *dsgaCyclicFailure) Error() string { panic("diagnostic must never format arbitrary errors") }
func (e *dsgaCyclicFailure) Unwrap() error { return e }

func TestDSGAOwnerCallbackDiagnosticIsBoundedAndRedacted(t *testing.T) {
	credentialErr := &credential.Error{Code: "issuer_response_rejected"}
	resourceErr := &resourceguard.ClientError{Code: "response_unconfirmed", MayHaveExecuted: true}
	for _, tc := range []struct {
		err  error
		want string
	}{
		{fmt.Errorf("Bearer sensitive-token %w", credentialErr), "credential_issuer_response_rejected"},
		{errors.Join(errors.New("https://user:password/private"), resourceErr), "resource_response_unconfirmed"},
		{&credential.Error{Code: "Bearer sensitive-token"}, "credential_other"},
		{&resourceguard.ClientError{Code: "password=private"}, "resource_other"},
		{&dsgaCyclicFailure{}, "unclassified"},
		{context.Canceled, "context_canceled"},
	} {
		require.Equal(t, tc.want, dsgaOwnerFailureCode(tc.err))
		core, observed := observer.New(zap.WarnLevel)
		require.NotPanics(t, func() { logDSGAOwnerCallbackFailure(zap.New(core), "Bearer sensitive-token", tc.err) })
		entries := observed.All()
		require.Len(t, entries, 1)
		require.Equal(t, map[string]interface{}{"stage": "unclassified", "code": tc.want}, entries[0].ContextMap())
	}
	require.Equal(t, "issuer_response_rejected", credentialErr.Code)
	require.True(t, resourceErr.MayHaveExecuted, "diagnostics cannot erase execution uncertainty")
}

func TestDSGATickDiagnosticReportsUnknownWithoutChangingIt(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	logger := zap.New(core)
	var d dsgaTickDiagnostic
	original := &resourceguard.ClientError{Code: "array_response_unconfirmed", MayHaveExecuted: true}
	tick := owner.Tick{Progress: pgexec.AwaitingFact, Err: original}
	d.observe(logger, tick)
	d.observe(logger, tick)
	require.Len(t, observed.All(), 1)
	require.Equal(t, map[string]interface{}{"stage": "pg_reconcile", "progress": "awaiting_fact", "code": "resource_array_response_unconfirmed"}, observed.All()[0].ContextMap())
	require.Same(t, original, tick.Err)
	require.True(t, original.MayHaveExecuted)
	d.observe(logger, owner.Tick{Progress: pgexec.Recorded})
	require.Len(t, observed.All(), 2)
	require.Equal(t, "DSGA reconciliation observation recovered", observed.All()[1].Message)
	d.observe(logger, tick)
	require.Len(t, observed.All(), 3)
	require.True(t, original.MayHaveExecuted)
}
func TestDSGATickDiagnosticNeverFormatsArbitraryFields(t *testing.T) {
	core, observed := observer.New(zap.WarnLevel)
	var d dsgaTickDiagnostic
	require.NotPanics(t, func() {
		d.observe(zap.New(core), owner.Tick{Progress: pgexec.Progress("Bearer private"), Err: &dsgaCyclicFailure{}})
	})
	require.Equal(t, map[string]interface{}{"stage": "pg_reconcile", "progress": "unclassified", "code": "unclassified"}, observed.All()[0].ContextMap())
}
