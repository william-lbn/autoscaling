package qualificationissuer

import (
	"context"
	"encoding/json"
	"errors"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/hostguard"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"
	"strings"
	"testing"
)

// 模型只检原Issuer/Create/不可变Source协议；不是Linux资格或真实资源验收。
func allocationReportFixture(t *testing.T) (*Issuer, *kubefake.Clientset, Bundle, budget.QualificationSource) {
	t.Helper()
	issuer, client, _, bundle, parent := issuerFixture(t)
	original, e := issuer.Publish(context.Background(), bundle)
	require.NoError(t, e)
	parent.ProtocolVersion = 2
	parent.Scope = KubernetesBudgetReportScope
	parent.GuestArtifactSHA256 = strings.Repeat("d", 64)
	parent.HostKubernetesBudget = &hostguard.KubernetesBudgetPolicy{ProtocolVersion: 1, Scope: hostguard.KubernetesBudgetScope,
		NodeName: parent.HostBinding.NodeName, NodeUID: string(parent.HostBinding.NodeUID), CapacityBytes: 20 << 30, AllocatableBytes: 16 << 30,
		SystemReservationBytes: parent.HostSystemBytes, ProfileSHA256: parent.HostProfileSHA256}
	require.NoError(t, parent.Validate())
	q := budget.AllocationQualification{ProtocolVersion: 2, VMUID: parent.Qualification.Identity.VMUID, Incarnation: parent.Qualification.Incarnation,
		ReportID: "explicit-unit-allocation-not-platform-certificate", Base: original.Source, HoldUntilExplicitSettlement: true}
	for i, id := range []string{"sb-heavy", "lfc-heavy"} {
		limits := parent.Qualification.Targets[0].Limits
		limits.MemoryMaxBytes = control.Uint64((192 + 48 + 32) << 20)
		q.Targets = append(q.Targets, budget.QualifiedAllocation{ID: id, VMMemoryBytes: parent.Qualification.Targets[0].VMMemoryBytes,
			Buffers: uint32(4096 - i*2048), LFCLimitBytes: control.Uint64((32 + i*16) << 20), ArrayNormalUpperBytes: control.Uint64((48 - i*16) << 20),
			NoncachePrivateUpperBytes: 192 << 20, CacheNormalUpperBytes: control.Uint64((32 + i*16) << 20), BackingProfileSHA256: strings.Repeat("e", 64), Limits: limits})
	}
	q.Transitions = []budget.QualifiedAllocationTransition{{From: "sb-heavy", To: "lfc-heavy", PeakLimits: parent.Qualification.Targets[0].Limits}}
	report, e := NewAllocationReport(parent, original.Source, q)
	require.NoError(t, e)
	bundle.Report, e = json.Marshal(report)
	require.NoError(t, e)
	issuer.config.ReportSHA256 = SHA256(bundle.Report)
	return issuer, client, bundle, original.Source
}
func TestAllocationIssuerPublishesOriginalProtectedSourceAndBase(t *testing.T) {
	issuer, _, bundle, base := allocationReportFixture(t)
	result, e := issuer.PublishAllocation(context.Background(), bundle)
	require.NoError(t, e)
	require.NotNil(t, result.BaseSource)
	require.Equal(t, base, *result.BaseSource)
	require.NotEqual(t, base, result.Source)
	require.Len(t, result.Source.Claim.Name, 63)
	reader, e := budgetclaims.New(issuer.core, issuer.config.Binding)
	require.NoError(t, e)
	require.NoError(t, reader.AuthenticateAllocationSource(context.Background(), result.Source, base))
	_, e = reader.Qualification(context.Background(), result.Source)
	require.Error(t, e, "new allocation cannot be adopted as old budget")
	cm, e := issuer.core.ConfigMaps(issuer.config.Binding.Namespace).Get(context.Background(), result.Source.Claim.Name, metav1.GetOptions{})
	require.NoError(t, e)
	require.Equal(t, budgetclaims.AllocationQualificationKind, cm.Labels[budgetclaims.ClaimLabel])
	require.True(t, *cm.Immutable)
	require.Contains(t, cm.Finalizers, budgetclaims.ProtectionFinalizer)
	_, e = issuer.Publish(context.Background(), bundle)
	require.Error(t, e, "v3 cannot publish a second Base budget")
}
func TestAllocationIssuerOriginalReportAndLostCreateRemainExplicit(t *testing.T) {
	issuer, client, bundle, _ := allocationReportFixture(t)
	broken := bundle
	broken.Report = append(append([]byte(nil), bundle.Report...), byte(' '))
	_, e := issuer.PublishAllocation(context.Background(), broken)
	require.Error(t, e)
	// 模型丢CREATE回复：真实受保护对象仍在，必须记录原UID后显式恢复，不能按名称收养。
	lost := false
	client.Fake.PrependReactor("create", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
		cm := action.(kt.CreateAction).GetObject().(*core.ConfigMap)
		if lost || cm.Labels[budgetclaims.ClaimLabel] != budgetclaims.AllocationQualificationKind {
			return false, nil, nil
		}
		lost = true
		copy := cm.DeepCopy()
		copy.UID = types.UID("allocation-created-but-reply-lost")
		copy.ResourceVersion = "1"
		require.NoError(t, client.Tracker().Add(copy))
		return true, nil, errors.New("fixture reply lost after create")
	})
	_, e = issuer.PublishAllocation(context.Background(), bundle)
	var unknown *Unconfirmed
	require.ErrorAs(t, e, &unknown)
	require.Empty(t, unknown.Claim.UID)
	cm, e := client.CoreV1().ConfigMaps(issuer.config.Binding.Namespace).Get(context.Background(), unknown.Claim.Name, metav1.GetOptions{})
	require.NoError(t, e)
	require.Equal(t, types.UID("allocation-created-but-reply-lost"), cm.UID)
	issuer.config.ResumeClaimUID = string(cm.UID)
	result, e := issuer.PublishAllocation(context.Background(), bundle)
	require.NoError(t, e)
	require.Equal(t, string(cm.UID), result.Source.Claim.UID)
}
