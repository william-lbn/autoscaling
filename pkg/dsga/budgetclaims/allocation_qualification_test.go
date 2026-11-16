package budgetclaims

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
)

type allocationReaderFixture struct {
	binding       journal.Binding
	base          budget.PlatformQualification
	qualification budget.AllocationQualification
	baseSource    budget.QualificationSource
	source        budget.QualificationSource
	selected      budget.QualificationSelection
	baseCM, cm    *core.ConfigMap
}

// 沿用仓库原资格测试的 fake Kube 形状及原 Go DTO，不模拟 Linux 资格已经
// 完成。摘要来自真实 json.Marshal 原文，所有网络效果只能是固定 namespace GET。
func newAllocationReaderFixture(t *testing.T) allocationReaderFixture {
	t.Helper()
	b := journal.Binding{Namespace: "testing", Name: "vm-one", VMUID: "vm-uid", Incarnation: strings.Repeat("a", 32)}
	parent := budget.PlatformQualification{ProtocolVersion: 1, Identity: resourceguard.Identity{VMUID: string(b.VMUID), ComputeInstanceID: "compute-1", GuestBootID: "12345678-1234-1234-1234-123456789abc", CgroupDevice: 2, CgroupInode: 3, SystemCgroupDevice: 2, SystemCgroupInode: 4}, Incarnation: b.Incarnation, ReportID: "explicit-platform-unit-fixture", RootBootstrapSHA256: strings.Repeat("b", 64), RunnerBirth: budget.ObjectRef{Name: "birth", UID: "birth-uid"}, RunnerIdentitySHA256: strings.Repeat("c", 64), RunnerPodName: "runner", RunnerPodUID: "runner-uid", NodeName: "node-one", NodeUID: "node-uid", HoldUntilExplicitSettlement: true, Targets: []budget.QualifiedTarget{{VMMemoryBytes: 1 << 30, GuestUsableMemoryBytes: 768 << 20, Limits: resourceguard.Limits{MemoryMaxBytes: 512 << 20, OutsideUpperBytes: 32 << 20, PidsMaxTasks: 128, SystemMemoryMaxBytes: 16 << 20, HugePools: []resourceguard.HugePool{{PageSizeKB: 2048, Pages: 8}}}, SystemOutsidePGBytes: 24 << 20, TransitionBytes: 4 << 20, SafetyBytes: 4 << 20}}}
	makeCM := func(name, uid, kind string, value any) (budget.QualificationSource, *core.ConfigMap) {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		digest := sha256.Sum256(raw)
		source := budget.QualificationSource{Claim: budget.ObjectRef{Name: name, UID: uid}, SHA256: hex.EncodeToString(digest[:])}
		controller := true
		owner := []metav1.OwnerReference{{APIVersion: "vm.neon.tech/v1", Kind: "VirtualMachine", Name: b.Name, UID: b.VMUID, Controller: &controller, BlockOwnerDeletion: &controller}}
		cm := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: b.Namespace, Name: name, UID: types.UID(uid), ResourceVersion: "original-rv", OwnerReferences: owner, Finalizers: []string{ProtectionFinalizer}, Labels: map[string]string{ClaimLabel: kind}}, Immutable: &controller, Data: map[string]string{ClaimKey: string(raw)}}
		return source, cm
	}
	baseSource, baseCM := makeCM("original-platform", "platform-original-uid", QualificationKind, parent)
	q := budget.AllocationQualification{ProtocolVersion: 2, VMUID: string(b.VMUID), Incarnation: b.Incarnation, ReportID: "allocation-unit-not-platform-certification", Base: baseSource, HoldUntilExplicitSettlement: true}
	for i, id := range []string{"sb-heavy", "lfc-heavy"} {
		limits := parent.Targets[0].Limits
		limits.MemoryMaxBytes = 272 << 20
		q.Targets = append(q.Targets, budget.QualifiedAllocation{ID: id, VMMemoryBytes: 1 << 30, Buffers: uint32(4096 - i*2048), LFCLimitBytes: control.Uint64((32 + i*16) << 20), ArrayNormalUpperBytes: control.Uint64((48 - i*16) << 20), NoncachePrivateUpperBytes: 192 << 20, CacheNormalUpperBytes: control.Uint64((32 + i*16) << 20), BackingProfileSHA256: strings.Repeat("d", 64), Limits: limits})
	}
	q.Transitions = []budget.QualifiedAllocationTransition{{From: "sb-heavy", To: "lfc-heavy", PeakLimits: parent.Targets[0].Limits}}
	source, cm := makeCM("original-allocation", "allocation-original-uid", AllocationQualificationKind, q)
	selector, err := q.Targets[0].Selector()
	require.NoError(t, err)
	return allocationReaderFixture{binding: b, base: parent, qualification: q, baseSource: baseSource, source: source, selected: budget.QualificationSelection{ProtocolVersion: 2, Source: source, Allocation: &selector}, baseCM: baseCM, cm: cm}
}

func requireOnlyQualificationGET(t *testing.T, client *kubefake.Clientset, namespace string) {
	t.Helper()
	for _, action := range client.Actions() {
		require.Equal(t, "get", action.GetVerb(), "只读入口不能创建、改写、降限或结算")
		require.Equal(t, "configmaps", action.GetResource().Resource)
		require.Equal(t, namespace, action.GetNamespace())
	}
}

func TestReadQualificationDispatchKeepsLegacyAndAllocationSeparate(t *testing.T) {
	require.Equal(t, journal.MaxObservationBytes, budget.MaxAllocationQualificationBytes, "两类正式读者必须共享同一载荷上限")
	f := newAllocationReaderFixture(t)
	client := kubefake.NewSimpleClientset(f.baseCM, f.cm)
	r, err := New(client.CoreV1(), f.binding)
	require.NoError(t, err)
	legacy, err := r.Qualification(context.Background(), f.baseSource)
	require.NoError(t, err)
	require.Equal(t, f.base, legacy)
	oldRead, err := r.ReadQualification(context.Background(), budget.QualificationSelection{ProtocolVersion: 1, Source: f.baseSource})
	require.NoError(t, err)
	_, err = oldRead.Allocation()
	require.Error(t, err)
	read, err := r.ReadQualification(context.Background(), f.selected)
	require.NoError(t, err)
	_, err = read.Platform()
	require.Error(t, err, "不能通过旧 VM-only API 收养新分配")
	observed, err := read.Allocation()
	require.NoError(t, err)
	require.Equal(t, f.base.Targets[0], observed.RetainedTarget)
	require.Equal(t, control.Uint64(512<<20), observed.RetainedTarget.Limits.MemoryMaxBytes)
	require.Equal(t, control.Uint64(272<<20), observed.Candidate.Limits.MemoryMaxBytes)
	require.Equal(t, *f.selected.Allocation, observed.Selector)
	second, err := f.qualification.Targets[1].Selector()
	require.NoError(t, err)
	secondRead, err := r.ReadQualification(context.Background(), budget.QualificationSelection{ProtocolVersion: 2, Source: f.source, Allocation: &second})
	require.NoError(t, err)
	secondObserved, err := secondRead.Allocation()
	require.NoError(t, err)
	require.NotEqual(t, observed.SourceID, secondObserved.SourceID)
	require.Equal(t, observed.RetainedTarget, secondObserved.RetainedTarget)
	observed.Candidate.Limits.HugePools[0].Pages++
	observed.RetainedTarget.Limits.HugePools[0].Pages++
	again, err := read.Allocation()
	require.NoError(t, err)
	require.Equal(t, f.base.Targets[0], again.RetainedTarget)
	_, err = r.Qualification(context.Background(), f.source)
	require.Error(t, err, "旧入口连新 kind 都不收养")
	_, err = r.ReadQualification(context.Background(), budget.QualificationSelection{ProtocolVersion: 2, Source: f.source})
	require.Error(t, err, "缺 allocation 不能按 VM 回退")
	requireOnlyQualificationGET(t, client, f.binding.Namespace)
}

func TestReadAllocationQualificationAtOriginalClaimByteBoundary(t *testing.T) {
	f := newAllocationReaderFixture(t)
	seeds := f.qualification.Targets
	f.qualification.Targets = nil
	for i := 0; i < 32; i++ {
		candidate := seeds[i%len(seeds)]
		candidate.ID = fmt.Sprintf("allocation-%02d", i)
		f.qualification.Targets = append(f.qualification.Targets, candidate)
	}
	f.qualification.Transitions = []budget.QualifiedAllocationTransition{}
	for i := 0; i < 32 && len(f.qualification.Transitions) < 128; i++ {
		for j := 0; j < 32 && len(f.qualification.Transitions) < 128; j++ {
			if i != j {
				f.qualification.Transitions = append(f.qualification.Transitions, budget.QualifiedAllocationTransition{From: f.qualification.Targets[i].ID, To: f.qualification.Targets[j].ID, PeakLimits: f.base.Targets[0].Limits})
			}
		}
	}
	var fitting []byte
	for n := 0; n <= len(f.qualification.Transitions); n++ {
		candidate := f.qualification
		candidate.Transitions = candidate.Transitions[:n]
		raw, err := json.Marshal(candidate)
		require.NoError(t, err)
		if len(raw) > budget.MaxAllocationQualificationBytes {
			break
		}
		fitting = raw
	}
	require.Greater(t, len(fitting), budget.MaxAllocationQualificationBytes-2048)
	oversized, err := json.Marshal(f.qualification)
	require.NoError(t, err)
	for _, raw := range [][]byte{fitting, oversized} {
		cm := f.cm.DeepCopy()
		cm.Data[ClaimKey] = string(raw)
		digest := sha256.Sum256(raw)
		selected := f.selected
		selected.Source.SHA256 = hex.EncodeToString(digest[:])
		selector, err := f.qualification.Targets[0].Selector()
		require.NoError(t, err)
		selected.Allocation = &selector
		client := kubefake.NewSimpleClientset(f.baseCM, cm)
		r, err := New(client.CoreV1(), f.binding)
		require.NoError(t, err)
		read, err := r.ReadQualification(context.Background(), selected)
		if len(raw) <= budget.MaxAllocationQualificationBytes {
			require.NoError(t, err)
			observed, e := read.Allocation()
			require.NoError(t, e)
			require.Equal(t, f.base.Targets[0], observed.RetainedTarget)
			t.Logf("formal fake Kube read succeeded at %d bytes", len(raw))
		} else {
			require.Error(t, err)
			require.Nil(t, read)
		}
		requireOnlyQualificationGET(t, client, f.binding.Namespace)
	}
}

func TestReadAllocationQualificationRejectsOriginalSourceDrift(t *testing.T) {
	for _, side := range []string{"allocation", "base"} {
		for _, mode := range []string{"uid", "body", "kind", "owner", "namespace", "rv", "mutable", "unprotected", "deleting", "binary", "missing", "second-get-deleting"} {
			t.Run(side+"/"+mode, func(t *testing.T) {
				f := newAllocationReaderFixture(t)
				client := kubefake.NewSimpleClientset(f.baseCM, f.cm)
				original := f.cm
				if side == "base" {
					original = f.baseCM
				}
				calls := 0
				client.PrependReactor("get", "configmaps", func(action kt.Action) (bool, runtime.Object, error) {
					if action.(kt.GetAction).GetName() != original.Name {
						return false, nil, nil
					}
					calls++
					if mode == "second-get-deleting" && calls == 1 {
						return false, nil, nil
					}
					changed := original.DeepCopy()
					switch mode {
					case "uid":
						changed.UID = "same-name-new-uid"
					case "body":
						changed.Data[ClaimKey] += " "
					case "kind":
						changed.Labels[ClaimLabel] = "other-version"
					case "owner":
						changed.OwnerReferences[0].UID = "other-vm"
					case "namespace":
						changed.Namespace = "other"
					case "rv":
						changed.ResourceVersion = ""
					case "mutable":
						changed.Immutable = nil
					case "unprotected":
						changed.Finalizers = nil
					case "deleting", "second-get-deleting":
						now := metav1.Now()
						changed.DeletionTimestamp = &now
					case "binary":
						changed.BinaryData = map[string][]byte{"unexpected": {1}}
					case "missing":
						return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, original.Name)
					}
					return true, changed, nil
				})
				r, err := New(client.CoreV1(), f.binding)
				require.NoError(t, err)
				read, err := r.ReadQualification(context.Background(), f.selected)
				require.Error(t, err)
				require.Nil(t, read)
				if mode == "missing" {
					require.True(t, apierrors.IsNotFound(err), "404 原错误不得变成已释放或零预算")
				}
				requireOnlyQualificationGET(t, client, f.binding.Namespace)
			})
		}
	}
}

func TestReadAllocationCopiesSelectorBeforeOriginalGET(t *testing.T) {
	f := newAllocationReaderFixture(t)
	client := kubefake.NewSimpleClientset(f.baseCM, f.cm)
	original := *f.selected.Allocation
	other, err := f.qualification.Targets[1].Selector()
	require.NoError(t, err)
	client.PrependReactor("get", "configmaps", func(kt.Action) (bool, runtime.Object, error) {
		*f.selected.Allocation = other
		return false, nil, nil
	})
	r, err := New(client.CoreV1(), f.binding)
	require.NoError(t, err)
	read, err := r.ReadQualification(context.Background(), f.selected)
	require.NoError(t, err)
	observed, err := read.Allocation()
	require.NoError(t, err)
	require.Equal(t, original, observed.Selector)
	requireOnlyQualificationGET(t, client, f.binding.Namespace)
}

func TestReadAllocationRejectsValidlyEncodedForeignBinding(t *testing.T) {
	for _, mode := range []string{"vm", "incarnation", "base-reference", "borrow-vector", "parent-normal", "legacy-kind-new-body"} {
		t.Run(mode, func(t *testing.T) {
			f := newAllocationReaderFixture(t)
			switch mode {
			case "vm":
				f.qualification.VMUID = "another-vm"
			case "incarnation":
				f.qualification.Incarnation = strings.Repeat("f", 32)
			case "base-reference":
				f.qualification.Base.Claim.UID = "another-base-uid"
			case "borrow-vector":
				f.selected.Allocation.AllocationID = "lfc-heavy"
			case "parent-normal":
				f.qualification.Targets[0].NoncachePrivateUpperBytes += 512 << 20
				f.qualification.Targets[0].Limits.MemoryMaxBytes += 512 << 20
			case "legacy-kind-new-body":
				f.cm.Labels[ClaimLabel] = QualificationKind
			}
			raw, err := json.Marshal(f.qualification)
			require.NoError(t, err)
			digest := sha256.Sum256(raw)
			f.cm.Data[ClaimKey] = string(raw)
			f.selected.Source.SHA256 = hex.EncodeToString(digest[:])
			client := kubefake.NewSimpleClientset(f.baseCM, f.cm)
			r, err := New(client.CoreV1(), f.binding)
			require.NoError(t, err)
			if mode == "legacy-kind-new-body" {
				_, err = r.Qualification(context.Background(), f.selected.Source)
			} else {
				_, err = r.ReadQualification(context.Background(), f.selected)
			}
			require.Error(t, err)
			requireOnlyQualificationGET(t, client, f.binding.Namespace)
		})
	}
}

func TestAllocationReadKeepsActualLegacyV1JournalReplay(t *testing.T) {
	// 仅存两份已归档的公开协议 DTO 与原摘要，无凭据/租户数据。使用仓库真实
	// RequestDigest、Begin、Takeover、Lookup 和固定步骤验证，而非复制 Python 算法。
	raw, err := os.ReadFile("testdata/allocation_qualification_v1_requests.json")
	require.NoError(t, err)
	var fixture struct {
		Records []struct {
			PostgresMajor int             `json:"postgres_major"`
			Request       control.Request `json:"request"`
			SHA256        string          `json:"journal_request_sha256"`
		} `json:"records"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.Len(t, fixture.Records, 2)
	for _, record := range fixture.Records {
		t.Run(fmt.Sprint(record.PostgresMajor), func(t *testing.T) {
			hash, e := journal.RequestDigest(record.Request)
			require.NoError(t, e)
			require.Equal(t, record.SHA256, hash)
			m, e := journal.New(record.Request.Key.Instance.VMUID, strings.Repeat("a", 32), "legacy-holder-1")
			require.NoError(t, e)
			for m.Owner.Epoch < record.Request.Key.IssuerEpoch {
				m, e = m.Takeover(m.Owner, fmt.Sprintf("legacy-holder-%d", m.Owner.Epoch+1))
				require.NoError(t, e)
			}
			m, disposition, e := m.Begin(m.Owner, record.Request, 0)
			require.NoError(t, e)
			require.Equal(t, journal.Accepted, disposition)
			if record.Request.Kind == control.Grow {
				require.Len(t, m.Active.Steps, 7)
			} else {
				require.Len(t, m.Active.Steps, 9)
			}
			m, e = m.Takeover(m.Owner, "legacy-holder-after-accept")
			require.NoError(t, e)
			m, disposition, e = m.Begin(m.Owner, record.Request, uint64(record.Request.DeadlineUnixMillis)+1)
			require.NoError(t, e)
			require.Equal(t, journal.Replayed, disposition)
			require.Equal(t, record.Request, m.Active.Request)
			require.Equal(t, record.SHA256, m.Active.RequestSHA256)
			changed := record.Request
			changed.TargetBuffers++
			require.Equal(t, journal.Conflict, m.Lookup(changed))
		})
	}
}
