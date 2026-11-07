package runtimeselection

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func selectedFixture() (journal.Binding, *core.ConfigMap) {
	b := journal.Binding{Namespace: "neon", Name: "compute", VMUID: "vm-original", Incarnation: strings.Repeat("1", 32)}
	s := Selection{Namespace: b.Namespace, Name: b.Name, ProtocolVersion: 1, Instance: control.Instance{VMUID: string(b.VMUID), ComputeInstanceID: strings.Repeat("2", 32), PGBootID: 1}, Incarnation: b.Incarnation, Generation: 1, Qualification: budget.QualificationSource{Claim: budget.ObjectRef{Name: "qualification", UID: "original-claim"}, SHA256: strings.Repeat("a", 64)}}
	raw, _ := json.Marshal(s)

	return b, &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: Name(string(b.VMUID)), Namespace: "platform", UID: "original-selection", ResourceVersion: "1", Labels: map[string]string{LabelKey: LabelValue}, OwnerReferences: nil}, Data: map[string]string{DataKey: string(raw)}}
}
func TestRuntimeSelectionRejectsWrongIdentityAndReplacement(t *testing.T) {
	b, cm := selectedFixture()
	_, err := Decode(cm, b, "platform")
	require.NoError(t, err)
	for name, change := range map[string]func(*core.ConfigMap){
		"owner":     func(c *core.ConfigMap) { c.OwnerReferences = []metav1.OwnerReference{{UID: "other"}} },
		"label":     func(c *core.ConfigMap) { delete(c.Labels, LabelKey) },
		"name":      func(c *core.ConfigMap) { c.Name = "other" },
		"namespace": func(c *core.ConfigMap) { c.Namespace = "other" },
		"unknown-field": func(c *core.ConfigMap) {
			c.Data[DataKey] = strings.TrimSuffix(c.Data[DataKey], "}") + ",\"injected\":true}"
		},
		"instance": func(c *core.ConfigMap) {
			c.Data[DataKey] = strings.ReplaceAll(c.Data[DataKey], "vm-original", "another-vm")
		},
		"zero-generation": func(c *core.ConfigMap) {
			c.Data[DataKey] = strings.Replace(c.Data[DataKey], "\"generation\":\"1\"", "\"generation\":\"0\"", 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			copy := cm.DeepCopy()
			change(copy)
			_, err := Decode(copy, b, "platform")
			require.Error(t, err)
		})
	}
	client := fake.NewSimpleClientset()
	reader, err := NewReader(client.CoreV1(), b, "platform")
	require.NoError(t, err)
	s, err := reader.Read(context.Background())
	require.NoError(t, err)
	require.Nil(t, s)
	_, err = client.CoreV1().ConfigMaps("platform").Create(context.Background(), cm, metav1.CreateOptions{})
	require.NoError(t, err)
	s, err = reader.Read(context.Background())
	require.NoError(t, err)
	require.NotNil(t, s)
	replacement := cm.DeepCopy()
	replacement.UID = types.UID("replacement")
	_, err = client.CoreV1().ConfigMaps("platform").Update(context.Background(), replacement, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = reader.Read(context.Background())
	require.Error(t, err)
}
