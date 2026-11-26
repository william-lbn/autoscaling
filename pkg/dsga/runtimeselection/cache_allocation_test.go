package runtimeselection

import (
	"encoding/json"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/policy"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestCacheRuntimeSelectionPreservesLegacyAndRequiresExactSource(t *testing.T) {
	b, cm := selectedFixture()
	var original Selection
	require.NoError(t, json.Unmarshal([]byte(cm.Data[DataKey]), &original))
	legacy, e := json.Marshal(original)
	require.NoError(t, e)
	require.Equal(t, cm.Data[DataKey], string(legacy))
	source := budget.QualificationSource{Claim: budget.ObjectRef{Name: "allocation-source", UID: "allocation-original-uid"}, SHA256: strings.Repeat("b", 64)}
	selected, e := WithCacheAllocationQualification(original, source)
	require.NoError(t, e)
	require.EqualValues(t, 2, selected.ProtocolVersion)
	require.Equal(t, original.Qualification, selected.Qualification)
	require.Equal(t, source, *selected.CacheAllocationQualification)
	source.Claim.UID = "caller-new-uid"
	require.NotEqual(t, source, *selected.CacheAllocationQualification)
	raw, e := json.Marshal(selected)
	require.NoError(t, e)
	cm.Data[DataKey] = string(raw)
	decoded, e := Decode(cm, b, "platform")
	require.NoError(t, e)
	require.Equal(t, selected, decoded)
	require.NotEqual(t, selected.Digest(), original.Digest())
	for name, mutate := range map[string]func(*Selection){
		"legacy-extra-source": func(s *Selection) { s.ProtocolVersion = 1 },
		"missing-v2-source":   func(s *Selection) { s.CacheAllocationQualification = nil },
		"same-base-as-extra":  func(s *Selection) { v := s.Qualification; s.CacheAllocationQualification = &v },
		"vm-only-automatic":   func(s *Selection) { s.Automatic = &policy.Config{} },
		"wrong-version":       func(s *Selection) { s.ProtocolVersion = 3 },
	} {
		t.Run(name, func(t *testing.T) { changed := selected; mutate(&changed); require.Error(t, changed.Validate()) })
	}
}
