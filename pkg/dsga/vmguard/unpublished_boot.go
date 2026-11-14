package vmguard

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

// UnpublishedBoot pins the original birth and its empty state. This identifies
// a recorded registration without a runtime publication. Physical absence is
// a separate node-side observation.
type UnpublishedBoot struct {
	Birth       budget.ObjectRef
	State       budget.ObjectRef
	StateSHA256 string
}

func ReadUnpublishedBoot(ctx context.Context, client coreclient.CoreV1Interface, binding journal.Binding, original *budget.ObjectRef) (Birth, UnpublishedBoot, error) {
	birthCM, err := client.ConfigMaps(binding.Namespace).Get(ctx, BindingName(binding), metav1.GetOptions{})
	if err != nil {
		return Birth{}, UnpublishedBoot{}, err
	}
	birth, ref, err := InspectBirthForExit(birthCM, binding, original)
	if err != nil {
		return Birth{}, UnpublishedBoot{}, err
	}
	cm, err := client.ConfigMaps(binding.Namespace).Get(ctx, birth.StateClaim.Name, metav1.GetOptions{})
	if err != nil {
		return Birth{}, UnpublishedBoot{}, err
	}
	if err := validClaimState(cm, binding, birth.StateClaim, stateKind, false, true); err != nil {
		return Birth{}, UnpublishedBoot{}, err
	}
	raw := []byte(cm.Data[ClaimKey])
	var p Publication
	if json.Unmarshal(raw, &p) != nil || p.ProtocolVersion != birth.ProtocolVersion || p.Identity != birth.Identity ||
		p.BindingClaim != nil || p.State != nil || p.Actual != nil || p.InitialBootstrap != nil {
		return Birth{}, UnpublishedBoot{}, fmt.Errorf("original Runner already published initial execution state")
	}
	return birth, UnpublishedBoot{Birth: ref, State: birth.StateClaim, StateSHA256: SourceDigest(raw)}, nil
}
