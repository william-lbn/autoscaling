package runnercpu

import (
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const NodeCommitmentScope = "original-node-pod-cpu-commitment-v1"

// NodeCommitment 来自原Node持久账本，含整个Pod的调度承诺，不是独占CPU或SLO证明。
// Origin/Sequence/RV用于独立重读比对，不能自报提高Owner/Node floor。
type NodeCommitment struct {
	ProtocolVersion     uint32 `json:"protocol_version"`
	Scope               string `json:"scope"`
	LedgerOrigin        string `json:"ledger_origin"`
	LedgerSequence      uint64 `json:"ledger_sequence"`
	NodeResourceVersion string `json:"node_resource_version"`
	CommittedMillicores uint64 `json:"committed_millicores"`
}

func (o NodeCommitment) Validate() error {
	if o.ProtocolVersion != 1 || o.Scope != NodeCommitmentScope || control.ValidateIncarnation(o.LedgerOrigin) != nil ||
		o.LedgerSequence == 0 || control.ValidateID(o.NodeResourceVersion, "original Node resourceVersion") != nil ||
		o.CommittedMillicores == 0 {
		return fmt.Errorf("original Node-Pod CPU commitment source unavailable")
	}
	return nil
}
