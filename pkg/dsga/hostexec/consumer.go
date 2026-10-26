package hostexec

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
	"github.com/neondatabase/autoscaling/pkg/dsga/budgetclaims"
	"github.com/neondatabase/autoscaling/pkg/dsga/journal"
	"github.com/neondatabase/autoscaling/pkg/dsga/pgexec"
	"github.com/neondatabase/autoscaling/pkg/dsga/resourceguard"
	"github.com/neondatabase/autoscaling/pkg/dsga/vmguard"
)

// Consumer只读原Host来源并在同一VM WAL结算ReserveHost，不持Node/CM写权限。
// Publisher属于独立Host分配者；不得为了Agent方便授它创建/释放预算的权限。
type Consumer struct {
	store  *journal.Store
	claims *budgetclaims.Reader
	actor  journal.Owner
}

func NewConsumer(store *journal.Store, claims *budgetclaims.Reader, actor journal.Owner) (*Consumer, error) {
	if store == nil || claims == nil || actor.Validate() != nil {
		return nil, fmt.Errorf("host consumer requires original VM/claims/owner")
	}
	return &Consumer{store: store, claims: claims, actor: actor}, nil
}

type hostCommand struct {
	ProtocolVersion uint32           `json:"protocol_version"`
	Request         control.Request  `json:"request"`
	Claim           budget.ObjectRef `json:"claim"`
	PreparedSHA256  string           `json:"prepared_sha256"`
	HostSHA256      string           `json:"host_sha256"`
}

// Advance不分配容量、不发送VM动作。首次先核两端原持有，再将完整来源/摘要
// 写入VM UID/RV命令；回复未知后只查询该原UID，不能重新发现同名替代对象。
// Node持有与CM在Start之后丢失时保留Unconfirmed，不用当前数值补Completed。
func (c *Consumer) Advance(ctx context.Context) (pgexec.Progress, error) {
	s, err := c.store.Load(ctx)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	m := s.State()
	if m.Owner != c.actor {
		return pgexec.AwaitingFact, fmt.Errorf("host consumer owner changed")
	}
	if m.Active == nil {
		return pgexec.Idle, nil
	}
	op := m.Active
	var step, preparedStep *journal.Step
	for i := range op.Steps {
		current := &op.Steps[i]
		if current.Phase != journal.Settled {
			step = current
			break
		}
		if current.Observation == nil || current.Observation.Result != journal.Completed {
			return pgexec.AwaitingFact, fmt.Errorf("host consumer prior original step did not complete")
		}
		if current.Action == journal.PrepareBudget {
			preparedStep = current
		}
	}
	if step == nil || step.Action != journal.ReserveHost {
		return pgexec.ExternalStep, nil
	}
	var p budget.Prepared
	if !s.Dispatchable() || op.Request.Kind != control.Grow || m.ResourceIdentity == nil || m.PGInstance == nil || *m.PGInstance != op.Request.Key.Instance || preparedStep == nil || preparedStep.TargetID != m.PGInstance.ComputeInstanceID || json.Unmarshal(preparedStep.Observation.Data, &p) != nil || p.ValidateFor(op.Request, *m.ResourceIdentity) != nil || p.OutsideClaim == nil {
		return pgexec.AwaitingFact, fmt.Errorf("host consumer lacks complete original prepared context")
	}
	cmd := hostCommand{ProtocolVersion: 1, Request: op.Request, Claim: budget.ObjectRef{Name: "", UID: ""}, PreparedSHA256: vmguard.SourceDigest(preparedStep.Observation.Data), HostSHA256: ""}
	if step.Phase == journal.Planned {
		if op.AbortRequested || op.Origin != c.actor || op.Request.Key.IssuerEpoch != c.actor.Epoch {
			return pgexec.AwaitingFact, fmt.Errorf("host consumer cannot start cancelled/old-owner hold")
		}
		cmd.Claim, err = c.claims.HostReference(ctx, op.Request)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return pgexec.AwaitingFact, fmt.Errorf("%w: original Host source is not yet published", resourceguard.ErrUnknown)
			}
			return pgexec.AwaitingFact, err
		}
	} else if control.UnmarshalObject(step.Command, &cmd) != nil || cmd.ProtocolVersion != 1 || cmd.Request != op.Request || cmd.Claim.Validate() != nil || cmd.PreparedSHA256 != vmguard.SourceDigest(preparedStep.Observation.Data) {
		return pgexec.AwaitingFact, fmt.Errorf("original Host consumer command changed")
	}
	observed, err := c.claims.Read(ctx, s, p, cmd.Claim)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	rawHost, err := json.Marshal(observed.Host)
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	if step.Phase == journal.Planned {
		cmd.HostSHA256 = vmguard.SourceDigest(rawHost)
		raw, err := json.Marshal(cmd)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		var dispatch *journal.Dispatch
		s, dispatch, err = c.store.Start(ctx, s, c.actor, op.Request.Key, journal.ReserveHost, observed.Host.SourceID, raw)
		if err != nil {
			return pgexec.AwaitingFact, err
		}
		original := dispatch.Step
		step = &original
	} else if step.TargetID != observed.Host.SourceID || cmd.HostSHA256 != vmguard.SourceDigest(rawHost) {
		return pgexec.AwaitingFact, fmt.Errorf("original Host consumer source/complete body changed")
	}
	if err := c.claims.Recheck(ctx, s, p, cmd.Claim, observed); err != nil {
		return pgexec.AwaitingFact, err
	}
	_, err = c.store.Record(ctx, s, c.actor, op.Request.Key, journal.Observation{Key: op.Request.Key, RequestSHA256: op.RequestSHA256, CommandSHA256: step.CommandSHA256, Ordinal: step.Ordinal, Action: journal.ReserveHost, Result: journal.Completed, SourceID: observed.Host.SourceID, Data: rawHost})
	if err != nil {
		return pgexec.AwaitingFact, err
	}
	return pgexec.Recorded, nil
}
