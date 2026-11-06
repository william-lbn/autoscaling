package budget

import (
	"encoding/json"
	"testing"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"

	"github.com/stretchr/testify/require"
)

func quiesceAwaitingACK(q *Quiesced) {
	c := &q.DSM.Prepared.Connections
	c.AppliedSerial, c.AppliedOpen = c.RequestedSerial-1, true
	c.Acknowledged, c.PublicConnectionsBlocked = false, false
}

// 使用既有完整normal1资格/Root原聚合夹具；合法等待仍不是可落账的Quiesced。
func TestQuiesceReadinessWaitsWithoutBecomingProof(t *testing.T) {
	for _, phase := range []string{"ack-pending", "startup", "read", "execute", "retained-transaction"} {
		t.Run(phase, func(t *testing.T) {
			p, _, q := normalQuiesceLowerPrepared(t)
			if phase == "ack-pending" {
				quiesceAwaitingACK(&q)
			} else {
				w := &q.DSM.Prepared.Connections.Work
				w.ClientProcesses, w.ForegroundIdle = 1, false
				switch phase {
				case "startup":
					w.StartupClients = 1
				case "read":
					w.FrontendReads = 1
				case "execute":
					w.FrontendExecutes = 1
				case "retained-transaction":
					w.IdleClients, w.RetainedTransactions = 1, 1
				}
			}
			require.NoError(t, q.DSM.ValidateFor(p.Request.Key.Instance), "负例必须是合法同点未就绪事实")
			ready, err := q.ReadyFor(p, p.Request.Key.IssuerEpoch)
			require.NoError(t, err)
			require.False(t, ready)
			require.ErrorContains(t, q.ValidateFor(p, p.Request.Key.IssuerEpoch), "original close is not continuously acknowledged and foreground idle")
		})
	}
}

func TestQuiesceReadinessRejectsIllegalSourceBeforeWaiting(t *testing.T) {
	for _, fault := range []string{"boot", "floor", "generation", "close-revision", "high-buffer", "maximum-buffer", "unknown-backing", "private-channel", "observer", "accounting", "root-aggregate", "budget", "permit", "contradictory-ack-blockade"} {
		t.Run(fault, func(t *testing.T) {
			p, _, q := normalQuiesceLowerPrepared(t)
			// 同一夹具的Shared/DSM共享数组切片；负控只能污染本次DSM，
			// 不能先污染Prepared后靠另一条拒绝路径假装验证了未知backing。
			q.DSM.Prepared.Connections.Work.Observed.Arrays = append([]control.ArrayFact(nil), q.DSM.Prepared.Connections.Work.Observed.Arrays...)
			quiesceAwaitingACK(&q)
			c := &q.DSM.Prepared.Connections
			s := &c.Work.Observed
			switch fault {
			case "boot":
				s.Instance.PGBootID++
			case "floor":
				s.IssuerFloorEpoch++
			case "generation":
				s.CapacityGeneration++
			case "close-revision":
				q.Close.Revision++
			case "high-buffer":
				s.HighBuffers++
			case "maximum-buffer":
				s.MaximumBuffers++
			case "unknown-backing":
				s.Arrays[0].AllocationUnknown = true
			case "private-channel":
				c.PrivateManagementReady = false
			case "observer":
				c.Work.ObserverExcluded = false
			case "accounting":
				q.DSM.AccountingValid = false
			case "root-aggregate":
				q.PGAggregate.Bound.UpperBytes++
			case "budget":
				q.Close.BudgetCommitmentID += "-other"
			case "permit":
				q.VMShrinkPermit = true
			case "contradictory-ack-blockade":
				c.AppliedSerial, c.AppliedOpen, c.Acknowledged, c.PublicConnectionsBlocked = c.RequestedSerial, false, true, false
			}
			require.NoError(t, p.ValidateFor(p.Request, p.Identity), "非法负控不能污染原Prepared")
			ready, err := q.ReadyFor(p, p.Request.Key.IssuerEpoch)
			require.Error(t, err, "非法事实不能被并存的未ACK吞成等待")
			require.False(t, ready)
			require.Error(t, q.ValidateFor(p, p.Request.Key.IssuerEpoch))
		})
	}
}

func TestQuiesceReadinessReadyRetainsAllOtherCosts(t *testing.T) {
	p, _, q := normalQuiesceLowerPrepared(t)
	ready, err := q.ReadyFor(p, p.Request.Key.IssuerEpoch)
	require.NoError(t, err)
	require.True(t, ready)
	require.NoError(t, q.ValidateFor(p, p.Request.Key.IssuerEpoch))
	require.Positive(t, q.DSM.UnknownSegments)
	require.Positive(t, q.DSM.Prepared.PreparedSlots)
	require.Positive(t, q.DSM.Prepared.Connections.Work.BackgroundProcesses)
	require.False(t, q.VMShrinkPermit)
}

// 前台事务与后台状态继续按原硬域计费。在线分支不能用“空闲”取得硬预算，
// 也不得把旧关闭意图或不同 revision 解释成一次成功的开放观察。
func TestOnlineShrinkKeepsOriginalServingRevisionWithAnActiveWriter(t *testing.T) {
	p, _, q := normalQuiesceLowerPrepared(t)
	p.OnlineShrinkProtocolVersion = 1
	p.Shared.Observed.AdmissionOpen = true
	p.NormalQuotaFence.Observed = p.Shared.Observed
	q.ProtocolVersion = 2
	q.ServingRevision = p.Shared.Observed.AdmissionRevision
	q.DSM.Prepared.RuntimePreparesBlocked = false
	q.Close = control.AdmissionRequest{}
	connection := &q.DSM.Prepared.Connections
	connection.RequestedOpen, connection.AppliedOpen, connection.PublicConnectionsBlocked = true, true, false
	work := &connection.Work
	work.Observed = p.Shared.Observed
	work.ClientProcesses, work.FrontendExecutes, work.ForegroundIdle = 1, 1, false
	require.NoError(t, q.DSM.ValidateFor(p.Request.Key.Instance))
	require.NoError(t, q.ValidateFor(p, p.Request.Key.IssuerEpoch))
	// Store 写前命令和完成回执会各自再次解码。在线没有 Close 字段，
	// v1 仍须携带真实原关闭请求，跨版本不能借零值伪造它。
	raw, err := json.Marshal(q.OriginalCommand())
	require.NoError(t, err)
	var command QuiesceCommand
	require.NoError(t, json.Unmarshal(raw, &command))
	require.Equal(t, q.OriginalCommand(), command)
	raw, err = json.Marshal(q)
	require.NoError(t, err)
	var retained Quiesced
	require.NoError(t, json.Unmarshal(raw, &retained))
	require.NoError(t, retained.ValidateFor(p, p.Request.Key.IssuerEpoch))
	legacyCommand := command
	legacyCommand.ProtocolVersion, legacyCommand.ServingRevision = 1, 0
	raw, err = json.Marshal(legacyCommand)
	require.NoError(t, err)
	require.Error(t, json.Unmarshal(raw, &command))
	require.True(t, q.MatchesAdmission(work.Observed))
	closed := work.Observed
	closed.AdmissionOpen = false
	require.False(t, q.MatchesAdmission(closed))
	changedBoot := work.Observed
	changedBoot.Instance.PGBootID++
	require.False(t, q.MatchesAdmission(changedBoot))

	changed := q
	changed.ServingRevision++
	require.Error(t, changed.ValidateFor(p, p.Request.Key.IssuerEpoch))
	changed = q
	changed.Close.Revision = 1
	require.Error(t, changed.ValidateFor(p, p.Request.Key.IssuerEpoch))
	require.Error(t, q.ValidateFor(p, p.Request.Key.IssuerEpoch+1))
	legacy := p
	legacy.OnlineShrinkProtocolVersion = 0
	require.Error(t, q.ValidateFor(legacy, p.Request.Key.IssuerEpoch))
	p.NormalQuotaProtocolVersion, p.NormalQuotaFence = 0, nil
	require.Error(t, q.ValidateFor(p, p.Request.Key.IssuerEpoch))
}
