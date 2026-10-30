package journal

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/budget"
)

var retainedSourceKinds = []string{RetainedInitialV1, RetainedFinalizationV1, RetainedFinalizationV2, RetainedCapacityV1, RetainedCapacityV2, RetainedCapacityV3}

// 只通过既有生产Model的落账/完成方法产生六类永久头，不手填Header。
// PG/Root/Runner值沿用显式领域夹具；本组不声称认证网络或实际资源执行。
func retainedSourceModel(t *testing.T, kind string) Model {
	t.Helper()
	var m Model
	var err error
	switch kind {
	case RetainedInitialV1:
		m = initialBoundModel(t)
	case RetainedFinalizationV1:
		var obs Observation
		m, obs = heldStart(t, heldBoundModel(t, testModel(t)), 1, runnerSources())
		m, err = m.Record(m.Owner, obs.Key, obs)
		require.NoError(t, err)
		m = heldFinish(t, m)
	case RetainedFinalizationV2:
		var live control.AdmissionLeaseResponse
		m, live = reopenedServiceFinalization(t)
		m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
		require.NoError(t, err)
	case RetainedCapacityV1:
		var command budget.ReconciledAbortCommand
		var receipt budget.ReconciledAbortReceipt
		m, command, receipt = partialCapacityModel(t)
		m = recordPartialCapacity(t, m, command, receipt)
		m, err = m.Finish(m.Owner, m.Active.Request.Key)
		require.NoError(t, err)
	case RetainedCapacityV2:
		var live control.AdmissionLeaseResponse
		m, live = reopenedCleanupRecovery(t, "actual", true, false, true)
		m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
		require.NoError(t, err)
	case RetainedCapacityV3:
		var live control.AdmissionLeaseResponse
		m, live = reopenedActualRecovery(t, "completed")
		m, err = m.CompleteServiceReopen(m.Owner, m.ServiceClosure.Reopen.CommandSHA256, live)
		require.NoError(t, err)
	default:
		t.Fatalf("unknown retained source fixture %q", kind)
	}
	require.NoError(t, m.Validate())
	return m
}

func retainedSourceExpected(t *testing.T, source RetainedBudgetSource) retainedBudgetFacts {
	t.Helper()
	if i := source.Initial; i != nil {
		return retainedBudgetFacts{instance: i.Instance, budgetID: i.PGAggregate.Bootstrap.BudgetCommitmentID, generation: i.Shared.Observed.CapacityGeneration, memory: i.VMMemoryBytes, usable: i.GuestUsableMemoryBytes, runner: i.RunnerPodName, restarts: i.RunnerRestartCount}
	}
	if c := source.Capacity; c != nil {
		return retainedBudgetFacts{instance: c.Request.Key.Instance, budgetID: c.Request.BudgetCommitmentID, generation: c.CapacityGeneration, memory: c.VMMemoryBytes, usable: c.GuestUsableMemoryBytes, runner: c.RunnerPodName, restarts: c.RunnerRestartCount}
	}
	var c budget.FinalizationCommand
	require.NoError(t, json.Unmarshal(source.Finalization.Command, &c))
	f := c.Finalized
	return retainedBudgetFacts{instance: f.Request.Key.Instance, budgetID: f.Request.BudgetCommitmentID, generation: f.Shared.Observed.CapacityGeneration, memory: f.Request.TargetVMMemoryBytes, usable: f.GuestUsableMemoryBytes, runner: f.RunnerPodName, restarts: f.RunnerRestartCount}
}

func TestRetainedBudgetSourceSixPermanentFormats(t *testing.T) {
	for _, kind := range retainedSourceKinds {
		t.Run(kind, func(t *testing.T) {
			m := retainedSourceModel(t, kind)
			original := m.Clone()
			source, err := m.CurrentRetainedSource()
			require.NoError(t, err)
			require.Equal(t, kind, source.Kind)
			require.NoError(t, source.Validate())
			require.NoError(t, m.CheckRetainedSource(source))
			require.Equal(t, original, m, "来源读取不得增加序列、写Header或制造关闭水位")
			expected := retainedSourceExpected(t, source)
			id, err := source.BudgetCommitmentID()
			require.NoError(t, err)
			require.Equal(t, expected.budgetID, id)
			serviceID, err := m.ServiceBudgetID()
			require.NoError(t, err)
			require.Equal(t, id, serviceID, "生产关闭入口必须使用同一个原永久预算身份")
			instance, err := source.Instance()
			require.NoError(t, err)
			require.Equal(t, expected.instance, instance)
			generation, err := source.CapacityGeneration()
			require.NoError(t, err)
			require.Equal(t, expected.generation, generation)
			memory, err := source.VMMemoryBytes()
			require.NoError(t, err)
			require.Equal(t, expected.memory, memory)
			usable, err := source.GuestUsableMemoryBytes()
			require.NoError(t, err)
			require.Equal(t, expected.usable, usable)
			runner, err := source.RunnerPodName()
			require.NoError(t, err)
			require.Equal(t, expected.runner, runner)
			restarts, err := source.RunnerRestartCount()
			require.NoError(t, err)
			require.Equal(t, expected.restarts, restarts)

			switch kind {
			case RetainedInitialV1:
				require.Equal(t, m.InitialHeld.RecordedSequence, source.RecordedSequence)
				require.Equal(t, m.InitialHeld.BudgetSHA256, source.SourceSHA256)
				require.Nil(t, source.Finalization, "启动预算不补造Resize Request或终态")
			case RetainedFinalizationV1, RetainedFinalizationV2:
				require.Equal(t, m.BudgetHeld.RecordedSequence, source.RecordedSequence)
				require.Equal(t, m.BudgetHeld.Finalization.CommandSHA256, source.SourceSHA256)
				require.Equal(t, m.BudgetHeld.Finalization, *source.Finalization)
			case RetainedCapacityV1, RetainedCapacityV2, RetainedCapacityV3:
				capacity, digest, at, err := m.currentCapacityRecord()
				require.NoError(t, err)
				require.Equal(t, at, source.RecordedSequence)
				require.Equal(t, digest, source.SourceSHA256)
				require.Equal(t, capacity, *source.Capacity)
			}
			wire, err := json.Marshal(source)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(wire, &fields))
			require.Len(t, fields, 4, "来源只含Kind/原序列/原摘要/一个叶子，不递归复制Recovery或Lifecycle")
			var restored RetainedBudgetSource
			require.NoError(t, json.Unmarshal(wire, &restored))
			require.Equal(t, source, restored)
			require.NoError(t, m.CheckRetainedSource(restored))
			_, raw, err := encode(m, MaxEncodedBytes)
			require.NoError(t, err)
			decoded, err := decode(raw, m.VMUID, m.Incarnation)
			require.NoError(t, err)
			require.NoError(t, decoded.CheckRetainedSource(source))
		})
	}
}

func TestRetainedBudgetSourceDeepCopiesEveryLeaf(t *testing.T) {
	for _, kind := range retainedSourceKinds {
		t.Run(kind, func(t *testing.T) {
			m := retainedSourceModel(t, kind)
			source, err := m.CurrentRetainedSource()
			require.NoError(t, err)
			baseline := cloneRetainedBudgetSource(&source)
			copySource := cloneRetainedBudgetSource(&source)
			for _, changed := range []*RetainedBudgetSource{&source, copySource} {
				if i := changed.Initial; i != nil {
					require.NotEmpty(t, i.PGAggregate.Bootstrap.Limits.HugePools)
					i.PGAggregate.Bootstrap.Limits.HugePools[0].Pages++
					require.NotEmpty(t, i.Shared.Observed.Arrays)
					i.Shared.Observed.Arrays[0].CommittedUpperBytes++
				}
				if f := changed.Finalization; f != nil {
					f.Command[0], f.Observation.Data[0] = '!', '!'
					f.DispatchedBy.HolderID = "caller-mutated-owner"
				}
				if c := changed.Capacity; c != nil {
					if c.PGOperation != nil {
						c.PGOperation.Key.OperationID.Low++
					}
					if i := c.InitialOutside; i != nil {
						i.PGAggregate.Bootstrap.Limits.HugePools[0].Pages++
						i.Shared.Observed.Arrays[0].CommittedUpperBytes++
					}
					if i := c.InitialVM; i != nil {
						i.IdentitySHA256 = strings.Repeat("f", 64)
					}
					if p := c.UnchangedPG; p != nil {
						p.Observed.Arrays[0].CommittedUpperBytes++
					}
				}
			}
			again, err := m.CurrentRetainedSource()
			require.NoError(t, err)
			require.Equal(t, *baseline, again)
			require.NoError(t, m.CheckRetainedSource(*baseline))
		})
	}
	t.Run("independent-native-pg-proof", func(t *testing.T) {
		m := reviewPGActualModel(t)
		p := m.ServiceClosure.ActualRecovery
		capacity := cloneActualCapacity(p.Capacity)
		source := RetainedBudgetSource{Kind: RetainedCapacityV3, RecordedSequence: p.RecordedSequence, SourceSHA256: p.CapacitySHA256, Capacity: &capacity, Initial: nil, Finalization: nil}
		require.NoError(t, source.Validate())
		copySource := cloneRetainedBudgetSource(&source)
		copySource.Capacity.RecoveryPG.Fence.Observed.Arrays[0].CommittedUpperBytes++
		copySource.Capacity.RecoveryPG.Status.Observed.Arrays[0].CommittedUpperBytes++
		require.Equal(t, p.Capacity, *source.Capacity, "Fence/Status各自的完整native快照都要深拷贝")
	})
	require.Nil(t, cloneRetainedBudgetSource(nil))
}

func TestRetainedBudgetSourceCannotAuthenticateAnotherHeader(t *testing.T) {
	for _, kind := range retainedSourceKinds {
		t.Run(kind, func(t *testing.T) {
			m := retainedSourceModel(t, kind)
			source, err := m.CurrentRetainedSource()
			require.NoError(t, err)
			wrongSequence := cloneRetainedBudgetSource(&source)
			wrongSequence.RecordedSequence++
			require.NoError(t, wrongSequence.Validate())
			require.Error(t, m.CheckRetainedSource(*wrongSequence))
			wrongDigest := cloneRetainedBudgetSource(&source)
			wrongDigest.SourceSHA256 = strings.Repeat("f", 64)
			if source.Finalization == nil {
				require.NoError(t, wrongDigest.Validate(), "Initial/Capacity原头摘要不能机械从叶子重算，身份由当前头精确绑定")
			} else {
				require.Error(t, wrongDigest.Validate(), "完整Finalize Step另有可重算的原摘要域")
			}
			require.Error(t, m.CheckRetainedSource(*wrongDigest))
			changed := cloneRetainedBudgetSource(&source)
			switch {
			case changed.Initial != nil:
				changed.Initial.RunnerPodName = "different-valid-runner"
			case changed.Capacity != nil:
				changed.Capacity.RunnerPodName = "different-valid-runner"
				if changed.Capacity.InitialOutside != nil {
					changed.Capacity.InitialOutside.RunnerPodName = changed.Capacity.RunnerPodName
				}
			case changed.Finalization != nil:
				var command budget.FinalizationCommand
				require.NoError(t, json.Unmarshal(changed.Finalization.Command, &command))
				command.Finalized.RunnerPodName = "different-valid-runner"
				rewriteRetainedFinalization(t, changed, command)
			}
			require.NoError(t, changed.Validate(), "改变合法叶子且重算Finalize摘要，确保命中当前Header身份守卫")
			require.ErrorContains(t, m.CheckRetainedSource(*changed), "exact original current header")
			if source.Finalization != nil {
				ownerAlias := cloneRetainedBudgetSource(&source)
				ownerAlias.Finalization.DispatchedBy.HolderID = "same-epoch-different-holder"
				var command budget.FinalizationCommand
				require.NoError(t, json.Unmarshal(ownerAlias.Finalization.Command, &command))
				rewriteRetainedFinalization(t, ownerAlias, command)
				require.NoError(t, ownerAlias.Validate())
				require.Error(t, m.CheckRetainedSource(*ownerAlias), "重算Step摘要不能把相同epoch另一holder升级为当前原头")
			}
			if kind == RetainedCapacityV1 || kind == RetainedCapacityV2 {
				alias := cloneRetainedBudgetSource(&source)
				if kind == RetainedCapacityV1 {
					alias.Kind = RetainedCapacityV2
				} else {
					alias.Kind = RetainedCapacityV1
				}
				require.NoError(t, alias.Validate())
				require.Error(t, m.CheckRetainedSource(*alias), "同叶子不能混淆普通/独立恢复头格式")
			}
		})
	}
}

func rewriteRetainedFinalization(t *testing.T, source *RetainedBudgetSource, command budget.FinalizationCommand) {
	t.Helper()
	f := source.Finalization
	var err error
	f.Command, err = json.Marshal(command)
	require.NoError(t, err)
	f.Observation.Data, err = json.Marshal(command.Finalized)
	require.NoError(t, err)
	requestSHA, err := RequestDigest(command.Finalized.Request)
	require.NoError(t, err)
	f.CommandSHA256, err = commandDigest(requestSHA, *f)
	require.NoError(t, err)
	source.SourceSHA256 = f.CommandSHA256
	f.Observation.CommandSHA256 = f.CommandSHA256
}

func TestRetainedFinalizationSourceChecksFullStepDomain(t *testing.T) {
	t.Run("planned-recovery-keeps-new-settlement-owner", func(t *testing.T) {
		m, command, current := finalizationRecoveryModel(t, true, true)
		_, _, _, selector := m.ServiceClosure.Selection()
		m, err := m.RecordServiceFinalization(m.Owner, selector, command, current)
		require.NoError(t, err)
		held, err := m.ServiceClosure.Finalization.held(m)
		require.NoError(t, err)
		source := RetainedBudgetSource{Kind: RetainedFinalizationV2, RecordedSequence: held.RecordedSequence, SourceSHA256: held.Finalization.CommandSHA256, Initial: nil, Finalization: &held.Finalization, Capacity: nil}
		require.NoError(t, source.Validate())
		require.NotZero(t, command.Finalized.SettlementEpoch)
		require.Equal(t, m.Owner.Epoch, command.Finalized.SettlementEpoch)
		command.Finalized.SettlementEpoch = 0
		rewriteRetainedFinalization(t, &source, command)
		require.Error(t, source.Validate(), "去掉SettlementEpoch但保留新floor不能冒充旧Unknown完整命令")
	})
	t.Run("shrink-finalization-ordinal", func(t *testing.T) {
		m := heldBoundModel(t, testModel(t))
		req := testRequest(m, 1, control.Shrink)
		m = beginModel(t, m, req)
		command := heldCommand(t, req)
		command.Finalized.Sources.Runner = ptrRunner(runnerSources())
		for _, original := range m.Active.Steps {
			if original.Action == ResumeAdmission {
				break
			}
			raw, target := testCommand, "explicit-shrink-domain"
			var err error
			if original.Action == FinalizeBudget {
				raw, err = json.Marshal(command)
				require.NoError(t, err)
				target = req.Key.Instance.ComputeInstanceID
			}
			m, err = m.Start(m.Owner, req.Key, original.Action, target, raw)
			require.NoError(t, err)
			obs := observation(m, m.Active.Steps[int(original.Ordinal)-1], Completed)
			if original.Action == FinalizeBudget {
				obs.Data, err = json.Marshal(command.Finalized)
				require.NoError(t, err)
			}
			m, err = m.Record(m.Owner, req.Key, obs)
			require.NoError(t, err)
		}
		m = heldFinish(t, m)
		source, err := m.CurrentRetainedSource()
		require.NoError(t, err)
		require.EqualValues(t, 8, source.Finalization.Ordinal)
		require.NoError(t, m.CheckRetainedSource(source))
	})
	for _, kind := range []string{RetainedFinalizationV1, RetainedFinalizationV2} {
		t.Run(kind, func(t *testing.T) {
			m := retainedSourceModel(t, kind)
			source, err := m.CurrentRetainedSource()
			require.NoError(t, err)
			for _, mutate := range []struct {
				name string
				edit func(*RetainedBudgetSource)
			}{
				{"ordinal", func(s *RetainedBudgetSource) { s.Finalization.Ordinal++ }},
				{"action", func(s *RetainedBudgetSource) { s.Finalization.Action = AbortReconcile }},
				{"owner", func(s *RetainedBudgetSource) { s.Finalization.DispatchedBy.HolderID = "substituted-owner" }},
				{"observation-key", func(s *RetainedBudgetSource) { s.Finalization.Observation.Key.OperationID.Low++ }},
				{"observation-source", func(s *RetainedBudgetSource) { s.Finalization.Observation.SourceID = "another-compute" }},
				{"observation-result", func(s *RetainedBudgetSource) { s.Finalization.Observation.Result = Rejected }},
				{"receipt", func(s *RetainedBudgetSource) { s.Finalization.Observation.Data = []byte(`{"different":"receipt"}`) }},
				{"digest", func(s *RetainedBudgetSource) {
					s.Finalization.CommandSHA256 = strings.Repeat("f", 64)
					s.SourceSHA256 = s.Finalization.CommandSHA256
				}},
				{"kind", func(s *RetainedBudgetSource) {
					if s.Kind == RetainedFinalizationV1 {
						s.Kind = RetainedFinalizationV2
					} else {
						s.Kind = RetainedFinalizationV1
					}
				}},
			} {
				t.Run(mutate.name, func(t *testing.T) {
					changed := cloneRetainedBudgetSource(&source)
					mutate.edit(changed)
					require.Error(t, changed.Validate())
				})
			}
			var command budget.FinalizationCommand
			require.NoError(t, json.Unmarshal(source.Finalization.Command, &command))
			if kind == RetainedFinalizationV1 {
				command.Finalized.SettlementEpoch = command.Finalized.IssuerEpoch
			} else {
				command.Finalized.SettlementEpoch++
			}
			changed := cloneRetainedBudgetSource(&source)
			rewriteRetainedFinalization(t, changed, command)
			require.ErrorContains(t, changed.Validate(), "owner", "重算命令/回执摘要后仍拒绝SettlementEpoch与派发owner矛盾")
		})
	}
}

func TestRetainedBudgetSourceDoesNotRequireHistoryOrInventBoundary(t *testing.T) {
	m := retainedSourceModel(t, RetainedFinalizationV1)
	source, err := m.CurrentRetainedSource()
	require.NoError(t, err)
	m, err = m.Takeover(m.Owner, "retained-history-owner")
	require.NoError(t, err)
	for revision := uint64(1); revision <= HistoryLimit+1; revision++ {
		req := testRequest(m, revision, control.Grow)
		m = beginModel(t, m, req)
		require.NoError(t, m.CheckRetainedSource(source))
		m, err = m.RequestAbort(m.Owner, req.Key)
		require.NoError(t, err)
		m, err = m.Finish(m.Owner, req.Key)
		require.NoError(t, err)
	}
	require.Positive(t, m.ExpiredResults)
	// 显式旧格式兼容夹具：保留真实永久头与已淘汰历史，去掉旧版本原本
	// 没有的关闭水位。生产读取没有降版入口，也不得据同值预算补造水位。
	m.ProtocolVersion, m.BudgetBoundary = 4, nil
	require.NoError(t, m.Validate())
	require.Nil(t, m.BudgetBoundary)
	require.NoError(t, m.CheckRetainedSource(source))
	again, err := m.CurrentRetainedSource()
	require.NoError(t, err)
	require.Equal(t, source, again)
	var command budget.FinalizationCommand
	require.NoError(t, json.Unmarshal(source.Finalization.Command, &command))
	require.Error(t, m.CheckClosedBudget(command.Finalized.Request), "无关闭锚不能被来源getter升级为关闭证明")
	req := testRequest(m, HistoryLimit+2, control.Grow)
	m = beginModel(t, m, req)
	require.Nil(t, m.Active.PriorBudgetBoundary)
	require.Nil(t, m.BudgetBoundary)
	require.NoError(t, m.CheckRetainedSource(source), "早期Active没有PriorBoundary也只能读取旧头，不取得释放权")
}

func TestRetainedBudgetSourceStrictDecodeAndExclusiveLeaf(t *testing.T) {
	m := retainedSourceModel(t, RetainedInitialV1)
	source, err := m.CurrentRetainedSource()
	require.NoError(t, err)
	raw, err := json.Marshal(source)
	require.NoError(t, err)
	for _, wire := range [][]byte{
		append([]byte(`{"unexpected":true,`), raw[1:]...),
		append([]byte(`{"kind":"initial-v1",`), raw[1:]...),
		[]byte(`null`),
		[]byte(`[]`),
		[]byte(`{}`),
		[]byte(`{"kind":"` + strings.Repeat("x", MaxRawBytes/2) + `"}`),
	} {
		var restored RetainedBudgetSource
		require.Error(t, json.Unmarshal(wire, &restored))
	}
	for _, mutate := range []func(*RetainedBudgetSource){
		func(s *RetainedBudgetSource) { s.Kind = "initial-v2" },
		func(s *RetainedBudgetSource) { s.RecordedSequence = 0 },
		func(s *RetainedBudgetSource) { s.SourceSHA256 = "not-a-digest" },
		func(s *RetainedBudgetSource) { s.Initial = nil },
		func(s *RetainedBudgetSource) { var step Step; s.Finalization = &step },
		func(s *RetainedBudgetSource) { var capacity budget.ReconciledCapacity; s.Capacity = &capacity },
	} {
		changed := cloneRetainedBudgetSource(&source)
		mutate(changed)
		require.Error(t, changed.Validate())
	}
	emptyModel := testModel(t)
	_, err = emptyModel.CurrentRetainedSource()
	require.Error(t, err)
	wrongModel := m.Clone()
	wrongModel.Sequence = 0
	_, err = wrongModel.CurrentRetainedSource()
	require.Error(t, err)
	require.Error(t, wrongModel.CheckRetainedSource(source))
}
