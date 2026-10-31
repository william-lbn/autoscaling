//go:build linux

package resourceguard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const (
	preSystemScope        = "pre-bootstrap-system-placement-only"
	preSystemIntentName   = "pre-bootstrap-system.intent.json"
	preSystemCompleteName = "pre-bootstrap-system.complete.json"
)

// 先行阶段只允许在原base-only Guest中施加原System额度、迁移原用户任务。
// 它不创建state.json/Bootstrap.Applied/RootHeld/PG Birth，不给normal/VM许可。
// 原完整预算仍须在Runner原Host预留下首次增到目标后，由原Bootstrap检查落实。
type prebootstrapSystemPolicy struct {
	ProtocolVersion         uint32         `json:"protocol_version"`
	Scope                   string         `json:"scope"`
	PhysicalProfileID       string         `json:"physical_profile_id"`
	RuntimeDocumentSHA256   string         `json:"runtime_document_sha256"`
	FilesystemProfileSHA256 string         `json:"filesystem_profile_sha256"`
	BootMemoryUpperBytes    control.Uint64 `json:"boot_memory_upper_bytes"`
}

func (p prebootstrapSystemPolicy) validate() error {
	if p.Scope != preSystemScope || !preSystemPolicyVersionMatches(p.PhysicalProfileID, p.ProtocolVersion) ||
		!physicalDigestValid(p.RuntimeDocumentSHA256) || !physicalDigestValid(p.FilesystemProfileSHA256) ||
		p.BootMemoryUpperBytes < 128<<20 || p.BootMemoryUpperBytes > 16<<30 || uint64(p.BootMemoryUpperBytes)%(128<<20) != 0 {
		return fmt.Errorf("incomplete original pre-bootstrap System policy")
	}
	return nil
}

func (p *prebootstrapSystemPolicy) UnmarshalJSON(b []byte) error {
	type plain prebootstrapSystemPolicy
	var v plain
	if err := control.UnmarshalObject(b, &v); err != nil {
		return err
	}
	n := prebootstrapSystemPolicy(v)
	if err := n.validate(); err != nil {
		return err
	}
	*p = n
	return nil
}

// Snapshot中的物理/任务/限额必须由本机只读内核backend给出。FSProfile只是
// 原引用，不代表完整F已认证。持久state盘身份与四namespace封闭阶段间的漂移。
type prebootstrapSystemSnapshot struct {
	Physical               PhysicalKernelObservation `json:"physical"`
	System                 SystemEvidence            `json:"system"`
	Namespaces             [4]control.Uint64         `json:"namespaces"`
	StateDevice            control.Uint64            `json:"state_device"`
	StateInode             control.Uint64            `json:"state_inode"`
	TaskCount              control.Uint64            `json:"task_count"`
	MaximumTaskID          control.Uint64            `json:"maximum_task_id"`
	PGEmpty                bool                      `json:"pg_empty"`
	MemoryHighUnlimited    bool                      `json:"memory_high_unlimited"`
	RootFilesystemReadonly bool                      `json:"root_filesystem_readonly"`
}

type prebootstrapSystemInputs struct {
	source    SourceConfig
	policy    prebootstrapSystemPolicy
	policySHA string
	origin    PhysicalKernelOrigin
	originSHA string
	sourceRaw []byte
	policyRaw []byte
}

func (i prebootstrapSystemInputs) validate(config Config) error {
	// 原准确字节必须与typed对象同时相符，内部组合者也不能仅自报hash或只比额度。
	var source SourceConfig
	var policy prebootstrapSystemPolicy
	if len(i.sourceRaw) == 0 || len(i.sourceRaw) > 16*1024 || len(i.policyRaw) == 0 || len(i.policyRaw) > 64*1024 ||
		physicalDigest(i.sourceRaw) != i.origin.ResourceSourceSHA256 || physicalDigest(i.policyRaw) != i.policySHA ||
		json.Unmarshal(i.sourceRaw, &source) != nil || json.Unmarshal(i.policyRaw, &policy) != nil ||
		!reflect.DeepEqual(source, i.source) || policy != i.policy {
		return fmt.Errorf("pre-bootstrap original source/policy bytes differ from typed binding")
	}
	if i.source.Validate() != nil || i.source.Config != config || !config.SystemCgroup || i.source.Limits.SystemMemoryMaxBytes == 0 ||
		i.policy.validate() != nil || !physicalDigestValid(i.policySHA) || !physicalDigestValid(i.originSHA) ||
		i.origin.Validate() != nil || i.origin.Config != config || i.origin.Policy.ProfileID != i.policy.PhysicalProfileID ||
		i.origin.RuntimeDocumentSHA256 != i.policy.RuntimeDocumentSHA256 ||
		sameBootstrapSource(i.source, i.origin.ExpectedBootstrap) != nil {
		return fmt.Errorf("pre-bootstrap System source is not the original normal-v1 binding")
	}
	for _, pool := range i.source.Limits.HugePools {
		if pool.Pages != 0 {
			return fmt.Errorf("pre-bootstrap normal-v1 requires zero whole HugeTLB pools")
		}
	}
	for _, zone := range i.origin.First.Zones {
		if zone.Name == "Movable" && zone.PresentPages != 0 {
			return fmt.Errorf("original physical capture was not base-only")
		}
	}
	digest, err := i.origin.ReferenceSHA256()
	if err != nil || digest != i.originSHA {
		return fmt.Errorf("original physical Origin digest differs")
	}
	return nil
}

type prebootstrapSystemIntent struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Scope           string                     `json:"scope"`
	VMShrinkPermit  bool                       `json:"vm_shrink_permit"`
	Nonce           string                     `json:"nonce"`
	Policy          prebootstrapSystemPolicy   `json:"policy"`
	PolicySHA256    string                     `json:"policy_sha256"`
	OriginSHA256    string                     `json:"origin_sha256"`
	Source          SourceConfig               `json:"source"`
	Original        prebootstrapSystemSnapshot `json:"original"`
}

type prebootstrapSystemCompletion struct {
	ProtocolVersion uint32                     `json:"protocol_version"`
	Scope           string                     `json:"scope"`
	VMShrinkPermit  bool                       `json:"vm_shrink_permit"`
	IntentSHA256    string                     `json:"intent_sha256"`
	Observed        prebootstrapSystemSnapshot `json:"observed"`
}

// DurabilityConfirmed只描述本次调用同步了原阶段文件；Observe永远false。
// Runner仍须核自己的原Host预留/原Source/初始QMP状态，不能把此对象当增/缩许可。
type PrebootstrapSystemResult struct {
	ProtocolVersion     uint32                       `json:"protocol_version"`
	Scope               string                       `json:"scope"`
	VMShrinkPermit      bool                         `json:"vm_shrink_permit"`
	DurabilityConfirmed bool                         `json:"durability_confirmed"`
	IntentSHA256        string                       `json:"intent_sha256"`
	CompletionSHA256    string                       `json:"completion_sha256"`
	Original            prebootstrapSystemIntent     `json:"original"`
	Completion          prebootstrapSystemCompletion `json:"completion"`
	Current             prebootstrapSystemSnapshot   `json:"current"`
}

type prebootstrapSystemKernel interface {
	prebootstrapSystemRead() (prebootstrapSystemSnapshot, error)
	prebootstrapSystemApply(control.Uint64) error
	bootPlacement
}

type prebootstrapSystemOperation uint8

const (
	preSystemPrepare prebootstrapSystemOperation = iota
	preSystemObserve
	preSystemReplay
)

func preSystemCanonical(v any) ([]byte, error) { return json.Marshal(v) }

func preSystemMatchSource(i prebootstrapSystemInputs, v prebootstrapSystemIntent) error {
	if v.ProtocolVersion != 1 || v.Scope != preSystemScope || v.VMShrinkPermit ||
		control.ValidateIncarnation(v.Nonce) != nil || v.Nonce == "00000000000000000000000000000000" ||
		v.Policy != i.policy || v.PolicySHA256 != i.policySHA || v.OriginSHA256 != i.originSHA || !reflect.DeepEqual(v.Source, i.source) {
		return fmt.Errorf("pre-bootstrap System intent differs from original source")
	}
	return preSystemBase(i, v.Original)
}

func preSystemSnapshotMatches(i prebootstrapSystemInputs, s prebootstrapSystemSnapshot) error {
	if err := i.origin.First.Match(s.Physical); err != nil {
		return err
	}
	if !s.MemoryHighUnlimited || !s.RootFilesystemReadonly || s.StateDevice == 0 || s.StateInode == 0 || s.TaskCount == 0 || s.TaskCount >= control.Uint64(s.Physical.PIDMax) || s.MaximumTaskID == 0 || s.MaximumTaskID >= control.Uint64(s.Physical.PIDMax) {
		return fmt.Errorf("pre-bootstrap kernel/task/state sources missing or changed")
	}
	if s.Physical.RuntimeCapabilities == nil || s.Namespaces[0] != s.Physical.RuntimeCapabilities.InitialPIDNamespaceInode || s.TaskCount > s.MaximumTaskID {
		return fmt.Errorf("pre-bootstrap initial namespace or task census differs")
	}
	for _, id := range s.Namespaces {
		if id == 0 {
			return fmt.Errorf("original namespace identity missing")
		}
	}
	return nil
}

func preSystemBase(i prebootstrapSystemInputs, s prebootstrapSystemSnapshot) error {
	if err := preSystemSnapshotMatches(i, s); err != nil {
		return err
	}
	if !s.PGEmpty || s.Physical.NonMovableUpperBytes > i.policy.BootMemoryUpperBytes {
		return fmt.Errorf("pre-bootstrap requires empty PG and original finite base memory")
	}
	for _, z := range s.Physical.Zones {
		if z.Name == "Movable" && z.PresentPages != 0 {
			return fmt.Errorf("Movable appeared before original System placement")
		}
	}
	return nil
}

func preSystemStable(original, current prebootstrapSystemSnapshot) error {
	if original.Namespaces != current.Namespaces || original.StateDevice != current.StateDevice || original.StateInode != current.StateInode {
		return fmt.Errorf("pre-bootstrap original namespaces or state storage changed")
	}
	return nil
}

func preSystemPlaced(i prebootstrapSystemInputs, s prebootstrapSystemSnapshot) error {
	if err := preSystemSnapshotMatches(i, s); err != nil {
		return err
	}
	cap := i.source.Limits.SystemMemoryMaxBytes
	if s.System.Scope != SystemChargedScope || s.System.MemoryMaxBytes != cap || s.System.MemoryCurrentBytes > cap ||
		s.System.SwapMaxBytes != 0 || s.System.SwapCurrentBytes != 0 || !s.System.PID1Member || s.System.RootUserspaceCount != 0 {
		return fmt.Errorf("original System placement/bound not observed")
	}
	return nil
}

// 原资源flock贯穿读取/intent fsync/同步内核写/读回/complete fsync。
// 不跨服务锁，不启动PG，不在ctx取消后留下后台写；未知文件/部分动作永久保留。
func (g *Guard) prebootstrapSystem(ctx context.Context, in prebootstrapSystemInputs, op prebootstrapSystemOperation) (PrebootstrapSystemResult, error) {
	if err := in.validate(g.config); err != nil {
		return PrebootstrapSystemResult{}, err
	}
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		return PrebootstrapSystemResult{}, err
	}
	defer s.close()
	return g.prebootstrapSystemLocked(ctx, in, op, s)
}

// 目录FD由唯一生产入口在原Config.StateDir加锁取得。U测试仅注入独立隔离WAL
// 目录FD和明确模型kernel，不放宽SourceConfig/Origin/策略验证，也不接受请求路径。
func (g *Guard) prebootstrapSystemLocked(ctx context.Context, in prebootstrapSystemInputs, op prebootstrapSystemOperation, s *lockedState) (PrebootstrapSystemResult, error) {
	var zero PrebootstrapSystemResult
	if err := in.validate(g.config); err != nil {
		return zero, err
	}
	k, ok := g.kernel.(prebootstrapSystemKernel)
	if !ok {
		return zero, fmt.Errorf("pre-bootstrap System backend unavailable")
	}
	// 同原锁重读准确Origin/确认文件，不能使用锁外旧对象替换原文件。
	rawOrigin, exists, err := s.readPhysicalFile(physicalOriginDataName)
	if err != nil || !exists || physicalDigest(rawOrigin) != in.originSHA {
		return zero, fmt.Errorf("%w: original physical data unavailable", ErrUnknown)
	}
	confirmation, exists, err := s.readPhysicalFile(physicalOriginConfirmName)
	var pc physicalOriginConfirmation
	if err != nil || !exists || control.UnmarshalObject(confirmation, &pc) != nil || pc.ProtocolVersion != 1 || pc.OriginSHA256 != in.originSHA {
		return zero, fmt.Errorf("%w: original physical confirmation unavailable", ErrUnknown)
	}
	r, err := s.load()
	if err != nil {
		return zero, err
	}
	raw, hasIntent, err := s.readPhysicalFile(preSystemIntentName)
	if err != nil {
		return zero, fmt.Errorf("%w: System intent read: %v", ErrUnknown, err)
	}
	done, hasComplete, err := s.readPhysicalFile(preSystemCompleteName)
	if err != nil {
		return zero, fmt.Errorf("%w: System completion read: %v", ErrUnknown, err)
	}
	var intent prebootstrapSystemIntent
	current, err := k.prebootstrapSystemRead()
	if err != nil {
		return zero, err
	}
	if err := preSystemLockedDirectory(s, current); err != nil {
		return zero, err
	}
	if !hasIntent {
		if hasComplete || op != preSystemPrepare || r != nil {
			return zero, fmt.Errorf("historical state cannot acquire new pre-bootstrap System intent")
		}
		if err := preSystemBase(in, current); err != nil {
			return zero, err
		}
		// 首次只接受尚无用户任务/计费页的原空System组；不降低一个旧工作组。
		if current.System.ProcessCount != 0 || current.System.MemoryCurrentBytes != 0 || current.System.SwapCurrentBytes != 0 || current.System.PID1Member || current.System.RootUserspaceCount == 0 {
			return zero, fmt.Errorf("pre-bootstrap first intent requires original empty System group")
		}
		intent = prebootstrapSystemIntent{ProtocolVersion: 1, Scope: preSystemScope, Policy: in.policy, PolicySHA256: in.policySHA, OriginSHA256: in.originSHA, Source: in.source, Original: current}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return zero, err
		}
		intent.Nonce = hex.EncodeToString(nonce[:])
		raw, err = preSystemCanonical(intent)
		if err != nil {
			return zero, err
		}
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if err := s.createPhysicalFile(preSystemIntentName, raw); err != nil {
			return zero, fmt.Errorf("%w: System intent persistence: %v", ErrUnknown, err)
		}
	} else {
		if err := control.UnmarshalObject(raw, &intent); err != nil {
			return zero, fmt.Errorf("%w: malformed original System intent", ErrUnknown)
		}
		canonical, err := preSystemCanonical(intent)
		if err != nil || string(raw) != string(canonical) {
			return zero, fmt.Errorf("%w: noncanonical original System intent", ErrUnknown)
		}
		if err := preSystemMatchSource(in, intent); err != nil {
			return zero, err
		}
	}
	if err := preSystemStable(intent.Original, current); err != nil {
		return zero, err
	}
	if hasComplete {
		var complete prebootstrapSystemCompletion
		if err := control.UnmarshalObject(done, &complete); err != nil {
			return zero, fmt.Errorf("%w: malformed original System completion", ErrUnknown)
		}
		canonical, err := preSystemCanonical(complete)
		if err != nil || string(done) != string(canonical) || complete.ProtocolVersion != 1 || complete.Scope != preSystemScope || complete.VMShrinkPermit || complete.IntentSHA256 != physicalDigest(raw) {
			return zero, fmt.Errorf("%w: original System completion differs", ErrUnknown)
		}
		if err := preSystemBase(in, complete.Observed); err != nil {
			return zero, err
		}
		if err := preSystemStable(intent.Original, complete.Observed); err != nil {
			return zero, err
		}
		if err := preSystemPlaced(in, complete.Observed); err != nil {
			return zero, err
		}
		if err := preSystemPlaced(in, current); err != nil {
			return zero, err
		}
		if !current.PGEmpty && (r == nil || r.Bootstrap == nil || !r.Bootstrap.Applied || !r.Bootstrap.SystemPlacementObserved) {
			return zero, fmt.Errorf("PG roles appeared before original applied bootstrap and System placement")
		}
		if r != nil && (r.validate(in.origin.First.Identity) != nil || r.Bootstrap == nil || sameBootstrapSource(in.source, r.Bootstrap.Original) != nil) {
			return zero, fmt.Errorf("resource state differs from original System stage")
		}
		durable := op != preSystemObserve
		if durable {
			if err := s.syncPhysicalFile(preSystemIntentName); err != nil {
				return zero, fmt.Errorf("%w: original intent sync: %v", ErrUnknown, err)
			}
			if err := s.syncPhysicalFile(preSystemCompleteName); err != nil {
				return zero, fmt.Errorf("%w: original completion sync: %v", ErrUnknown, err)
			}
		}
		return PrebootstrapSystemResult{ProtocolVersion: 1, Scope: preSystemScope, DurabilityConfirmed: durable, IntentSHA256: physicalDigest(raw), CompletionSHA256: physicalDigest(done), Original: intent, Completion: complete, Current: current}, nil
	}
	if hasIntent && op != preSystemPrepare && op != preSystemReplay {
		return zero, fmt.Errorf("%w: original System placement pending", ErrUnknown)
	}
	// 已有pending不能由普通Prepare悄悄重放。只有首次创建或显式Replay允许写。
	if hasIntent && op != preSystemReplay {
		return zero, fmt.Errorf("%w: explicit original System replay required", ErrUnknown)
	}
	if r != nil {
		return zero, fmt.Errorf("%w: cannot finish System placement after resource initialization", ErrUnknown)
	}
	if err := preSystemBase(in, current); err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	// Replay先同步唯一原intent；无论上次何处失败都不替换nonce或改原Source。
	if err := s.syncPhysicalFile(preSystemIntentName); err != nil {
		return zero, fmt.Errorf("%w: original System intent sync: %v", ErrUnknown, err)
	}
	if err := k.prebootstrapSystemApply(in.source.Limits.SystemMemoryMaxBytes); err != nil {
		return zero, fmt.Errorf("%w: partial System bound: %v", ErrUnknown, err)
	}
	if err := moveBootUserspace(ctx, k); err != nil {
		return zero, fmt.Errorf("%w: partial original System placement: %v", ErrUnknown, err)
	}
	current, err = k.prebootstrapSystemRead()
	if err != nil {
		return zero, fmt.Errorf("%w: System readback: %v", ErrUnknown, err)
	}
	if err := preSystemLockedDirectory(s, current); err != nil {
		return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	if err := preSystemBase(in, current); err != nil {
		return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	if err := preSystemStable(intent.Original, current); err != nil {
		return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	if err := preSystemPlaced(in, current); err != nil {
		return zero, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	complete := prebootstrapSystemCompletion{ProtocolVersion: 1, Scope: preSystemScope, IntentSHA256: physicalDigest(raw), Observed: current}
	done, err = preSystemCanonical(complete)
	if err != nil {
		return zero, err
	}
	if err := s.createPhysicalFile(preSystemCompleteName, done); err != nil {
		return zero, fmt.Errorf("%w: System completion persistence: %v", ErrUnknown, err)
	}
	return PrebootstrapSystemResult{ProtocolVersion: 1, Scope: preSystemScope, DurabilityConfirmed: true, IntentSHA256: physicalDigest(raw), CompletionSHA256: physicalDigest(done), Original: intent, Completion: complete, Current: current}, nil
}

// 内核profile适配在此一处显式选择；上层Prepare/Observe/Replay及结果合同不按PG版本分叉。
// v1历史552字节/marker不改；v2必须369新Origin，不允许旧policy/nonce悄悄升级。
func preSystemPolicyVersionMatches(profile string, version uint32) bool {
	switch profile {
	case NormalV1PhysicalKernelProfileID:
		return version == 1
	case NormalSplitV1PhysicalKernelProfileID:
		ref, err := LookupPhysicalKernelBuild(profile)
		return version == 2 && err == nil && ref.StrictPageCounterIRQBoundary()
	default:
		return false
	}
}
