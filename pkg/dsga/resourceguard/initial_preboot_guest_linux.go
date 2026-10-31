//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	bootprotocol "github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"golang.org/x/sys/unix"
)

const (
	initialPrebootGuestScope        = "initial-preboot-guest-preparation-only"
	initialPrebootGuestIntentName   = "initial-preboot-guest.intent.json"
	initialPrebootGuestCompleteName = "initial-preboot-guest.complete.json"
)

type initialPrebootGuestInputs struct {
	challenge    bootprotocol.InitialPrebootChallenge
	runtime      []byte
	source       SourceConfig
	requirements RuntimeBootRequirements
}

// 只校语法及Guest可独立读取的原ISO引用。Host SourceSHA/HostStageSHA及Runner
// Node/进程来源在Guest无法从JSON认证，完整challenge原样保留供Host固定reader匹配。
func initialPrebootBindInputs(c bootprotocol.InitialPrebootChallenge, raw []byte, assets map[string][]byte, config Config) (initialPrebootGuestInputs, error) {
	var zero initialPrebootGuestInputs
	if err := c.Validate(); err != nil {
		return zero, err
	}
	var d RuntimeDocument
	if err := json.Unmarshal(raw, &d); err != nil {
		return zero, err
	}
	source, err := d.Source()
	if err != nil {
		return zero, err
	}
	req, err := DeriveRuntimeBootRequirements(raw, assets)
	if err != nil {
		return zero, err
	}
	if d.ProtocolVersion != 2 || !req.Enabled() || req.ProfileID() != NormalSplitV1PhysicalKernelProfileID || source.Config != config || c.Identity.VMUID != config.VMUID || c.Identity.Incarnation != d.Incarnation || c.RuntimeSHA256 != req.RuntimeDocumentSHA256() || c.ResourceSHA256 != req.ResourceSourceSHA256() || c.ConfigSecretUID != req.ConfigSecretUID() || c.KernelBuildSHA256 != req.KernelBuildReference().KernelArtifactSHA256 || uint64(c.OriginalBase) > req.BootMemoryUpperBoundBytes() {
		return zero, fmt.Errorf("initial Guest challenge differs from fixed full original runtime source")
	}
	upper, err := source.Limits.totalUpper()
	if err != nil || upper > uint64(c.BusinessMinimum) {
		return zero, fmt.Errorf("original business minimum lacks explicit total resource budget")
	}
	return initialPrebootGuestInputs{challenge: c, runtime: bytes.Clone(raw), source: source, requirements: req}, nil
}
func (g *Guard) initialPrebootInputs(c bootprotocol.InitialPrebootChallenge) (initialPrebootGuestInputs, error) {
	var zero initialPrebootGuestInputs
	if os.Getuid() != 0 || os.Geteuid() != 0 {
		return zero, fmt.Errorf("initial Guest producer requires real Root")
	}
	source, raw, err := runtimeSource()
	if err != nil {
		return zero, err
	}
	if source.Config != g.config {
		return zero, fmt.Errorf("initial Guest fixed resource config differs")
	}
	expected, err := RenderRuntimePolicyAssets(raw)
	if err != nil {
		return zero, err
	}
	actual := map[string][]byte{RuntimeResourceAssetName: raw}
	for logical, want := range expected {
		physical, err := RuntimeISOAssetName(logical)
		if err != nil {
			return zero, err
		}
		got, err := readKernelBootstrapISO("/neonvm/runtime/" + physical)
		if err != nil {
			return zero, err
		}
		if !bytes.Equal(got, want) {
			return zero, fmt.Errorf("initial Guest effective ISO asset differs: %s", logical)
		}
		actual[logical] = got
	}
	in, err := initialPrebootBindInputs(c, raw, actual, g.config)
	if err != nil {
		return zero, err
	}
	line, err := kernelText("/proc/cmdline")
	if err != nil {
		return zero, err
	}
	if err := in.requirements.ValidateKernelCommandLine(line); err != nil {
		return zero, err
	}
	if _, err := kernelBootstrapNamespaces(); err != nil {
		return zero, err
	}
	return in, nil
}

type initialPrebootGuestIntent struct {
	ProtocolVersion         uint32                               `json:"protocol_version"`
	Scope                   string                               `json:"scope"`
	Challenge               bootprotocol.InitialPrebootChallenge `json:"challenge"`
	GuestBootID             string                               `json:"guest_boot_id"`
	OriginalRoot            nativeBirthConsumer                  `json:"original_root"`
	KernelPreparationSHA256 string                               `json:"kernel_preparation_sha256"`
	StateDevice             control.Uint64                       `json:"state_device"`
	StateInode              control.Uint64                       `json:"state_inode"`
}
type initialPrebootGuestComplete struct {
	IssuingRoot             nativeBirthConsumer       `json:"issuing_root"`
	ProtocolVersion         uint32                    `json:"protocol_version"`
	Scope                   string                    `json:"scope"`
	IntentSHA256            string                    `json:"intent_sha256"`
	KernelPreparationSHA256 string                    `json:"kernel_preparation_sha256"`
	PhysicalOriginSHA256    string                    `json:"physical_origin_sha256"`
	PreSystemSHA256         string                    `json:"pre_system_sha256"`
	PreparedPhysical        PhysicalKernelObservation `json:"prepared_physical"`
}
type initialPrebootGuestOperation uint8

const (
	initialPrebootGuestPrepare initialPrebootGuestOperation = iota
	initialPrebootGuestObserve
	initialPrebootGuestRecover
)

var initialPrebootGuestFiles = [...]string{kernelPrepIntentName, kernelPrepCompleteName, physicalOriginDataName, physicalOriginConfirmName, preSystemIntentName, preSystemCompleteName, initialPrebootGuestIntentName, initialPrebootGuestCompleteName}

type initialPrebootGuestWitnessKey struct {
	Device, Inode, LockDevice, LockInode uint64
	ChallengeSHA256                      string
	Files                                [8]arrayPopulateHistoryIdentity
	SHA256                               [8]string
}

var initialPrebootGuestWitnesses sync.Map

func initialPrebootReadRecord(s *lockedState, name string, value any) ([]byte, bool, error) {
	raw, exists, err := s.readPhysicalFile(name)
	if err != nil || !exists {
		return raw, exists, err
	}
	if err := control.UnmarshalObject(raw, value); err != nil {
		return nil, true, errors.Join(ErrUnknown, err)
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, true, errors.Join(ErrUnknown, fmt.Errorf("noncanonical original initial Guest stage"), err)
	}
	return raw, true, nil
}
func initialPrebootStateIdentity(s *lockedState) (unix.Stat_t, error) {
	var st unix.Stat_t
	if s == nil || s.dir == nil || s.lock == nil {
		return st, fmt.Errorf("original Guest resource lock missing")
	}
	if err := unix.Fstat(int(s.dir.Fd()), &st); err != nil {
		return st, err
	}
	if privateRegular(int(s.dir.Fd()), unix.S_IFDIR) != nil || privateRegular(int(s.lock.Fd()), unix.S_IFREG) != nil {
		return st, fmt.Errorf("original Guest state ownership differs")
	}
	return st, nil
}
func initialPrebootIntentMatch(s *lockedState, in initialPrebootGuestInputs, intent initialPrebootGuestIntent, boot, kernelSHA string) error {
	st, err := initialPrebootStateIdentity(s)
	if err != nil {
		return err
	}
	if intent.ProtocolVersion != 1 || intent.Scope != initialPrebootGuestScope || intent.Challenge != in.challenge || intent.GuestBootID != boot || intent.OriginalRoot.PID <= 1 || intent.OriginalRoot.StartTicks == 0 || !physicalDigestValid(intent.OriginalRoot.InstanceNonce) || !physicalDigestValid(intent.KernelPreparationSHA256) || intent.KernelPreparationSHA256 != kernelSHA || intent.StateDevice != control.Uint64(st.Dev) || intent.StateInode != control.Uint64(st.Ino) {
		return fmt.Errorf("initial Guest original challenge/boot/kernel/state differs")
	}
	return nil
}

// 三入口均不接收policy/path、既有cmd/vminit没有自动调用。此producer只准备实际
// Kernel/Origin/PreSys阶段，返回的typed receipt不发Host grow、PG出生或normal许可。
func (g *Guard) PrepareInitialPreboot(ctx context.Context, c bootprotocol.InitialPrebootChallenge) (bootprotocol.InitialPrebootReceipt, error) {
	return g.initialPrebootGuest(ctx, c, initialPrebootGuestPrepare)
}
func (g *Guard) ObserveInitialPreboot(ctx context.Context, c bootprotocol.InitialPrebootChallenge) (bootprotocol.InitialPrebootReceipt, error) {
	return g.initialPrebootGuest(ctx, c, initialPrebootGuestObserve)
}
func (g *Guard) RecoverInitialPreboot(ctx context.Context, c bootprotocol.InitialPrebootChallenge) (bootprotocol.InitialPrebootReceipt, error) {
	return g.initialPrebootGuest(ctx, c, initialPrebootGuestRecover)
}

func (g *Guard) initialPrebootGuest(ctx context.Context, c bootprotocol.InitialPrebootChallenge, op initialPrebootGuestOperation) (bootprotocol.InitialPrebootReceipt, error) {
	var zero bootprotocol.InitialPrebootReceipt
	if ctx == nil || ctx.Err() != nil {
		return zero, fmt.Errorf("live original initial Guest context required")
	}
	in, err := g.initialPrebootInputs(c)
	if err != nil {
		return zero, err
	}
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		return zero, err
	}
	var intent initialPrebootGuestIntent
	_, exists, err := initialPrebootReadRecord(s, initialPrebootGuestIntentName, &intent)
	_, complete, ce := s.readPhysicalFile(initialPrebootGuestCompleteName)
	if err == nil {
		err = ce
	}
	if err == nil && complete && !exists {
		err = errors.Join(ErrUnknown, fmt.Errorf("initial Guest completion without intent"))
	}
	if err == nil {
		r, e := s.load()
		if e != nil {
			err = e
		} else if r != nil {
			err = fmt.Errorf("initial Guest phase cannot acquire historical resource/bootstrap/birth journal")
		}
	}
	if err != nil {
		s.close()
		return zero, err
	}
	if exists {
		// 先完整核不可变challenge，拒绝改变来源后才Replay任何旧阶段。
		boot, e := kernelText("/proc/sys/kernel/random/boot_id")
		if e == nil {
			e = initialPrebootIntentMatch(s, in, intent, boot, intent.KernelPreparationSHA256)
		}
		if e != nil {
			s.close()
			return zero, e
		}
		if op != initialPrebootGuestRecover {
			defer s.close()
			return g.initialPrebootReceiptLocked(ctx, s, in, false)
		}
	} else if op == initialPrebootGuestObserve {
		s.close()
		return zero, errors.Join(ErrUnknown, fmt.Errorf("initial Guest original preparation missing"))
	}
	// 不在resource flock内调用会再次取锁的公开阶段入口。每阶段持自己的同一原锁；
	// 最终组合重新持单锁核全部原文件与当前内核，而非拼接锁外旧快照。
	s.close()
	var kp NormalV1KernelBootstrapEvidence
	if op == initialPrebootGuestRecover {
		// 无原kernel intent时是明确恢复尚未开始的准备；已有partial只能显式Replay。
		probe, e := lockState(ctx, g.config.StateDir)
		if e != nil {
			return zero, e
		}
		_, hasKernel, e := probe.readPhysicalFile(kernelPrepIntentName)
		probe.close()
		if e != nil {
			return zero, e
		}
		if hasKernel {
			kp, err = g.ReplayKernelBeforeOrigin(ctx)
		} else {
			kp, err = g.PrepareKernelBeforeOrigin(ctx)
		}
	} else {
		kp, err = g.PrepareKernelBeforeOrigin(ctx)
	}
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	if !kp.DurabilityConfirmed() {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("original kernel preparation requires explicit durable recovery"))
	}
	kernelSHA, err := kp.ReferenceSHA256()
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	if !exists {
		s, err = lockState(ctx, g.config.StateDir)
		if err != nil {
			return zero, errors.Join(ErrUnknown, err)
		}
		// KernelPrep首次boundary只允许其两文件，所以aggregate意图只能在其成功后落盘。
		_, already, e := s.readPhysicalFile(initialPrebootGuestIntentName)
		if e == nil && already {
			e = fmt.Errorf("initial Guest intent appeared concurrently; explicit recovery required")
		}
		consumer, consumerErr := nativeBirthCurrentConsumerIdentity()
		if e == nil {
			e = consumerErr
		}
		st, statErr := initialPrebootStateIdentity(s)
		if e == nil {
			e = statErr
		}
		if e == nil {
			intent = initialPrebootGuestIntent{ProtocolVersion: 1, Scope: initialPrebootGuestScope, Challenge: c, GuestBootID: kp.After.Identity.GuestBootID, OriginalRoot: consumer, KernelPreparationSHA256: kernelSHA, StateDevice: control.Uint64(st.Dev), StateInode: control.Uint64(st.Ino)}
			raw, merr := json.Marshal(intent)
			if merr != nil {
				e = merr
			} else {
				e = s.createPhysicalFile(initialPrebootGuestIntentName, raw)
			}
		}
		s.close()
		if e != nil {
			return zero, errors.Join(ErrUnknown, e)
		}
	}
	// Capture/Replay都沿原唯一nonce，不覆盖旧Origin；PreSys同样不把partial当成功。
	if op == initialPrebootGuestRecover {
		probe, e := lockState(ctx, g.config.StateDir)
		if e != nil {
			return zero, e
		}
		_, hasOrigin, e := probe.readPhysicalFile(physicalOriginDataName)
		probe.close()
		if e != nil {
			return zero, e
		}
		if hasOrigin {
			_, err = g.ReplayPhysicalKernelOrigin(ctx)
		} else {
			_, err = g.CapturePhysicalKernelOrigin(ctx)
		}
	} else {
		_, err = g.CapturePhysicalKernelOrigin(ctx)
	}
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	if op == initialPrebootGuestRecover {
		probe, e := lockState(ctx, g.config.StateDir)
		if e != nil {
			return zero, e
		}
		_, hasSystem, e := probe.readPhysicalFile(preSystemIntentName)
		probe.close()
		if e != nil {
			return zero, e
		}
		if hasSystem {
			_, err = g.ReplaySystemBeforeBootstrap(ctx)
		} else {
			_, err = g.PrepareSystemBeforeBootstrap(ctx)
		}
	} else {
		_, err = g.PrepareSystemBeforeBootstrap(ctx)
	}
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	s, err = lockState(ctx, g.config.StateDir)
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	defer s.close()
	return g.initialPrebootReceiptLocked(ctx, s, in, true)
}

// 单锁完整实读：原kernel完成+Origin确认+PreSys原阶段与当前归组/物理不漂移。
// 不调用公开Observe，不重入flock。要求尚无正式Bootstrap/Birth且PG递归为空。
func (g *Guard) initialPrebootFactsLocked(ctx context.Context, s *lockedState, in initialPrebootGuestInputs, allowMovable bool) (initialPrebootGuestIntent, initialPrebootGuestComplete, []byte, error) {
	var intent initialPrebootGuestIntent
	var facts initialPrebootGuestComplete
	currentInputs, err := g.initialPrebootInputs(in.challenge)
	if err != nil || !bytes.Equal(currentInputs.runtime, in.runtime) {
		return intent, facts, nil, errors.Join(ErrUnknown, fmt.Errorf("initial Guest fixed source changed"), err)
	}
	r, err := s.load()
	if err != nil || r != nil {
		return intent, facts, nil, errors.Join(ErrUnknown, fmt.Errorf("initial Guest requires pre-Bootstrap resource phase"), err)
	}
	rawIntent, exists, err := initialPrebootReadRecord(s, initialPrebootGuestIntentName, &intent)
	if err != nil || !exists {
		return intent, facts, nil, errors.Join(ErrUnknown, fmt.Errorf("initial Guest original intent missing"), err)
	}
	ki, err := g.normalKernelBootstrapInputs()
	if err != nil {
		return intent, facts, nil, err
	}
	kp, err := g.normalKernelPreparationLocked(ctx, ki, kernelPrepObserve, s)
	if err != nil {
		return intent, facts, nil, err
	}
	ksha, err := kp.ReferenceSHA256()
	if err != nil {
		return intent, facts, nil, err
	}
	backend, ok := g.kernel.(bootstrapPreSystemBackend)
	if !ok {
		return intent, facts, nil, fmt.Errorf("fixed original PreSys reader unavailable")
	}
	pi, err := backend.bootstrapPreSystemInputs(s)
	if err != nil {
		return intent, facts, nil, err
	}
	placed, err := g.prebootstrapSystemLocked(ctx, pi, preSystemObserve, s)
	if err != nil {
		return intent, facts, nil, err
	}
	if !placed.Current.PGEmpty {
		return intent, facts, nil, fmt.Errorf("PG role appeared before initial Guest preparation finished")
	}
	if err := initialPrebootIntentMatch(s, in, intent, placed.Current.Physical.Identity.GuestBootID, ksha); err != nil {
		return intent, facts, nil, err
	}
	if pi.origin.Policy.ProfileID != in.requirements.ProfileID() || pi.origin.RuntimeDocumentSHA256 != in.requirements.RuntimeDocumentSHA256() || pi.origin.ResourceSourceSHA256 != in.requirements.ResourceSourceSHA256() {
		return intent, facts, nil, fmt.Errorf("initial Guest original physical source differs")
	}
	if uint64(pi.origin.First.NonMovableUpperBytes) > uint64(in.challenge.OriginalBase) {
		return intent, facts, nil, fmt.Errorf("original fixed physical pages exceed planned Host base")
	}
	for _, z := range pi.origin.First.Zones {
		if z.Name == "Movable" && z.PresentPages != 0 {
			return intent, facts, nil, fmt.Errorf("original Origin was not base-only")
		}
	}
	if !allowMovable {
		for _, z := range placed.Current.Physical.Zones {
			if z.Name == "Movable" && z.PresentPages != 0 {
				return intent, facts, nil, fmt.Errorf("Movable appeared before Guest prepared receipt")
			}
		}
	}
	facts = initialPrebootGuestComplete{ProtocolVersion: 1, Scope: initialPrebootGuestScope, IntentSHA256: physicalDigest(rawIntent), KernelPreparationSHA256: ksha, PhysicalOriginSHA256: pi.originSHA, PreSystemSHA256: placed.CompletionSHA256, PreparedPhysical: placed.Current.Physical}
	return intent, facts, rawIntent, nil
}
func initialPrebootGuestWitnessLocked(s *lockedState, c bootprotocol.InitialPrebootChallenge) (initialPrebootGuestWitnessKey, error) {
	var key initialPrebootGuestWitnessKey
	st, err := initialPrebootStateIdentity(s)
	if err != nil {
		return key, err
	}
	var lock unix.Stat_t
	if err := unix.Fstat(int(s.lock.Fd()), &lock); err != nil {
		return key, err
	}
	key.Device, key.Inode, key.LockDevice, key.LockInode = st.Dev, st.Ino, lock.Dev, lock.Ino
	raw, _ := json.Marshal(c)
	key.ChallengeSHA256 = physicalDigest(raw)
	for i, name := range initialPrebootGuestFiles {
		raw, stamp, err := arrayPopulateHistoryRaw(s, name)
		if err != nil {
			return key, err
		}
		key.Files[i], key.SHA256[i] = stamp, physicalDigest(raw)
	}
	return key, nil
}
func (g *Guard) initialPrebootReceiptLocked(ctx context.Context, s *lockedState, in initialPrebootGuestInputs, durable bool) (bootprotocol.InitialPrebootReceipt, error) {
	var zero bootprotocol.InitialPrebootReceipt
	var completed initialPrebootGuestComplete
	raw, exists, err := initialPrebootReadRecord(s, initialPrebootGuestCompleteName, &completed)
	if err != nil {
		return zero, err
	}
	intent, facts, _, err := g.initialPrebootFactsLocked(ctx, s, in, exists)
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	if exists {
		// grow后的显式恢复仍引用原base-only历史，不改成新的首次准备；当前页几何另读。
		if err := initialPrebootMatchHistoricalComplete(facts, completed); err != nil {
			return zero, errors.Join(ErrUnknown, err)
		}
		facts.PreparedPhysical = completed.PreparedPhysical
		facts.IssuingRoot = completed.IssuingRoot
	} else {
		consumer, err := nativeBirthCurrentConsumerIdentity()
		if err != nil {
			return zero, err
		}
		facts.IssuingRoot = consumer // 真正完成发行者；跨Root首次完成不伪装intent创建者。
	}
	want, _ := json.Marshal(facts)
	if exists && !bytes.Equal(raw, want) {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("initial Guest complete facts differ"))
	}
	if !exists {
		if !durable {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("initial Guest original completion missing"))
		}
		if err := s.createPhysicalFile(initialPrebootGuestCompleteName, want); err != nil {
			return zero, errors.Join(ErrUnknown, err)
		}
	}
	before, err := initialPrebootGuestWitnessLocked(s, in.challenge)
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	if durable {
		initialPrebootGuestWitnesses.Delete(before)
		for _, name := range initialPrebootGuestFiles {
			if err := ctx.Err(); err != nil {
				return zero, errors.Join(ErrUnknown, err)
			}
			if err := s.syncPhysicalFile(name); err != nil {
				return zero, errors.Join(ErrUnknown, err)
			}
		}
		if err := s.dir.Sync(); err != nil {
			return zero, errors.Join(ErrUnknown, err)
		}
		_, afterFacts, _, err := g.initialPrebootFactsLocked(ctx, s, in, true)
		if err != nil {
			return zero, errors.Join(ErrUnknown, err)
		}
		if err := initialPrebootMatchHistoricalComplete(afterFacts, facts); err != nil {
			return zero, errors.Join(ErrUnknown, err)
		}
		afterFacts.PreparedPhysical = facts.PreparedPhysical
		afterFacts.IssuingRoot = facts.IssuingRoot
		after, err := initialPrebootGuestWitnessLocked(s, in.challenge)
		afterRaw, _ := json.Marshal(afterFacts)
		if err != nil || after != before || !bytes.Equal(want, afterRaw) {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("initial Guest prefix changed while syncing"), err)
		}
		initialPrebootGuestWitnesses.Store(after, struct{}{})
	} else if _, ok := initialPrebootGuestWitnesses.Load(before); !ok {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("initial Guest preparation lacks this Root durability confirmation; explicit Recover required"))
	}
	return initialPrebootOriginalReceipt(intent, facts)

}

// grow后独立只读观察，不创建/重放阶段文件；Host必须另外核原QMP实际target。
func (g *Guard) InspectInitialPrebootMovable(ctx context.Context, prepared bootprotocol.InitialPrebootReceipt) (InitialPrebootMovableObservation, error) {
	var zero InitialPrebootMovableObservation
	if err := prepared.Validate(); err != nil {
		return zero, err
	}
	in, err := g.initialPrebootInputs(prepared.Challenge)
	if err != nil {
		return zero, err
	}
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		return zero, err
	}
	defer s.close()
	intent, current, _, err := g.initialPrebootFactsLocked(ctx, s, in, true)
	if err != nil {
		return zero, errors.Join(ErrUnknown, err)
	}
	var original initialPrebootGuestComplete
	_, exists, err := initialPrebootReadRecord(s, initialPrebootGuestCompleteName, &original)
	if err != nil || !exists {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("initial Guest original prepared facts unavailable"), err)
	}
	key, err := initialPrebootGuestWitnessLocked(s, in.challenge)
	if err != nil {
		return zero, err
	}
	if _, ok := initialPrebootGuestWitnesses.Load(key); !ok {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("original prepared prefix lacks current Root confirmation"))
	}
	consumer, err := nativeBirthCurrentConsumerIdentity()
	if err != nil {
		return zero, err
	}
	expected, err := initialPrebootOriginalReceipt(intent, original)
	if err != nil || prepared != expected || initialPrebootMatchHistoricalComplete(current, original) != nil {
		return zero, fmt.Errorf("exact historical prepared receipt or original stage source differs: %v", err)
	}
	pages, err := initialPrebootMovablePages(in.challenge, original.PreparedPhysical, current.PreparedPhysical)
	if err != nil {
		return zero, err
	}
	rawCurrent, _ := json.Marshal(current.PreparedPhysical)
	rawReceipt, _ := json.Marshal(prepared)
	return InitialPrebootMovableObservation{ProtocolVersion: 1, Scope: "initial-preboot-movable-observation-only", Challenge: in.challenge, PreparedReceiptSHA256: physicalDigest(rawReceipt), GuestBootID: prepared.GuestBootID, RootInstanceNonce: consumer.InstanceNonce, PhysicalOriginSHA256: original.PhysicalOriginSHA256, PreSystemSHA256: original.PreSystemSHA256, CurrentPhysical: current.PreparedPhysical, CurrentPhysicalSHA256: physicalDigest(rawCurrent), ExpectedIncrementBytes: in.challenge.BusinessMinimum - in.challenge.OriginalBase, MovablePresentPages: control.Uint64(pages)}, nil
}

// 完整历史receipt只从原intent+complete重建；不接受调用者替换的旧nonce/摘要。
// 当前Root的恢复见证与历史发行者分开，Movable回执另写当前实际Root身份。
func initialPrebootOriginalReceipt(intent initialPrebootGuestIntent, complete initialPrebootGuestComplete) (bootprotocol.InitialPrebootReceipt, error) {
	var zero bootprotocol.InitialPrebootReceipt
	if complete.IssuingRoot.PID <= 1 || complete.IssuingRoot.StartTicks == 0 || !physicalDigestValid(complete.IssuingRoot.InstanceNonce) {
		return zero, fmt.Errorf("original receipt issuer missing")
	}
	raw, _ := json.Marshal(complete.PreparedPhysical)
	receipt := bootprotocol.InitialPrebootReceipt{Challenge: intent.Challenge, GuestBootID: complete.PreparedPhysical.Identity.GuestBootID, RootInstanceNonce: complete.IssuingRoot.InstanceNonce, KernelPreparationSHA256: complete.KernelPreparationSHA256, PhysicalOriginSHA256: complete.PhysicalOriginSHA256, PreSystemSHA256: complete.PreSystemSHA256, CurrentPhysicalSHA256: physicalDigest(raw)}
	if err := receipt.MatchChallenge(intent.Challenge); err != nil {
		return zero, err
	}
	return receipt, nil
}

// 已完成准备是原base-only历史；当前允许grow但固定内核/域/原阶段引用必须一致。
func initialPrebootMatchHistoricalComplete(current, original initialPrebootGuestComplete) error {
	if original.IssuingRoot.PID <= 1 || original.IssuingRoot.StartTicks == 0 || !physicalDigestValid(original.IssuingRoot.InstanceNonce) {
		return fmt.Errorf("original prepared receipt issuer missing")
	}
	if original.ProtocolVersion != 1 || original.Scope != initialPrebootGuestScope || current.ProtocolVersion != original.ProtocolVersion || current.Scope != original.Scope || current.IntentSHA256 != original.IntentSHA256 || current.KernelPreparationSHA256 != original.KernelPreparationSHA256 || current.PhysicalOriginSHA256 != original.PhysicalOriginSHA256 || current.PreSystemSHA256 != original.PreSystemSHA256 {
		return fmt.Errorf("original initial Guest completion source differs")
	}
	for _, z := range original.PreparedPhysical.Zones {
		if z.Name == "Movable" && z.PresentPages != 0 {
			return fmt.Errorf("original prepared observation was not base-only")
		}
	}
	return original.PreparedPhysical.Match(current.PreparedPhysical)
}
func initialPrebootMovablePages(c bootprotocol.InitialPrebootChallenge, before, current PhysicalKernelObservation) (uint64, error) {
	if err := c.Validate(); err != nil {
		return 0, err
	}
	if before.Identity.VMUID != c.Identity.VMUID {
		return 0, fmt.Errorf("Movable source VM differs")
	}
	if err := before.Match(current); err != nil {
		return 0, err
	}
	var old, pages uint64
	for _, z := range before.Zones {
		if z.Name == "Movable" {
			old += uint64(z.PresentPages)
		}
	}
	for _, z := range current.Zones {
		if z.Name == "Movable" {
			pages += uint64(z.PresentPages)
		}
	}
	expected := uint64(c.BusinessMinimum - c.OriginalBase)
	if old != 0 || expected%4096 != 0 || pages != expected/4096 {
		return 0, fmt.Errorf("actual MOVABLE present pages do not match original Host grow increment")
	}
	return pages, nil
}
