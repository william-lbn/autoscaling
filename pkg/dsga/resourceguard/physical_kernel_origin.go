//go:build linux

package resourceguard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

const (
	PhysicalKernelOriginArg   = "neonvm.dsga.physical_profile=kernel-v1"
	physicalOriginScope       = "pre-bootstrap-physical-component-only"
	physicalOriginProfileID   = "pg17-linux-6.12.26-physical-component-v1"
	physicalOriginDataName    = "physical-kernel-origin.json"
	physicalOriginConfirmName = "physical-kernel-origin.confirmed.json"
	physicalKernelArtifactSHA = "0ab3f71456c783452a49d50752a9b1945c2da399588febed409637d7af6891a0"
	physicalKernelConfigSHA   = "639ea751eb4ae7743e2695368286d1aeb0417f6680d6ac24ae3b051948a94b96"
)

// PhysicalKernelOriginPolicy 仅由 root 读取原只读 runtime ISO，不能来自请求参数。
// artifact/config 是独立构建收据的准确引用，不是 Guest 自行测得整个镜像的证明；
// notes 必须实际读回相符。此组件没有 private/FS/恢复全包络或降限消费者。
type PhysicalKernelOriginPolicy struct {
	ProtocolVersion       uint32 `json:"protocol_version"`
	ProfileID             string `json:"profile_id"`
	KernelRelease         string `json:"kernel_release"`
	KernelNotesSHA256     string `json:"kernel_notes_sha256"`
	KernelArtifactSHA256  string `json:"kernel_artifact_sha256"`
	KernelConfigSHA256    string `json:"kernel_config_sha256"`
	RuntimeDocumentSHA256 string `json:"runtime_document_sha256"`
}

func (p PhysicalKernelOriginPolicy) Validate() error {
	if p.ProtocolVersion != 1 || !physicalDigestValid(p.KernelNotesSHA256) || !physicalDigestValid(p.RuntimeDocumentSHA256) {
		return fmt.Errorf("invalid exact physical component policy")
	}
	if p.ProfileID == physicalOriginProfileID {
		// 保留旧组件策略的原字节/语义；旧记录不能借新 lookup 自动升级资格。
		if p.KernelRelease != physicalKernelRelease || p.KernelArtifactSHA256 != physicalKernelArtifactSHA || p.KernelConfigSHA256 != physicalKernelConfigSHA {
			return fmt.Errorf("historical physical component build differs")
		}
	} else {
		ref, err := LookupPhysicalKernelBuild(p.ProfileID)
		if err != nil || (p.ProfileID != NormalV1PhysicalKernelProfileID && !physicalKernelHasPIDHardSource(p.ProfileID)) || p.KernelRelease != ref.KernelRelease || p.KernelArtifactSHA256 != ref.KernelArtifactSHA256 || p.KernelConfigSHA256 != ref.KernelConfigSHA256 || p.KernelNotesSHA256 != ref.KernelNotesSHA256 {
			return fmt.Errorf("normal-v1 exact physical component build differs")
		}
	}
	return nil
}
func (p *PhysicalKernelOriginPolicy) UnmarshalJSON(b []byte) error {
	type plain PhysicalKernelOriginPolicy
	var v plain
	if err := control.UnmarshalObject(b, &v); err != nil {
		return err
	}
	n := PhysicalKernelOriginPolicy(v)
	if err := n.Validate(); err != nil {
		return err
	}
	*p = n
	return nil
}

// PhysicalKernelOrigin 只证明同原资源锁内的组件首次捕获与来源字节绑定。
// state 缺失/新 marker/O_EXCL 均不能独立证明整个 Guest 历史从无旧工作负载、
// Root 永远不漂移、或 PG 原出生已受限。未来完整消费者还须独立验证这些门槛。
// 原 nonce/完整首次事实终身保留；PG boot/revision/owner 变动不重造此来源。
type PhysicalKernelOrigin struct {
	ProtocolVersion       uint32                     `json:"protocol_version"`
	VMShrinkPermit        bool                       `json:"vm_shrink_permit"`
	Scope                 string                     `json:"scope"`
	Nonce                 string                     `json:"nonce"`
	Policy                PhysicalKernelOriginPolicy `json:"policy"`
	PolicySHA256          string                     `json:"policy_sha256"`
	ResourceSourceSHA256  string                     `json:"resource_source_sha256"`
	RuntimeDocumentSHA256 string                     `json:"runtime_document_sha256"`
	RuntimeIncarnation    string                     `json:"runtime_incarnation"`
	ConfigSecretUID       string                     `json:"config_secret_uid"`
	Config                Config                     `json:"config"`
	ExpectedBootstrap     BootstrapCommand           `json:"expected_bootstrap"`
	First                 PhysicalKernelObservation  `json:"first"`
}

func (o PhysicalKernelOrigin) Validate() error {
	if o.ProtocolVersion != 1 || o.VMShrinkPermit || o.Scope != physicalOriginScope || control.ValidateIncarnation(o.Nonce) != nil || o.Nonce == "00000000000000000000000000000000" || o.Policy.Validate() != nil || !physicalDigestValid(o.PolicySHA256) || !physicalDigestValid(o.ResourceSourceSHA256) || o.RuntimeDocumentSHA256 != o.Policy.RuntimeDocumentSHA256 || control.ValidateIncarnation(o.RuntimeIncarnation) != nil || control.ValidateID(o.ConfigSecretUID, "original config Secret UID") != nil || o.First.Validate() != nil || o.ExpectedBootstrap.Validate() != nil || o.ExpectedBootstrap.Identity != o.First.Identity || o.Config.VMUID != o.First.Identity.VMUID || o.Config.ComputeInstanceID != o.First.Identity.ComputeInstanceID || o.First.KernelNotesSHA256 != o.Policy.KernelNotesSHA256 || o.First.KernelRelease != o.Policy.KernelRelease {
		return fmt.Errorf("invalid original physical component capture")
	}
	return nil
}

// ReferenceSHA256 只产生本组件的准确规范文件摘要，便于后续原出生绑定引用。
// 摘要不认证调用者自行构造的结构；生产必须先从原 Capture/Observe/Replay 成功取得。
func (o PhysicalKernelOrigin) ReferenceSHA256() (string, error) {
	if err := o.Validate(); err != nil {
		return "", err
	}
	b, err := json.Marshal(o)
	if err != nil {
		return "", err
	}
	return physicalDigest(b), nil
}

func (o *PhysicalKernelOrigin) UnmarshalJSON(b []byte) error {
	type plain PhysicalKernelOrigin
	var v plain
	if err := control.UnmarshalObject(b, &v); err != nil {
		return err
	}
	n := PhysicalKernelOrigin(v)
	if err := n.Validate(); err != nil {
		return err
	}
	*o = n
	return nil
}

type physicalOriginConfirmation struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	OriginSHA256    string `json:"origin_sha256"`
}
type physicalOriginInputs struct {
	policy                                                   PhysicalKernelOriginPolicy
	policySHA, sourceSHA, runtimeSHA, incarnation, secretUID string
	config                                                   Config
	bootstrap                                                BootstrapCommand
	bootMarker                                               bool
}

func (i physicalOriginInputs) makeOrigin(first PhysicalKernelObservation) PhysicalKernelOrigin {
	return PhysicalKernelOrigin{ProtocolVersion: 1, Scope: physicalOriginScope, Policy: i.policy, PolicySHA256: i.policySHA, ResourceSourceSHA256: i.sourceSHA, RuntimeDocumentSHA256: i.runtimeSHA, RuntimeIncarnation: i.incarnation, ConfigSecretUID: i.secretUID, Config: i.config, ExpectedBootstrap: i.bootstrap, First: first}
}

// 读取能力只实现于真实 Linux backend；U 测试另有明确标记的模型 backend。
// 单独接口不扩宽旧 kernel 资源写接口，也不会自动安装/改变任何资源限额。
type physicalOriginKernel interface {
	physicalOriginRead() (PhysicalKernelObservation, error)
	physicalOriginPGEmpty() error
}
type physicalOriginOperation uint8

const (
	physicalOriginCapture physicalOriginOperation = iota
	physicalOriginObserve
	physicalOriginReplay
)

func (g *Guard) physicalOrigin(ctx context.Context, in physicalOriginInputs, op physicalOriginOperation) (PhysicalKernelOrigin, error) {
	var zero PhysicalKernelOrigin
	if !in.bootMarker || in.config != g.config || in.policy.Validate() != nil || in.runtimeSHA != in.policy.RuntimeDocumentSHA256 || !physicalDigestValid(in.policySHA) || !physicalDigestValid(in.sourceSHA) {
		return zero, fmt.Errorf("physical origin requires original explicit boot/source policy")
	}
	k, ok := g.kernel.(physicalOriginKernel)
	if !ok {
		return zero, fmt.Errorf("physical origin backend unavailable")
	}
	// 锁序只取原 StateDir resource flock；不跨 PG/网络取锁，不碰运行期源/限额。
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		return zero, err
	}
	defer s.close()
	r, err := s.load()
	if err != nil {
		return zero, err
	}
	raw, exists, err := s.readPhysicalFile(physicalOriginDataName)
	if err != nil {
		return zero, fmt.Errorf("%w: physical data: %v", ErrUnknown, err)
	}
	confirm, confirmed, err := s.readPhysicalFile(physicalOriginConfirmName)
	if err != nil {
		return zero, fmt.Errorf("%w: physical confirmation: %v", ErrUnknown, err)
	}
	if !exists {
		if confirmed || op != physicalOriginCapture {
			return zero, fmt.Errorf("%w: original physical capture missing", ErrUnknown)
		}
		// 旧日志、旧 Birth/未知写或任何 Initialize 都不能补签新的首次组件来源。
		if r != nil {
			return zero, fmt.Errorf("historical resource journal cannot acquire new physical origin")
		}
		if err := k.physicalOriginPGEmpty(); err != nil {
			return zero, err
		}
		first, err := k.physicalOriginRead()
		if err != nil {
			return zero, err
		}
		o := in.makeOrigin(first)
		var nonce [16]byte
		if _, err = rand.Read(nonce[:]); err != nil {
			return zero, err
		}
		o.Nonce = hex.EncodeToString(nonce[:])
		if err = o.Validate(); err != nil {
			return zero, err
		}
		if err = ctx.Err(); err != nil {
			return zero, err
		}
		raw, err = json.Marshal(o)
		if err != nil {
			return zero, err
		}
		// data 与确认分两次 O_EXCL/fsync。data 成功后任一失败保留原 nonce；
		// 不删部分文件、不回报 NoEffect、不在普通重试中重造另一份原来源。
		if err = s.createPhysicalFile(physicalOriginDataName, raw); err != nil {
			return zero, fmt.Errorf("%w: physical first write: %v", ErrUnknown, err)
		}
		if err = s.confirmPhysical(raw); err != nil {
			return zero, fmt.Errorf("%w: physical confirmation write: %v", ErrUnknown, err)
		}
		return o, nil
	}
	var o PhysicalKernelOrigin
	if err = json.Unmarshal(raw, &o); err != nil {
		return zero, fmt.Errorf("%w: malformed original physical data: %v", ErrUnknown, err)
	}
	expected := in.makeOrigin(o.First)
	expected.Nonce = o.Nonce
	want, err := json.Marshal(expected)
	if err != nil {
		return zero, err
	}
	existing, err := json.Marshal(o)
	if err != nil {
		return zero, err
	}
	// 本文件由唯一 producer 规范编码；不把重排/追加空白后的另一份字节来源
	// 悄悄规范化成原来源，确保公开引用摘要就是已确认的原文件准确摘要。
	if string(raw) != string(existing) {
		return zero, fmt.Errorf("%w: original physical data is not canonical producer output", ErrUnknown)
	}
	if string(want) != string(existing) {
		return zero, fmt.Errorf("original physical capture source differs")
	}
	current, err := k.physicalOriginRead()
	if err != nil {
		return zero, err
	}
	if err = o.First.Match(current); err != nil {
		return zero, err
	}
	if r != nil {
		if r.validate(o.First.Identity) != nil || r.Bootstrap == nil || sameBootstrapSource(SourceConfig{Config: in.config, IssuerEpoch: in.bootstrap.IssuerEpoch, BudgetCommitmentID: in.bootstrap.BudgetCommitmentID, Limits: in.bootstrap.Limits}, r.Bootstrap.Original) != nil {
			return zero, fmt.Errorf("resource journal differs from original physical component binding")
		}
	}
	if confirmed {
		var c physicalOriginConfirmation
		if err = control.UnmarshalObject(confirm, &c); err != nil || c.ProtocolVersion != 1 || c.OriginSHA256 != physicalDigest(raw) {
			return zero, fmt.Errorf("%w: original physical confirmation differs", ErrUnknown)
		}
		// 完整确认文件的可见性不能证明前次调用已收到 fsync 成功回复；显式 Replay
		// 即使已有确认也重做原文件/目录同步，不重写字节、nonce 或任何资源限额。
		if op == physicalOriginReplay {
			if err = s.syncPhysicalFile(physicalOriginDataName); err != nil {
				return zero, fmt.Errorf("%w: original physical data sync: %v", ErrUnknown, err)
			}
			if err = s.syncPhysicalFile(physicalOriginConfirmName); err != nil {
				return zero, fmt.Errorf("%w: original physical confirmation sync: %v", ErrUnknown, err)
			}
		}
		return o, nil
	}
	if op != physicalOriginReplay {
		return zero, fmt.Errorf("%w: original physical data lacks durable confirmation; explicit replay required", ErrUnknown)
	}
	if r != nil {
		return zero, fmt.Errorf("%w: cannot complete pending physical capture after resource initialization", ErrUnknown)
	}
	if err = k.physicalOriginPGEmpty(); err != nil {
		return zero, err
	}
	if err = ctx.Err(); err != nil {
		return zero, err
	}
	// 显式恢复只 fsync 原完整 data，再补其原 hash 的确认；从不覆盖 data/nonce。
	if err = s.syncPhysicalFile(physicalOriginDataName); err != nil {
		return zero, fmt.Errorf("%w: replay physical data sync: %v", ErrUnknown, err)
	}
	if err = s.confirmPhysical(raw); err != nil {
		return zero, fmt.Errorf("%w: replay physical confirmation: %v", ErrUnknown, err)
	}
	return o, nil
}

func (s *lockedState) readPhysicalFile(name string) ([]byte, bool, error) {
	fd, err := unix.Openat(int(s.dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	if err = privateRegular(fd, unix.S_IFREG); err != nil {
		return nil, false, err
	}
	b, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return nil, false, err
	}
	if len(b) > 64*1024 {
		return nil, false, fmt.Errorf("physical state exceeds bound")
	}
	return b, true, nil
}
func (s *lockedState) createPhysicalFile(name string, b []byte) error {
	if len(b) > 64*1024 {
		return fmt.Errorf("physical state exceeds bound")
	}
	fd, err := unix.Openat(int(s.dir.Fd()), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), name)
	n, err := f.Write(b)
	if err != nil {
		f.Close()
		return err
	}
	if n != len(b) {
		f.Close()
		return io.ErrShortWrite
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return s.dir.Sync()
}
func (s *lockedState) syncPhysicalFile(name string) error {
	fd, err := unix.Openat(int(s.dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	if err = privateRegular(fd, unix.S_IFREG); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return s.dir.Sync()
}
func (s *lockedState) confirmPhysical(raw []byte) error {
	b, err := json.Marshal(physicalOriginConfirmation{ProtocolVersion: 1, OriginSHA256: physicalDigest(raw)})
	if err != nil {
		return err
	}
	return s.createPhysicalFile(physicalOriginConfirmName, b)
}
