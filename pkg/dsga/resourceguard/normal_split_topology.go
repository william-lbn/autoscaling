//go:build linux

package resourceguard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const (
	normalSplitScope        = "normal-split-topology-preparation-only"
	normalSplitIntentName   = "normal-split-topology.intent.json"
	normalSplitCompleteName = "normal-split-topology.complete.json"
)

// NormalSplitTopologyPolicy只来自原只读ISO。P是出生就存在的新profile硬配额，
// 不是从RSS推算的兼容预算；A只指定首次数组额度，不给后继收费/降限授权。
type NormalSplitTopologyPolicy struct {
	ProtocolVersion             uint32         `json:"protocol_version"`
	Scope                       string         `json:"scope"`
	PhysicalProfileID           string         `json:"physical_profile_id"`
	RuntimeDocumentSHA256       string         `json:"runtime_document_sha256"`
	ResourceSourceSHA256        string         `json:"resource_source_sha256"`
	PrivateMemoryMaxBytes       control.Uint64 `json:"private_memory_max_bytes"`
	InitialArraysMemoryMaxBytes control.Uint64 `json:"initial_arrays_memory_max_bytes"`
	PrivatePidsMaxTasks         control.Uint64 `json:"private_pids_max_tasks"`
}

func (p NormalSplitTopologyPolicy) validate() error {
	if p.ProtocolVersion != 1 || p.Scope != normalSplitScope || p.PhysicalProfileID != NormalSplitV1PhysicalKernelProfileID ||
		!physicalDigestValid(p.RuntimeDocumentSHA256) || !physicalDigestValid(p.ResourceSourceSHA256) ||
		p.PrivateMemoryMaxBytes == 0 || p.InitialArraysMemoryMaxBytes == 0 || uint64(p.PrivateMemoryMaxBytes)%(64<<10) != 0 || uint64(p.InitialArraysMemoryMaxBytes)%(64<<10) != 0 ||
		uint64(p.PrivateMemoryMaxBytes) >= math.MaxInt64 || uint64(p.InitialArraysMemoryMaxBytes) >= math.MaxInt64 || p.PrivatePidsMaxTasks == 0 || p.PrivatePidsMaxTasks >= control.Uint64(math.MaxInt64) || uint64(p.PrivateMemoryMaxBytes) > math.MaxInt64-uint64(p.InitialArraysMemoryMaxBytes) {
		return fmt.Errorf("invalid original normal split topology policy")
	}
	return nil
}
func (p *NormalSplitTopologyPolicy) UnmarshalJSON(b []byte) error {
	type plain NormalSplitTopologyPolicy
	var v plain
	if err := control.UnmarshalObject(b, &v); err != nil {
		return err
	}
	n := NormalSplitTopologyPolicy(v)
	if err := n.validate(); err != nil {
		return err
	}
	*p = n
	return nil
}

type normalSplitNode struct {
	Device              control.Uint64 `json:"device"`
	Inode               control.Uint64 `json:"inode"`
	MemoryMaxBytes      control.Uint64 `json:"memory_max_bytes"`
	PidsMaxTasks        control.Uint64 `json:"pids_max_tasks"`
	MemoryCurrentBytes  control.Uint64 `json:"memory_current_bytes"`
	SwapCurrentBytes    control.Uint64 `json:"swap_current_bytes"`
	SwapMaxBytes        control.Uint64 `json:"swap_max_bytes"`
	MemoryHighUnlimited bool           `json:"memory_high_unlimited"`
	Empty               bool           `json:"empty"`
	RootControlled      bool           `json:"root_controlled"`
	Controllers         string         `json:"controllers"`
}

// Snapshot是同步内核读回。Current>A并不自动否定原来源，也绝不推导A为严格物理
// 上界；force-charge/counter/I和原业务容量必须由后续完整资格链另行证明。
type NormalSplitTopologySnapshot struct {
	Identity    Identity         `json:"identity"`
	Parent      normalSplitNode  `json:"parent"`
	Private     *normalSplitNode `json:"private,omitempty"`
	Arrays      *normalSplitNode `json:"arrays,omitempty"`
	StateDevice control.Uint64   `json:"state_device"`
	StateInode  control.Uint64   `json:"state_inode"`
}
type normalSplitInputs struct {
	source    SourceConfig
	sourceRaw []byte
	policy    NormalSplitTopologyPolicy
	policyRaw []byte
	origin    PhysicalKernelOrigin
	originSHA string
}

func (i normalSplitInputs) validate(c Config) error {
	var source SourceConfig
	var policy NormalSplitTopologyPolicy
	if len(i.sourceRaw) == 0 || len(i.sourceRaw) > 16<<10 || len(i.policyRaw) == 0 || len(i.policyRaw) > 64<<10 ||
		json.Unmarshal(i.sourceRaw, &source) != nil || json.Unmarshal(i.policyRaw, &policy) != nil || !reflect.DeepEqual(source, i.source) || policy != i.policy ||
		i.source.Validate() != nil || i.source.Config != c || !c.SystemCgroup || i.policy.validate() != nil ||
		i.policy.ResourceSourceSHA256 != physicalDigest(i.sourceRaw) || i.origin.ResourceSourceSHA256 != i.policy.ResourceSourceSHA256 ||
		i.origin.Validate() != nil || i.origin.Config != c || i.origin.Policy.ProfileID != NormalSplitV1PhysicalKernelProfileID ||
		i.origin.RuntimeDocumentSHA256 != i.policy.RuntimeDocumentSHA256 || sameBootstrapSource(i.source, i.origin.ExpectedBootstrap) != nil ||
		uint64(i.policy.PrivateMemoryMaxBytes)+uint64(i.policy.InitialArraysMemoryMaxBytes) > uint64(i.source.Limits.MemoryMaxBytes) ||
		i.source.Limits.PidsMaxTasks <= i.policy.PrivatePidsMaxTasks {
		return fmt.Errorf("split topology requires original runtime/source/369 Origin binding")
	}
	sha, err := i.origin.ReferenceSHA256()
	if err != nil || sha != i.originSHA {
		return fmt.Errorf("split original physical reference differs")
	}
	return nil
}

type normalSplitIntent struct {
	ProtocolVersion uint32                      `json:"protocol_version"`
	Scope           string                      `json:"scope"`
	Nonce           string                      `json:"nonce"`
	Policy          NormalSplitTopologyPolicy   `json:"policy"`
	PolicySHA256    string                      `json:"policy_sha256"`
	OriginSHA256    string                      `json:"origin_sha256"`
	Original        NormalSplitTopologySnapshot `json:"original"`
}
type normalSplitCompletion struct {
	ProtocolVersion uint32                      `json:"protocol_version"`
	Scope           string                      `json:"scope"`
	IntentSHA256    string                      `json:"intent_sha256"`
	Observed        NormalSplitTopologySnapshot `json:"observed"`
}

// NormalSplitTopologyResult永远不是出生/Normal/VM许可。Observe不确认本次持久性。
type NormalSplitTopologyResult struct {
	ProtocolVersion      uint32                      `json:"protocol_version"`
	Scope                string                      `json:"scope"`
	NormalDecreasePermit bool                        `json:"normal_decrease_permit"`
	DurabilityConfirmed  bool                        `json:"durability_confirmed"`
	IntentSHA256         string                      `json:"intent_sha256"`
	CompletionSHA256     string                      `json:"completion_sha256"`
	Original             normalSplitIntent           `json:"original"`
	Completion           normalSplitCompletion       `json:"completion"`
	Current              NormalSplitTopologySnapshot `json:"current"`
}
type normalSplitKernel interface {
	normalSplitRead() (NormalSplitTopologySnapshot, error)
	normalSplitEnable() error
	normalSplitCreate(string) error
	normalSplitSet(string, control.Uint64, control.Uint64) error
}
type normalSplitOperation uint8

const (
	normalSplitPrepare normalSplitOperation = iota
	normalSplitObserve
	normalSplitReplay
)

type normalSplitLeafIdentity struct {
	ProtocolVersion uint32         `json:"protocol_version"`
	IntentSHA256    string         `json:"intent_sha256"`
	Name            string         `json:"name"`
	Device          control.Uint64 `json:"device"`
	Inode           control.Uint64 `json:"inode"`
}

func splitIdentityName(name string) string { return "normal-split-topology." + name + ".identity.json" }
func splitSameNode(a, b normalSplitNode) bool {
	return a.Device == b.Device && a.Inode == b.Inode && a.RootControlled && b.RootControlled
}
func splitBasic(s NormalSplitTopologySnapshot) error {
	if s.Identity.Validate() != nil || s.Parent.Device == 0 || s.Parent.Inode == 0 || !s.Parent.RootControlled || s.StateDevice == 0 || s.StateInode == 0 ||
		s.Parent.Device != s.Identity.CgroupDevice || s.Parent.Inode != s.Identity.CgroupInode || s.Parent.Controllers != "" && s.Parent.Controllers != "memory pids" {
		return fmt.Errorf("split parent identity/controller/state source missing")
	}
	return nil
}
func splitStable(a, b NormalSplitTopologySnapshot) error {
	if splitBasic(b) != nil || a.Identity != b.Identity || !splitSameNode(a.Parent, b.Parent) || a.StateDevice != b.StateDevice || a.StateInode != b.StateInode {
		return fmt.Errorf("split original parent/state identity drift")
	}
	return nil
}
func splitEmpty(s NormalSplitTopologySnapshot) error {
	if !s.Parent.Empty || s.Parent.MemoryCurrentBytes != 0 || s.Parent.SwapCurrentBytes != 0 {
		return fmt.Errorf("split preparation requires recursively empty uncharged PG parent")
	}
	for _, n := range []*normalSplitNode{s.Private, s.Arrays} {
		if n != nil && (!n.Empty || n.MemoryCurrentBytes != 0 || n.SwapCurrentBytes != 0) {
			return fmt.Errorf("split preparation found existing leaf tasks/charges")
		}
	}
	return nil
}
func splitPlaced(p NormalSplitTopologyPolicy, s NormalSplitTopologySnapshot) error {
	if splitBasic(s) != nil || s.Parent.Controllers != "memory pids" || s.Private == nil || s.Arrays == nil {
		return fmt.Errorf("split exact two-leaf topology incomplete")
	}
	for j, n := range []*normalSplitNode{s.Private, s.Arrays} {
		if n.Device != s.Parent.Device || n.Inode == 0 || !n.RootControlled || n.Controllers != "" || !n.MemoryHighUnlimited || n.SwapMaxBytes != 0 || n.MemoryMaxBytes == 0 || uint64(n.MemoryMaxBytes) >= math.MaxInt64 || uint64(n.MemoryMaxBytes)%(64<<10) != 0 {
			return fmt.Errorf("split leaf bound/control differs")
		}
		if j == 0 && (n.MemoryMaxBytes != p.PrivateMemoryMaxBytes || n.PidsMaxTasks != p.PrivatePidsMaxTasks) {
			return fmt.Errorf("split original private P/pids changed")
		}
		if j == 1 && n.PidsMaxTasks != 1 {
			return fmt.Errorf("split arrays pids must remain one")
		}
	}
	if s.Private.Inode == s.Arrays.Inode || s.Parent.Inode == s.Private.Inode || s.Parent.Inode == s.Arrays.Inode {
		return fmt.Errorf("split cgroup identities alias")
	}
	return nil
}
func (g *Guard) normalSplit(ctx context.Context, in normalSplitInputs, op normalSplitOperation) (NormalSplitTopologyResult, error) {
	if err := in.validate(g.config); err != nil {
		return NormalSplitTopologyResult{}, err
	}
	s, err := lockState(ctx, g.config.StateDir)
	if err != nil {
		return NormalSplitTopologyResult{}, err
	}
	defer s.close()
	return g.normalSplitLocked(ctx, in, op, s)
}

// 所有写入在同原resource flock内完成；不跨PG RPC、不触旧state.json，不删除任何组。
func (g *Guard) normalSplitLocked(ctx context.Context, in normalSplitInputs, op normalSplitOperation, s *lockedState) (NormalSplitTopologyResult, error) {
	var zero NormalSplitTopologyResult
	unknown := func(err error) (NormalSplitTopologyResult, error) {
		return zero, fmt.Errorf("%w: split topology: %v", ErrUnknown, err)
	}
	if err := in.validate(g.config); err != nil {
		return zero, err
	}
	k, ok := g.kernel.(normalSplitKernel)
	if !ok {
		return zero, fmt.Errorf("split kernel backend unavailable")
	}
	physical, ok := g.kernel.(physicalOriginKernel)
	if !ok {
		return zero, fmt.Errorf("split original physical backend unavailable")
	}
	now, err := physical.physicalOriginRead()
	if err != nil {
		return unknown(err)
	}
	if err := in.origin.First.Match(now); err != nil {
		return unknown(err)
	}
	rawOrigin, exists, err := s.readPhysicalFile(physicalOriginDataName)
	if err != nil || !exists || physicalDigest(rawOrigin) != in.originSHA {
		return unknown(fmt.Errorf("original physical data unavailable"))
	}
	confirmation, exists, err := s.readPhysicalFile(physicalOriginConfirmName)
	var pc physicalOriginConfirmation
	if err != nil || !exists || control.UnmarshalObject(confirmation, &pc) != nil || pc.ProtocolVersion != 1 || pc.OriginSHA256 != in.originSHA {
		return unknown(fmt.Errorf("original physical confirmation unavailable"))
	}
	history, err := s.load()
	if err != nil {
		return unknown(err)
	}
	raw, hasIntent, err := s.readPhysicalFile(normalSplitIntentName)
	if err != nil {
		return unknown(err)
	}
	done, hasComplete, err := s.readPhysicalFile(normalSplitCompleteName)
	if err != nil {
		return unknown(err)
	}
	current, err := k.normalSplitRead()
	if err != nil {
		return unknown(err)
	}
	if err := splitBasic(current); err != nil {
		return unknown(err)
	}
	if current.Identity != in.origin.First.Identity {
		return unknown(fmt.Errorf("parent differs from original physical identity"))
	}
	// state目录FD本身必须是当前snapshot的同一个持久目录，防误换挂载或模型注入。
	if err := normalSplitLockedDirectory(s, current); err != nil {
		return unknown(err)
	}
	var intent normalSplitIntent
	if !hasIntent {
		if hasComplete || op != normalSplitPrepare || history != nil || current.Private != nil || current.Arrays != nil || current.Parent.Controllers != "" {
			return unknown(fmt.Errorf("historical state/topology cannot acquire new split intent"))
		}
		for _, name := range []string{"private", "arrays"} {
			_, yes, e := s.readPhysicalFile(splitIdentityName(name))
			if e != nil || yes {
				return unknown(fmt.Errorf("orphan split identity WAL"))
			}
		}
		if err := splitEmpty(current); err != nil {
			return zero, err
		}
		intent = normalSplitIntent{ProtocolVersion: 1, Scope: normalSplitScope, Policy: in.policy, PolicySHA256: physicalDigest(in.policyRaw), OriginSHA256: in.originSHA, Original: current}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return zero, err
		}
		intent.Nonce = hex.EncodeToString(nonce[:])
		raw, err = json.Marshal(intent)
		if err != nil {
			return zero, err
		}
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if err := s.createPhysicalFile(normalSplitIntentName, raw); err != nil {
			return unknown(err)
		}
	} else {
		if control.UnmarshalObject(raw, &intent) != nil {
			return unknown(fmt.Errorf("malformed original intent"))
		}
		canonical, e := json.Marshal(intent)
		if e != nil || string(canonical) != string(raw) || intent.ProtocolVersion != 1 || intent.Scope != normalSplitScope || control.ValidateIncarnation(intent.Nonce) != nil || intent.Nonce == "00000000000000000000000000000000" || intent.Policy != in.policy || intent.PolicySHA256 != physicalDigest(in.policyRaw) || intent.OriginSHA256 != in.originSHA || splitBasic(intent.Original) != nil || intent.Original.Private != nil || intent.Original.Arrays != nil || intent.Original.Parent.Controllers != "" || splitEmpty(intent.Original) != nil {
			return unknown(fmt.Errorf("original intent binding differs"))
		}
	}
	if err := splitStable(intent.Original, current); err != nil {
		return unknown(err)
	}
	if hasComplete {
		var complete normalSplitCompletion
		if control.UnmarshalObject(done, &complete) != nil {
			return unknown(fmt.Errorf("malformed completion"))
		}
		canonical, e := json.Marshal(complete)
		if e != nil || string(canonical) != string(done) || complete.ProtocolVersion != 1 || complete.Scope != normalSplitScope || complete.IntentSHA256 != physicalDigest(raw) || splitStable(intent.Original, complete.Observed) != nil || splitEmpty(complete.Observed) != nil || splitPlaced(in.policy, complete.Observed) != nil || complete.Observed.Arrays.MemoryMaxBytes != in.policy.InitialArraysMemoryMaxBytes {
			return unknown(fmt.Errorf("original split completion differs"))
		}
		if err := splitPlaced(in.policy, current); err != nil {
			return unknown(err)
		}
		if !splitSameNode(*complete.Observed.Private, *current.Private) || !splitSameNode(*complete.Observed.Arrays, *current.Arrays) {
			return unknown(fmt.Errorf("completed split leaf identity drift"))
		}
		for _, name := range []string{"private", "arrays"} {
			n := current.Private
			if name == "arrays" {
				n = current.Arrays
			}
			if err := splitCheckIdentity(s, name, n, physicalDigest(raw)); err != nil {
				return unknown(err)
			}
		}
		// 已有resource日志必须仍属于原VM/Source；观察不替旧bootstrap补签。
		if history != nil && (history.validate(in.origin.First.Identity) != nil || history.Bootstrap == nil || sameBootstrapSource(in.source, history.Bootstrap.Original) != nil) {
			return unknown(fmt.Errorf("later resource history differs"))
		}
		durable := op != normalSplitObserve
		if durable {
			for _, name := range []string{normalSplitIntentName, splitIdentityName("private"), splitIdentityName("arrays"), normalSplitCompleteName} {
				if err := s.syncPhysicalFile(name); err != nil {
					return unknown(err)
				}
			}
		}
		return NormalSplitTopologyResult{ProtocolVersion: 1, Scope: normalSplitScope, DurabilityConfirmed: durable, IntentSHA256: physicalDigest(raw), CompletionSHA256: physicalDigest(done), Original: intent, Completion: complete, Current: current}, nil
	}
	if hasIntent && op != normalSplitReplay {
		return unknown(fmt.Errorf("pending split requires explicit same-original Replay"))
	}
	if history != nil {
		return unknown(fmt.Errorf("cannot complete topology after resource Birth/bootstrap"))
	}
	if err := splitEmpty(current); err != nil {
		return unknown(err)
	}
	if err := ctx.Err(); err != nil {
		return unknown(err)
	}
	if err := s.syncPhysicalFile(normalSplitIntentName); err != nil {
		return unknown(err)
	}
	if err := k.normalSplitEnable(); err != nil {
		return unknown(err)
	}
	for _, name := range []string{"private", "arrays"} {
		current, err = k.normalSplitRead()
		if err != nil {
			return unknown(err)
		}
		if splitStable(intent.Original, current) != nil || splitEmpty(current) != nil {
			return unknown(fmt.Errorf("parent changed during preparation"))
		}
		node := current.Private
		if name == "arrays" {
			node = current.Arrays
		}
		// mkdir成功、identity WAL之前崩溃的现存组没有原inode凭据；Replay必须停在Unknown。
		if node != nil {
			if err := splitCheckIdentity(s, name, node, physicalDigest(raw)); err != nil {
				return unknown(err)
			}
		} else {
			_, yes, e := s.readPhysicalFile(splitIdentityName(name))
			if e != nil || yes {
				return unknown(fmt.Errorf("missing original leaf cannot be recreated"))
			}
			if err := k.normalSplitCreate(name); err != nil {
				return unknown(err)
			}
			current, err = k.normalSplitRead()
			if err != nil {
				return unknown(err)
			}
			if splitStable(intent.Original, current) != nil || splitEmpty(current) != nil {
				return unknown(fmt.Errorf("new leaf parent changed"))
			}
			node = current.Private
			if name == "arrays" {
				node = current.Arrays
			}
			if node == nil || !node.RootControlled || node.Device != current.Parent.Device || node.Inode == 0 || node.Controllers != "" {
				return unknown(fmt.Errorf("new root-controlled leaf unavailable"))
			}
			identity := normalSplitLeafIdentity{ProtocolVersion: 1, IntentSHA256: physicalDigest(raw), Name: name, Device: node.Device, Inode: node.Inode}
			b, e := json.Marshal(identity)
			if e != nil {
				return unknown(e)
			}
			if err := s.createPhysicalFile(splitIdentityName(name), b); err != nil {
				return unknown(err)
			}
		}
		// 旧identity可能上轮已完整可见但file/dir fsync失败。canonical读成功不
		// 等于原inode来源已持久，必须先重同步原文件，才施加任何后继cap。
		if err := s.syncPhysicalFile(splitIdentityName(name)); err != nil {
			return unknown(err)
		}
		cap, pids := in.policy.PrivateMemoryMaxBytes, in.policy.PrivatePidsMaxTasks
		if name == "arrays" {
			cap, pids = in.policy.InitialArraysMemoryMaxBytes, 1
		}
		if err := k.normalSplitSet(name, cap, pids); err != nil {
			return unknown(err)
		}
	}
	current, err = k.normalSplitRead()
	if err != nil {
		return unknown(err)
	}
	if err := normalSplitLockedDirectory(s, current); err != nil {
		return unknown(err)
	}
	if splitStable(intent.Original, current) != nil || splitEmpty(current) != nil || splitPlaced(in.policy, current) != nil || current.Arrays.MemoryMaxBytes != in.policy.InitialArraysMemoryMaxBytes {
		return unknown(fmt.Errorf("split final readback differs"))
	}
	for _, name := range []string{"private", "arrays"} {
		n := current.Private
		if name == "arrays" {
			n = current.Arrays
		}
		if err := splitCheckIdentity(s, name, n, physicalDigest(raw)); err != nil {
			return unknown(err)
		}
	}
	complete := normalSplitCompletion{ProtocolVersion: 1, Scope: normalSplitScope, IntentSHA256: physicalDigest(raw), Observed: current}
	done, err = json.Marshal(complete)
	if err != nil {
		return unknown(err)
	}
	if err := s.createPhysicalFile(normalSplitCompleteName, done); err != nil {
		return unknown(err)
	}
	return NormalSplitTopologyResult{ProtocolVersion: 1, Scope: normalSplitScope, DurabilityConfirmed: true, IntentSHA256: physicalDigest(raw), CompletionSHA256: physicalDigest(done), Original: intent, Completion: complete, Current: current}, nil
}
func splitCheckIdentity(s *lockedState, name string, n *normalSplitNode, intentSHA string) error {
	raw, yes, err := s.readPhysicalFile(splitIdentityName(name))
	var id normalSplitLeafIdentity
	if err != nil || !yes || control.UnmarshalObject(raw, &id) != nil {
		return fmt.Errorf("unrecorded/malformed split leaf identity")
	}
	canonical, e := json.Marshal(id)
	if e != nil || string(raw) != string(canonical) || id.ProtocolVersion != 1 || id.IntentSHA256 != intentSHA || id.Name != name || n == nil || id.Device != n.Device || id.Inode != n.Inode {
		return fmt.Errorf("original split leaf identity differs")
	}
	return nil
}
