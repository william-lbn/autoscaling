//go:build linux

package resourceguard

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

// RPC级意图与helper四链分别持久。helper退出不等于PG已经发布H/gen；本记录
// 只允许相同原报文回读原结果，决不重跑未确认的populate，也不签normal许可。
type arrayRPCIntent struct {
	ProtocolVersion   uint32         `json:"protocol_version"`
	Scope             string         `json:"scope"`
	Nonce             string         `json:"nonce"`
	Birth             NativeBirthRef `json:"birth"`
	RegistrationNonce string         `json:"registration_nonce"`
	RequestHex        string         `json:"request_hex"`
	RequestSHA256     string         `json:"request_sha256"`
	ResourceFloor     control.Uint64 `json:"resource_floor"`
}
type arrayRPCComplete struct {
	ProtocolVersion uint32   `json:"protocol_version"`
	IntentSHA256    string   `json:"intent_sha256"`
	HelperNonces    []string `json:"helper_nonces"`
}
type arrayRPCWitness struct {
	StateDevice, StateInode, LockDevice, LockInode uint64
	Nonce                                          string
	SHA                                            [2]string
	Files                                          [2]arrayPopulateHistoryIdentity
}
type arrayRPCJournal struct{ durable sync.Map }

func arrayRPCName(key, phase string) string { return "array-rpc." + key + "." + phase + ".json" }
func arrayRPCRead(s *lockedState, key string) (arrayRPCIntent, *arrayRPCComplete, arrayRPCWitness, error) {
	var intent arrayRPCIntent
	var witness arrayRPCWitness
	fail := func(err error) (arrayRPCIntent, *arrayRPCComplete, arrayRPCWitness, error) {
		return intent, nil, witness, errors.Join(ErrUnknown, err)
	}
	state, e := arrayPopulateStateKey(s, key)
	if e != nil {
		return fail(e)
	}
	witness.StateDevice, witness.StateInode, witness.LockDevice, witness.LockInode = state.StateDevice, state.StateInode, state.LockDevice, state.LockInode
	raw, id, e := arrayPopulateHistoryRaw(s, arrayRPCName(key, "pending"))
	if e != nil {
		return fail(e)
	}
	if control.UnmarshalObject(raw, &intent) != nil || intent.ProtocolVersion != 1 || intent.Scope != "original-four-array-rpc-v1" || intent.RequestSHA256 != key || intent.Birth.validate() != nil || intent.ResourceFloor == 0 {
		return fail(fmt.Errorf("array RPC original intent invalid"))
	}
	if _, e = nativeBirthDigest(intent.Nonce); e != nil {
		return fail(e)
	}
	if _, e = nativeBirthDigest(intent.RegistrationNonce); e != nil {
		return fail(e)
	}
	packet, e := hex.DecodeString(intent.RequestHex)
	if e != nil || physicalDigest(packet) != key {
		return fail(fmt.Errorf("array RPC original bytes differ"))
	}
	q, e := decodeArrayRegistryWire(packet)
	if e != nil || q.Op != arrayRegistryPopulate || hex.EncodeToString(q.RegistrationNonce[:]) != intent.RegistrationNonce {
		return fail(fmt.Errorf("array RPC original wire invalid"))
	}
	witness.Nonce = intent.Nonce
	witness.SHA[0] = physicalDigest(raw)
	witness.Files[0] = id
	_, found, e := s.readPhysicalFile(arrayRPCName(key, "complete"))
	if e != nil {
		return fail(e)
	}
	if !found {
		return intent, nil, witness, nil
	}
	raw, id, e = arrayPopulateHistoryRaw(s, arrayRPCName(key, "complete"))
	if e != nil {
		return fail(e)
	}
	var complete arrayRPCComplete
	if control.UnmarshalObject(raw, &complete) != nil || complete.ProtocolVersion != 1 || complete.IntentSHA256 != witness.SHA[0] || len(complete.HelperNonces) == 0 || len(complete.HelperNonces) > 8192 {
		return fail(fmt.Errorf("array RPC completion differs"))
	}
	seen := map[string]bool{}
	for _, nonce := range complete.HelperNonces {
		if _, e = nativeBirthDigest(nonce); e != nil || seen[nonce] {
			return fail(fmt.Errorf("array RPC helper identity invalid"))
		}
		seen[nonce] = true
		helperKey, e := arrayPopulateCompleteKeyLocked(s, nonce)
		if e != nil {
			return fail(e)
		}
		if _, ok := arrayPopulateDurabilityWitnesses.Load(helperKey); !ok {
			return fail(fmt.Errorf("array RPC helper lacks current durability witness"))
		}
	}
	witness.SHA[1] = physicalDigest(raw)
	witness.Files[1] = id
	return intent, &complete, witness, nil
}
func (j *arrayRPCJournal) known(s *lockedState, key string, expected arrayRPCIntent) error {
	intent, done, witness, e := arrayRPCRead(s, key)
	if e != nil || done == nil || intent != expected {
		return errors.Join(ErrUnknown, e, fmt.Errorf("array RPC original completion is unknown"))
	}
	if _, ok := j.durable.Load(witness); !ok {
		return errors.Join(ErrUnknown, fmt.Errorf("array RPC visible completion is not original fsync evidence"))
	}
	return nil
}
func (j *arrayRPCJournal) confirm(s *lockedState, key string) error {
	_, done, before, e := arrayRPCRead(s, key)
	if e != nil || done == nil {
		return errors.Join(ErrUnknown, e)
	}
	for _, phase := range []string{"pending", "complete"} {
		if e = s.syncPhysicalFile(arrayRPCName(key, phase)); e != nil {
			return errors.Join(ErrUnknown, e)
		}
	}
	if e = s.dir.Sync(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	_, done, after, e := arrayRPCRead(s, key)
	if e != nil || done == nil || before != after {
		return errors.Join(ErrUnknown, e, fmt.Errorf("array RPC changed while confirming durability"))
	}
	j.durable.Store(after, struct{}{})
	return nil
}

// 零写门禁。存在任何未知/未确认RPC时，不能以一次current或可见complete洗账。
func (b *arrayGuardBinding) rejectRPCUnknown(s *lockedState) error {
	fd, e := unix.Openat(int(s.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	scan := os.NewFile(uintptr(fd), "array-rpc-scan")
	names, e := scan.Readdirnames(-1)
	scan.Close()
	if e != nil {
		return e
	}
	seen := map[string]bool{}
	for _, name := range names {
		if !strings.HasPrefix(name, "array-rpc.") {
			continue
		}
		parts := strings.Split(name, ".")
		if len(parts) != 4 || !physicalDigestValid(parts[1]) || parts[3] != "json" || parts[2] != "pending" && parts[2] != "complete" {
			return errors.Join(ErrUnknown, fmt.Errorf("foreign array RPC history"))
		}
		key := parts[1]
		if seen[key] {
			continue
		}
		seen[key] = true
		intent, done, witness, e := arrayRPCRead(s, key)
		if e != nil || done == nil || intent.Birth != b.original.birthRef {
			return errors.Join(ErrUnknown, e, fmt.Errorf("array RPC historical prefix unconfirmed"))
		}
		if _, ok := b.rpc.durable.Load(witness); !ok {
			return errors.Join(ErrUnknown, fmt.Errorf("array RPC history lacks current durability witness"))
		}
	}
	return nil
}
func (s *arrayNativeServices) populateAuthorityLocked(ctx context.Context, state *lockedState, r *record, b *arrayGuardBinding, q arrayRegistryWireRequest, peer *unix.Ucred) (string, error) {
	if q.keyZero() {
		if int(peer.Pid) != b.original.pending.Original.PostmasterPID || q.Generation != 1 || q.OriginalHigh != q.StartupBuffers {
			return "", fmt.Errorf("startup populate is not original PM N0")
		}
		return "", nil
	}
	// 报文没有VM/compute来源；只能从Root既有完整签名授权族中找唯一匹配原Key，
	// 然后再次按当前资源/WAL/预算/floor核验，不能从报文拼出新授权。
	fd, e := unix.Openat(int(state.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return "", e
	}
	scan := os.NewFile(uintptr(fd), "array-authority-original")
	names, e := scan.Readdirnames(-1)
	scan.Close()
	if e != nil {
		return "", e
	}
	var found *arrayAuthorityOriginal
	for _, name := range names {
		parts := strings.Split(name, ".")
		if len(parts) != 4 || parts[0] != "array-authority" || parts[2] != "pending" || parts[3] != "json" {
			continue
		}
		intent, done, witness, e := arrayAuthorityReadLocked(state, parts[1])
		if e != nil {
			return "", e
		}
		if arrayAuthorityMatchesPopulate(intent.Original.Command, q) != nil {
			continue
		}
		if done == nil {
			return "", errors.Join(ErrUnknown, fmt.Errorf("original array authority pending"))
		}
		if _, ok := b.journal.durable.Load(witness); !ok {
			return "", errors.Join(ErrUnknown, fmt.Errorf("original authority lacks durability witness"))
		}
		current, e := s.guard.arrayAuthorityOriginalLocked(ctx, state, r, intent.Original.Command, b)
		if e != nil || current != intent.Original {
			return "", errors.Join(ErrUnknown, e, fmt.Errorf("original array authority source changed"))
		}
		if found != nil {
			return "", errors.Join(ErrUnknown, fmt.Errorf("ambiguous original array authority"))
		}
		copy := current
		found = &copy
	}
	if found == nil {
		return "", errors.Join(ErrUnknown, fmt.Errorf("signed original Start command unavailable"))
	}
	return found.Command.Request.ContextSHA256()
}

func (s *arrayNativeServices) populateLocked(ctx context.Context, state *lockedState, r *record, b *arrayGuardBinding, q arrayRegistryWireRequest, raw []byte, peer *unix.Ucred, pidfd, procfd int) error {
	o := b.original
	if err := o.matchesCurrent(q); err != nil {
		return err
	}
	if o.current.retiring || o.failed {
		return errors.Join(ErrUnknown, fmt.Errorf("array generation is retiring or unknown"))
	}
	// 即使backend继承同地址，也核其真实map_files四inode；不把同UID或地址当backing。
	files, ids, e := arrayRegistryOpenMappings(procfd, pidfd, q)
	if e != nil {
		return e
	}
	for _, f := range files {
		defer f.Close()
	}
	if ids != o.current.identities {
		return errors.Join(ErrUnknown, fmt.Errorf("array peer no longer maps registered original four inodes"))
	}
	contextSHA, e := s.populateAuthorityLocked(ctx, state, r, b, q, peer)
	if e != nil {
		return e
	}
	if contextSHA == "" {
		contextSHA = physicalDigest(raw)
	}
	topology, e := s.guard.normalSplitLocked(ctx, s.split, normalSplitObserve, state)
	if e != nil {
		return e
	}
	if topology.Current.Private == nil || topology.Current.Arrays == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("original split topology missing"))
	}
	p := o.pending.Source.Policy
	a := topology.Current.Arrays
	system, e := splitReadNode(systemCgroup)
	if e != nil {
		return e
	}
	limits := r.Bootstrap.Original.Limits
	if r.Held != nil {
		limits = r.Held.limits()
	}
	if system.MemoryMaxBytes != limits.SystemMemoryMaxBytes || !system.RootControlled || system.SwapMaxBytes != 0 || !system.MemoryHighUnlimited {
		return errors.Join(ErrUnknown, fmt.Errorf("original System cap drifted"))
	}
	base := arrayPopulateContext{Original: o.pending.Original, PGBootID: control.Uint64(q.PGBoot), ResourceFloor: r.Floor, OriginalContextSHA256: contextSHA, HelperSHA256: p.ArrayHelperELFSHA256, MemlockUpperBytes: p.ArrayHelperMemlockUpperBytes, AddressSpaceMaxBytes: p.ArrayHelperAddressSpaceMaxBytes, ArraysDevice: a.Device, ArraysInode: a.Inode, ArraysMaxBytes: a.MemoryMaxBytes, HelperReserveBytes: p.ArrayHelperReserveBytes, SystemDevice: system.Device, SystemInode: system.Inode, SystemMaxBytes: system.MemoryMaxBytes, Files: ids}
	key := physicalDigest(raw)
	expected := arrayRPCIntent{ProtocolVersion: 1, Scope: "original-four-array-rpc-v1", Birth: o.birthRef, RegistrationNonce: hex.EncodeToString(o.current.nonce[:]), RequestHex: hex.EncodeToString(raw), RequestSHA256: key, ResourceFloor: r.Floor}
	if _, yes, e := state.readPhysicalFile(arrayRPCName(key, "pending")); e != nil {
		return e
	} else if yes {
		prior, _, _, e := arrayRPCRead(state, key)
		if e != nil {
			return e
		}
		expected.Nonce = prior.Nonce
		if e = b.rpc.known(state, key, expected); e != nil {
			return e
		}
		if q.keyZero() {
			o.current.startup[q.Array] = true
		}
		return nil // 只回读原持久结果，不重新执行helper。
	}
	if e = b.rejectRPCUnknown(state); e != nil {
		return e
	}
	if e = rejectOutstandingArrayPopulateLocked(state); e != nil {
		return e
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	nonce, e := newNativeBirthNonce()
	if e != nil {
		return e
	}
	expected.Nonce = nonce
	intentRaw, e := json.Marshal(expected)
	if e != nil {
		return e
	}
	if e = state.createPhysicalFile(arrayRPCName(key, "pending"), intentRaw); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	// 每个chunk由同一原helper AS硬限覆盖，有限cap不从RSS调大。若实际静态
	// 私有映射无法容纳，本次helper失败且RPC保Unknown；绝不改AS或退回PG自触。
	chunk := uint64(base.AddressSpaceMaxBytes-base.MemlockUpperBytes) &^ uint64(4095)
	if base.HelperReserveBytes >= base.ArraysMaxBytes {
		return errors.Join(ErrUnknown, fmt.Errorf("original A has no helper payload allowance"))
	}
	available := uint64(base.ArraysMaxBytes-base.HelperReserveBytes) &^ uint64(4095)
	if chunk > available {
		chunk = available
	}
	if chunk == 0 {
		return errors.Join(ErrUnknown, fmt.Errorf("original helper AS has no array span"))
	}
	complete := arrayRPCComplete{ProtocolVersion: 1, IntentSHA256: physicalDigest(intentRaw)}
	for offset, left := q.Offset, q.Bytes; left > 0; {
		length := left
		if length > chunk {
			length = chunk
		}
		one := base
		one.Files[q.Array].Offset = control.Uint64(offset)
		one.Files[q.Array].Length = control.Uint64(length)
		result, e := runNativeArrayPopulate(ctx, state, one, o.current.files, nil)
		if e != nil || !result.Completed || !result.ExitObserved || result.NormalAuthorized {
			return errors.Join(ErrUnknown, e, fmt.Errorf("original array helper did not durably finish"))
		}
		complete.HelperNonces = append(complete.HelperNonces, result.Nonce)
		if len(complete.HelperNonces) > 8192 {
			return errors.Join(ErrUnknown, fmt.Errorf("array RPC helper count exceeded bounded history"))
		}
		offset += length
		left -= length
	}
	doneRaw, e := json.Marshal(complete)
	if e != nil {
		return e
	}
	if e = state.createPhysicalFile(arrayRPCName(key, "complete"), doneRaw); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if e = b.rpc.confirm(state, key); e != nil {
		return e
	}
	if q.keyZero() {
		o.current.startup[q.Array] = true
	}
	return arrayRegistryPIDFDAlive(pidfd)
}
