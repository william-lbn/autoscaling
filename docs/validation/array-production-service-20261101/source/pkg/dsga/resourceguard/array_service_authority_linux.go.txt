//go:build linux

package resourceguard

import (
	"bytes"
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

const arrayAuthorityWALScope = "original-array-authority-v1"

type arrayAuthorityOriginal struct {
	Command           ArrayAuthorityCommand `json:"command"`
	Birth             NativeBirthRef        `json:"birth"`
	ReleasedSHA256    string                `json:"released_sha256"`
	RegistrationNonce string                `json:"registration_nonce"`
	MappingGeneration control.Uint64        `json:"mapping_generation"`
}

func (b arrayAuthorityOriginal) validate() error {
	if b.Command.Validate() != nil || b.Birth.validate() != nil || b.MappingGeneration == 0 {
		return fmt.Errorf("array authority original source incomplete")
	}
	for _, digest := range []string{b.ReleasedSHA256, b.RegistrationNonce} {
		if _, err := nativeBirthDigest(digest); err != nil {
			return err
		}
	}
	return nil
}

type arrayAuthorityIntent struct {
	ProtocolVersion uint32                 `json:"protocol_version"`
	Scope           string                 `json:"scope"`
	Nonce           string                 `json:"nonce"`
	Original        arrayAuthorityOriginal `json:"original"`
}
type arrayAuthorityComplete struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Scope           string `json:"scope"`
	Nonce           string `json:"nonce"`
	IntentSHA256    string `json:"intent_sha256"`
	CommandSHA256   string `json:"command_sha256"`
}
type arrayAuthorityWitness struct {
	StateDevice, StateInode, LockDevice, LockInode uint64
	Nonce                                          string
	SHA                                            [2]string
	Files                                          [2]arrayPopulateHistoryIdentity
}

// 每个原service实例私有；Root重建不会从磁盘可见complete推导fsync成功。
type arrayAuthorityJournal struct{ durable sync.Map }
type arrayAuthorityIO struct {
	create   func(*lockedState, string, []byte) error
	syncFile func(*lockedState, string) error
	syncDir  func(*lockedState) error
}

func arrayAuthorityDiskIO() arrayAuthorityIO {
	return arrayAuthorityIO{func(s *lockedState, n string, b []byte) error { return s.createPhysicalFile(n, b) }, func(s *lockedState, n string) error { return s.syncPhysicalFile(n) }, func(s *lockedState) error { return s.dir.Sync() }}
}
func arrayAuthorityKey(c ArrayAuthorityCommand) string {
	raw, _ := json.Marshal(c.Request.Key)
	return physicalDigest(raw)
}
func arrayAuthorityName(key, phase string) string {
	return "array-authority." + key + "." + phase + ".json"
}
func arrayAuthorityEvidence(c ArrayAuthorityCommand) ArrayAuthorityEvidence {
	raw, _ := json.Marshal(c)
	return ArrayAuthorityEvidence{ProtocolVersion: 1, Command: c, CommandSHA256: physicalDigest(raw), ResourceFloor: c.IssuerEpoch, Recorded: true}
}

func arrayAuthorityReadLocked(s *lockedState, key string) (arrayAuthorityIntent, *arrayAuthorityComplete, arrayAuthorityWitness, error) {
	var intent arrayAuthorityIntent
	var witness arrayAuthorityWitness
	unknown := func(err error) (arrayAuthorityIntent, *arrayAuthorityComplete, arrayAuthorityWitness, error) {
		return intent, nil, witness, errors.Join(ErrUnknown, err)
	}
	if s == nil || s.dir == nil || s.lock == nil || !physicalDigestValid(key) {
		return unknown(fmt.Errorf("array authority original state/key missing"))
	}
	var dir, lock unix.Stat_t
	if unix.Fstat(int(s.dir.Fd()), &dir) != nil || unix.Fstat(int(s.lock.Fd()), &lock) != nil || privateRegular(int(s.dir.Fd()), unix.S_IFDIR) != nil || privateRegular(int(s.lock.Fd()), unix.S_IFREG) != nil || dir.Dev != lock.Dev {
		return unknown(fmt.Errorf("array authority original state identity differs"))
	}
	witness.StateDevice, witness.StateInode, witness.LockDevice, witness.LockInode = dir.Dev, dir.Ino, lock.Dev, lock.Ino
	raw, id, err := arrayPopulateHistoryRaw(s, arrayAuthorityName(key, "pending"))
	if err != nil {
		return unknown(err)
	}
	if control.UnmarshalObject(raw, &intent) != nil || intent.ProtocolVersion != 1 || intent.Scope != arrayAuthorityWALScope || intent.Original.validate() != nil || arrayAuthorityKey(intent.Original.Command) != key {
		return unknown(fmt.Errorf("array authority original pending invalid"))
	}
	if _, err = nativeBirthDigest(intent.Nonce); err != nil {
		return unknown(err)
	}
	witness.Nonce = intent.Nonce
	witness.SHA[0] = physicalDigest(raw)
	witness.Files[0] = id
	_, found, err := s.readPhysicalFile(arrayAuthorityName(key, "complete"))
	if err != nil {
		return unknown(err)
	}
	if !found {
		return intent, nil, witness, nil
	}
	raw, id, err = arrayPopulateHistoryRaw(s, arrayAuthorityName(key, "complete"))
	if err != nil {
		return unknown(err)
	}
	var done arrayAuthorityComplete
	commandRaw, _ := json.Marshal(intent.Original.Command)
	if control.UnmarshalObject(raw, &done) != nil || done.ProtocolVersion != 1 || done.Scope != arrayAuthorityWALScope || done.Nonce != intent.Nonce || done.IntentSHA256 != witness.SHA[0] || done.CommandSHA256 != physicalDigest(commandRaw) {
		return unknown(fmt.Errorf("array authority completion differs from original"))
	}
	witness.SHA[1] = physicalDigest(raw)
	witness.Files[1] = id
	return intent, &done, witness, nil
}
func (j *arrayAuthorityJournal) confirmLocked(s *lockedState, key string, disk arrayAuthorityIO) error {
	_, done, before, err := arrayAuthorityReadLocked(s, key)
	if err != nil || done == nil {
		return errors.Join(ErrUnknown, err, fmt.Errorf("array authority full original prefix missing"))
	}
	j.durable.Delete(before)
	if disk.syncFile == nil || disk.syncDir == nil {
		return errors.Join(ErrUnknown, fmt.Errorf("array authority durability source missing"))
	}
	for _, phase := range []string{"pending", "complete"} {
		if err = disk.syncFile(s, arrayAuthorityName(key, phase)); err != nil {
			return errors.Join(ErrUnknown, err)
		}
	}
	if err = disk.syncDir(s); err != nil {
		return errors.Join(ErrUnknown, err)
	}
	_, afterDone, after, err := arrayAuthorityReadLocked(s, key)
	if err != nil || afterDone == nil || before != after {
		return errors.Join(ErrUnknown, err, fmt.Errorf("array authority original prefix changed during fsync"))
	}
	j.durable.Store(after, struct{}{})
	return nil
}

// mode authorize/observe/replay；调用者先在同原resource flock内验证真实原Birth、
// Registry及当前held budget。这里仅持久化数组动作原权限，不写任何cap或启动helper。
// 普通Observe零写；看见complete而没有本实例durability见证依然Unknown。
func (j *arrayAuthorityJournal) applyLocked(ctx context.Context, s *lockedState, bound arrayAuthorityOriginal, mode string, disk arrayAuthorityIO) (ArrayAuthorityEvidence, error) {
	var zero ArrayAuthorityEvidence
	if ctx == nil || ctx.Err() != nil || bound.validate() != nil {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority original source unavailable"))
	}
	if mode != "authorize" && mode != "observe" && mode != "replay" {
		return zero, fmt.Errorf("array authority action unsupported")
	}
	key := arrayAuthorityKey(bound.Command)
	// 全族完整性/顺序检查：未知文件或另一未结清pending不允许洗成新授权。
	scanfd, err := unix.Openat(int(s.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return zero, err
	}
	scan := os.NewFile(uintptr(scanfd), "array-authority-scan")
	names, err := scan.Readdirnames(-1)
	_ = scan.Close()
	if err != nil {
		return zero, err
	}
	exists := false
	for _, name := range names {
		if !strings.HasPrefix(name, "array-authority.") {
			continue
		}
		fields := strings.Split(name, ".")
		if len(fields) != 4 || fields[0] != "array-authority" || len(fields[1]) != 64 || fields[3] != "json" || fields[2] != "pending" && fields[2] != "complete" {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority foreign history"))
		}
		historicalKey := fields[1]
		if !physicalDigestValid(historicalKey) {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority invalid historical key"))
		}
		if historicalKey == key {
			exists = true
			continue
		}
		previous, complete, witness, e := arrayAuthorityReadLocked(s, historicalKey)
		if e != nil || complete == nil {
			return zero, errors.Join(ErrUnknown, e, fmt.Errorf("another array authority is unconfirmed"))
		}
		if _, durable := j.durable.Load(witness); !durable {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("historical array authority lacks current durability witness"))
		}
		a, b := previous.Original.Command.Request.Key, bound.Command.Request.Key
		if a.Instance == b.Instance && (a.IssuerEpoch > b.IssuerEpoch || a.IssuerEpoch == b.IssuerEpoch && a.Revision >= b.Revision) {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority original order regressed"))
		}
	}
	if !exists {
		if mode == "observe" {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority original was not recorded"))
		}
		nonce, e := newNativeBirthNonce()
		if e != nil {
			return zero, e
		}
		intent := arrayAuthorityIntent{1, arrayAuthorityWALScope, nonce, bound}
		raw, e := json.Marshal(intent)
		if e != nil {
			return zero, e
		}
		if disk.create == nil {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority persistence source missing"))
		}
		if e = disk.create(s, arrayAuthorityName(key, "pending"), raw); e != nil {
			return zero, errors.Join(ErrUnknown, e)
		}
	}
	intent, complete, witness, err := arrayAuthorityReadLocked(s, key)
	if err != nil {
		return zero, err
	}
	if intent.Original != bound {
		return zero, errors.Join(ErrUnknown, fmt.Errorf("same array Key changed original source/bytes"))
	}
	if complete != nil {
		if _, ok := j.durable.Load(witness); ok {
			return arrayAuthorityEvidence(bound.Command), nil
		}
		if mode != "replay" {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("array complete visibility is not durability"))
		}
	} else {
		if mode == "observe" || exists && mode != "replay" {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority original pending needs explicit replay"))
		}
		commandRaw, _ := json.Marshal(bound.Command)
		entry := arrayAuthorityComplete{1, arrayAuthorityWALScope, intent.Nonce, witness.SHA[0], physicalDigest(commandRaw)}
		raw, e := json.Marshal(entry)
		if e != nil {
			return zero, e
		}
		if disk.create == nil {
			return zero, errors.Join(ErrUnknown, fmt.Errorf("array authority persistence source missing"))
		}
		if e = disk.create(s, arrayAuthorityName(key, "complete"), raw); e != nil {
			return zero, errors.Join(ErrUnknown, e)
		}
	}
	if err = j.confirmLocked(s, key, disk); err != nil {
		return zero, err
	}
	return arrayAuthorityEvidence(bound.Command), nil
}

// signed Key与native raw Key/context逐项精确对照；packet不能提升original权限。
func arrayAuthorityMatchesPopulate(c ArrayAuthorityCommand, r arrayRegistryWireRequest) error {
	hash, err := c.Request.ContextSHA256()
	if err != nil {
		return err
	}
	contextHash, err := hex.DecodeString(hash)
	if err != nil {
		return err
	}
	if r.Op != arrayRegistryPopulate || r.PGBoot != uint64(c.Request.Key.Instance.PGBootID) || r.IssuerEpoch != uint64(c.IssuerEpoch) || r.Revision != uint64(c.Request.Key.Revision) || r.OperationHigh != uint64(c.Request.Key.OperationID.High) || r.OperationLow != uint64(c.Request.Key.OperationID.Low) || r.Generation != uint64(c.Request.ExpectedCapacityGeneration) || r.TargetBuffers != uint64(c.Request.TargetBuffers) || c.Request.Kind != control.Grow || !bytes.Equal(r.ContextSHA[:], contextHash) {
		return fmt.Errorf("native populate differs from original signed journal command")
	}
	return nil
}
