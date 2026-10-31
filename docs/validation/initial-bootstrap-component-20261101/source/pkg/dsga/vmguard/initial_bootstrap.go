//go:build linux

package vmguard

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"golang.org/x/sys/unix"
)

var errInitialBootUnavailable = errors.New("initial bootstrap requires the fixed real Guest/process consumer; ordinary memory operations remain closed")

const initialBootScope = "neon-initial-base-bootstrap-v2-non-authorizing"

var initialBootStages = [...]string{"source_frozen", "base_observed", "guest_prepared", "grow_prepared", "grow_may_have_sent", "actual_target_observed", "guest_movable_observed", "complete_non_authorizing"}

// initialBootRef不是Birth/Publication或预算。旧v1仍以Min为启动base；
// 本引用只固定新的物理base与业务Min，不得隐式安装到旧Tick/InitialCapacity。
type initialBootRef struct {
	ProtocolVersion uint32 `json:"protocol_version"`
	Scope           string `json:"scope"`
	OriginalSHA256  string `json:"original_sha256"`
	Nonce           string `json:"nonce"`
	Stage           string `json:"stage"`
	TailSHA256      string `json:"tail_sha256"`
}
type initialBootEntry struct {
	ProtocolVersion uint32            `json:"protocol_version"`
	Scope           string            `json:"scope"`
	Sequence        uint32            `json:"sequence"`
	Stage           string            `json:"stage"`
	Source          initialBootSource `json:"source"`
	SourceSHA256    string            `json:"source_sha256"`
	Nonce           string            `json:"nonce"`
	PreviousSHA256  string            `json:"previous_sha256"`
	Evidence        json.RawMessage   `json:"evidence"`
}

func initialBootSHA(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func initialBootNonce(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func initialBootCanonical(v any) []byte { b, _ := json.Marshal(v); return b }
func initialBootFile(i int) string      { return fmt.Sprintf("initial-boot-v2-%02d.json", i) }

// reader/append只接受持有原Runner flock的lockedState；所有阶段是非授权原文。
// 链可描述已发生事实，但缺少真实consumer时不能用complete日志开启旧协议。
func initialBootRead(s *lockedState) ([]initialBootEntry, error) {
	if s == nil || s.dir == nil {
		return nil, fmt.Errorf("original locked state missing")
	}
	fd, e := unix.Openat(int(s.dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), "initial-stage-directory")
	names, e := f.Readdirnames(2049)
	f.Close()
	if e != nil && e != io.EOF {
		return nil, e
	}
	if len(names) > 2048 {
		return nil, fmt.Errorf("original state directory exceeds scan bound")
	}
	known := map[string]bool{}
	for i := range initialBootStages {
		known[initialBootFile(i)] = true
	}
	for _, n := range names {
		if strings.HasPrefix(n, "initial-boot-v2-") && !known[n] {
			return nil, fmt.Errorf("unknown initial stage file")
		}
	}
	var entries []initialBootEntry
	gap := false
	for i, stage := range initialBootStages {
		raw, e := initialBootReadFile(s, initialBootFile(i))
		if e != nil {
			return nil, e
		}
		if raw == nil {
			gap = true
			continue
		}
		if gap {
			return nil, fmt.Errorf("initial stage chain has a gap")
		}
		var v initialBootEntry
		if control.UnmarshalObject(raw, &v) != nil || !bytes.Equal(raw, initialBootCanonical(v)) || v.ProtocolVersion != 2 || v.Scope != initialBootScope || v.Sequence != uint32(i+1) || v.Stage != stage || v.Source.validate() != nil || !initialBootNonce(v.Nonce) || v.SourceSHA256 != initialBootSHA(initialBootCanonical(v.Source)) || control.ValidateJSON(v.Evidence, 8192, 512) != nil {
			return nil, fmt.Errorf("initial stage body differs")
		}
		var evidence map[string]json.RawMessage
		if json.Unmarshal(v.Evidence, &evidence) != nil || evidence == nil || !bytes.Equal(v.Evidence, initialBootCanonical(evidence)) {
			return nil, fmt.Errorf("initial evidence must be canonical object")
		}
		if i == 0 {
			if v.PreviousSHA256 != "" || !bytes.Equal(v.Evidence, []byte("{}")) {
				return nil, fmt.Errorf("initial source stage has foreign evidence")
			}
		} else {
			prev := entries[i-1]
			if v.SourceSHA256 != prev.SourceSHA256 || v.Nonce != prev.Nonce || v.PreviousSHA256 != initialBootSHA(initialBootCanonical(prev)) {
				return nil, fmt.Errorf("initial stage source/nonce/chain changed")
			}
		}
		entries = append(entries, v)
	}
	return entries, nil
}
func initialBootReadFile(s *lockedState, name string) ([]byte, error) {
	fd, e := unix.Openat(int(s.dir.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(e, unix.ENOENT) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	if e = privateFile(fd, unix.S_IFREG); e != nil {
		return nil, e
	}
	b, e := io.ReadAll(io.LimitReader(f, 16385))
	if e != nil {
		return nil, e
	}
	if len(b) == 0 || len(b) > 16384 {
		return nil, fmt.Errorf("initial stage file exceeds bound")
	}
	return b, nil
}

// sync接口只用于验证WAL错误传播；生产入口固定POSIX实现，不能从协议注入。
type initialBootSyncIO interface {
	file(*os.File) error
	directory(*os.File) error
}
type initialBootPOSIXSync struct{}

func (initialBootPOSIXSync) file(f *os.File) error      { return f.Sync() }
func (initialBootPOSIXSync) directory(f *os.File) error { return f.Sync() }
func initialBootInstall(s *lockedState, name string, raw []byte) error {
	return initialBootInstallSync(s, name, raw, initialBootPOSIXSync{})
}
func initialBootInstallSync(s *lockedState, name string, raw []byte, syncIO initialBootSyncIO) error {
	if syncIO == nil || len(raw) == 0 || len(raw) > 16384 {
		return fmt.Errorf("initial stage exceeds bound or sync implementation missing")
	}
	allowed := false
	for i := range initialBootStages {
		if name == initialBootFile(i) {
			allowed = true
		}
	}
	if !allowed {
		return fmt.Errorf("unknown fixed initial stage")
	}
	old, e := initialBootReadFile(s, name)
	if e != nil {
		return e
	}
	if old != nil {
		if !bytes.Equal(old, raw) {
			return fmt.Errorf("original initial stage cannot be replaced")
		}
		// 可见旧文件不是耐久证明：固定原fd、重新核私有regular与同字节，
		// 先实际file.Sync再dir.Sync，任一失败均不返回恢复ref。
		fd, e := unix.Openat(int(s.dir.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if e != nil {
			return e
		}
		f := os.NewFile(uintptr(fd), name)
		defer f.Close()
		if e = privateFile(fd, unix.S_IFREG); e != nil {
			return e
		}
		present, e := io.ReadAll(io.LimitReader(f, 16385))
		if e != nil {
			return e
		}
		if !bytes.Equal(present, raw) {
			return fmt.Errorf("original initial stage changed before sync")
		}
		if e = syncIO.file(f); e != nil {
			return e
		}
		return syncIO.directory(s.dir)
	}
	n, e := randomProcessNonce()
	if e != nil {
		return e
	}
	tmp := "initial-stage-" + n + ".tmp"
	fd, e := unix.Openat(int(s.dir.Fd()), tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), tmp)
	defer unix.Unlinkat(int(s.dir.Fd()), tmp, 0)
	if n, e := f.Write(raw); e != nil || n != len(raw) {
		f.Close()
		return errOrShortWrite(e)
	}
	if e = syncIO.file(f); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = unix.Linkat(int(s.dir.Fd()), tmp, int(s.dir.Fd()), name, 0); e != nil {
		return e
	}
	return syncIO.directory(s.dir)
}
func initialBootFreeze(s *lockedState, source initialBootSource) (initialBootRef, error) {
	return initialBootFreezeSync(s, source, initialBootPOSIXSync{})
}
func initialBootFreezeSync(s *lockedState, source initialBootSource, syncIO initialBootSyncIO) (initialBootRef, error) {
	var none initialBootRef
	if e := source.validate(); e != nil {
		return none, e
	}
	entries, e := initialBootRead(s)
	if e != nil {
		return none, e
	}
	if len(entries) > 0 {
		if entries[0].SourceSHA256 != initialBootSHA(initialBootCanonical(source)) {
			return none, fmt.Errorf("initial source changed; adoption forbidden")
		}
		// 丢目录sync回复后先同步既有文件与目录；不重造challenge nonce。
		for i, v := range entries {
			if e = initialBootInstallSync(s, initialBootFile(i), initialBootCanonical(v), syncIO); e != nil {
				return none, e
			}
		}
		return initialBootReference(entries), nil
	}
	var nonce [32]byte
	if _, e = rand.Read(nonce[:]); e != nil {
		return none, e
	}
	v := initialBootEntry{ProtocolVersion: 2, Scope: initialBootScope, Sequence: 1, Stage: initialBootStages[0], Source: source, SourceSHA256: initialBootSHA(initialBootCanonical(source)), Nonce: hex.EncodeToString(nonce[:]), Evidence: json.RawMessage("{}")}
	if e = initialBootInstallSync(s, initialBootFile(0), initialBootCanonical(v), syncIO); e != nil {
		return none, e
	}
	return initialBootReference([]initialBootEntry{v}), nil
}
func initialBootReference(entries []initialBootEntry) initialBootRef {
	if len(entries) == 0 {
		return initialBootRef{}
	}
	first, last := entries[0], entries[len(entries)-1]
	return initialBootRef{2, initialBootScope, first.SourceSHA256, first.Nonce, last.Stage, initialBootSHA(initialBootCanonical(last))}
}

// append只保存非授权组件事实；不接受阶段数字、不跳阶段、不覆盖旧事实，
// 不发QMP、不改旧record，也不提供正常启动/resize许可。
func initialBootAppend(s *lockedState, original initialBootRef, evidence json.RawMessage) (initialBootRef, error) {
	none := initialBootRef{}
	entries, e := initialBootRead(s)
	if e != nil {
		return none, e
	}
	if len(entries) == 0 || initialBootReference(entries) != original || len(entries) == len(initialBootStages) {
		return none, fmt.Errorf("original initial stage no longer matches")
	}
	if control.ValidateJSON(evidence, 8192, 512) != nil {
		return none, fmt.Errorf("invalid bounded initial evidence")
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(evidence, &obj) != nil || obj == nil {
		return none, fmt.Errorf("initial evidence must be an object")
	}
	canonical, e := json.Marshal(obj)
	if e != nil {
		return none, e
	}
	for i, v := range entries {
		if e = initialBootInstall(s, initialBootFile(i), initialBootCanonical(v)); e != nil {
			return none, e
		}
	}
	prev := entries[len(entries)-1]
	v := prev
	v.Sequence++
	v.Stage = initialBootStages[len(entries)]
	v.PreviousSHA256 = original.TailSHA256
	v.Evidence = canonical
	if e = initialBootInstall(s, initialBootFile(len(entries)), initialBootCanonical(v)); e != nil {
		return none, e
	}
	return initialBootReference(append(entries, v)), nil
}

// typed wire是后继固定Guest reader的最小合同，不是HTTP/Root资格生产者。
// 仅使用中立wire包，避免Host/Guest各复制一份合同或引入resourceguard循环依赖。
type initialPrebootChallenge = bootprotocol.InitialPrebootChallenge
type initialPrebootReceipt = bootprotocol.InitialPrebootReceipt

func initialBootGuestReceipt(context.Context, initialPrebootChallenge) (initialPrebootReceipt, error) {
	// 固定真实reader尚缺；绝不从模型seam或prepared:true造资格。
	return initialPrebootReceipt{}, errInitialBootUnavailable
}

// challenge只在原base实际观察已落盘后构造。此函数只核绑定，不认证Guest；
// receipt.match同样不能替代固定Root reader对原ISO/WAL/当前内核事实的复核。
func initialBootChallenge(source initialBootSource, ref initialBootRef) (initialPrebootChallenge, error) {
	if source.validate() != nil || ref.ProtocolVersion != 2 || ref.Scope != initialBootScope || ref.Stage != "base_observed" || ref.OriginalSHA256 != initialBootSHA(initialBootCanonical(source)) || !initialBootNonce(ref.Nonce) || !digestValid(ref.TailSHA256) {
		return initialPrebootChallenge{}, fmt.Errorf("original base observation challenge missing")
	}
	challenge := initialPrebootChallenge{ProtocolVersion: 1, Scope: bootprotocol.InitialPrebootScope, SourceSHA256: ref.OriginalSHA256, Nonce: ref.Nonce, HostStageSHA256: ref.TailSHA256, Identity: bootprotocol.RunnerIdentity(source.Identity), RuntimeSHA256: source.RuntimeSHA256, ResourceSHA256: source.ResourceSHA256, ConfigSecretUID: source.ConfigSecretUID, KernelBuildSHA256: source.KernelBuildSHA256, OriginalBase: source.OriginalBase, BusinessMinimum: source.Identity.MinimumBytes, Maximum: source.Identity.MaximumBytes}
	return challenge, challenge.Validate()
}
