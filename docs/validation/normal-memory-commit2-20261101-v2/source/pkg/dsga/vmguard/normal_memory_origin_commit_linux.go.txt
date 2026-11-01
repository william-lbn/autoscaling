//go:build linux

package vmguard

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/neondatabase/autoscaling/pkg/dsga/bootprotocol"
	"golang.org/x/sys/unix"
)

const normalMemoryHostCommitFile = "normal-memory-origin-host.commit.json"
const normalMemoryHostCommitScope = "normal-memory-origin-host-durability-v2"

type normalMemoryHostCommitRecord struct {
	ProtocolVersion uint32                                    `json:"protocol_version"`
	Scope           string                                    `json:"scope"`
	Commit          bootprotocol.NormalMemoryOriginHostCommit `json:"commit"`
	OriginalWAL     [8]string                                 `json:"original_wal"`
	Directory       [4]uint64                                 `json:"directory"`
	OriginalFiles   [2][3]uint64                              `json:"original_files"`
}
type normalMemoryHostCommitEvidence struct {
	record   normalMemoryHostCommitRecord
	rawSHA   string
	identity [3]uint64
}

// 只同步已经存在且与原opaque identity/bytes一致的固定文件，绝不补造缺失历史。
func normalMemoryHostSyncExisting(s *lockedState, name string, want []byte, id [3]uint64) error {
	allowed := name == normalMemoryHostReferenceFile || name == normalMemoryHostAckFile || name == normalMemoryHostCommitFile
	for i := range initialBootStages {
		allowed = allowed || name == initialBootFile(i)
	}
	if !allowed || s == nil || s.dir == nil || s.lock == nil {
		return fmt.Errorf("fixed original Host sync required")
	}
	fd, e := unix.Openat(int(s.dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if e != nil {
		return e
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	if e = privateFile(fd, unix.S_IFREG); e != nil {
		return e
	}
	var a, b unix.Stat_t
	if e = unix.Fstat(fd, &a); e != nil {
		return e
	}
	if [3]uint64{a.Dev, a.Ino, uint64(a.Size)} != id || a.Size != int64(len(want)) {
		return fmt.Errorf("original Host source identity changed before sync")
	}
	raw, e := io.ReadAll(io.LimitReader(f, int64(len(want))+1))
	if e != nil || !bytes.Equal(raw, want) {
		return fmt.Errorf("original Host source bytes differ before sync: %v", e)
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = unix.Fstatat(int(s.dir.Fd()), name, &b, unix.AT_SYMLINK_NOFOLLOW); e != nil || a.Dev != b.Dev || a.Ino != b.Ino || a.Size != b.Size {
		return fmt.Errorf("original Host source path changed across sync: %v", e)
	}
	return nil
}

func (o *normalMemoryHostOriginSource) recheckCommit2Locked(s *lockedState) error {
	if o == nil || o.ack == nil || o.commit2 == nil {
		return fmt.Errorf("Host memory2 requires original durable commit8 evidence")
	}
	v := o.commit2.record
	if v.ProtocolVersion != 2 || v.Scope != normalMemoryHostCommitScope || v.OriginalWAL != o.wal || v.Directory != o.directory || v.OriginalFiles != o.files || v.Commit.MatchReferenceAndAck(o.reference, *o.ack) != nil {
		return fmt.Errorf("Host original commit2 source differs")
	}
	raw := initialBootCanonical(v)
	if initialBootSHA(raw) != o.commit2.rawSHA {
		return fmt.Errorf("Host commit2 evidence changed")
	}
	id, e := normalMemoryHostFileIdentity(s, normalMemoryHostCommitFile, raw)
	if e != nil || id != o.commit2.identity {
		return fmt.Errorf("Host commit2 durable inode changed: %v", e)
	}
	return nil
}

// 只由本次真实7消费后的Host producer调用；该路径不是Observe/任意RPC。
// 同资源锁下逐原8文件/ref6/ack7同步和重核，再同步固定8记录。send8只是通知，
// 从不表示Guest已经收到8、完成FS或PG Ready；不增加ACK9/无穷共同知识假设。
func normalMemoryPersistHostCommit2Locked(ctx context.Context, s *lockedState, o *normalMemoryHostOriginSource) (bootprotocol.NormalMemoryOriginHostCommit, error) {
	var zero bootprotocol.NormalMemoryOriginHostCommit
	if ctx == nil || ctx.Err() != nil || s == nil || s.lock == nil || o == nil || o.owner == nil || o.process == nil || o.ack == nil || o.selection != 2 {
		return zero, fmt.Errorf("real Host memory2 producer missing")
	}
	if e := o.recheckOriginalLocked(ctx, s, o.owner, &o.proof); e != nil {
		return zero, e
	}
	// 初次8仍须在原businessMin；普通运行期最大几何观察不替代此处实际target门。
	probe := &initialBaseQMP{source: o.source, q: o.owner.q, process: o.process}
	actual, e := probe.inspect(ctx)
	if e != nil {
		return zero, e
	}
	if actual.RequestedBytes != o.source.Identity.MinimumBytes || actual.ActualBytes != o.source.Identity.MinimumBytes {
		return zero, fmt.Errorf("Host commit2 current target differs from original initial target")
	}
	for i := range initialBootStages {
		if e = ctx.Err(); e != nil {
			return zero, e
		}
		raw, e := initialBootReadFile(s, initialBootFile(i))
		if e != nil || initialBootSHA(raw) != o.wal[i] {
			return zero, fmt.Errorf("Host commit2 lost original8 prefix: %v", e)
		}
		if e = normalMemoryHostSyncExisting(s, initialBootFile(i), raw, o.walFiles[i]); e != nil {
			return zero, e
		}
	}
	for i, n := range []string{normalMemoryHostReferenceFile, normalMemoryHostAckFile} {
		raw := initialBootCanonical(o.reference)
		if i == 1 {
			raw = initialBootCanonical(*o.ack)
		}
		if e = normalMemoryHostSyncExisting(s, n, raw, o.files[i]); e != nil {
			return zero, e
		}
	}
	if e = s.dir.Sync(); e != nil {
		return zero, e
	}
	if e = o.recheckOriginalLocked(ctx, s, o.owner, &o.proof); e != nil {
		return zero, e
	}
	commit, e := bootprotocol.NewNormalMemoryOriginHostCommit(o.reference, *o.ack)
	if e != nil {
		return zero, e
	}
	record := normalMemoryHostCommitRecord{2, normalMemoryHostCommitScope, commit, o.wal, o.directory, o.files}
	raw := initialBootCanonical(record)
	// 与原initial WAL同样先file Sync再Link+dir Sync；可见但同步失败不构造私有见证。
	if e = normalMemoryHostWriteOnce(s, normalMemoryHostCommitFile, raw); e != nil {
		return zero, e
	}
	id, e := normalMemoryHostFileIdentity(s, normalMemoryHostCommitFile, raw)
	if e != nil {
		return zero, e
	}
	evidence := &normalMemoryHostCommitEvidence{record, initialBootSHA(raw), id}
	o.commit2 = evidence
	if e = o.recheckLocked(ctx, s, o.owner, &o.proof); e != nil {
		o.commit2 = nil
		return zero, e
	}
	return commit, nil
}

// 单次5→6→7→durable8→send8。任何部分帧、错误nonce/peer、失败同步均Unknown，
// 不双发、不重新连接、不重拍来源、不重发QMP；同源恢复是后继独立合同。
func (c *initialBootChannel) completeWithNormalMemoryOriginV2(ctx context.Context, s *lockedState, o *normalMemoryHostOriginSource) (bootprotocol.NormalMemoryOriginGuestAck, error) {
	var zero bootprotocol.NormalMemoryOriginGuestAck
	if c == nil || o == nil || o.owner == nil || o.process == nil || o.selection != 2 {
		return zero, fmt.Errorf("original Host memory2 source required")
	}
	selected, e := normalMemoryHostSelection(o.owner.config.RuntimeJSON, o.source)
	if e != nil || selected != 2 || !bytes.Equal(o.runtime, o.owner.config.RuntimeJSON) {
		return zero, fmt.Errorf("original ISO did not select complete memory2: %v", e)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.process != o.process || !reflect.DeepEqual(c.source, o.source) || c.prepared != o.reference.Prepared || c.observed != o.reference.Movable {
		return zero, c.failLocked(fmt.Errorf("memory2 channel changed original1..4"))
	}
	w, dir, e := normalMemoryHostWAL(s, &o.proof)
	if e != nil || w != o.wal || dir != o.directory {
		return zero, c.failLocked(fmt.Errorf("memory2 original Host8 changed: %v", e))
	}
	ids, e := normalMemoryHostWALFileIDs(s, w)
	if e != nil || ids != o.walFiles {
		return zero, c.failLocked(fmt.Errorf("memory2 Host8 inode drift: %v", e))
	}
	id, e := normalMemoryHostFileIdentity(s, normalMemoryHostReferenceFile, initialBootCanonical(o.reference))
	if e != nil || id != o.files[0] {
		return zero, c.failLocked(fmt.Errorf("memory2 reference inode drift: %v", e))
	}
	stop, e := c.beginLocked(ctx, 2)
	if e != nil {
		return zero, e
	}
	defer stop()
	if e = bootprotocol.WriteInitialCompletion(c.conn, o.reference.Completion); e != nil {
		return zero, c.failLocked(e)
	}
	if e = bootprotocol.WriteNormalMemoryOriginHostReference(c.conn, o.reference); e != nil {
		return zero, c.failLocked(e)
	}
	ack, e := bootprotocol.ReadNormalMemoryOriginGuestAck(c.conn)
	if e != nil {
		return zero, c.failLocked(e)
	}
	if e = ack.MatchReference(o.reference); e != nil {
		return zero, c.failLocked(e)
	}
	if e = c.process.checkPeer(&qmpConnection{conn: c.conn}, c.source); e != nil {
		return zero, c.failLocked(e)
	}
	if e = normalMemoryHostWriteOnce(s, normalMemoryHostAckFile, initialBootCanonical(ack)); e != nil {
		return zero, c.failLocked(e)
	}
	o.files[1], e = normalMemoryHostFileIdentity(s, normalMemoryHostAckFile, initialBootCanonical(ack))
	if e != nil {
		return zero, c.failLocked(e)
	}
	o.ack = &ack
	commit, e := normalMemoryPersistHostCommit2Locked(ctx, s, o)
	if e != nil {
		return zero, c.failLocked(e)
	}
	if e = bootprotocol.WriteNormalMemoryOriginHostCommit(c.conn, commit); e != nil {
		return zero, c.failLocked(e)
	}
	c.phase = 3
	c.closeLocked()
	return ack, nil
}
