//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

type arrayBirthPrivateMigration struct {
	ProtocolVersion uint32          `json:"protocol_version"`
	Scope           string          `json:"scope"`
	Birth           NativeBirthRef  `json:"birth"`
	Original        ProcessIdentity `json:"original"`
	AuditSHA256     string          `json:"audit_sha256"`
	PrivateDevice   control.Uint64  `json:"private_device"`
	PrivateInode    control.Uint64  `json:"private_inode"`
	PIDFDDevice     control.Uint64  `json:"pidfd_device"`
	PIDFDInode      control.Uint64  `json:"pidfd_inode"`
}

func arrayBirthMigrationName(nonce, phase string) string {
	return "array-birth." + nonce + ".private-" + phase + ".json"
}

// caller持同resource flock；driver是当前M独占且已确认stop+EXITKILL的原peer。
// 不接受HTTP PID，不重开numeric PIDFD；直到本M wait/detach前目标ptrace僵尸
// 不由我们回收，故迁组只会作用原目标或失败，不能在此窗口收旧PID再写新PID。
func moveArrayNativeBirthPrivateLocked(ctx context.Context, g *Guard, stage *nativeBirthStage, driver *nativeBirthTraceKernel, in normalSplitInputs, tr nativeBirthTraceTranscript) error {
	if g == nil || stage == nil || driver == nil || !driver.attached || !driver.stopped || !tr.BaseVerified || tr.BasePersisted || tr.FinalVerified || tr.Original != stage.snapshot.Pending.Original {
		return errors.Join(ErrUnknown, fmt.Errorf("original stopped native base source missing"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := driver.verify(&tr.Request, driver.tid); err != nil {
		return errors.Join(ErrUnknown, err)
	}
	s := stage.state
	p := stage.snapshot.Pending
	current, err := g.normalSplitLocked(ctx, in, normalSplitObserve, s)
	if err != nil {
		return err
	}
	if current.Current.Private == nil || current.Current.Arrays == nil || !current.Current.Private.Empty || !current.Current.Arrays.Empty || current.Current.Private.Device != p.PrivateDevice || current.Current.Private.Inode != p.PrivateInode {
		return errors.Join(ErrUnknown, fmt.Errorf("original private/arrays topology is not empty or changed"))
	}
	group, err := nativeTraceProcText(driver.procfd, "cgroup")
	if err != nil || strings.TrimSpace(group) != "0::"+strings.TrimPrefix(systemCgroup, "/sys/fs/cgroup") {
		return errors.Join(ErrUnknown, fmt.Errorf("original native left System before verified enrollment"))
	}
	var fd unix.Stat_t
	if err = unix.Fstat(driver.pidfd, &fd); err != nil {
		return err
	}
	intent := arrayBirthPrivateMigration{ProtocolVersion: 1, Scope: "original-native-private-migration-v1", Birth: stage.snapshot.Ref, Original: p.Original, AuditSHA256: fmt.Sprintf("%x", tr.AuditSHA256), PrivateDevice: p.PrivateDevice, PrivateInode: p.PrivateInode, PIDFDDevice: control.Uint64(fd.Dev), PIDFDInode: control.Uint64(fd.Ino)}
	raw, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	if err = s.createPhysicalFile(arrayBirthMigrationName(p.Nonce, "intent"), raw); err != nil {
		return errors.Join(ErrUnknown, err)
	}
	if err = driver.verify(&tr.Request, driver.tid); err != nil {
		return errors.Join(ErrUnknown, err)
	}
	if err = writeNumber(pgCgroup+"/private/cgroup.procs", uint64(p.Original.PostmasterPID)); err != nil {
		return errors.Join(ErrUnknown, err)
	}
	group, err = nativeTraceProcText(driver.procfd, "cgroup")
	if err != nil || strings.TrimSpace(group) != "0::"+strings.TrimPrefix(pgCgroup+"/private", "/sys/fs/cgroup") {
		return errors.Join(ErrUnknown, fmt.Errorf("original native private placement unconfirmed"))
	}
	after, err := splitReadNode(pgCgroup + "/private")
	if err != nil || !splitSameNode(after, *current.Current.Private) || after.Empty {
		return errors.Join(ErrUnknown, err, fmt.Errorf("original native private inode/tasks differ after migration"))
	}
	members, memberErr := groupPIDs(pgCgroup + "/private")
	if memberErr != nil || len(members) != 1 || members[0] != p.Original.PostmasterPID {
		return errors.Join(ErrUnknown, memberErr, fmt.Errorf("original private group contains unexpected task"))
	}
	if after.MemoryMaxBytes != current.Current.Private.MemoryMaxBytes || after.PidsMaxTasks != current.Current.Private.PidsMaxTasks || after.SwapMaxBytes != 0 || !after.MemoryHighUnlimited {
		return errors.Join(ErrUnknown, fmt.Errorf("original private caps drifted during native migration"))
	}
	if err = driver.verify(&tr.Request, driver.tid); err != nil {
		return errors.Join(ErrUnknown, err)
	}
	// complete只说明同原任务迁组事实，不代表normal/FS/I许可。后继阶段仍独立核。
	if err = s.createPhysicalFile(arrayBirthMigrationName(p.Nonce, "complete"), raw); err != nil {
		return errors.Join(ErrUnknown, err)
	}
	for _, phase := range []string{"intent", "complete"} {
		if err = s.syncPhysicalFile(arrayBirthMigrationName(p.Nonce, phase)); err != nil {
			return errors.Join(ErrUnknown, err)
		}
	}
	return s.dir.Sync()
}

func publishArrayBirthConsumer(source nativeBirthSource) (arrayBirthConsumerMetadata, error) {
	var zero arrayBirthConsumerMetadata
	consumer, err := nativeBirthCurrentConsumerIdentity()
	if err != nil {
		return zero, err
	}
	boot, err := kernelText("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return zero, err
	}
	next := arrayBirthConsumerMetadata{ProtocolVersion: 1, Scope: arrayBirthConsumerScope, GuestBootID: boot, Consumer: consumer, Source: source.binding}
	if err = next.validate(); err != nil {
		return zero, err
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return zero, err
	}
	// 调用方已持独立server.lock，固定root-only目录内原子更新当前实例元数据。
	// 它不是持久授权WAL，不能据其可见性清旧Pending；原stage还核真实consumer。
	temp, err := os.CreateTemp("/run/neon-dsga", ".birth-consumer-")
	if err != nil {
		return zero, err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err = temp.Chmod(0600); err == nil {
		_, err = temp.Write(raw)
	}
	if err == nil {
		err = temp.Sync()
	}
	closeErr := temp.Close()
	if err != nil {
		return zero, err
	}
	if closeErr != nil {
		return zero, closeErr
	}
	if err = os.Rename(name, fixedArrayBirthConsumerPath); err != nil {
		return zero, err
	}
	dir, err := os.Open("/run/neon-dsga")
	if err != nil {
		return zero, err
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return zero, err
	}
	return next, nil
}
