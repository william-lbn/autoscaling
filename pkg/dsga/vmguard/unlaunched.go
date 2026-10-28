package vmguard

import (
	"context"
	"encoding/json"
	"fmt"
)

// CloseUnlaunched只关闭“完整Runner出生已经固定，但还没有原启动意图”的
// init失败窗口。正常Start必须先在同一目录flock内同步process-launch.json；
// 本方法在相同锁内核其缺失并同步process-exit.json，二者只能有一方先获许可。
// 写关闭后任何迟到RunQEMU/Tick均被原engine屏障拒绝，不依赖当前PID或QMP状态。
//
// 返回false表示启动意图已存在：它可能已Start，也可能尚未Start，本方法不
// 推导NoEffect、补Wait或修改原内存WAL。缺出生/坏日志同样不创建新来源。
// 返回true仅证明原QEMU没有获启动许可；API发布失败仍保留本地关闭，可用原
// 对象/同目录重试。整Pod/Node成本由Scheduler独立核全部容器终态后结清。
func (r *Runner) CloseUnlaunched(ctx context.Context) (bool, error) {
	exited, launched, err := r.sealUnlaunched(ctx, "")
	if err != nil || launched {
		return false, err
	}
	if exited == nil {
		// 网络不持文件锁。先固定原容器实例，随后重新持锁检查launch；
		// 这次GET期间如果Start赢得许可，下面只能返回false，不能封住它。
		container, err := r.originalContainer(ctx, true)
		if err != nil {
			return false, err
		}
		exited, launched, err = r.sealUnlaunched(ctx, container)
		if err != nil || launched {
			return false, err
		}
	}
	return true, r.publishProcessExit(ctx, *exited)
}

// container为空时仅读取已有关闭事实；有值时才在原锁内安装新屏障。先读本地
// 再访问API使已关闭实例在Pod删除/网络故障后仍能补发同一原文，不重新选容器。
// 锁内只有固定文件/WAL读写，不发网络或QMP；不增加Sequence或清Pending/Last。
func (r *Runner) sealUnlaunched(ctx context.Context, container string) (*ProcessExit, bool, error) {
	var exited *ProcessExit
	launched := false
	err := r.engine.withRecordState(ctx, true, func(state *record, s *lockedState) error {
		if err := state.validate(); err != nil {
			return err
		}
		raw, err := s.readProcessFile("process-launch.json")
		if err != nil {
			return err
		}
		if raw != nil {
			launched = true
			return nil
		}
		raw, err = s.readProcessFile("process-exit.json")
		if err != nil {
			return err
		}
		if raw != nil {
			var original ProcessExit
			if json.Unmarshal(raw, &original) != nil || original.ProtocolVersion != 3 || original.StateClaim != r.birth.StateClaim {
				return fmt.Errorf("unlaunched closure differs from original Runner birth")
			}
			exited = &original
			// 处理先前安装成功但目录fsync回复丢失；不能仅凭能读到文件
			// 就发布。原engine已核完整身份/Sequence/WAL摘要与关闭事实。
			return s.dir.Sync()
		}
		if container == "" {
			return nil
		}
		body, err := json.Marshal(state)
		if err != nil {
			return err
		}
		closed := ProcessExit{ProtocolVersion: 3, Identity: r.birth.Identity, BirthClaim: r.engine.binding, StateClaim: r.birth.StateClaim, LaunchNonce: "", CommandSHA256: "", ProcessID: 0, ExitCode: 0, Signal: 0, RecordSequence: state.Sequence, RecordSHA256: SourceDigest(body), ContainerID: container}
		if err := closed.Validate(); err != nil {
			return err
		}
		raw, err = json.Marshal(closed)
		if err != nil {
			return err
		}
		if err := s.writeOnce("process-exit.json", raw); err != nil {
			return err
		}
		exited = &closed
		return nil
	})
	return exited, launched, err
}
