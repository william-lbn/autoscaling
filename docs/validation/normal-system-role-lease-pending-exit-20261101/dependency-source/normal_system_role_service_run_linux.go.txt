//go:build linux

package resourceguard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"
)

// 仅Root新增的typed FS薄adapter可满足此接口；没有实现时明确Unknown，
// 不调用旧resource/serve恢复路径，更不把raw reference直接当opaque来源。
type normalSystemRoleFilesystemBrokerRebinder interface {
	rebindNormalFilesystemOriginForBrokerLocked(context.Context, *lockedState, *normalSystemLifecycleSource, *normalSystemRoleBrokerExecProof, normalFilesystemOriginReference, *os.File, *os.File) (*normalFilesystemOriginSource, error)
}

// 固定CLI入口不收参数：原source/enum/startup通道来自RO镜像和原Root同一
// kernel socket。不能通过新进程扫描已有socket/WAL收养旧broker实例。
func RunNormalSystemRoleBroker(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrUnknown
	}
	if e := normalSystemRoleServiceCaller(); e != nil {
		return e
	}
	source, e := inspectNormalSystemLifecycleOriginal(ctx)
	if e != nil {
		return e
	}
	defer source.close()
	if source.manifest.ProtocolVersion != 3 {
		return errors.Join(ErrUnknown, fmt.Errorf("production broker requires fixed System v3"))
	}
	declaration, e := readNormalSystemRoleDeclarationOriginal(ctx, source)
	if e != nil {
		return e
	}
	defer declaration.close()
	sealed, e := sealedRuntimeSource()
	if e != nil {
		return e
	}
	g, e := Open(sealed.Config)
	if e != nil {
		return e
	}
	connection, e := normalSystemRoleGateConnection(5)
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	defer connection.Close()
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, _ := call.Deadline()
	if e = connection.SetDeadline(deadline); e != nil {
		return e
	}
	proof, e := receiveNormalSystemRoleBrokerExecProof(call, g, source, declaration, connection)
	if e != nil {
		return e
	}
	defer proof.close()
	rebinder, ok := any(g).(normalSystemRoleFilesystemBrokerRebinder)
	if !ok {
		return errors.Join(ErrUnknown, fmt.Errorf("original broker filesystem typed adapter is not installed"))
	}
	var filesystem *normalFilesystemOriginSource
	e = g.withRecord(call, func(r *record, state *lockedState) error {
		if e := proof.verifyLocked(call, state, source); e != nil {
			return e
		}
		var err error
		filesystem, err = rebinder.rebindNormalFilesystemOriginForBrokerLocked(call, state, source, proof, proof.transfer.Origin.Filesystem, proof.namespace, proof.root)
		return err
	})
	if e != nil {
		return e
	}
	defer filesystem.close()
	if e = normalSystemRoleTransferPreparedCheckpoint(ctx, g, filesystem, proof); e != nil {
		return e
	}
	namespaces, e := captureNormalSystemRoleNamespaces()
	if e != nil {
		return e
	}
	defer namespaces.close()
	metadata := normalSystemRoleServiceMetadata{ProtocolVersion: 1, Origin: proof.transfer.Origin, Broker: source.binding, ReleasedSHA256: proof.transfer.GateSHA256[2]}
	var authority *normalSystemResourceAuthority
	if declaration.declaration.brokerOwnsAuthority() {
		authority, e = prepareNormalSystemResourceAuthority(ctx, call, g, source, declaration, proof)
		if e != nil {
			return e
		}
		defer authority.close()
		reference := authority.reference
		metadata.ResourceAuthority = &reference
	}
	service, e := openNormalSystemRoleService(ctx, g, source, declaration, namespaces, proof.parent, metadata)
	if e != nil {
		return e
	}
	service.authority = authority
	defer service.close()
	closeRoleV2, e := installNormalSystemRoleV2BrokerRuntime(ctx, g, source, filesystem, proof, service.broker)
	if e != nil {
		return e
	}
	defer closeRoleV2()
	serviceErrors := make(chan error, 1)
	go func() { serviceErrors <- service.serve() }()
	if e = normalSystemRoleServiceWriteObject(connection, "NSYSBR01", service.metadata); e != nil {
		return e
	}
	var commit normalSystemRoleServiceCommit
	if e = normalSystemRoleServiceReadObject(connection, call, "NSYSBC01", &commit); e != nil {
		return e
	}
	if commit.ProtocolVersion != 1 || commit.Nonce != proof.transfer.Origin.Nonce || !normalSystemRoleDeclarationDigest(commit.ReadySHA256) {
		return fmt.Errorf("original broker commit binding differs")
	}
	// Parent此刻持同flock并已完成ready fsync。这里只按原Root受信控制流核
	// 完整可见字节，不能反取flock造成Parent等ACK的锁环；ordinary仍未启用。
	stage, e := normalSystemRoleServiceReadStageWithoutLock(sealed.Config.StateDir, proof.transfer.Origin, "ready", commit.ReadySHA256)
	if e != nil {
		return e
	}
	if stage.PreviousSHA256 != proof.transfer.GateSHA256[2] || stage.BrokerTask == nil || !reflect.DeepEqual(*stage.BrokerTask, proof.transfer.Task) || stage.Broker == nil || *stage.Broker != source.binding || stage.SocketDevice != service.metadata.SocketDevice || stage.SocketInode != service.metadata.SocketInode || !reflect.DeepEqual(stage.ResourceAuthority, service.metadata.ResourceAuthority) {
		return fmt.Errorf("original broker ready prefix differs")
	}
	if e = service.publishReady(call, commit.ReadySHA256); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if e = normalSystemRoleServiceWriteObject(connection, "NSYSBA01", commit); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	_ = connection.Close()
	// original initialRoot允许正常退出；new broker角色、原task/FS对象继续存活。
	// Source消费者必须另以本原pidfd/ready前缀监测broker，死亡即Unknown。
	var authorityDone <-chan struct{}
	if authority != nil {
		authorityDone = authority.admission.done
	}
	select {
	case <-authorityDone:
		return errors.Join(ErrUnknown, fmt.Errorf("original broker resource authority failed"))
	case e := <-serviceErrors:
		return errors.Join(ErrUnknown, e)
	case <-ctx.Done():
		return errors.Join(ErrUnknown, ctx.Err())
	}
}
func normalSystemRoleServiceReadStageWithoutLock(stateDir string, origin normalSystemRoleServiceOrigin, stage, sha string) (normalSystemRoleServiceStage, error) {
	var zero normalSystemRoleServiceStage
	f, e := rootFile(stateDir+"/"+normalSystemRoleServiceStageName(origin.Nonce, stage), false)
	if e != nil {
		return zero, e
	}
	defer f.Close()
	raw, e := nativeBirthReadBounded(f, 64<<10)
	if e != nil || physicalDigest(raw) != sha {
		return zero, fmt.Errorf("original broker ready stage bytes differ")
	}
	return decodeNormalSystemRoleServiceStage(raw, origin, stage)
}
