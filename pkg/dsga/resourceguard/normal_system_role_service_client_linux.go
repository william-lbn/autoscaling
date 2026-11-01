//go:build linux

package resourceguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
)

// 仅为固定Root CLI提供；PG SUID真实UID非0即拒绝，声明/argv/env/预算均不从
// 请求补全。环境值不写错误或输出；原socket peer由SO_PEERPIDFD真实钉住。
func RunNormalSystemRoleClient(ctx context.Context, role string) (exitCode int, err error) {
	err = runNormalSystemRoleClient(ctx, role)
	if err != nil {
		var target *normalSystemRoleServiceExitError
		if errors.As(err, &target) {
			return target.code, nil
		}
		return 0, err
	}
	return 0, nil
}

type normalSystemRoleServiceExitError struct{ code int }

func (e *normalSystemRoleServiceExitError) Error() string {
	return "fixed System role exited unsuccessfully"
}
func runNormalSystemRoleClient(ctx context.Context, role string) error {
	if e := normalSystemRoleServiceCaller(); e != nil {
		return e
	}
	if os.Getppid() != 1 {
		return fmt.Errorf("fixed role client requires original namespace PID1 parent")
	}
	proc, e := unix.Open("/proc/self", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return e
	}
	defer unix.Close(proc)
	actualEnv, e := normalSystemReadVector(proc, "environ")
	if e != nil {
		return e
	}
	if e = normalSystemRoleServiceClientEnvironment(actualEnv); e != nil {
		return e
	}
	if e = normalSystemRoleServiceClientDescriptors(proc, role); e != nil {
		return e
	}
	if role == "compute" {
		pin, e := normalSystemRoleServiceCaptureComputeLockSelf()
		if e != nil {
			return e
		}
		defer pin.Close()
	}
	code, e := normalSystemRoleServiceRoleCode(role)
	if e != nil {
		return e
	}
	if ctx == nil || ctx.Err() != nil {
		return ErrUnknown
	}
	source, e := inspectNormalSystemLifecycleOriginal(ctx)
	if e != nil {
		return e
	}
	defer source.close()
	if source.manifest.ProtocolVersion != 3 {
		return errors.Join(ErrUnknown, fmt.Errorf("production role client requires fixed System v3"))
	}
	decl, e := readNormalSystemRoleDeclarationOriginal(ctx, source)
	if e != nil {
		return e
	}
	defer decl.close()
	ns, e := captureNormalSystemRoleNamespaces()
	if e != nil {
		return e
	}
	defer ns.close()
	metadata, e := readNormalSystemRoleServiceMetadata()
	if e != nil {
		return e
	}
	if metadata.ReadySHA256 == "" || metadata.Origin.DeclarationSHA256 != decl.rawSHA || !normalSystemRoleImmutableBindingsEqual(metadata.Broker, source.binding) {
		return errors.Join(ErrUnknown, fmt.Errorf("original role broker metadata/source differs"))
	}
	connected, e := (&net.Dialer{}).DialContext(ctx, "unixpacket", normalSystemRoleServiceSocketPath)
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	defer connected.Close()
	conn, ok := connected.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("fixed role transport differs")
	}
	peer, e := normalSystemRoleServicePeer(conn, source.binding.GuestBootID)
	if e != nil {
		return e
	}
	defer peer.close()
	if peer.source.PID != metadata.Broker.RootConsumer.PID || peer.source.StartTicks != uint64(metadata.Broker.RootConsumer.StartTicks) {
		return errors.Join(ErrUnknown, fmt.Errorf("original broker instance PID/start differs"))
	}
	if e = normalSystemRoleServiceVerifyTask(ctx, decl, ns, peer, normalSystemRoleBrokerArgv()); e != nil {
		return e
	}
	var socket unix.Stat_t
	if unix.Lstat(normalSystemRoleServiceSocketPath, &socket) != nil || socket.Uid != 0 || socket.Mode&0777 != 0600 || socket.Dev != metadata.SocketDevice || socket.Ino != metadata.SocketInode {
		return errors.Join(ErrUnknown, fmt.Errorf("original broker socket identity differs"))
	}
	brokerNonce, e := nativeBirthDigest(metadata.Broker.RootConsumer.InstanceNonce)
	if e != nil {
		return e
	}
	requestNonce, e := normalSystemRoleServiceNonce()
	if e != nil {
		return e
	}
	request := normalSystemRoleServiceFrame{Operation: normalSystemRoleServiceExecute, Role: code, BrokerNonce: brokerNonce, RequestNonce: requestNonce}
	raw := encodeNormalSystemRoleServiceFrame(request)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	if e = conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); e != nil {
		return e
	}
	n, e := conn.Write(raw)
	if e != nil || n != len(raw) {
		return errors.Join(ErrUnknown, e, fmt.Errorf("fixed role request unconfirmed"))
	}
	reply, e := arrayServicePacket(conn, time.Now().Add(100*time.Second), normalSystemRoleServiceRequestBytes, false)
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	status, exit, e := decodeNormalSystemRoleServiceReply(reply, request)
	if e != nil {
		return e
	}
	if status != normalSystemRoleServiceAccepted || exit != 0 {
		return errors.Join(ErrUnknown, fmt.Errorf("original broker did not accept fixed role"))
	}
	if e = conn.SetDeadline(time.Time{}); e != nil {
		return e
	}
	reply, e = arrayServicePacket(conn, time.Time{}, normalSystemRoleServiceRequestBytes, false)
	if e != nil {
		return errors.Join(ErrUnknown, e)
	}
	status, exit, e = decodeNormalSystemRoleServiceReply(reply, request)
	if e != nil {
		return e
	}
	if status != normalSystemRoleServiceComplete {
		return ErrUnknown
	}
	if e = peer.verify(); e != nil {
		return errors.Join(ErrUnknown, e)
	}
	if exit != 0 {
		return &normalSystemRoleServiceExitError{int(exit)}
	}
	return ctx.Err()
}
func readNormalSystemRoleServiceMetadata() (normalSystemRoleServiceMetadata, error) {
	var out normalSystemRoleServiceMetadata
	f, e := rootFile(normalSystemRoleServiceMetadataPath, false)
	if e != nil {
		return out, errors.Join(ErrUnknown, e)
	}
	defer f.Close()
	var st unix.Stat_t
	if unix.Fstat(int(f.Fd()), &st) != nil || st.Mode&0777 != 0600 {
		return out, fmt.Errorf("original broker metadata mode differs")
	}
	raw, e := nativeBirthReadBounded(f, 16<<10)
	if e != nil {
		return out, e
	}
	if e = control.UnmarshalObject(raw, &out); e != nil {
		return out, e
	}
	if e = out.validate(); e != nil {
		return out, e
	}
	return out, nil
}
func normalSystemRoleServiceMetadataBytes(m normalSystemRoleServiceMetadata) ([]byte, error) {
	if e := m.validate(); e != nil {
		return nil, e
	}
	return json.Marshal(m)
}
