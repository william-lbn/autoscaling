//go:build linux

package resourceguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
	"golang.org/x/sys/unix"
	"os"
	"reflect"
	"strings"
)

// 新的动态reference只在Linux transfer2；common构建声明不依赖Linux DTO。
func (o *normalSystemRoleServiceOrigin) UnmarshalJSON(raw []byte) error {
	type plain normalSystemRoleServiceOrigin
	var p plain
	if e := control.UnmarshalObject(raw, &p); e != nil {
		return e
	}
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(raw, &fields); e != nil {
		return e
	}
	value, present := fields["kernel_preparation_sha256"]
	if p.ProtocolVersion == 1 && present || p.ProtocolVersion == 2 && (!present || string(value) == "null") {
		return ErrUnknown
	}
	next := normalSystemRoleServiceOrigin(p)
	if p.ProtocolVersion == 2 {
		if e := normalSystemRoleTransferExactJSON(raw, reflect.TypeOf(normalSystemRoleServiceOrigin{})); e != nil {
			return e
		}
	}
	if e := next.validate(); e != nil {
		return e
	}
	*o = next
	return nil
}
func (t *normalSystemRoleServiceTransfer) UnmarshalJSON(raw []byte) error {
	type plain normalSystemRoleServiceTransfer
	var p plain
	if e := control.UnmarshalObject(raw, &p); e != nil {
		return e
	}
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(raw, &fields); e != nil {
		return e
	}
	value, present := fields["kernel_preparation"]
	if present && string(value) != "null" {
		if e := normalSystemRoleTransferExactJSON(value, reflect.TypeOf(normalKernelPrepV2HandoffReference{})); e != nil {
			return e
		}
	}
	if p.ProtocolVersion == 1 && present || p.ProtocolVersion == 2 && (!present || string(value) == "null") {
		return ErrUnknown
	}
	next := normalSystemRoleServiceTransfer(p)
	if e := next.validate(); e != nil {
		return e
	}
	*t = next
	return nil
}
func (t normalSystemRoleServiceTransfer) descriptorCount() int {
	if t.ProtocolVersion == 1 {
		return 4
	}
	if t.ProtocolVersion == 2 {
		return 5
	}
	return -1
}
func (t normalSystemRoleServiceTransfer) transferMagic() string {
	if t.ProtocolVersion == 1 {
		return "NSYSBF01"
	}
	if t.ProtocolVersion == 2 {
		return "NSYSBF02"
	}
	return ""
}
func (t normalSystemRoleServiceTransfer) validateKernelBinding() error {
	if t.ProtocolVersion == 1 {
		if t.Origin.ProtocolVersion != 1 || t.KernelPreparation != nil {
			return ErrUnknown
		}
		return nil
	}
	if t.ProtocolVersion != 2 || t.Origin.ProtocolVersion != 2 || t.KernelPreparation == nil {
		return ErrUnknown
	}
	k := *t.KernelPreparation
	if e := k.validate(); e != nil {
		return e
	}
	sum, e := k.referenceSHA256()
	if e != nil || sum != t.Origin.KernelPreparationSHA256 {
		return ErrUnknown
	}
	if k.Identity != t.Origin.ResourceIdentity || k.OriginalRoot != t.Origin.Parent.RootConsumer || k.RuntimeDocumentSHA256 != t.Origin.Parent.RuntimeDocumentSHA256 || k.StateDevice != t.Origin.StateDevice || k.StateInode != t.Origin.StateInode {
		return ErrUnknown
	}
	for _, n := range t.Origin.Namespace {
		found := false
		for _, o := range k.OriginalNamespaces {
			if n == o {
				found = true
				break
			}
		}
		if !found {
			return ErrUnknown
		}
	}
	return nil
}
func (t normalSystemRoleServiceTransfer) matchesDeclaration(d normalSystemRoleBrokerDeclaration) error {
	if d.kernelPreparationV2() {
		if t.ProtocolVersion != 2 || t.validate() != nil || t.KernelPreparation.ProducerELFSHA256 != d.ExecutableSHA256 {
			return ErrUnknown
		}
		return nil
	}
	if d.ProtocolVersion != 1 && d.ProtocolVersion != 2 || t.ProtocolVersion != 1 || t.KernelPreparation != nil {
		return ErrUnknown
	}
	return nil
}

// 仅从原exec proof接收的真实SCM slot5 dup；不接raw ref/FD，不打开sysctl路径。
// Caller已持resource锁，先完成typedFS，再由Kernel producer消费；全程Pread由后者负责。
func (p *normalSystemRoleBrokerExecProof) originalKernelPrepV2TransferLocked(ctx context.Context, state *lockedState, system *normalSystemLifecycleSource) (normalKernelPrepV2HandoffReference, *os.File, error) {
	var zero normalKernelPrepV2HandoffReference
	if p == nil || ctx == nil || ctx.Err() != nil || state == nil || system == nil {
		return zero, nil, ErrUnknown
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.verifyLocked(ctx, state, system); e != nil {
		return zero, nil, e
	}
	if !p.declaration.declaration.kernelPreparationV2() || p.transfer.ProtocolVersion != 2 || p.kernelPreparation == nil || p.transfer.validate() != nil || p.transfer.matchesDeclaration(p.declaration.declaration) != nil {
		return zero, nil, ErrUnknown
	}
	flags, e := unix.FcntlInt(p.kernelPreparation.Fd(), unix.F_GETFL, 0)
	if e != nil || flags&unix.O_ACCMODE != unix.O_RDONLY {
		return zero, nil, errors.Join(ErrUnknown, e)
	}
	clo, e := unix.FcntlInt(p.kernelPreparation.Fd(), unix.F_GETFD, 0)
	if e != nil || clo&unix.FD_CLOEXEC == 0 {
		return zero, nil, ErrUnknown
	}
	fd, e := unix.FcntlInt(p.kernelPreparation.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return zero, nil, e
	}
	return *p.transfer.KernelPreparation, os.NewFile(uintptr(fd), "original-SCM-KernelPrep2-readonly-control"), nil
}

var _ normalKernelPrepV2TransferReader = (*normalSystemRoleBrokerExecProof)(nil)

// transfer2的Origin/第五FD reference递归核准确JSON键/必填/定长数组，
// 防标准解码器将嵌套大小写别名当原字段。这里只核语法，不能生产FD来源能力。
func normalSystemRoleTransferExactJSON(raw []byte, t reflect.Type) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ErrUnknown
	}
	switch t.Kind() {
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil || fields == nil {
			return ErrUnknown
		}
		known := map[string]bool{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			tags := strings.Split(f.Tag.Get("json"), ",")
			// 原value witness已有固定Go默认字段名；严格沿其真实编码，不改历史DTO。
			if tags[0] == "-" {
				continue
			}
			if tags[0] == "" {
				tags[0] = f.Name
			}
			known[tags[0]] = true
			v, ok := fields[tags[0]]
			optional := false
			for _, tag := range tags[1:] {
				optional = optional || tag == "omitempty"
			}
			if !ok {
				if optional {
					continue
				}
				return fmt.Errorf("%w: missing exact reference field %s.%s", ErrUnknown, t.Name(), tags[0])
			}
			if e := normalSystemRoleTransferExactJSON(v, f.Type); e != nil {
				return fmt.Errorf("%s.%s: %w", t.Name(), tags[0], e)
			}
		}
		for k := range fields {
			if !known[k] {
				return fmt.Errorf("%w: unknown exact reference field %s.%s", ErrUnknown, t.Name(), k)
			}
		}
	case reflect.Array, reflect.Slice:
		var values []json.RawMessage
		if json.Unmarshal(raw, &values) != nil || t.Kind() == reflect.Array && len(values) != t.Len() {
			return ErrUnknown
		}
		for _, v := range values {
			if e := normalSystemRoleTransferExactJSON(v, t.Elem()); e != nil {
				return e
			}
		}
	}
	return nil
}

// 私有DEV只读检查点只能continue/error；生产Background没有该键。
// 不选择来源、FD或许可，回调仍通过原g.withRecord取得同resource锁。
type normalSystemRoleTransferPreparedCheckpointKey struct{}

func normalSystemRoleTransferPreparedCheckpoint(ctx context.Context, g *Guard, fs *normalFilesystemOriginSource, proof *normalSystemRoleBrokerExecProof) error {
	if f, ok := ctx.Value(normalSystemRoleTransferPreparedCheckpointKey{}).(func(*Guard, *normalFilesystemOriginSource, *normalSystemRoleBrokerExecProof) error); ok {
		return f(g, fs, proof)
	}
	return nil
}
