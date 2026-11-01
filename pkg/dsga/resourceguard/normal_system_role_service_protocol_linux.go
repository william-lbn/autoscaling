//go:build linux

package resourceguard

import (
	"encoding/binary"
	"fmt"
)

const normalSystemRoleServiceRequestBytes = 96
const normalSystemRoleServiceRequestMagic = "NSYSRL01"
const normalSystemRoleServiceReplyMagic = "NSYSRA01"
const normalSystemRoleServiceExecute = 1
const normalSystemRoleServiceProbeReady = 2
const normalSystemRoleServiceAccepted = 1
const normalSystemRoleServiceComplete = 0
const normalSystemRoleServiceUnknown = 2

var normalSystemRoleServiceRoles = [...]string{"allow-start", "kernel-version", "network", "udev", "udev-init", "acpi", "logs", "clock", "daemon", "resource", "compute"}

func normalSystemRoleServiceRoleCode(role string) (uint32, error) {
	for i, v := range normalSystemRoleServiceRoles {
		if v == role {
			return uint32(i + 1), nil
		}
	}
	return 0, fmt.Errorf("unsupported fixed System role")
}
func normalSystemRoleServiceRoleName(code uint32) (string, error) {
	if code == 0 || code > uint32(len(normalSystemRoleServiceRoles)) {
		return "", fmt.Errorf("unsupported fixed System role")
	}
	return normalSystemRoleServiceRoles[code-1], nil
}

type normalSystemRoleServiceFrame struct {
	Operation, Role           uint32
	BrokerNonce, RequestNonce [32]byte
}

func decodeNormalSystemRoleServiceFrame(b []byte) (normalSystemRoleServiceFrame, error) {
	var r normalSystemRoleServiceFrame
	if len(b) != normalSystemRoleServiceRequestBytes || string(b[:8]) != normalSystemRoleServiceRequestMagic || binary.BigEndian.Uint32(b[8:12]) != 1 {
		return r, fmt.Errorf("fixed System role frame differs")
	}
	r.Operation = binary.BigEndian.Uint32(b[12:16])
	r.Role = binary.BigEndian.Uint32(b[16:20])
	for _, v := range b[20:32] {
		if v != 0 {
			return r, fmt.Errorf("fixed System role reserved bytes differ")
		}
	}
	if r.Operation != normalSystemRoleServiceExecute && r.Operation != normalSystemRoleServiceProbeReady {
		return r, fmt.Errorf("fixed System role operation unsupported")
	}
	if r.Operation == normalSystemRoleServiceExecute {
		if _, e := normalSystemRoleServiceRoleName(r.Role); e != nil {
			return r, e
		}
	} else if r.Role != 0 {
		return r, fmt.Errorf("ready probe cannot choose role")
	}
	copy(r.BrokerNonce[:], b[32:64])
	copy(r.RequestNonce[:], b[64:96])
	if r.BrokerNonce == [32]byte{} || r.RequestNonce == [32]byte{} {
		return r, fmt.Errorf("fixed System role nonce absent")
	}
	return r, nil
}
func encodeNormalSystemRoleServiceFrame(r normalSystemRoleServiceFrame) []byte {
	b := make([]byte, normalSystemRoleServiceRequestBytes)
	copy(b, normalSystemRoleServiceRequestMagic)
	binary.BigEndian.PutUint32(b[8:12], 1)
	binary.BigEndian.PutUint32(b[12:16], r.Operation)
	binary.BigEndian.PutUint32(b[16:20], r.Role)
	copy(b[32:64], r.BrokerNonce[:])
	copy(b[64:96], r.RequestNonce[:])
	return b
}
func normalSystemRoleServiceReply(r normalSystemRoleServiceFrame, status, exit uint32) []byte {
	b := make([]byte, normalSystemRoleServiceRequestBytes)
	copy(b, normalSystemRoleServiceReplyMagic)
	binary.BigEndian.PutUint32(b[8:12], 1)
	binary.BigEndian.PutUint32(b[12:16], status)
	binary.BigEndian.PutUint32(b[16:20], r.Role)
	binary.BigEndian.PutUint32(b[20:24], exit)
	copy(b[32:64], r.BrokerNonce[:])
	copy(b[64:96], r.RequestNonce[:])
	return b
}
func decodeNormalSystemRoleServiceReply(raw []byte, r normalSystemRoleServiceFrame) (uint32, uint32, error) {
	if len(raw) != normalSystemRoleServiceRequestBytes || string(raw[:8]) != normalSystemRoleServiceReplyMagic || binary.BigEndian.Uint32(raw[8:12]) != 1 || binary.BigEndian.Uint32(raw[16:20]) != r.Role {
		return 0, 0, fmt.Errorf("fixed System role reply differs")
	}
	for _, v := range raw[24:32] {
		if v != 0 {
			return 0, 0, fmt.Errorf("fixed System reply reserved bytes differ")
		}
	}
	var a, b [32]byte
	copy(a[:], raw[32:64])
	copy(b[:], raw[64:96])
	if a != r.BrokerNonce || b != r.RequestNonce {
		return 0, 0, fmt.Errorf("fixed System reply original nonce differs")
	}
	status := binary.BigEndian.Uint32(raw[12:16])
	exit := binary.BigEndian.Uint32(raw[20:24])
	if status > normalSystemRoleServiceUnknown || exit > 255 {
		return 0, 0, fmt.Errorf("fixed System role reply status differs")
	}
	return status, exit, nil
}
