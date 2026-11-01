package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// 仅中立有界请求接口；没有dial/uid/PID/路径/来源摘要参数，不能自行产生授权。
// 后继Root adapter须绑定原daemon clone pidfd/private lease，并保AtomicWriter轮转。
type ordinaryTLSReplacement struct {
	ProtocolVersion uint32          `json:"protocol_version"`
	Operation       string          `json:"operation"`
	Files           map[string]File `json:"files"`
}
type ordinaryTLSRootDelegate interface {
	replaceManagementTLS(context.Context, ordinaryTLSReplacement) error
	managementTLSChecksum(context.Context) (string, error)
}

var errOrdinaryTLSRootDelegateUnavailable = errors.New("original ordinary daemon role lease and fixed Root TLS delegate unavailable")

func ordinaryTLSDelegateUnsupported() (ordinaryTLSRootDelegate, error) {
	return nil, errOrdinaryTLSRootDelegateUnavailable
}
func decodeOrdinaryTLSReplacement(raw []byte) (ordinaryTLSReplacement, error) {
	var z ordinaryTLSReplacement
	if control.ValidateJSON(raw, 64<<10, 4096) != nil {
		return z, fmt.Errorf("bounded original TLS object required")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) < 1 || len(fields) > 3 {
		return z, fmt.Errorf("fixed TLS files required")
	}
	out := ordinaryTLSReplacement{ProtocolVersion: 2, Operation: "replace-management-tls", Files: map[string]File{}}
	for name, v := range fields {
		if name != "tls.crt" && name != "tls.key" && name != "ca.crt" {
			return z, fmt.Errorf("unknown fixed TLS file")
		}
		var file File
		if e := control.UnmarshalObject(v, &file); e != nil {
			return z, e
		}
		if _, e := base64.StdEncoding.DecodeString(file.Data); e != nil {
			return z, fmt.Errorf("invalid TLS data encoding")
		}
		out.Files[name] = file
	}
	return out, nil
}
