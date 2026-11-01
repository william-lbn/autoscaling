//go:build linux

package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrdinaryTLSRootChecksumCanonicalReply(t *testing.T) {
	expected := base64.RawStdEncoding.EncodeToString(make([]byte, 32))
	sum, e := ordinaryTLSRootChecksumReply([]byte(expected))
	require.NoError(t, e)
	require.Equal(t, expected, sum)
	for _, bad := range []string{"", expected + "=", expected + "x", expected[:42], "../../caller/path", base64.RawStdEncoding.EncodeToString(make([]byte, 31))} {
		_, e := ordinaryTLSRootChecksumReply([]byte(bad))
		require.Error(t, e)
	}
}
func TestOrdinaryTLSRootReplyRejectsOtherActionsAndPartialACK(t *testing.T) {
	ack := func(action uint32, body []byte) []byte {
		b := make([]byte, 16+len(body))
		copy(b, "NDSGOAOK")
		binary.BigEndian.PutUint32(b[8:12], 2)
		binary.BigEndian.PutUint32(b[12:16], action)
		copy(b[16:], body)
		return b
	}
	_, e := ordinaryTLSRootReply(ack(2, nil), 2)
	require.NoError(t, e)
	expected := []byte(base64.RawStdEncoding.EncodeToString(make([]byte, 32)))
	body, e := ordinaryTLSRootReply(ack(4, expected), 4)
	require.NoError(t, e)
	require.Equal(t, expected, body)
	for _, raw := range [][]byte{nil, ack(2, nil)[:15], append(ack(2, nil), 0), ack(1, nil), ack(3, nil), ack(4, nil), ack(4, expected)} {
		_, e := ordinaryTLSRootReply(raw, 2)
		require.Error(t, e)
	}
	raw := ack(2, nil)
	raw[0] = 'x'
	_, e = ordinaryTLSRootReply(raw, 2)
	require.Error(t, e)
	raw = ack(2, nil)
	binary.BigEndian.PutUint32(raw[8:12], 1)
	_, e = ordinaryTLSRootReply(raw, 2)
	require.Error(t, e)
}
func TestOrdinaryTLSRootMissingIdentityNeverDials(t *testing.T) {
	if os.Getuid() == 0 {
		delegate, e := newOrdinaryTLSRootDelegate()
		require.Error(t, e)
		require.Nil(t, delegate)
	}
	for _, tc := range []struct {
		action uint32
		body   []byte
	}{{1, nil}, {3, []byte("x")}, {2, nil}, {4, []byte("path")}, {2, make([]byte, (64<<10)+1)}} {
		_, e := ordinaryTLSRootExchange(context.Background(), tc.action, tc.body)
		require.Error(t, e)
	}
	c := &ordinaryTLSRootClient{}
	require.Error(t, c.replaceManagementTLS(context.Background(), ordinaryTLSReplacement{ProtocolVersion: 1, Operation: "replace-management-tls"}))
	require.Error(t, c.replaceManagementTLS(context.Background(), ordinaryTLSReplacement{ProtocolVersion: 2, Operation: "caller-operation"}))
}
