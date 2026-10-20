package journal

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

func TestJournalCodecRoundTripIntegrityAndIsolation(t *testing.T) {
	model := testModel(t)
	model = completeModel(t, beginModel(t, model, testRequest(model, 1, control.Grow)))
	trimmed, data, err := encode(model, MaxEncodedBytes)
	require.NoError(t, err)
	again, err := decode(data, model.VMUID, testIncarnation)
	require.NoError(t, err)
	require.Equal(t, trimmed, again)
	_, err = decode(data, "another-vm", testIncarnation)
	require.Error(t, err)
	_, err = decode(data, model.VMUID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.Error(t, err)
	var wrapped envelope
	require.NoError(t, json.Unmarshal([]byte(data), &wrapped))
	wrapped.SHA256 = strings.Repeat("0", 64)
	bad, err := json.Marshal(wrapped)
	require.NoError(t, err)
	_, err = decode(string(bad), model.VMUID, testIncarnation)
	require.Error(t, err)
	require.NoError(t, json.Unmarshal([]byte(data), &wrapped))
	wrapped.Data = append(wrapped.Data, 1)
	bad, err = json.Marshal(wrapped)
	require.NoError(t, err)
	_, err = decode(string(bad), model.VMUID, testIncarnation)
	require.ErrorContains(t, err, "trailing")
}

func TestJournalCodecDecompressionBombAndUnknownEnvelopeAreRejected(t *testing.T) {
	var buffer bytes.Buffer
	writer := zlib.NewWriter(&buffer)
	_, err := writer.Write(bytes.Repeat([]byte("x"), MaxRawBytes+1))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	hash := sha256.Sum256(bytes.Repeat([]byte("x"), MaxRawBytes+1))
	wrapped := envelope{Version: 1, Encoding: "zlib-v1", Incarnation: testIncarnation, SHA256: hex.EncodeToString(hash[:]), Data: buffer.Bytes()}
	data, err := json.Marshal(wrapped)
	require.NoError(t, err)
	_, err = decode(string(data), "vm-uid", testIncarnation)
	require.ErrorContains(t, err, "decompression")
	model := testModel(t)
	_, dataString, err := encode(model, MaxEncodedBytes)
	require.NoError(t, err)
	_, err = decode(strings.Replace(dataString, "{", `{"version":1,`, 1), "vm-uid", testIncarnation)
	require.Error(t, err)
	_, err = decode(strings.Replace(dataString, "{", `{"extra":1,`, 1), "vm-uid", testIncarnation)
	require.Error(t, err)
}

func TestJournalCodecBytePressureOnlyExpiresTerminalHistory(t *testing.T) {
	model := testModel(t)
	for i := uint64(1); i <= HistoryLimit; i++ {
		model = completeModel(t, beginModel(t, model, testRequest(model, i, control.Shrink)))
	}
	model = beginModel(t, model, testRequest(model, HistoryLimit+1, control.Grow))
	var err error
	model, err = model.Start(model.Owner, model.Active.Request.Key, PrepareBudget, "budget", testCommand)
	require.NoError(t, err)
	_, full, err := encode(model, MaxEncodedBytes)
	require.NoError(t, err)
	// 不依赖压缩比猜测固定阈值：逐步收紧到首次发生淘汰，验证活动意图和未知区间
	// 一字不变，高水位仍保留。只验证持久状态，不宣称Provider/VM物理回收。
	var reduced Model
	found := false
	for budget := len(full) + 4095; budget > 4096; budget -= 64 {
		candidate, _, err := encode(model, budget)
		if err == nil && len(candidate.History) < len(model.History) {
			reduced = candidate
			found = true
			break
		}
	}
	require.True(t, found)
	require.Equal(t, model.Active, reduced.Active)
	require.Equal(t, model.HighWater, reduced.HighWater)
	require.Greater(t, reduced.ExpiredResults, model.ExpiredResults)
	_, _, err = encode(model, 4096)
	require.Error(t, err)
}

// 同时变异原始封装与已重算checksum的解压后JSON。若只变异压缩封装，大部分输入
// 会停在checksum/zlib之前，无法覆盖恢复时对完整模型的结构/身份/必填约束检查。
// 这里的成功只表示账本可以安全恢复，不表示回执来源或任何物理动作已经认证。
func FuzzJournalDecodeRecovery(f *testing.F) {
	model := testModel(f)
	raw, err := json.Marshal(model)
	require.NoError(f, err)
	_, encoded, err := encode(model, MaxEncodedBytes)
	require.NoError(f, err)
	f.Add(raw, false)
	f.Add([]byte(encoded), true)
	active := beginModel(f, model, testRequest(model, 1, control.Shrink))
	raw, err = json.Marshal(active)
	require.NoError(f, err)
	f.Add(raw, false)
	f.Add([]byte(`{"active":null,"active":{}}`), false)
	f.Fuzz(func(t *testing.T, data []byte, outer bool) {
		if len(data) > MaxRawBytes {
			return
		}
		input := data
		if !outer {
			var buffer bytes.Buffer
			writer := zlib.NewWriter(&buffer)
			_, err := writer.Write(data)
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			hash := sha256.Sum256(data)
			input, err = json.Marshal(envelope{Version: 1, Encoding: "zlib-v1", Incarnation: testIncarnation, SHA256: hex.EncodeToString(hash[:]), Data: buffer.Bytes()})
			require.NoError(t, err)
		}
		recovered, err := decode(string(input), model.VMUID, testIncarnation)
		if err != nil {
			return
		}
		require.NoError(t, recovered.Validate())
		roundTrip, err := json.Marshal(recovered)
		require.NoError(t, err)
		var restored Model
		require.NoError(t, json.Unmarshal(roundTrip, &restored))
		require.Equal(t, recovered, restored)
		trimmed, encoded, err := encode(recovered, MaxEncodedBytes)
		if err != nil {
			// 合法解码结果未必为下一次写入预留了4KiB关闭空间；写入可以因字节预算
			// 拒绝，但不能因失败修改原模型或丢弃未确认动作。
			require.Equal(t, restored, recovered)
			return
		}
		again, err := decode(encoded, model.VMUID, testIncarnation)
		require.NoError(t, err)
		require.Equal(t, trimmed, again)
	})
}
