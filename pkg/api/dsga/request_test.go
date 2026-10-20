package dsga

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 与自有Neon 6e63d8d的libs/compute_api/testdata/dsg_request_v1.json逐字节相同；
// C正式管理SQL、Rust和Go共享高位向量。hash固定来源，避免测试数据被随实现改写。
func goldenRequest(t testing.TB) ([]byte, Request) {
	t.Helper()
	data, err := os.ReadFile("testdata/request-v1.json")
	require.NoError(t, err)
	hash := sha256.Sum256(data)
	require.Equal(t, "91a4adf021a7078a3a11edb0a4084ac0165d5d83fc44ac5a212be69eb968d006", hex.EncodeToString(hash[:]))
	var request Request
	require.NoError(t, json.Unmarshal(data, &request))
	return data, request
}

func TestSharedNeonRequestVectorKeepsEveryUnsignedBit(t *testing.T) {
	_, request := goldenRequest(t)
	require.Equal(t, Uint64(math.MaxUint64), request.Key.Instance.PGBootID)
	require.Equal(t, Uint64(9007199254740993), request.Key.IssuerEpoch)
	require.Equal(t, Uint64(1<<63), request.Key.Revision)
	require.Equal(t, Uint64(18364758544493064720), request.Key.OperationID.High)
	require.Equal(t, Uint64(81985529216486895), request.Key.OperationID.Low)
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	var again Request
	require.NoError(t, json.Unmarshal(encoded, &again))
	require.Equal(t, request, again)
	bytes := [16]byte{0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	require.Equal(t, OperationIDFromBytes(bytes), request.Key.OperationID)
}

func TestUint64RejectsLossyAndNoncanonicalRepresentations(t *testing.T) {
	for _, value := range []string{"0", "1", "9007199254740991", "9007199254740992", "9007199254740993", "9223372036854775808", "18446744073709551615"} {
		t.Run(value, func(t *testing.T) {
			var number Uint64
			require.NoError(t, json.Unmarshal([]byte(`"`+value+`"`), &number))
			encoded, err := json.Marshal(number)
			require.NoError(t, err)
			require.Equal(t, `"`+value+`"`, string(encoded))
		})
	}
	for _, raw := range []string{`0`, `9007199254740993`, `null`, `true`, `""`, `"00"`, `"01"`, `"+1"`, `"-1"`, `" 1"`, `"1 "`, `"1.0"`, `"1e0"`, `"１"`, `"18446744073709551616"`, `"111111111111111111111"`} {
		value := Uint64(7)
		require.Error(t, json.Unmarshal([]byte(raw), &value), raw)
		require.Equal(t, Uint64(7), value)
	}
}

func TestRequestRejectsDuplicateUnknownMissingAndCaseFoldedFields(t *testing.T) {
	data, original := goldenRequest(t)
	for _, raw := range []string{
		strings.Replace(string(data), "{", `{"protocol_version":1,`, 1),
		strings.Replace(string(data), "{", `{"PROTOCOL_VERSION":1,`, 1),
		strings.Replace(string(data), "{", `{"unexpected":1,`, 1),
		strings.Replace(string(data), `"protocol_version"`, `"Protocol_Version"`, 1),
		strings.Replace(string(data), `"protocol_version": 1`, `"protocol_version": 1.0`, 1),
		strings.Replace(string(data), `"high":`, `"high":"1","high":`, 1),
		strings.Replace(string(data), `"high":`, `"\u0068igh":"1","high":`, 1),
		strings.Replace(string(data), `"issuer_epoch": "9007199254740993"`, `"issuer_epoch": 9007199254740993`, 1),
		string(data) + ` {}`,
		`null`, `[]`, `"object"`, string([]byte{'{', '"', 0xff, '"', ':', '1', '}'}),
	} {
		request := original
		require.Error(t, json.Unmarshal([]byte(raw), &request), raw)
		require.Equal(t, original, request, "failed decoding must not partially overwrite intent")
	}
	// 所有层级必填，包括允许单半部为0的operation_id.high/low。先使用RawMessage
	// 改单个合法向量而不经过float，逐层核对缺失拒绝。
	var object map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &object))
	for field := range object {
		remainingFields := make(map[string]json.RawMessage)
		for k, v := range object {
			if k != field {
				remainingFields[k] = v
			}
		}
		body, err := json.Marshal(remainingFields)
		require.NoError(t, err)
		var request Request
		require.Error(t, json.Unmarshal(body, &request), field)
	}
	for _, field := range []string{"high", "low"} {
		bad := strings.Replace(string(data), `"`+field+`":`, `"wrong_`+field+`":`, 1)
		var request Request
		require.Error(t, json.Unmarshal([]byte(bad), &request))
	}
}

func TestRequestSemanticLimitsAndExpiredReplayAreExplicit(t *testing.T) {
	_, original := goldenRequest(t)
	for _, change := range []func(*Request){
		func(r *Request) { r.ProtocolVersion = 2 }, func(r *Request) { r.Kind = "other" },
		func(r *Request) { r.Key.Instance.VMUID = "" }, func(r *Request) { r.Key.Instance.ComputeInstanceID = "bad id" },
		func(r *Request) { r.Key.Instance.PGBootID = 0 }, func(r *Request) { r.Key.IssuerEpoch = 0 }, func(r *Request) { r.Key.Revision = 0 },
		func(r *Request) { r.Key.OperationID = OperationID{High: 0, Low: 0} }, func(r *Request) { r.ExpectedCapacityGeneration = 0 },
		func(r *Request) { r.TargetBuffers = 15 }, func(r *Request) { r.TargetBuffers = 1 << 30 }, func(r *Request) { r.TargetVMMemoryBytes = 0 },
		func(r *Request) { r.BudgetCommitmentID = strings.Repeat("x", 129) }, func(r *Request) { r.DeadlineUnixMillis = 0 },
	} {
		request := original
		change(&request)
		require.Error(t, request.Validate())
		body, err := json.Marshal(request)
		require.NoError(t, err)
		var decoded Request
		require.Error(t, json.Unmarshal(body, &decoded))
	}
	request := original
	request.DeadlineUnixMillis = 1
	// 历史重放不因墙钟已过期而在结构解析阶段消失，先查幂等/历史，再拒绝新执行。
	encoded, err := json.Marshal(request)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(encoded, &request))
	_, ok := request.RemainingWaitMillis(math.MaxUint64)
	require.False(t, ok)
	request.DeadlineUnixMillis = Uint64(math.MaxUint64)
	wait, ok := request.RemainingWaitMillis(0)
	require.True(t, ok)
	require.EqualValues(t, 60000, wait)
	wait, ok = request.RemainingWaitMillis(math.MaxUint64 - 1)
	require.True(t, ok)
	require.EqualValues(t, 1, wait)
	_, ok = request.RemainingWaitMillis(math.MaxUint64)
	require.False(t, ok)
}

func TestQueryIsBoundToFullOriginalIdentity(t *testing.T) {
	_, request := goldenRequest(t)
	query := Query{ProtocolVersion: ProtocolVersion, Key: request.Key}
	body, err := json.Marshal(query)
	require.NoError(t, err)
	var again Query
	require.NoError(t, json.Unmarshal(body, &again))
	require.Equal(t, query, again)
	body = []byte(strings.Replace(string(body), `"key":`, `"extra":1,"key":`, 1))
	require.Error(t, json.Unmarshal(body, &again))
}

func TestBoundedStrictJSONDoesNotAcceptTrailingOrDeepData(t *testing.T) {
	data, _ := goldenRequest(t)
	_, err := DecodeRequest(append([]byte(strings.Repeat(" ", 16*1024)), data...))
	require.Error(t, err)
	type fixture struct {
		Value any `json:"value"`
	}
	var value fixture
	require.Error(t, UnmarshalObject([]byte(`{"value":`+strings.Repeat("[", 34)+`0`+strings.Repeat("]", 34)+`}`), &value))
	require.Error(t, UnmarshalObject([]byte(`{"value":[`+strings.Repeat("0,", 2048)+`0]}`), &value))
	require.Error(t, UnmarshalObject([]byte(`{"value":1} {}`), &value))
	require.Error(t, UnmarshalObject([]byte(`{"value":1}`), nil))
	var recursive Request
	require.Error(t, UnmarshalObject(data, &recursive))
	require.NoError(t, UnmarshalObject([]byte(`{"value":{"a":1,"b":{"a":2}}}`), &value))
	require.NoError(t, UnmarshalObject([]byte(`{"value":9007199254740993}`), &value))
	require.Equal(t, json.Number("9007199254740993"), value.Value)
	require.Error(t, UnmarshalObject([]byte(`{"value":{"a":1,"a":2}}`), &value))
}

func TestSecureOperationIDsAreNonzeroAndDoNotUsePropagationRevision(t *testing.T) {
	first, err := NewOperationID()
	require.NoError(t, err)
	second, err := NewOperationID()
	require.NoError(t, err)
	require.NotEqual(t, OperationID{High: 0, Low: 0}, first)
	require.NotEqual(t, first, second)
}

func FuzzRequestJSON(f *testing.F) {
	data, _ := goldenRequest(f)
	for _, seed := range [][]byte{data, []byte(`null`), []byte(`{"key":{"issuer_epoch":9007199254740993}}`)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		request, err := DecodeRequest(data)
		if err != nil {
			return
		}
		require.NoError(t, request.Validate())
		body, err := json.Marshal(request)
		require.NoError(t, err)
		var again Request
		require.NoError(t, json.Unmarshal(body, &again))
		require.Equal(t, request, again)
	})
}
