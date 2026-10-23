package dsga

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// ContextSHA256复现Neon管理SQL对本协议Request的jsonb_out文本摘要。仅适用于
// 经过Validate的固定schema：所有ID为ASCII graphic，数值为无指数uint32或字符串。
// 对象按键的字节长度/字节序排序，分隔符含空格。不是通用PostgreSQL JSONB编码器，
// 不能用于任意Unicode/浮点/重复键数据，也不同于journal的领域分离摘要。
func (request Request) ContextSHA256() (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	return contextSHA256(request)
}

// 仅供已经完成固定schema校验的控制意图复用；准入增加bool字面量支持，
// 不改变已有resize对象排序/分隔/字符串和整数规则。
func contextSHA256(request any) (string, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	var canonical bytes.Buffer
	if err := writeContext(&canonical, value); err != nil {
		return "", err
	}
	hash := sha256.Sum256(canonical.Bytes())
	return hex.EncodeToString(hash[:]), nil
}

func writeContext(output *bytes.Buffer, value any) error {
	switch value := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if len(keys[i]) != len(keys[j]) {
				return len(keys[i]) < len(keys[j])
			}
			return keys[i] < keys[j]
		})
		output.WriteByte('{')
		for i, key := range keys {
			if i != 0 {
				output.WriteString(", ")
			}
			output.WriteString(strconv.Quote(key))
			output.WriteString(": ")
			if err := writeContext(output, value[key]); err != nil {
				return err
			}
		}
		output.WriteByte('}')
	case string:
		output.WriteString(strconv.Quote(value))
	case bool:
		output.WriteString(strconv.FormatBool(value))
	case json.Number:
		if _, err := strconv.ParseUint(string(value), 10, 32); err != nil {
			return fmt.Errorf("context requires canonical uint32 numbers")
		}
		output.WriteString(string(value))
	default:
		return fmt.Errorf("unsupported DSGA context value")
	}
	return nil
}
