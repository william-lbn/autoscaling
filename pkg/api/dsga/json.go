// Package dsga 定义与Neon正式管理SQL/HTTP相同的独立协议，不把旧Monitor的Ok或
// VM传播Revision解释为内存许可。所有64位值以规范字符串传输，拒绝静默丢失字段。
package dsga

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Uint64的JSON只能是无符号规范十进制字符串；0是否允许由所在业务字段决定。
// 不用encoding/json的,string选项，因为还须显式拒绝前导零、符号和其他模糊表示。
type Uint64 uint64

func (v Uint64) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatUint(uint64(v), 10) + `"`), nil
}

func (v *Uint64) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return fmt.Errorf("DSGA uint64 requires a string: %w", err)
	}
	if len(text) == 0 || len(text) > 20 || (len(text) > 1 && text[0] == '0') {
		return fmt.Errorf("noncanonical DSGA uint64")
	}
	for _, b := range []byte(text) {
		if b < '0' || b > '9' {
			return fmt.Errorf("nondecimal DSGA uint64")
		}
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return fmt.Errorf("DSGA uint64 exceeds its range: %w", err)
	}
	*v = Uint64(value)
	return nil
}

// UnmarshalObject供本协议的显式UnmarshalJSON使用，into必须是无方法的结构别名，
// 否则会递归调用自身。先按原始token拒绝任意层重复键，再只用RawMessage检查必填
// 与精确字段名，最后从原始字节做有类型解码；绝不经float/map归一化意图。
// 所有无omitempty的字段（包括可为null的指针）必须出现；Go默认的大小写折叠也拒绝。
// 最多64KiB/32层/2048个值节点，供有界持久状态复用；单次Resize原始入口另限制16KiB。
func UnmarshalObject(data []byte, into any) error {
	return UnmarshalObjectBounded(data, into, 64*1024, 2048)
}

// 持久账本有多条完整历史，显式扩大对象边界；请求入口仍使用原64KiB/2048默认值。
// 上限封顶1MiB/65536个值，禁止以可配置的无限制解码破坏内存边界。
func UnmarshalObjectBounded(data []byte, into any, maxBytes, maxValues int) error {
	if err := ValidateJSON(data, maxBytes, maxValues); err != nil {
		return err
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("DSGA value requires an object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	destinationType := reflect.TypeOf(into)
	if destinationType == nil || destinationType.Kind() != reflect.Pointer || destinationType.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("DSGA decoder requires a struct alias pointer")
	}
	if _, recursive := into.(json.Unmarshaler); recursive {
		return fmt.Errorf("DSGA decoder requires a plain struct alias without UnmarshalJSON")
	}
	allowed := make(map[string]bool)
	for i := 0; i < destinationType.Elem().NumField(); i++ {
		field := destinationType.Elem().Field(i)
		if !field.IsExported() {
			continue
		}
		tag := strings.Split(field.Tag.Get("json"), ",")
		if tag[0] == "" || tag[0] == "-" || allowed[tag[0]] {
			return fmt.Errorf("DSGA schema requires unique explicit JSON field names")
		}
		optional := false
		for _, flag := range tag[1:] {
			optional = optional || flag == "omitempty"
		}
		allowed[tag[0]] = true
		raw, present := fields[tag[0]]
		if !present && !optional {
			return fmt.Errorf("missing DSGA field %q", tag[0])
		}
		// encoding/json会把null布尔/整数静默变成零值，Rust/C则不接受。可为null
		// 的协议字段必须显式使用指针；字节slice的null由各模型的Planned语义处理。
		if present && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) && field.Type.Kind() != reflect.Pointer && field.Type.Kind() != reflect.Slice {
			return fmt.Errorf("nonnull DSGA field %q is null", tag[0])
		}
	}
	for key := range fields {
		if !allowed[key] {
			return fmt.Errorf("unknown DSGA field %q", key)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	return decoder.Decode(into)
}

// 只检查有界原始JSON/任意层重复键，不把此检查当作领域回执已被核验。
func ValidateJSON(data []byte, maxBytes, maxValues int) error {
	if maxBytes < 1 || maxBytes > 1024*1024 || maxValues < 1 || maxValues > 65536 || len(data) > maxBytes || !utf8.Valid(data) {
		return fmt.Errorf("invalid DSGA JSON size or UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	tokens := 0
	if err := uniqueValue(decoder, 0, &tokens, maxValues); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("trailing DSGA JSON data")
	}
	return nil
}

func uniqueValue(decoder *json.Decoder, depth int, tokens *int, maxValues int) error {
	*tokens += 1
	if depth > 32 || *tokens > maxValues {
		return fmt.Errorf("DSGA JSON exceeds structural limits")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("non-string DSGA JSON key")
			}
			if seen[key] {
				return fmt.Errorf("duplicate DSGA field %q", key)
			}
			seen[key] = true
			if err := uniqueValue(decoder, depth+1, tokens, maxValues); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := uniqueValue(decoder, depth+1, tokens, maxValues); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected DSGA JSON delimiter")
	}
	_, err = decoder.Token()
	return err
}
