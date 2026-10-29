package journal

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

const MaxEncodedBytes = 192 * 1024

type envelope struct {
	Version     uint32 `json:"version"`
	Encoding    string `json:"encoding"`
	Incarnation string `json:"incarnation"`
	SHA256      string `json:"sha256"`
	Data        []byte `json:"data"`
}

// 压缩只减少元数据成本，不是能力/真实性证明。校验原始长度和SHA后仍检查完整
// 有类型模型。任何损坏均失败关闭，不把无法解析的账本当成空闲状态重新初始化。
func encode(model Model, budget int) (Model, string, error) {
	if budget < 1 || budget > MaxEncodedBytes {
		return model, "", fmt.Errorf("invalid journal byte budget")
	}
	// 服务关闭有独立WAL，即使无Resize Active，也必须为原生关闭回执及
	// owner接管保留既有4KiB余量。不能先发出关闭、再发现注解只能保存
	// 写前命令而无法落账；淘汰规则仍只处理历史，不裁掉未知或原预算。
	if model.ServiceClosure != nil && model.ServiceClosure.Reopen != nil {
		// 开放后可能立即发现来源异常，必须在发送前就预留原回执与
		// 独立再关闭的完整空间。关闭也须保留原命令和未知，不能为了
		// 落账压掉旧预算/关闭。已确认开放仍保留紧急关闭窗口。
		r := model.ServiceClosure.Reopen
		window := MaxObservationBytes + 4096
		switch {
		case r.Rearm != nil && r.Rearm.Phase == Unconfirmed:
			// 再关闭后的期限恢复仍需完整原生历史/当前回执；不能沿用
			// 已完成关闭仅4KiB的默认余量，先发送再发现无法保存事实。
			budget -= window
		case r.Closing == nil && r.Phase == Unconfirmed:
			// 原开放回执 + 再关闭观察 + 更高 owner 接续观察 + 最后回执。
			budget -= 4 * window
		case r.Closing == nil:
			budget -= 3 * window
		case r.Closing.Close == nil || r.Closing.Close.Pending() && r.Closing.LatestBefore == nil:
			budget -= 2 * window
		case r.Closing.Close.Pending():
			budget -= window
		default:
			budget -= 4096
		}
	} else if model.ServiceClosure != nil && model.ServiceClosure.Rearm != nil && model.ServiceClosure.Rearm.Phase == Unconfirmed {
		// rearm回执同时携带历史应用和独立当前快照，不能只给已持有的
		// 原关闭增加4KiB。按完整HTTP观察上限另留空间，保证写前票据不会
		// 消耗原回执/接管所需容量；不裁原预算、关闭或期限未知。
		budget -= MaxObservationBytes + 4096
	} else if model.Active != nil || model.ServiceClosure != nil && model.ServiceClosure.Pending() {
		budget -= 4096
	}
	model = model.Clone()
	if err := model.Validate(); err != nil {
		return model, "", err
	}
	for {
		raw, err := json.Marshal(model)
		if err != nil {
			return model, "", err
		}
		if len(raw) > MaxRawBytes {
			if model.Active == nil && len(model.History) <= 1 {
				return model, "", fmt.Errorf("latest terminal result exceeds raw journal limit")
			}
			if err := model.expireOldest(); err != nil {
				return model, "", err
			}
			continue
		}
		var buffer bytes.Buffer
		writer := zlib.NewWriter(&buffer)
		if _, err := writer.Write(raw); err != nil {
			return model, "", err
		}
		if err := writer.Close(); err != nil {
			return model, "", err
		}
		hash := sha256.Sum256(raw)
		encoded, err := json.Marshal(envelope{Version: 1, Encoding: "zlib-v1", Incarnation: model.Incarnation, SHA256: hex.EncodeToString(hash[:]), Data: buffer.Bytes()})
		if err != nil {
			return model, "", err
		}
		if len(encoded) <= budget {
			return model, string(encoded), nil
		}
		// 只淘汰终态，保留Active、完整高水位与过期计数，旧请求不可再执行。历史最多
		// 16条，同时受字节界限；不能为保存新证据裁掉未确认动作或预算引用。
		if model.Active == nil && len(model.History) <= 1 {
			return model, "", fmt.Errorf("latest terminal result exceeds journal byte budget")
		}
		if err := model.expireOldest(); err != nil {
			return model, "", err
		}
	}
}

func decode(data string, vmUID, incarnation string) (Model, error) {
	var wrapped envelope
	if err := control.UnmarshalObjectBounded([]byte(data), &wrapped, MaxEncodedBytes, 2048); err != nil {
		return Model{}, err
	}
	if wrapped.Version != 1 || wrapped.Encoding != "zlib-v1" || wrapped.Incarnation != incarnation || !validIncarnation(incarnation) || len(wrapped.SHA256) != 64 {
		return Model{}, fmt.Errorf("invalid journal envelope identity")
	}
	compressed := bytes.NewReader(wrapped.Data)
	reader, err := zlib.NewReader(compressed)
	if err != nil {
		return Model{}, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(reader, MaxRawBytes+1))
	closeErr := reader.Close()
	if readErr != nil {
		return Model{}, readErr
	}
	if closeErr != nil {
		return Model{}, closeErr
	}
	if len(raw) > MaxRawBytes {
		return Model{}, fmt.Errorf("journal decompression limit exceeded")
	}
	if compressed.Len() != 0 {
		return Model{}, fmt.Errorf("trailing compressed journal data")
	}
	hash := sha256.Sum256(raw)
	if wrapped.SHA256 != hex.EncodeToString(hash[:]) {
		return Model{}, fmt.Errorf("journal checksum differs")
	}
	var model Model
	if err := json.Unmarshal(raw, &model); err != nil {
		return Model{}, err
	}
	if model.VMUID != vmUID || model.Incarnation != incarnation {
		return Model{}, fmt.Errorf("journal binding differs")
	}
	return model, model.Validate()
}

// 每层必填/nullable字段均显式存在，不能让Go的缺省零值伪装成Planned或无在途动作。
func (value *Owner) UnmarshalJSON(data []byte) error {
	type plain Owner
	var decoded plain
	if err := control.UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	next := Owner(decoded)
	if err := next.Validate(); err != nil {
		return err
	}
	*value = next
	return nil
}

func (value *Observation) UnmarshalJSON(data []byte) error {
	type plain Observation
	var decoded plain
	if err := control.UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	next := Observation(decoded)
	if err := next.Validate(); err != nil {
		return err
	}
	*value = next
	return nil
}

func (value *Step) UnmarshalJSON(data []byte) error {
	type plain Step
	var decoded plain
	if err := control.UnmarshalObject(data, &decoded); err != nil {
		return err
	}
	*value = Step(decoded)
	return nil
}

func (value *Operation) UnmarshalJSON(data []byte) error {
	type plain Operation
	var decoded plain
	if err := control.UnmarshalObjectBounded(data, &decoded, MaxRawBytes, 16384); err != nil {
		return err
	}
	next := Operation(decoded)
	if err := next.Validate(); err != nil {
		return err
	}
	*value = next
	return nil
}

func (value *Record) UnmarshalJSON(data []byte) error {
	type plain Record
	var decoded plain
	if err := control.UnmarshalObjectBounded(data, &decoded, MaxRawBytes, 16384); err != nil {
		return err
	}
	*value = Record(decoded)
	return nil
}

func (value *Model) UnmarshalJSON(data []byte) error {
	type plain Model
	var decoded plain
	if err := control.UnmarshalObjectBounded(data, &decoded, MaxRawBytes, 16384); err != nil {
		return err
	}
	next := Model(decoded)
	if err := next.Validate(); err != nil {
		return err
	}
	*value = next
	return nil
}
