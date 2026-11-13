// Package runnerdiagnostic只描述失败位置，不产生运行、退出或资源释放许可。
package runnerdiagnostic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const MaxBytes = 4096
const TerminationPath = "/dev/termination-log"

type Stage string

const (
	StageUnknown          Stage = "unknown"
	StageConfig           Stage = "config"
	StageDecodeInput      Stage = "decodeInput"
	StageRuntimeAssets    Stage = "runtimeAssets"
	StageRuntimeBoot      Stage = "runtimeBoot"
	StageRuntimeSpec      Stage = "runtimeSpec"
	StageRunnerBirth      Stage = "runnerBirth"
	StageInitScript       Stage = "initScript"
	StageRuntimeISO       Stage = "runtimeISO"
	StageRootDisk         Stage = "rootDisk"
	StageCommandBuild     Stage = "commandBuild"
	StageQEMURun          Stage = "qemuRun"
	StageInitialOwnerWait Stage = "initialOwnerWait"
	StageInitialFreeze    Stage = "initialFreeze"
	StageCommandStart     Stage = "commandStart"
	StageContextCanceled  Stage = "contextCanceled"
	StageInitContainer    Stage = "initContainer"
	StagePodFailure       Stage = "podFailure"
)

type Code string

const (
	CodeUnknown          Code = "unknown"
	CodeFailed           Code = "failed"
	CodeAPIForbidden     Code = "apiForbidden"
	CodeAPINotFound      Code = "apiNotFound"
	CodeContextCanceled  Code = "contextCanceled"
	CodeDeadlineExceeded Code = "deadlineExceeded"
	CodeOOMKilled        Code = "oomKilled"
	CodeContainerExited  Code = "containerExited"
)

// Report只有固定枚举；身份、退出码必须由Controller读取原Pod，而非采信进程自报。
// 不接受error.Error、URL、文件路径、命令行或任何日志文本，整个诊断不是退役事实。
type Report struct {
	Version int   `json:"version"`
	Stage   Stage `json:"stage"`
	Code    Code  `json:"code"`
}

type stagedError struct {
	stage Stage
	cause error
}

func (e *stagedError) Error() string { return fmt.Sprintf("%s: %v", e.stage, e.cause) }
func (e *stagedError) Unwrap() error { return e.cause }

// At保留最早的具体失败边界和原错误链。外层qemuRun不能覆盖initialFreeze等
// 已定位阶段；包装不改变errors.Is/As、API错误分类或原业务返回/重试行为。
func At(stage Stage, err error) error {
	if err == nil {
		return nil
	}
	var prior *stagedError
	if errors.As(err, &prior) {
		return err
	}
	if !validStage(stage) {
		stage = StageUnknown
	}
	return &stagedError{stage: stage, cause: err}
}

func FromError(err error) Report {
	out := Report{Version: 1, Stage: StageUnknown, Code: CodeFailed}
	var staged *stagedError
	if errors.As(err, &staged) {
		out.Stage = staged.stage
		// 后续关闭/发布错误仍留在业务error链，但不能把其取消或API类别
		// 错归到先前失败阶段；公开code只分类该阶段的原cause。
		err = staged.cause
	}
	switch {
	case errors.Is(err, context.Canceled):
		out.Code = CodeContextCanceled
	case errors.Is(err, context.DeadlineExceeded):
		out.Code = CodeDeadlineExceeded
	case apierrors.IsForbidden(err):
		out.Code = CodeAPIForbidden
	case apierrors.IsNotFound(err):
		out.Code = CodeAPINotFound
	}
	return out
}

func validStage(stage Stage) bool {
	switch stage {
	case StageUnknown, StageConfig, StageDecodeInput, StageRuntimeAssets, StageRuntimeBoot,
		StageRuntimeSpec, StageRunnerBirth, StageInitScript, StageRuntimeISO, StageRootDisk,
		StageCommandBuild, StageQEMURun, StageInitialOwnerWait, StageInitialFreeze,
		StageCommandStart, StageContextCanceled, StageInitContainer, StagePodFailure:
		return true
	}
	return false
}

func (r Report) Validate() error {
	if r.Version != 1 || !validStage(r.Stage) {
		return fmt.Errorf("unsupported runner diagnostic")
	}
	switch r.Code {
	case CodeUnknown, CodeFailed, CodeAPIForbidden, CodeAPINotFound, CodeContextCanceled,
		CodeDeadlineExceeded, CodeOOMKilled, CodeContainerExited:
		return nil
	}
	return fmt.Errorf("unsupported runner diagnostic")
}

func Encode(report Report) ([]byte, error) {
	if err := report.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(report)
}

// Parse要求版本、枚举与规范JSON全部吻合；重复字段、额外字段、截断和
// 超长输入都降级为未知诊断。此失败不能变成Host退役拒绝或执行授权。
func Parse(raw []byte) (Report, error) {
	var result Report
	if len(raw) == 0 || len(raw) > MaxBytes {
		return result, fmt.Errorf("invalid runner diagnostic size")
	}
	raw = bytes.TrimSpace(raw)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return Report{}, fmt.Errorf("invalid runner diagnostic format")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return Report{}, fmt.Errorf("invalid runner diagnostic tail")
	}
	canonical, err := Encode(result)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Report{}, fmt.Errorf("invalid runner diagnostic fields")
	}
	return result, nil
}
