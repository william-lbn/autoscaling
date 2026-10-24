package resourceguard

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	control "github.com/neondatabase/autoscaling/pkg/api/dsga"
)

// ProcessIdentity由root出生程序在exec前保存。PID/starttime贯穿原位exec；记录
// 本身只表示启动意图，InspectProcess还须验证实际程序/UID/cgroup，不能因有记录
// 就断言PG已启动。PG退出或exec失败时保留，绝不把not-found解释成资源NoEffect。
type ProcessIdentity struct {
	ProtocolVersion      uint32         `json:"protocol_version"`
	Identity             Identity       `json:"identity"`
	PostmasterPID        int            `json:"postmaster_pid"`
	PostmasterStartTicks control.Uint64 `json:"postmaster_start_ticks"`
}

func (p ProcessIdentity) Validate() error {
	if p.ProtocolVersion != 1 || p.Identity.Validate() != nil || p.PostmasterPID <= 1 || p.PostmasterStartTicks == 0 {
		return fmt.Errorf("incomplete original PG process identity")
	}
	return nil
}
func (p *ProcessIdentity) UnmarshalJSON(b []byte) error {
	type plain ProcessIdentity
	var next plain
	if err := control.UnmarshalObject(b, &next); err != nil {
		return err
	}
	*p = ProcessIdentity(next)
	return nil
}
func (p ProcessIdentity) matches(c Command) bool {
	return p.Identity == c.Identity && p.PostmasterPID == c.PostmasterPID && p.PostmasterStartTicks == c.PostmasterStartTicks
}

// InspectProcess只返回原出生记录且真实进程仍匹配时的事实。普通HTTP请求不能
// 提交一个自选PID让root“发现”；缺记录要求受控重启/恢复，不收养任意已有PG。
func (g *Guard) InspectProcess(ctx context.Context) (ProcessIdentity, error) {
	var result ProcessIdentity
	err := g.withRecord(ctx, func(r *record, _ *lockedState) error {
		if r.Birth == nil {
			return fmt.Errorf("original PG birth record missing")
		}
		if err := g.kernel.VerifyProcess(*r.Birth); err != nil {
			return err
		}
		result = *r.Birth
		return nil
	})
	return result, err
}

// 生产Compute只使用这两种受控形式：临时存储同步或正常-D启动。同步不是
// Postmaster出生，不覆盖原Birth；禁止单用户/bootstrap/查询GUC等特殊模式借用
// 正常出生身份。该规则不解析/记录含潜在敏感值的任意PG参数。
func launchInvocation(args []string) (bool, error) {
	if len(args) == 1 && args[0] == "--sync-safekeepers" {
		return false, nil
	}
	if len(args) < 2 || args[0] != "-D" || args[1] == "" {
		return false, fmt.Errorf("unsupported managed PG invocation")
	}
	// 当前正式Compute只传-D及-c键值对。用准确允许形式，不维护易遗漏的
	// 特殊模式黑名单；未来官方增加启动参数时须显式评审这个SUID权限边界。
	for i := 2; i < len(args); i += 2 {
		if args[i] != "-c" || i+1 >= len(args) || strings.IndexByte(args[i+1], '=') <= 0 {
			return false, fmt.Errorf("unsupported managed Postmaster argument form")
		}
	}
	return true, nil
}

// /proc status的四个UID必须全部等于降权后的postgres；不接受仅effective匹配、
// 仍保留saved-root或fsuid-root的进程。该解析在Mac纯逻辑验证，实际文件读回在Linux。
func processUID(status string, expected uint64) error {
	if expected == 0 {
		return fmt.Errorf("root is not an ordinary Postmaster")
	}
	count := 0
	for _, line := range strings.Split(status, "\n") {
		if !strings.HasPrefix(line, "Uid:") {
			continue
		}
		count++
		fields := strings.Fields(strings.TrimPrefix(line, "Uid:"))
		if len(fields) != 4 {
			return fmt.Errorf("invalid process UID facts")
		}
		for _, field := range fields {
			uid, err := strconv.ParseUint(field, 10, 32)
			if err != nil || uid != expected || field != strconv.FormatUint(uid, 10) {
				return fmt.Errorf("process retained foreign privileges")
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("missing or duplicate process UID facts")
	}
	return nil
}
