package resourceguard

import (
	"context"
	"fmt"
	"reflect"
)

const FixedConfigPath = "/etc/neon-dsga/resource.json"
const FixedPostgresPath = "/usr/local/bin/postgres"

// launch把资源日志与实际限额的最后复核和进程入组放在同一文件锁内。回调成功
// 时应直接exec，不产生父进程代理；锁FD在exec时CLOEXEC，PG不长期持有此锁。
// 原bootstrap必须已建立硬约束；裸Initialize或旧实验日志不能授权普通SQL出生。
// 仍有未知资源写时拒绝重启PG，不借新PG PID清空旧VM/资源动作的不确定性。
func (g *Guard) launch(ctx context.Context, execInGroup func() error) error {
	return g.launchWithSource(ctx, nil, execInGroup)
}

// 生产入口另核对原只读来源/封存来源与初始资源记录；不能让失败的provision
// 留下旧Config/已完成预算后，被另一个来源的启动路径误用。nil仅供内部已有
// 资源锁/核验逻辑复用，生产ExecPostgres和CheckGuest始终传已验证原来源。
func (g *Guard) launchWithSource(ctx context.Context, source *SourceConfig, execInGroup func() error) error {
	return g.launchWithBirth(ctx, source, nil, execInGroup)
}

// nil birth仅用于只读check和临时存储同步；正常生产PG必须传root读取的完整出生
// 身份。旧Birth只在空组/确认Held后替换，原资源Original/Held保留，不因新PG清除。
func (g *Guard) launchWithBirth(ctx context.Context, source *SourceConfig, birth *ProcessIdentity, execInGroup func() error) error {
	if source != nil && (source.Validate() != nil || source.Config != g.config) {
		return fmt.Errorf("PG birth original source differs from fixed configuration")
	}
	return g.withRecord(ctx, func(r *record, state *lockedState) error {
		if r.Bootstrap == nil || !r.Bootstrap.Applied || (r.Original != nil && !r.Applied) {
			return fmt.Errorf("PG birth requires confirmed original bootstrap and no unknown resource write")
		}
		if source != nil {
			if err := sameBootstrapSource(*source, r.Bootstrap.Original); err != nil {
				return err
			}
		}
		limits := r.Bootstrap.Original.Limits
		if r.Held != nil {
			limits = r.Held.limits()
		}
		e, err := g.kernel.ReadBounds(r.Identity, limits)
		if err != nil {
			return err
		}
		// 不把先前Applied当作当前约束证据。旧PG/helper尚在组内也禁止第二份出生；
		// 缺限额、私自放宽、Guest变小及池未回收，均在任何exec/入组之前拒绝。
		if err := bootstrapMatches(e, r.Identity, limits); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if birth != nil {
			if birth.Validate() != nil || birth.Identity != r.Identity {
				return fmt.Errorf("PG birth process belongs to another Guest")
			}
			copy := *birth
			r.Birth = &copy
			// 必须先fsync再入组/降权/exec；任何后续错误都保留此启动意图，不能
			// 从“exec未回复”推导没有出生。真实程序由独立只读查询再核验。
			if err := state.save(r); err != nil {
				return err
			}
		}
		return execInGroup()
	})
}

func sameBootstrapSource(source SourceConfig, original BootstrapCommand) error {
	if original.Identity.VMUID != source.Config.VMUID || original.Identity.ComputeInstanceID != source.Config.ComputeInstanceID || original.IssuerEpoch != source.IssuerEpoch || original.BudgetCommitmentID != source.BudgetCommitmentID || !reflect.DeepEqual(original.Limits, source.Limits) {
		return fmt.Errorf("PG birth source differs from retained original bootstrap")
	}
	return nil
}
