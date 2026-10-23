package resourceguard

import (
	"context"
	"fmt"
)

const FixedConfigPath = "/etc/neon-dsga/resource.json"
const FixedPostgresPath = "/usr/local/bin/postgres"

// launch把资源日志与实际限额的最后复核和进程入组放在同一文件锁内。回调成功
// 时应直接exec，不产生父进程代理；锁FD在exec时CLOEXEC，PG不长期持有此锁。
// 原bootstrap必须已建立硬约束；裸Initialize或旧实验日志不能授权普通SQL出生。
// 仍有未知资源写时拒绝重启PG，不借新PG PID清空旧VM/资源动作的不确定性。
func (g *Guard) launch(ctx context.Context, execInGroup func() error) error {
	return g.withRecord(ctx, func(r *record, _ *lockedState) error {
		if r.Bootstrap == nil || !r.Bootstrap.Applied || (r.Original != nil && !r.Applied) {
			return fmt.Errorf("PG birth requires confirmed original bootstrap and no unknown resource write")
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
		return execInGroup()
	})
}
