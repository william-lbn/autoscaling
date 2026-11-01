//go:build linux

package resourceguard

import (
	"context"
	"golang.org/x/sys/unix"
	"os"
)

// 固定RootAction服务只按自己编译的acpi/daemon/udev角色读取原main租约。
// role不能从请求正文给出；实现不存在/原候选未active/角色已退出均Unknown。
// 读取顺序必须是原resource flock→独立producerMu→原Task；不回broker/exec锁。
// Getter必须核原三份持久见证、同g/原Root/当前floor与原clone/creator线程存活，
// 不能凭UID、numeric PID、enum或WAL重开认领。这里仅冻结接口，不提供假producer。
type normalSystemOriginalRoleMainLeaseReader interface {
	normalSystemOriginalRoleMainLeaseLocked(context.Context, *lockedState, *record, string) (normalSystemOriginalRoleMainLease, error)
}

// 这是已发布原main的私有句柄接口，无JSON/请求构造器、路径或预算参数。
// Root服务必须先从实际accept对象钉SO_PEERPIDFD，再逐packet用SO_PASSCRED读取
// 内核SCM_CREDENTIALS，不能把正文里的PID/UID/GID填入cred。匹配既核peer真实
// kernel对象，也核本packet发送者是原main；继承同socket的fork worker必须拒绝。
// 调用者在同一原resource锁内读取/匹配并执行固定Root动作，结束close其独立dup。
// 单次匹配不是全Source/normal票据；每个packet都必须重新读取和核验。
type normalSystemOriginalRoleMainLease interface {
	matchOriginalMainPacket(context.Context, *unix.Ucred, *os.File) error
	close()
}
