ordinary services v2 — 显式开发组件，生产 Unsupported

此目录锁定 Alpine v3.19 eudev3.2.14/acpid2.0.34 原源码、原发行版 patch、
本项目受控 patch 和构建工具版本。旧 Root 启动分支保留；只显式新 flag 进入
nonRoot 检查。源码 archive/APK 不加入 Git，准确 URL/原 SHA 在 inputs.lock.json。
Dockerfile.rebuild 使用独立固定基础 digest、有界2CPU/2GiB、源逐 hash 核验，
保实际 compiler/包版本/ELF/日志。禁止把本目录静态合同当原 Source 或 normal 资格。

身份表见 resourceguard.NormalSystemOrdinaryV2RolePlan：9 role 固定60001..60009，
同号主组、无补充组、无 MKNOD，实际安装前拒镜像/PG 身份碰撞。新能力2还缺真实
Gate installer/stdio/原 clone lease/Root 动作组合，Require...Production 始终 Unsupported。

ACPI 新模式由原 daemon 单线程事件分发直接请求固定 Root socket，不 fork shell。
/run/neon-dsga Root0:0/0755；ordinary-acpi.sock Root0:60006/0660。
16B NDSGOA02 /2/1，ACK NDSGOAOK /2/1；CLOCK_MONOTONIC 总2秒。
仅完整 Root ACK 后缓存当前 daemon confirmed，处理 netlink/input 同一按钮重复事件；
缺/错 ACK 仍 Unknown 且不重发。Root 仍必须独立核原 clone pidfd/private lease、
逐包 SCM_CREDENTIALS 与持久 at-most-once。这里客户端不生成授权。

真实369隔离 Guest 已完成 DB worker+ACPI 单包机制正控，具体源/ELF/结果在独立
validation catalog；Root responder为测试夹具，不执行生产 PID1 graceful action，
未安装最终 Gate/LL/SIGNAL，不执行 PG，不属于 normal 或 E。

已证未闭：cap0 eudev 虽能更新数据库，但向组2 processed multicast sendmsg EPERM；
同图旧 Root 模式正控通过。并且原 libudev-monitor.c644–647无条件拒非Root sender uid，
内部 reusable worker 的 unicast 亦经过该检查。仅加 NET_ADMIN 不能证明完整兼容。
当前不修改旧接收来源为 UID 白名单，不删除广播；新模式对广播失败明确报错。
完整来源方案须后继版本化角色/Root可信emitter/接收消费者合同裁定，未闭保持Unsupported。

Root 必须先按原规则准备实际固定设备及别名/静态mode+owner；普通 eudev只核验，
不吞 chmod/chown 失败、不执行 mknod/删除设备。热移除/new未知设备未认证。
最后 SIGNAL scope 保留；早期“仅self PID filter”假设已撤回，不能用UID替scope。
SO_MEMALLOC=46旧建议亦已撤回：46是SO_BUSY_POLL，未据误判实现额外限制。

build-eudev.sh / build-acpid.sh 是受控源码重建 recipe；test_compat.c 与
 test_udev_monitor.c 是 TEST_ONLY 驱动，前者核真实worker/原pidfd/逐包凭据，后者
显式保留当前 nonRoot multicast 失败及旧 Root 正控；不据只编译成功签兼容。
