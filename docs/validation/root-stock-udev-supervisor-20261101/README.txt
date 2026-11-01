一次性Root stock udev监督原语（未接来源producer）

仅原System RO两ELF可进入固定前台udevd与subsystems/devices trigger、settle命令。没有通用Root命令/参数接口，不传daemon与ordinary标记；原clone pidfd、唯一cmd.Wait、有界20秒执行/5秒清理和32KiB日志。preordinary原R2须无孩子，subreaper使双fork/setsid后代仍归R2；直属cmd全Wait后才清退/reap收养后代，任何残后代即使已清理也令本次失败，不冒充规则完整执行。原Wait缺失、pidfd失配、早退、超时或日志溢出均不能生成成功来源。

首四U图263dce保留。Kernel独审指出官方argv后仍读kernel cmdline且never跳过符号GROUP；最终明确拒udev.命名域覆盖，去never恢复原default early。最终9ca+仅2源/1350Go/mod输入，实际3CPU/3GiB，五聚焦race根和Linux/Darwin CLI编译均0，独审增量解除两局部问题。原生子进程U确核原clone/超时不伪造Wait/唯一Wait与孩子清点，但不是实际stock Guest S。

剩余Source边界明确保留：RO69与实际账户/libc及nscd代理集合、真实完整设备事件处理/队列完成、原DB与独立sealed投影、同g FS/WAL/authority prepared来源和最终三actor复核尚未接入。官方settle零可能来自ping失败，故这里绝不将零Wait升级为队列完整或Source。当前未调用此run安装普通服务，未运行实际stock Guest、PG，normal/S/E均不授权。主业务归档与安全normal验收仍按最终总计划推进。
