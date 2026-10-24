# 原Postmaster出生身份与资源命令

本批补齐实际资源动作的进程事实来源，沿既有服务功能分支演进，不改变PG17内核。
独立认证通道本身不能证明请求中的PID就是原Postmaster；任意同组进程也不够。

正常受管启动由root helper读取自身进程PID、starttime及原Guest/cgroup身份，在
同一资源文件锁内确认原Bootstrap、当前Held及空组后，先将Birth写入日志并fsync，
再入组、全线程降权及原位exec。exec失败和进程死亡不删除Birth，也不代表旧资源
动作NoEffect；新出生不清除原Original/Held/floor或未知状态。未知资源写仍阻止
新出生。Birth为可选字段兼容读取旧未启用日志，旧生产Bootstrap无Birth时运行期
写拒绝，必须受控重启/恢复，不从一个请求PID或当前进程列表收养。

固定SUID启动只接受当前正式Compute使用的两种参数形式：唯一
`--sync-safekeepers`为临时存储同步，不创建/覆盖Postmaster Birth；正常服务器为
`-D <dir>`及后续`-c name=value`键值对。其他模式拒绝，未来官方新增启动形式须
复核这个权限边界。未受管启动沿用官方路径，不受此SUID约束。

Linux运行期资源写须匹配原Birth的完整Guest/PID/starttime。实际读取前后都核对
starttime，核对固定cgroup、四个UID均已降为postgres，且/proc/pid/exe必须与root
保护的固定PG文件为同一文件。因此仅记录了helper PID、PID复用、任意同组进程、
尚未exec或仍保留saved/fsuid root都不能被确认。文件升级/替换后的不同exe拒绝，
交给受控版本升级，而不是自动接受新文件。

新增GET `/dsg/v1/resource/process`，不接受调用方PID，只有原Birth存在且当前
实际进程核验通过才返回。正式Compute认证代理和中心固定只读路由已接线。
独立资源Client读取后返回完整身份，不修改已有Pending；计划者必须先持久保存
原过程事实与完整命令，再发送。新PG的查询结果不能改写旧未知命令。

## Linux源码依据与后验界限

Linux6.12非leader线程exec时保留原进程PID，并继承原leader的启动时间，见
[de_thread原实现](https://github.com/torvalds/linux/blob/v6.12/fs/exec.c#L997)。
proc stat的启动字段由start_boottime换算，见
[proc array原实现](https://github.com/torvalds/linux/blob/v6.12/fs/proc/array.c#L554)。
因此在不改变时间命名空间的本Guest受控启动路径中，保存进程PID与第22字段
符合原位exec约定；这只是源码核对，不能代替目标内核、SUID、全线程降权和
文件系统权限的实际验收。目标内核版本改变时仍须复核。

## 必要Mac结果

go1.25.13 race：3个新增出生用例及1个受影响Bootstrap→运行期限额用例，4顶层/
4pass、0fail/skip。覆盖真实Mac文件锁和日志落盘顺序、出生意图不等于exec、不收养
请求PID、HTTP/TLS及独立客户端、严格UID和特殊模式拒绝；内核及exec明确为fixture。
最后收紧为准确-c键值对后仅复跑该受改参数用例1pass，不相加或重复全套。
相关vet、最终Linux CGO_ENABLED=0 amd64/arm64 root程序目标编译通过，只编译。
早期未使用import编译失败日志保留，修正后才执行用例，不计通过。

证据：工作区build/dsga-process-birth-{go-final,args-final}.jsonl、vet-final.log、
linux-{amd64,arm64}-final.log；源SHA见build/dsga-process-birth-source-sha256.json。
Neon新增正式路由1项通过，clippy通过；控制面新增TLS只读路由1项race通过、vet
通过，不重复原资源通道、PG、SDK和其他无改功能。分段检查不是完整Linux端到端。

Agent资源领域持久执行/恢复、完整运行期预算、首次准入/完整排空、Host/最终VM
及受控跨boot/owner关闭仍属代码工作，继续实施。真实Linux root/SUID/exe/UID/
cgroup/HugeTLB/NeonVM及K8s、物理与性能后验。本批不构建镜像、不部署、不合main。
