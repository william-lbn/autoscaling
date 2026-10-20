# Go 正式Compute HTTP客户端

2026-10-20。`pkg/dsga/compute`实际消费Neon五个`/dsg/v1`路由；
`pkg/api/dsga/response.go`对应ABI5完整能力、快照、逐数组Provider与独立receipt。
客户端可执行，但尚未接入Agent持久协调循环，不代表完整自动伸缩。

## 身份、连接和失败

New要求可信配置中的HTTPS根端点、VM/Compute/PG boot和issuer epoch，实时
验证能力与固定boot；拒绝URL凭据/查询/片段、TLS跳过验证。执行和观察各自拥有
克隆Transport连接池，执行阻塞时仍能状态查询/业务取消；不跨网络持状态锁。
拒绝重定向，不带Idempotency-Key，不透明重放业务POST；响应要求JSON、no-store、
无压缩且原始32KiB以内。Bearer只由可并发凭据回调提供，不保存到命令或日志；
错误不回显服务端body、凭据提供器错误或URL。

本地校验/凭据失败为NotSent；开始Do后的错误含非200、超时、ctx取消、坏响应均
保守标为MayHaveExecuted。保留完整原Request，冲突新意图/同Key改预算均拒绝；
仅在没有执行调用在途时允许完整原意图显式重放。Preparing、Stale、Conflict、
过期/未找到结果均不能清栅栏，状态查询/业务取消本身也不能清除正在执行的请求。
原意图真实终态，或确证Busy/Rejected未建立本请求，才结束该客户端的PG执行栅栏。
这不释放外层预算，也不代表VM已完成。进程重启由Config.Pending恢复原请求；
新owner可查询/取消旧epoch，不能重发旧epoch执行。最终仍须持久journal与可信
owner驱动，不能重建空Client绕开未知效果。

## 事实与完整摘要

每层schema检查未知/重复/缺失/大小写字段，明确nullable；补强共用JSON解码器，
拒绝encoding/json把null布尔/整数/字符串静默补成零值。receipt.result与observed
分别校验，不借用当前成功去填缺失历史；四数组唯一、代次/范围/溢出、操作phase/
方向/取消窗口、Provider结果与物理证明均核验。PG的vm_shrink_permit必须false。

发现现有Rust validate_resize只核对kind/Buffer/generation和摘要格式，未重算
完整意图；这不能独立发现同Key下VM目标/预算/期限不同的回执。Go与Rust均补强
完整context_sha256比较。仅对已验证的固定ASCII/整数Request schema复现PG
jsonb_out：键按字节长度、字节序排序，分隔含空格，不是一般Unicode/浮点JSON
编码器，不同于journal领域分离hash。共享context-v1两个向量来自准确486338原生
PG的jsonb_out+sha256，包含全部高位及引号/反斜线/HTML字节；Rust原生门禁又独立
向实际PG查询比较。response-v1是人工跨语言schema夹具，不冒充物理回执。

## 实际验证与未验收

Go TLS/HTTP测试覆盖阻塞中独立查询/取消、丢回复后保持原意图/显式重放、客户端
ctx取消后远端仍完成、307不重放/不泄露凭据、坏头/超大/重复/null/串用回执、
换boot不自动绑定、进程重建恢复旧epoch、秘密错误不回显。完整响应审计上限32KiB、
命令8KiB分别约束，新增24KiB完整审计恢复测试；总持久字节预算不放宽。

本地最终race组合183pass事件，0fail/用例skip；10测试包通过，9无测试包单列。
Go HTTP16个pass（8顶层、8子测试）、API23、journal25；其余沿用受影响测试。
v2lint含plugin在内全部受影响范围0issues。日志工作区build/
autoscaling-dsga-http-{verified-final.jsonl,verified-final.stderr,lint-release.log}。
过程中既有Scheduler误判单独修复并保留失败证据，见对应文档。

Neon真实SDK8/HTTP11原生门禁另通过，证明正式生产端点/PG语义；Go这里的TLS
端是夹具，没有在同一测试中运行Go→真实Compute→PG，更不包含VM或预算执行。
该跨语言实链、Agent/Monitor/owner/预算/Scheduler/Runner控制循环仍须继续编码
集成，不能归入Linux待测。真实Linux/VM/物理回收/K8s/性能后验，不启用、不发布。
