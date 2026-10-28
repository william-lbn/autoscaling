# 初始预算领域夹具

`initial-domain.json`通过预算真实DTO/Root聚合生产函数构造并验证后序列化，
只供journal领域测试。PG成本、root/cgroup、平台资格、Runner/初始Host均为
明确模拟事实，数字不是生产默认或Linux认证，不是生产首次预算生产者。

该夹具没有Resize请求或伪造的Resize COMMITTED；只含完整Initial、首次
Admission原命令与原生响应合同的模拟审计。测试实际执行Model、codec及
typed HTTP UID/RV Store；native setter/平台来源认证另由生产组合验收。
