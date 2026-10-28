# 实际预算恢复领域夹具

`reconciled-domain.json`提取自源码`d99e4cb6998b3f1d248861ae9de72261b2dbf802`
的真实journal Model/codec测试导出：`model-v7.json`原Prepare，以及
`model-v8.json`永久Cleanup中的实际预算和Root聚合。仅包含显式领域输入，
没有凭据、真实用户数据库或平台资格。纯逻辑用例先逐项Validate，再验证路径、
来源选择和准入代次。它不替代当前网络认证、自动执行器组合或Linux物理证明。
