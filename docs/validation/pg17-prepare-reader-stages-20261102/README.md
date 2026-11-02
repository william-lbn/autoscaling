# PrepareBudget 固定调用边界诊断

真实 Grow 和 Shrink 完成后，再次 Grow 在 PrepareBudget 写前返回错误。为区分预算生产者、直接外部读取和账本落盘，本次在 prepare 的 advance、inspect、checkSnapshot 原失败返回后记录固定 scope/stage 数字。

不输出原错误、请求、URL 或资源配置；不调用 Error/Unwrap 扫描正文，不增加 RPC、重放或写入。原错误对象、判断顺序和返回表达式保留；移除诊断并格式化后与原执行源码字节一致。

ef98fd 直接单测 1 根 2 子共 3 项通过、零失败、零跳过。d7ef44 的相同生产依赖已完成 Host/seed 编译；原 nil-receiver 测试夹具失败保留，修正后的依赖测试不参与 Host 链接。

520499 真实运行已定位 scope2/stage276：第三 Prepare 的 Store.Start 返回错误，资格、Runner、PG fence/Shared、Root 和 Prepared 检查已越过。单命令大小修复单独开发，本提交不放宽任何账本、预算或硬限条件，也不代表完整伸缩循环或 Kubernetes 验收。
