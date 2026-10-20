# Scheduler 随机分数测试修复

2026-10-20，官方main a4bce09e已完整包含。DSGA受影响全量回归中的既有
TestNormalizeScoreRandomization实际抽到node3=25，原值也为25，测试失败。
生产算法明确为闭区间[1,oldScore]，原值是合法样本；原测试要求每个节点都改变
不符合这个契约。不能靠重跑掩盖失败。

最小修改：公开NormalizeScore仍使用官方rand.Intn，内部算法接受私有draw参数；
测试使用局部固定种子，不修改全局随机状态。移除错误的逐节点必变断言，保留
范围/零值断言，增加[1,1]及精确上端抽样；多次运行的多样性断言使用固定种子。
原作者注释保留，新增中文解释合法不变量。实际调度范围/均匀随机语义不变。

Mac竞态检测定向重复100轮通过（2顶层×100，命令1.929s），受影响完整组合
183个pass事件通过；v2lint包含全部plugin包，最终结果见当前执行记录。原失败
保留在工作区build/autoscaling-dsga-http-release-final.jsonl，定向日志为
autoscaling-plugin-randomization-repeat.log。不构建镜像、不部署，不改主分支。
