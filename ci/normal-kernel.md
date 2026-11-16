# 独立 normal 内核候选发行

`dockerhub.yml` 的 `workflow_call` 和手工入口增加 `normal_kernel: true`，默认 `false`。
只支持明确的 amd64 DSGA candidate。旧 `vm-kernel`、Runner 默认内核和 arm64 独立发行不变。
Neon orchestration 必须同时锁定本次 Autoscaling source SHA、tag、run/attempt，不能混用旧产物。

新 job 从锁定的 Linux 6.12.26 源码归档和 `normal-split-metadata-v1` 配置重新构建。
归档 SHA256、上下文每个输入及历史 README 的来源均由原 profile 校验，因此 checkout 使用完整历史。
Buildx 固定 v0.38.1，使用本 job 的实际 builder name；BuildKit 与 RUN 均限制 6 GiB、4 CPU，单并发。
不再依赖现场名为 `neon-data` 的构建器。实际 `memory.max`、`cpu.max` 必须匹配原审计值。

审计编译器为 Ubuntu GCC `13.3.0-6ubuntu2~24.04.1`，Binutils `2.42`。
编译器与链接器版本在任何内核编译前检查。APT 仍来自原 pinned Ubuntu 基盘的仓库；
仓库升级可能使该检查失败。此时保留实际版本和失败日志，必须先提供有来源的受审工具链输入，
不能删去检查、用旧摘要填充新内核或自动扩充运行时白名单。当前接线不声称 Hosted 已重现内核。

只有实际 kernel/config/notes/release 四元组和 `actual-source-SHA256SUMS` 全部匹配审计值，
才将这批文件封装到独立 `vm-kernel-normal`。发布元数据与 `published.json` 保留当次 OCI 摘要。
随后按 `image@digest` 创建不启动的容器，核源码标签、平台、原容器 UID、image config 和 created 状态，
独立复制原文件并再次验四元组。失败目录不删除；原 output 已存在即拒绝覆盖。

发行产物为：

- `images-autoscaling-normal-kernel-attempt<N>`：`vm-kernel-normal.json`，包含实际 `artifact_sha256` 等。
- `autoscaling-normal-kernel-receipt-attempt<N>`：不可变镜像读回的原文件及同 source/run/image 的 `binding.json`。
- `autoscaling-normal-kernel-build-attempt<N>`：成功或失败的日志、source-lock、构建命令、实际导出及已发布摘要。

`distribution.py collect --normal-kernel-directory <原收据目录>` 显式开启第十二份镜像合同。
缺记录、错 attempt、丢原收据、材料漂移或未显式开启均拒绝。`normal_kernel` 与旧 `kernel` 并列，
所有 `normal_qualification`、`production_ready`、`latest_eligible` 仍为 `false`。
这一步不授予 Guest 出生、Host 资格或物理缩容许可；这些必须由原实例的真实运行链证明。

本地无 Docker 构建的输入预检可运行原 profile 的 `--prepare-only --source-archive <原归档>`，
追加 `--reproduce-audited --builder <真实名字> --network default`。该结果只证明上下文准备，不能计作编译通过。
