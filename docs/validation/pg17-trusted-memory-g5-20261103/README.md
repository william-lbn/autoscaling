# PG17 内存伸缩定向测试阶段归档

这些是非默认构建标签下的 DEV 测试，真实运行 PG、Compute、Root、QEMU/KVM 与 Pageserver/Safekeeper；Host Kubernetes API 仍为明确模型，不计三节点端到端验收。未提交原始私密日志、数据盘或构建二进制。

## 用例组织

- 单元/race：固定诊断输出白名单、VM 7–8 GiB 原几何、故障模式闭合、HTTP 回复丢失边界、SDK transport 克隆及同原请求读确认。10 个唯一根、37 个子用例通过。原首次检查缺少真实 WAL fixture 导致的失败保留，补入原件后只重跑该 1 根/3 子，不重复其余已通过用例。
- 组件 API / Linux 集成：通过正式资源 HTTP、PG DSM 观察、Root 资源接口和 QMP，核验 Grow→Shrink→Regrow。SQL 忙时原 Quiesce 等待，三次实际采样均不降 N/P/A/VM；真实取消并排空后，同一请求完成数组释放、A/N 硬限下降、VM 降容及再扩容，数据校验一致。
- 恢复：原 normal 写入已完成但回复丢失，重建同 Session 的客户端/worker，只确认原命令，不重复写硬限；保留原 key/body digest/deadline。更高 owner 接管、Root 死亡与真实 buffer pin 不由该用例证明。
- Kubernetes 端到端：尚未执行；后续使用自有 Helm，在 worker-117/118/119 部署真实组件与 MinIO，再由 proxy/组件 API 进入完整回归。

源文件和对应实际 Host/Guest SHA 见 source-files.json；原 nonce、日志、资源限制及独立复核摘要见 receipt.json，全部大产物保留在 /data/neon-build/validation。新增测试的构建标签分别位于每个 _test.go 首行，默认业务构建不包含这些 DEV 入口。原构建配方、固定源码、精确输入和执行命令保留在对应 nonce 目录，复现时使用新的 nonce 与当前真实运行身份，不能复用旧 UID/资格作为新实例授权。

首版安全 normal、真实 Host 来源与三节点 K8s 尚未全部验收；阶段通过不表示封版。最终完成后按总计划将可复现 API/E2E 用例与详细中文说明归档到届时最新控制面的 docs/PG内存自动伸缩回归/。
