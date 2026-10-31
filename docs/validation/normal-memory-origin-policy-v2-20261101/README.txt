原ISO显式memory-origin协议选择（2026-11-01）

NormalSplitTopologyPolicy新增可选memory_origin_protocol_version。原v1必须不出现该字段，含null/0/大小写别名都拒；原v1 marshal继续省略，字段顺序/字节保持。新policy v2必须显式字段=1，仍同369物理profile与原P/A/pids约束；没有自动转换旧正常profile。
RuntimeBootRequirements只从同原raw重新渲染、逐字节核完整五policy+resource后派生私有能力及MemoryOriginProtocolVersion getter。旧文档/旧v1得0，新完整原ISO才得1；替换单asset、交叉raw/asset或请求布尔不能启用。内核参数8项完全相同，不改变原内核ABI或上层PG版本合同。

新增两个U根覆盖新旧版本/字段存在性与精确ISO能力派生；19race根146子例全部PASS零skip/fail，包含直接相关原拓扑/启动模板回归。实际4060冻结输入hash全过，BuildKit2CPU2GiB/noextraswap、禁网，22.537s。独立Helm复核全部四源及原v1字节/参数保持，通过。
此阶段只选择后继协议。Host kind5→6→7及Guest读6/写7实际调用点尚未接；不能从getter=1推导Mmax、Source齐备或normal许可。Host/Guest opaque生产者独立在研，旧normal369/v1沿旧路径。E0、未部署K8s、发行锁不变。
