# 完整 normal1 Prepare 命令的有界持久化

真实首次扩容和缩容完成后，再次扩容的 Prepare 在 journal.Store.Start 被拒。前两次原命令为6451、7338字节；第三次携带缩容后的完整双PG快照与物理释放事实，即使未知数值全取最短0，独立重建下界仍8831字节，超过普通8192字节门。原第三raw未落盘，下界不能冒称精确值或唯一后续问题。

本次抽取纯 budget.PrepareCommand 作为统一合同，保留原五字段顺序、JSON名字和Outside内容寻址。旧normal0合同保持；仅PrepareBudget的完整normal1命令允许至16384字节，且必须满足完整Prepared/Fence、资格与固定Bounds来源、无OutsideClaim、Root摘要、canonical OutsideName，并绑定原RequestDigest、派发epoch及Compute目标。

普通命令8192字节、JSON深度和2048节点、整个账本及Kubernetes注解总界、历史压缩/未知保留全部不变。不删PG释放来源，不因自报版本放大边界。重读使用统一DTO严格解码，避免嵌套Unmarshal接口冲突。

验证按唯一用例归档：b1bc30中8根64子通过；原artifact路径装配失败保留，b32c54仅重跑真实旧wire根，1根2子通过。合计9根66子、零跳过，不重复累加。两条原6451/7338字节命令的wire和OutsideName逐字相同；Host/seed编译通过。包含边界、篡改拒绝、Start/编码及原命令重读。

真实622368完整循环另行运行；本提交不是Linux全链、生产来源或Kubernetes验收。完整日志、输入和二进制留在/data/neon-build/validation。
