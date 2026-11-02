本批新增独立原Key Root耐久确认原语；现有A和关闭/过期准入规则均未改。

resourceexec.ConfirmNormalQuotaDurability复用当前Store owner/原Key、完整Prepared/Command/原Unconfirmed GuestReady及正常quota信封选择；新入口另拒已取消/cleanup或异PG实例。经原SDK调用Root Recover之后，重新Load并逐字段匹配原完整Operation和原owner/资源身份，漂移返回Unknown。成功也仅AwaitingFact，绝不advanceSelected、PG Fence/SharedCost或GuestReady CAS。它不读取Session lease：调用方必须持有同原Session/coupled ctx，不把actor字符串检查冒充真实续租许可。旧RecoverNormalQuota仍共用选择/确认后进入原advanceSelected，不改变旧恢复语义。

实际Linux race稳定去重8根18子：e6bf的原Root handoff3根4子；1cf的原ReleasePG/同Key信封1根6子；7a822b新增4根8子。新正例实际执行typed Store、HTTPS SDK和明确Root协议peer，Root recover一次，PG全部请求（含Fence）为零、后续Root Observe零、Kube CAS写零；同原WAL未变，成功仍等待。前置错误Key/取消/owner变更/新Planned精确拒；协议peer partial回复传播拒绝；Root RPC中取消/换owner/完整Command替换均fresh Unknown。旧Recover直接对照确实仍进入原Root Observe及PG Fence（peer有意503），没有被改成新Root-only语义。

下层已有直接Root handoff测试复核完整count4/失回复/fsync与partial保持旧Held；同N metadata新Original保留原语义，未擅自增加“所有新Original一律拒绝”的不兼容要求。下层使用原fixture kernel和真实临时文件操作，不是实际cgroup/PG/VM服务；新HTTPS Root为明确协议peer。QMP writer不属于该原语，也未启动VM。分类为Go单元/组件，S native0/E0/Source false/normal false；本批不能替代真实单机/Helm/K8s验收。

原始失败全保：e6bf抽公共helper后遗漏局部err声明导致resourceexec compile FAIL，仅Root原3根通过；1cf已编译、原信封1根通过，新夹具缺SDK规定no-store响应头；3b新增夹具日志注解缺原zlib-v1外壳，拒绝发生于正式Store读取前，早期4个负例子通过不计最终新增覆盖。7a仅测试夹具修为原codec/精确负例错误断言后4新根全绿。生产修复仅3个if采用局部err:=，7bc605生产字节自1cf到7a不变；无准入或业务门放宽。已绿旧目标不重复运行。

/data原始run/source/nonce/spec/终态/日志/cpu.stat/memory.peak/io.stat均保留，预算Go3CPU3GiB已按各原nonce reconcile，无prune。worker的runner_exit字段是遗留模板占位，本批CLI_builds0。8目标不是4次执行的累计次数。

closed-entry-durability-only-proposal.patch与effect-ticket仅独立后继审查提案，未应用：新单独动作接新Root-only原语，旧A动作语义及旧A闭门/过期拒绝保持；未来只有经正常独立审批和原关闭/过期真实前驱定向验证后，才可讨论该新动作准入。未绕过此前自动审批拒绝，不声称新Root opaque/helper恢复或livePG收养已完成。
