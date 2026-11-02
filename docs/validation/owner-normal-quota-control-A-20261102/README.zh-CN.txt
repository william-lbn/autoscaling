本批仅接通已有normal quota原Key恢复的显式管理动作A。

新增 recover_normal_quota 枚举/严格原Key校验、同Session PGWorker wrapper与串行executeControl分发；原ResourceOnly、ServiceClosure和原生lease过期白名单逐字保留。未知action、未来Key epoch、跨实例、零boot、混Request/Selector均拒绝。没有B准入例外，没有普通Advance错误后自动恢复，没有Root重启或过期恢复完成声明。

实际Linux Go race：a9287d，3根14子用例全部通过，无跳过。第1根为schema；第2根走正式Elector/Session/UID-RV日志/SDK/ControlEndpoint/Run，验证缺resources和原Unconfirmed GuestReady时明确拒绝、调用取消/错误owner拒绝、日志不变且无下游业务调用；第3根复用已存在的真实生产首次服务逻辑，原Advance生成Settled关闭WAL或原typed PG lease peer报告过期，新动作均ErrServiceSafety，原WAL/预算、Root写、PG admission/resize写及QMP写计数保持不变。Root/PG/Kube/QMP为明确协议peer，不是实际内核或存储服务。U/组件3、真实native服务0、E0，不能替代PG17大VM/K8s验收。

生产资源执行器保持原样：原完整Prepared/Command/PG事实/WAL恢复，最终调用advanceSelected(ctx,nil,&envelope)，要求同原PGRequest、同原Unconfirmed GuestReady及full Command。candidate-A原call-chain说明中旧e.advance字样不代表当前代码。Root成功恢复可能同步原完整四数组/normal记录并fsync、SDK只清同信封pending，随后仍有PG Fence/SharedCost及原CAS，所以入口不叫纯查询。partial caps和旧缺失前缀不由该动作重写。该底层原完整prefix/same-key定向证据由既有执行器归档保留，本批不重跑无关功能。

prebuild 1ea20d因unit名大写A被资源管理器拒绝，未reserve/未编译/未执行用例；新nonce a9287d修正单元名后通过。一次reconcile误传--spec的CLI错误原件保留，随后按--nonce正常reconcile，无业务或测试变化。worker模板的runner_exit字段是保留占位，未编译Runner CLI，不计构建目标。

资源：Go3CPU/3GiB，实际CPU43.839s，peak703471616B，OOM0；使用原/data缓存，未prune。完整COPY身份锁和nonce/spec/终态/原日志、cpu.stat/memory.peak/io.stat均保留。首版normal安全降限及真实Linux/K8s目标仍由主线继续，不以本组件测试替代。
