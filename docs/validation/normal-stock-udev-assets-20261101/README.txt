最终rootdisk stock udev静态资产生成器 / 2026-11-01

结果
本批实际开发vm-builder新资产2源+resourceguard纯桥接3源，非仅JSON parser。prepareNormalSystemStockUdevAssets(root,packageLockRaw,computeUID,computeGID,controlsVersion)显式只收controls2；在最终Linux隔离rootdisk完成实际完整盘点后写账户/组、固定ACL、原包锁及RO MF。它不写controlsJSON/broker declaration，不修改main/normal_split/Dockerfile，不修改冻结16源/P2 producer/P1 Inputs；Root另持最终CLI接线。

真实输入与生成
完整27规则、28hwdb文本、8helpers、原99virtio、udev.conf、账户以及实际ELF递归DT_NEEDED/PT_INTERP闭包；缺失、多余规则/hwdb/helper、未覆盖alternate根/loader覆盖、RPATH/RUNPATH/audit/filter及未知DSO名均失败。实际同ELF双模式provider e473：Root默认无fixedflag完整规则，ordinary显式flag进入privsep；禁止称其与原signed APK ELF字节等价。
真实Alpine eudev APK是26规则；strict固定库存是准确上游重建27，其中80-net-name-slot.rules 491B来自e473锁定build，不冒原APK来源。26原规则逐member来源关联保留；28hwdb若来自重建也明确标记build source。Programs用e473准确输出，8DSO从实际APK member逐文件绑定。public libudev不被实际Programs/8helpers DT_NEEDED消费，首夹具多声明因此被生产严格拒绝，最终按真实8DSO修夹具，未放宽生产检查。
原APK/source tar/Alpine patches/本补丁/build-record哈希及每个最终文件的input/member/sha/size均在package-lock/fixture-provenance；实际14个APK的当前SHA全记。原研究10个APK签名核验receipt作为历史引用，未冒本轮重新签名验证全部14包。二进制/APK不入Git，原内容保留/data。

账户和初态
复用ordinaryV2AccountPlan实际安装九600xx；已有完整集保原字节，局部或冲突拒绝。11符号组保已有名字/GID，缺失按固定61001..61011槽落实，任何ID/PG冲突拒绝；最终直接调用原Linux Inputs2账户parser+InstalledAccounts，不复制宽松账户解析。非Linux明确Unsupported，仅维持DarwinCLI可编译。
ACL仅input/event*且Power Button→60006:60006/0600、ptp0→60008:60008/0600。Root构建期落实/run、/var/log、空/var/lib/chrony以及已有可选空目录0:0/0755，DNS原字节及独立regular inode保持。不会预造/run/neon-ordinary子目录；运行期P2仍通过原intent/identity/complete事务创建和移交。MF最后写固定/neonvm/config/dsga-ordinary-stock-udev-manifest.json，包锁原字节写既有/neonvm/config/dsga-ordinary-v2-package-lock.json，两者0444。MF仍原严格2/70基础库存合同，实际追加8DSO共78项；原raw锁摘要准确绑定，同输入再次执行字节相等。

准确验证
fa4381：base17cb700+b0791a不可变33+Root固定5/selected新1+本5；other WIP excluded。实际Go1.24.3 RUN3CPU3GiB/GOMAX2，源码及fixture全部SHA校验；17race根58子项75PASS/0skip/0fail，Linux BuilderCLI与DarwinCLI0，总29.485642378秒。包含本新增真实文件正控/16漂移反例、九账号/11组有限冲突、Root显式2选择与prepareV3实际字节/模板回归。实际正控生成78项/8DSO，原raw锁b262b74e3b58f8c219b4a0d31668d949df84642163cb75e8ac856036edc87ffb，MF9b09a8615d28c67f4f3359c83008666148c0e769b9ca9c785f22965794a36f86。
Root6仅作为本图准确依赖列SHA，不归本agent stage/改源；共同提交/新版包锁接线由Root唯一控制。

范围与后继
这是实际文件资产生成U与CLI编译。正控provider是实际文件，账户/DNS/base root是独立显式fixture，不能称完整生产rootdisk、真实Guest/Root Source/九服务/PG/normal/Helm。Source qualification仍Unknown，E0/S0。Root已另裁原RO包锁必须实际System3库存+Inputs FD消费；fa4381保原历史，该后继绑定未在本图验证。P2将用准确CLI/provider/原Host8→FS2→5FD构建真实stock Source，不用模型回调代替。
同包锁的逻辑DSO路径必须是最终rootdisk实际musl搜索选择；已有/lib先于/usr/lib的原alias需在新最终输入锁中准确关联，不能删除旧alias或放宽matcher绿化。完整27默认规则的NIC/设备业务语义与privsep kind3/全catalog等仍须真实Guest后继验证，不由本批静态生成结论代签。
所有失败attempt保留：e337实际库存差异、d7驱动键名错误、314实际过声明DSO拒绝；没有覆盖原raw/删除失败/将组件通过改成normal。Git/index由Root唯一操作，本agent没有提交。
