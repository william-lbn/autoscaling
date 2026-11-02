Runner 受信控制器固定512MiB初始base接线（2026-11-02）

修复问题：Runner原先将所有已启用初始协议的模式统一选为256MiB，而本轮真实Linux trusted-controller-v1采用固定512MiB。只调整生产Runner两处接线，不新增Helm/request可配base，不改变上层公共协议、PG17发行锁或其他profile默认。

原DeriveRuntimeBootRequirements先验证完整原Runtime及五项资产。private initialBaseForRuntime复用RuntimeDocument.Source()严格解码：execution_model准确trusted-controller-v1选择固定512MiB，其余已支持模式保256MiB。最终QEMU参数入口先复核同一原raw SHA，再核内部OriginalBaseBytes等于该模型的固定选择；不能改内部数值/当前Use/BootUpper推导base。原amd64/sysfs/managed、BootUpper、min/max、溢出和8MiB几何校验保持。

定向race：新增3根（原绑定18c的trusted模型、同18c非trusted仍256、混原source/base/upper/几何拒绝）与直接受影响原3根，共6根17子例，0fail/0skip。8GiB max输出size=536870912b、backend=8053063680b、virtio requested-size=0；改变业务Min/Use不改变base。测试仅检查生产构造器/argv；不启动QEMU，不授实际QMP/Guest/normal资格或K8s E。

真实运行015f5b：61.686秒，race测试exit0，Runner Linux binary build0。ELF原件/data/neon-build/validation/runner-trusted-initial-base-go-20261102T033357Z-015f5b/export/neonvm-runner.real，SHA40102261558263889901f6ce9586eedef01b36b44cfefa6d0dc82f7f55b67c5f；二进制只留/data不入Git。源码3与实际编译1662输入完整锁定。

首7f789a真实失败保留：launcher误写-p2，test报告根目录无Go文件，CLI报告unknown flag；未执行任何功能根。后继仅改launcher为-p=2，同3源/同source-lock，无业务修补，不把BuildKit0冒Go通过。原失败/原日志未覆盖。

后继：Root按原AS功能分支中文提交，P1将对应Runner产物/源码用于真实镜像/Helm链。117/118/119真实K8s启动和完整内存自动扩缩容仍必须验收，本批U/B不能替代。PG18重型保持暂停给PG17大VM优先。
